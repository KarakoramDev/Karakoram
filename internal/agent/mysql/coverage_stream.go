// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

// Coverage retains object keys for ordering, not decoded historical sidecars or
// transactions. A rebuild streams one sidecar at a time in native ordinal order.
// Appends verify the old inventory before extending the compact lineage state.
type replayObjectRef struct {
	key      string
	sequence uint64
	native   bool
}

type replayCoverageState struct {
	have       recoveryGTIDs
	base       recoveryGTIDs
	response   myspec.PITRCoverageResponse
	lastCommit int64
	stopped    bool
}

func newReplayCoverageState(baseline myspec.BackupResult) (*replayCoverageState, error) {
	if !myspec.ValidUUID(baseline.SourceUUID) || !binlogName.MatchString(baseline.BinlogFile) || baseline.BinlogPosition < 4 {
		return nil, errors.New("backup_binlog_provenance_missing")
	}
	base, err := parseRecoveryGTIDs(baseline.GTIDSet)
	if err != nil {
		return nil, err
	}
	return &replayCoverageState{have: cloneCoverageGTIDs(base), base: base, lastCommit: baseline.EndTime.UnixMicro(), response: myspec.PITRCoverageResponse{SourceUUID: baseline.SourceUUID, Windows: []myspec.PITRWindow{{Start: baseline.EndTime.UTC(), End: baseline.EndTime.UTC()}}}}, nil
}
func (s *replayCoverageState) clone() *replayCoverageState {
	out := *s
	out.have = cloneCoverageGTIDs(s.have)
	out.base = cloneCoverageGTIDs(s.base)
	out.response.Windows = slices.Clone(s.response.Windows)
	return &out
}

func cloneCoverageGTIDs(set recoveryGTIDs) recoveryGTIDs {
	out := make(recoveryGTIDs, len(set))
	for sid, ranges := range set {
		out[sid] = slices.Clone(ranges)
	}
	return out
}

// Every existing cut must exclude the next transaction (cut < timestamp).
// The old final cut's tail extends until that transaction, then a new final cut
// includes it. Regressing commit times remove unsafe cuts without retaining a
// timestamp/suffix array. Exact disjoint windows are the response itself.
func (s *replayCoverageState) commit(stamp int64) {
	windows := s.response.Windows
	kept := windows[:0]
	for i, w := range windows {
		end := stamp - 1
		if i != len(windows)-1 {
			end = min(end, w.End.UnixMicro())
		}
		if w.Start.UnixMicro() <= end {
			w.End = time.UnixMicro(end).UTC()
			kept = append(kept, w)
		}
	}
	s.lastCommit = max(s.lastCommit, stamp)
	final := time.UnixMicro(s.lastCommit).UTC()
	if len(kept) > 0 && s.lastCommit <= kept[len(kept)-1].End.UnixMicro()+1 {
		kept[len(kept)-1].End = final
	} else {
		kept = append(kept, myspec.PITRWindow{Start: final, End: final})
	}
	s.response.Windows = kept
}

func (s *replayCoverageState) consume(log archivedBinlog, baseline myspec.BackupResult) error {
	if log.meta.ServerUUID != baseline.SourceUUID {
		for _, tx := range log.meta.Transactions {
			stamp := time.UnixMicro(tx.OriginalCommitTimestamp).UTC()
			if stamp.After(s.response.LatestForeignEventTime) {
				s.response.LatestForeignEventTime = stamp
			}
		}
	}
	var err error
	s.base, err = snapshotLoggedGTIDs(log, baseline, s.base)
	if err != nil {
		return err
	}
	anchored, eligible, err := anchorReplayLog(log, baseline, s.base)
	if err != nil {
		return err
	}
	if !eligible {
		// Foreign streams and pre-snapshot files are intentionally ignored.
		// A later same-source file failing the baseline check is blocked.
		prefix, ordinal, _ := strings.Cut(log.meta.File, ".")
		basePrefix, baseOrdinal, _ := strings.Cut(baseline.BinlogFile, ".")
		sequence, parseErr := strconv.ParseUint(ordinal, 10, 64)
		baseSequence, _ := strconv.ParseUint(baseOrdinal, 10, 64)
		if log.meta.ServerUUID == baseline.SourceUUID && prefix == basePrefix && parseErr == nil && sequence > baseSequence {
			s.stopped = true
			s.response.BlockedReason = "baseline_logged_gtids_missing"
		}
		return nil
	}
	if s.stopped {
		return nil
	}
	previous, err := parseRecoveryGTIDs(anchored.meta.PreviousGTIDSet)
	if err != nil {
		return err
	}
	if !s.have.contains(previous) {
		s.stopped = true
		s.response.BlockedReason = "binlog_dependency_missing"
		return nil
	}
	set, err := parseRecoveryGTIDs(anchored.meta.GTIDSet)
	if err != nil {
		return err
	}
	decoded := recoveryGTIDs{}
	for _, tx := range anchored.meta.Transactions {
		g, err := parseRecoveryGTIDs(tx.GTID)
		if err != nil || !set.contains(g) || tx.OriginalCommitTimestamp <= 0 {
			return errors.New("invalid_sidecar")
		}
		decoded.add(g)
		if s.have.contains(g) {
			continue
		}
		s.have.add(g)
		s.commit(tx.OriginalCommitTimestamp)
	}
	if !decoded.contains(set) || !set.contains(decoded) {
		return errors.New("invalid_sidecar")
	}
	s.base.add(set)
	return nil
}

func replayRef(key, objectPrefix, sourceUUID, basePrefix string) replayObjectRef {
	ref := replayObjectRef{key: key}
	relative := strings.TrimPrefix(key, objectPrefix)
	uuid, file, found := strings.Cut(relative, "/")
	file = strings.TrimSuffix(file, ".json")
	prefix, ordinal, _ := strings.Cut(file, ".")
	if found && uuid == sourceUUID && prefix == basePrefix && binlogName.MatchString(file) {
		ref.native = true
		ref.sequence, _ = strconv.ParseUint(ordinal, 10, 64)
	}
	return ref
}

type replayCoveragePlan struct {
	refs      []replayObjectRef
	cache     *replayCoverageCache
	continued bool
}

func planReplayCoverage(ctx context.Context, c *minio.Client, source myspec.Repository, baseline myspec.BackupResult, previous *replayCoverageCache) (*replayCoveragePlan, error) {
	digest := sha256.New()
	var oldDigest hash.Hash
	var oldEncoder *json.Encoder
	if previous != nil {
		oldDigest = sha256.New()
		oldEncoder = json.NewEncoder(oldDigest)
	}
	var refs []replayObjectRef
	lastKey, lastOrderedKey := "", ""
	var lastSequence uint64
	objectPrefix := path.Join(source.Path, "binlogs") + "/"
	basePrefix, _, _ := strings.Cut(baseline.BinlogFile, ".")
	err := walkReplayObjects(ctx, c, source, digest, func(obj minio.ObjectInfo) error {
		if obj.Key > lastKey {
			lastKey = obj.Key
		}
		ref := replayRef(obj.Key, objectPrefix, baseline.SourceUUID, basePrefix)
		if ref.native && (ref.sequence > lastSequence || ref.sequence == lastSequence && ref.key > lastOrderedKey) {
			lastSequence, lastOrderedKey = ref.sequence, ref.key
		}
		if previous != nil {
			appended := !ref.native && obj.Key > previous.lastKey || ref.native && (ref.sequence > previous.lastSequence || ref.sequence == previous.lastSequence && ref.key > previous.lastOrderedKey)
			if !appended {
				return encodeReplayObject(oldEncoder, obj)
			}
		}
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return nil, err
	}
	inventory := hex.EncodeToString(digest.Sum(nil))
	if previous != nil {
		// A changed/deleted old object or an out-of-order native ordinal is not an
		// append. Re-list and rebuild, still retaining only object keys + one decode.
		if hex.EncodeToString(oldDigest.Sum(nil)) != previous.inventory {
			return planReplayCoverage(ctx, c, source, baseline, nil)
		}
		if inventory == previous.inventory {
			return &replayCoveragePlan{cache: previous, continued: true}, nil
		}
	}
	var state *replayCoverageState
	if previous != nil {
		state = previous.state.clone()
	} else {
		state, err = newReplayCoverageState(baseline)
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		if a.native != b.native {
			return a.native
		}
		if a.native && a.sequence != b.sequence {
			return a.sequence < b.sequence
		}
		return a.key < b.key
	})
	return &replayCoveragePlan{refs: refs, cache: &replayCoverageCache{inventory: inventory, lastKey: lastKey, lastSequence: lastSequence, lastOrderedKey: lastOrderedKey, state: state}, continued: previous != nil}, nil
}
