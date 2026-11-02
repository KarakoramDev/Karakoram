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
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

// walkReplayObjects hashes a streaming inventory rather than retaining its keys.
// Cancellation also releases the SDK listing goroutine on an early read error.
func walkReplayObjects(ctx context.Context, c *minio.Client, source myspec.Repository, digest hash.Hash, visit func(minio.ObjectInfo) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var encoder *json.Encoder
	if digest != nil {
		encoder = json.NewEncoder(digest)
	}
	for obj := range c.ListObjects(ctx, source.S3.Bucket, minio.ListObjectsOptions{Prefix: path.Join(source.Path, "binlogs") + "/", Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		if encoder != nil {
			// Include every remotely observable identity field. A replacement,
			// deletion, append or repository/baseline change must invalidate.
			if err := encoder.Encode(struct {
				Key, ETag, VersionID string
				Size                 int64
				Modified             time.Time
			}{obj.Key, obj.ETag, obj.VersionID, obj.Size, obj.LastModified}); err != nil {
				return err
			}
		}
		if visit != nil {
			if err := visit(obj); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

func readReplaySidecars(ctx context.Context, c *minio.Client, source myspec.Repository) ([]archivedBinlog, error) {
	return readReplaySidecarsWithDigest(ctx, c, source, nil)
}

func readReplaySidecarsWithDigest(ctx context.Context, c *minio.Client, source myspec.Repository, digest hash.Hash) ([]archivedBinlog, error) {
	var logs []archivedBinlog
	err := walkReplayObjects(ctx, c, source, digest, func(obj minio.ObjectInfo) error {
		o, err := c.GetObject(ctx, source.S3.Bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		var metadata binlogSidecar
		err = json.NewDecoder(io.LimitReader(o, 32<<20)).Decode(&metadata)
		o.Close()
		if err != nil {
			return err
		}
		key := strings.TrimSuffix(obj.Key, ".json")
		if key != path.Join(source.Path, "binlogs", metadata.ServerUUID, metadata.File) {
			return errors.New("invalid_sidecar")
		}
		logs = append(logs, archivedBinlog{key, metadata})
		return nil
	})
	return logs, err
}

type replayCoverageCache struct {
	key, inventory string
	response       myspec.PITRCoverageResponse
}

// Only the most recent baseline's computed response is cached, never sidecars
// or transaction slices. Every hit still checks the full remote inventory.
// Changing history recomputes the exact existing lineage/window semantics.
func (e *Runner) cachedReplayCoverage(ctx context.Context, c *minio.Client, source myspec.Repository, baseline myspec.BackupResult) (myspec.PITRCoverageResponse, error) {
	e.coverageMu.Lock()
	defer e.coverageMu.Unlock()
	key := requestDigest([]any{source, baseline})
	if previous := e.coverageCache; previous != nil && previous.key == key {
		digest := sha256.New()
		if err := walkReplayObjects(ctx, c, source, digest, nil); err != nil {
			return myspec.PITRCoverageResponse{}, err
		}
		if hex.EncodeToString(digest.Sum(nil)) == previous.inventory {
			return previous.response, nil
		}
	}
	digest := sha256.New()
	logs, err := readReplaySidecarsWithDigest(ctx, c, source, digest)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	response, err := replayCoverage(logs, baseline)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	e.coverageCache = &replayCoverageCache{key: key, inventory: hex.EncodeToString(digest.Sum(nil)), response: response}
	return response, nil
}

func replayCoverageWindows(logs []archivedBinlog, backup myspec.BackupResult) ([]myspec.PITRWindow, error) {
	trusted, last, err := trustedReplayHistory(logs, backup)
	if err != nil {
		return nil, err
	}
	var timestamps []int64
	for _, l := range trusted {
		for _, tx := range l.meta.Transactions {
			timestamps = append(timestamps, tx.OriginalCommitTimestamp)
		}
	}
	// A time cut is valid iff every included predecessor is <= the cut and
	// every omitted successor is > it. Original commit times may regress.
	suffixMin := make([]int64, len(timestamps)+1)
	suffixMin[len(timestamps)] = math.MaxInt64
	for i := len(timestamps) - 1; i >= 0; i-- {
		suffixMin[i] = min(timestamps[i], suffixMin[i+1])
	}
	lower := backup.EndTime.UnixMicro()
	var windows []myspec.PITRWindow
	for i := 0; i <= len(timestamps); i++ {
		upper := last
		if suffixMin[i] != math.MaxInt64 {
			upper = min(upper, suffixMin[i]-1)
		}
		if lower <= upper {
			w := myspec.PITRWindow{Start: time.UnixMicro(lower).UTC(), End: time.UnixMicro(upper).UTC()}
			if len(windows) > 0 && !w.Start.After(windows[len(windows)-1].End.Add(time.Microsecond)) {
				windows[len(windows)-1].End = w.End
			} else {
				windows = append(windows, w)
			}
		}
		if i < len(timestamps) {
			lower = max(lower, timestamps[i])
		}
	}
	return windows, nil
}

func replayCoverage(logs []archivedBinlog, baseline myspec.BackupResult) (myspec.PITRCoverageResponse, error) {
	windows, err := replayCoverageWindows(logs, baseline)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	coverage := myspec.PITRCoverageResponse{Windows: windows, SourceUUID: baseline.SourceUUID}
	for _, log := range logs {
		if baseline.SourceUUID == "" || log.meta.ServerUUID == baseline.SourceUUID {
			continue
		}
		for _, tx := range log.meta.Transactions {
			stamp := time.UnixMicro(tx.OriginalCommitTimestamp).UTC()
			if stamp.After(coverage.LatestForeignEventTime) {
				coverage.LatestForeignEventTime = stamp
			}
		}
	}
	return coverage, nil
}

func loadPITRBaseline(ctx context.Context, s myspec.Spec, p paths, request myspec.PITRCoverageRequest) (myspec.BackupResult, error) {
	var empty myspec.BackupResult
	intent := myspec.Restore{Source: request.Source, BackupID: "coverage", Label: "coverage", ObjectPaths: request.ObjectPaths, ObjectManifests: request.ObjectManifests}
	if err := intent.Validate(); err != nil {
		return empty, err
	}
	dir, err := os.MkdirTemp(p.root, "pitr-coverage-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(dir)
	metadata := filepath.Join(dir, "metadata")
	if err = privateDir(metadata); err != nil {
		return empty, err
	}
	// The source UUID must come from the same stream as the binlog coordinates.
	last := request.ObjectManifests[len(request.ObjectManifests)-1]
	if err = extractVerifiedBackup(ctx, s, request.Source, last, metadata, []string{"xtrabackup_checkpoints", "xtrabackup_binlog_info", "xtrabackup_info", "backup-my.cnf"}); err != nil {
		return empty, err
	}
	baseline, err := ParseBackupMetadata(metadata)
	if err != nil {
		return empty, err
	}
	baseline.SourceUUID, err = backupSourceUUID(metadata)
	baseline.ManifestDigest = last.Digest
	baseline.ManifestSize = last.Size
	return baseline, err
}

func (e *Runner) pitrCoverage(ctx context.Context, request myspec.PITRCoverageRequest) (myspec.PITRCoverageResponse, error) {
	s, p := e.state()
	baseline, err := loadPITRBaseline(ctx, s, p, request)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	client, closeClient, err := s3Client(request.Source)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	defer closeClient()
	return e.cachedReplayCoverage(ctx, client, request.Source, baseline)
}
