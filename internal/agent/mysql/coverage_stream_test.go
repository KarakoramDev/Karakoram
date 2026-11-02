// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Existing lineage/native-output tests use the production streaming accumulator.
// The materializing pre-change implementation is a separate frozen oracle.
func replayCoverage(logs []archivedBinlog, baseline myspec.BackupResult) (myspec.PITRCoverageResponse, error) {
	state, err := newReplayCoverageState(baseline)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	ordered := slices.Clone(logs)
	sequence := func(log archivedBinlog) uint64 {
		_, ordinal, _ := strings.Cut(log.meta.File, ".")
		n, _ := strconv.ParseUint(ordinal, 10, 64)
		return n
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := sequence(ordered[i]), sequence(ordered[j])
		if a != b {
			return a < b
		}
		return ordered[i].key < ordered[j].key
	})
	for _, log := range ordered {
		if err = state.consume(log, baseline); err != nil {
			return myspec.PITRCoverageResponse{}, err
		}
	}
	return state.response, nil
}
func replayCoverageWindows(logs []archivedBinlog, baseline myspec.BackupResult) ([]myspec.PITRWindow, error) {
	out, err := replayCoverage(logs, baseline)
	return out.Windows, err
}

func TestStreamingCoverageMatchesFrozenRegressingCommitWindows(t *testing.T) {
	baseline := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4, EndTime: time.UnixMicro(100).UTC()}
	rng := rand.New(rand.NewPCG(11, 29))
	for run := range 40 {
		var logs []archivedBinlog
		for file := 1; file <= 40; file++ {
			first, last := (file-1)*8+1, file*8
			meta := binlogSidecar{ServerUUID: recoverySID, File: fmt.Sprintf("binlog.%06d", file), GTIDSet: fmt.Sprintf("%s:%d-%d", recoverySID, first, last)}
			if file > 1 {
				meta.PreviousGTIDSet = fmt.Sprintf("%s:1-%d", recoverySID, first-1)
			}
			for tx := first; tx <= last; tx++ {
				stamp := int64(100 + rng.IntN(250))
				if run == 0 {
					stamp = int64(100 + tx)
				}
				meta.Transactions = append(meta.Transactions, binlogTransaction{GTID: fmt.Sprintf("%s:%d", recoverySID, tx), OriginalCommitTimestamp: stamp, EndPosition: uint64(100 + tx)})
			}
			logs = append(logs, archivedBinlog{key: meta.File, meta: meta})
		}
		const foreign = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		logs = append(logs, archivedBinlog{key: "foreign", meta: binlogSidecar{ServerUUID: foreign, File: "binlog.000001", Transactions: []binlogTransaction{{GTID: foreign + ":1", OriginalCommitTimestamp: 900, EndPosition: 100}}}})
		want, err := referenceReplayCoverage(logs, baseline)
		if err != nil {
			t.Fatal(err)
		}
		got, err := replayCoverage(logs, baseline)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("run%d streaming=%+v reference=%+v error=%v", run, got, want, err)
		}
	}
}

func TestStreamingCoverageCloneDoesNotMutatePublishedState(t *testing.T) {
	baseline := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4, EndTime: time.UnixMicro(100).UTC()}
	original, err := newReplayCoverageState(baseline)
	if err != nil {
		t.Fatal(err)
	}
	next := original.clone()
	next.commit(200)
	next.have.add(recoveryGTIDs{recoverySID: []gtidInterval{{1, 1}}})
	if len(original.have) != 0 || len(original.response.Windows) != 1 || !original.response.Windows[0].End.Equal(baseline.EndTime) {
		t.Fatal("extending candidate mutated published cache")
	}
}
