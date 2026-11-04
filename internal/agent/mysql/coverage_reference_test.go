// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"math"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Frozen pre-streaming coverage algorithm: test oracle for exact lineage and
// regressing-commit-time semantics, not part of the agent's production path.
func referenceCoverageWindows(logs []archivedBinlog, backup myspec.BackupResult) ([]myspec.PITRWindow, error) {
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

func referenceReplayCoverage(logs []archivedBinlog, baseline myspec.BackupResult) (myspec.PITRCoverageResponse, error) {
	windows, err := referenceCoverageWindows(logs, baseline)
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
