// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCoverageCheckpointRecognizesPublishedCompletion(t *testing.T) {
	for _, refsMissing := range []bool{false, true} {
		t.Run(map[bool]string{false: "refs-present", true: "refs-already-removed"}[refsMissing], func(t *testing.T) {
			prefix := filepath.Join(t.TempDir(), "coverage")
			baseline := latencyBaseline()
			state, err := newReplayCoverageState(baseline)
			if err != nil {
				t.Fatal(err)
			}
			cache := &replayCoverageCache{inventory: strings.Repeat("a", 64), state: state}
			refs := []replayObjectRef{{key: "archive/binlog.000001.json", sequence: 1, native: true}}
			if err = saveCoverageRefs(prefix, cache.inventory, refs); err != nil {
				t.Fatal(err)
			}
			work := &coverageWork{coverageWorkDisk: coverageWorkDisk{Version: 1, Baseline: baseline, StartedAt: time.Now().UTC(), Position: 1, Total: 1}, cache: cache, refs: refs, prefix: prefix}
			if err = work.checkpoint(); err != nil {
				t.Fatal(err)
			}
			record := coverageCacheRecord(cache)
			if err = durableJSON(prefix+".verified.json", coverageVerifiedDisk{Version: 1, Baseline: baseline, Cache: &record, StartedAt: work.StartedAt, VerifiedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if refsMissing {
				if err = os.Remove(prefix + ".refs-" + cache.inventory); err != nil {
					t.Fatal(err)
				}
			}
			resumed, err := loadCoverageWork(prefix)
			if err != nil || resumed != nil {
				t.Fatalf("committed completion treated as unfinished: %+v %v", resumed, err)
			}
			for _, name := range []string{prefix + ".work.json", prefix + ".refs-" + cache.inventory} {
				if _, err = os.Stat(name); !os.IsNotExist(err) {
					t.Fatalf("obsolete checkpoint artifact retained: %s %v", name, err)
				}
			}
		})
	}
}
