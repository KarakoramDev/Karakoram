// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestFailedCoverageRootsCanBeReclaimedWithoutRevivingSuccess(t *testing.T) {
	for _, persist := range []bool{true, false} {
		t.Run(fmt.Sprint("failure-persisted=", persist), func(t *testing.T) {
			root := t.TempDir()
			s := &coverageRefresh{root: root, startedAt: time.Unix(1, 0), entries: map[string]*coverageEntry{}}
			blocked := filepath.Join(root, "not-a-directory")
			if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			var first myspec.PITRCoverageRequest
			for i := 0; i < 2*coverageResidentRoots; i++ {
				request := myspec.PITRCoverageRequest{ObjectPaths: []string{fmt.Sprint(i)}}
				if i == 0 {
					first = request
				}
				entry, err := s.entry(request)
				if err != nil {
					t.Fatalf("root %d: %v", i, err)
				}
				entry.requested = time.Unix(int64(i+1), 0)
				cache := coverageCacheDisk{Inventory: requestDigest(i), Have: recoverySID + ":1", Base: recoverySID + ":1"}
				entry.verified = &coverageVerifiedDisk{Version: 1, Cache: &cache, StartedAt: time.Now(), VerifiedAt: time.Now()}
				if err := durableJSON(coverageStatePrefix(root, entry.key)+".verified.json", entry.verified); err != nil {
					t.Fatal(err)
				}
				entry.err = errors.New("new refresh failed")
				if !persist {
					s.root = blocked
				}
				err = s.persistFailure(entry)
				s.root = root
				entry.failureDurable = err == nil
				if persist && err != nil {
					t.Fatal(err)
				}
				if !persist && err == nil {
					t.Fatal("failure persistence unexpectedly succeeded")
				}
			}
			if len(s.entries) != coverageResidentRoots {
				t.Fatalf("resident count=%d", len(s.entries))
			}
			prefix := coverageStatePrefix(root, coverageRequestKey(first))
			verified, err := loadCoverageVerified(prefix)
			if err != nil {
				t.Fatal(err)
			}
			if persist && (verified == nil || verified.Error == "") {
				t.Fatal("durable failure marker lost on resident eviction")
			}
			if !persist && verified != nil {
				t.Fatal("unpersisted failure revived an old proof")
			}
			entry, err := s.entry(first)
			if err != nil {
				t.Fatal(err)
			}
			if entry.verified != nil && entry.verified.Error == "" {
				t.Fatal("failed-root eviction revived old success")
			}
		})
	}
}

func TestCoverageDurableRetentionKeepsResumableRootsAndCollectsOrphans(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < coverageDurableRoots+8; i++ {
		key := requestDigest(i)
		prefix := coverageStatePrefix(root, key)
		inventory := requestDigest([]int{i, 1})
		work := coverageWorkDisk{Version: 1, Cache: coverageCacheDisk{Inventory: inventory, Have: recoverySID + ":1", Base: recoverySID + ":1"}, StartedAt: time.Unix(int64(i+1), 0), Position: 0, Total: 1}
		if err := durableJSON(prefix+".work.json", work); err != nil {
			t.Fatal(err)
		}
		if err := saveCoverageRefs(prefix, inventory, []replayObjectRef{{key: "binlog.000001", sequence: 1, native: true}}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(prefix+".refs-"+requestDigest("abandoned"), []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
		names, _ := filepath.Glob(prefix + ".*")
		for _, name := range names {
			stamp := time.Now().Add(-3*time.Minute + time.Duration(i)*time.Millisecond)
			if err := os.Chtimes(name, stamp, stamp); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, ".coverage-refs-interrupted"), []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := pruneCoverageStore(root, nil, true); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2*coverageDurableRoots {
		t.Fatalf("durable files=%d, want%d", len(files), 2*coverageDurableRoots)
	}
	for i := 0; i < coverageDurableRoots+8; i++ {
		work, err := loadCoverageWork(coverageStatePrefix(root, requestDigest(i)))
		if err != nil {
			t.Fatal(err)
		}
		if i < 8 && work != nil {
			t.Fatalf("abandoned root%d retained", i)
		}
		if i >= 8 && (work == nil || len(work.refs) != 1) {
			t.Fatalf("retained root%d lost resumable work", i)
		}
	}
}

func TestCoverageDurableRetentionProtectsWorkingSetAndExpiresOldRoots(t *testing.T) {
	root := t.TempDir()
	active := map[string]bool{}
	for i := 0; i < coverageDurableRoots+40; i++ {
		key := requestDigest(i)
		prefix := coverageStatePrefix(root, key)
		if err := durableJSON(prefix+".verified.json", coverageVerifiedDisk{Version: 1}); err != nil {
			t.Fatal(err)
		}
		stamp := time.Now().Add(-25 * time.Hour)
		if err := os.Chtimes(prefix+".verified.json", stamp, stamp); err != nil {
			t.Fatal(err)
		}
		if i < 40 {
			active[key] = true
		}
	}
	if err := pruneCoverageStore(root, active, false); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 40 {
		t.Fatalf("active working set retained%d files, want40", len(files))
	}
	for key := range active {
		if _, err := os.Stat(coverageStatePrefix(root, key) + ".verified.json"); err != nil {
			t.Fatal(err)
		}
	}
}
