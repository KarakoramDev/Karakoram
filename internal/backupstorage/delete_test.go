// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestSnapshotKeys(t *testing.T) {
	s := domain.BackupStorage{Name: "archive", Prefix: "production/"}
	b := domain.Backup{Storage: "archive", PBMPrefix: "production/karakoram/source-uid", PBMName: "2024-01-06T08:51:39Z"}
	dir, metadata, err := SnapshotKeys(s, b)
	if err != nil || dir != b.PBMPrefix+"/"+b.PBMName+"/" || metadata != b.PBMPrefix+"/"+b.PBMName+".pbm.json" {
		t.Fatal(dir, metadata, err)
	}
	for _, change := range []struct{ name, storage, prefix, snapshot string }{
		{"missing prefix", "archive", "", b.PBMName},
		{"missing snapshot", "archive", b.PBMPrefix, ""},
		{"different storage", "other", b.PBMPrefix, b.PBMName},
		{"outside configured prefix", "archive", "production-other/source", b.PBMName},
		{"storage root", "archive", "production", b.PBMName},
		{"parent prefix", "archive", "production/../other", b.PBMName},
		{"parent snapshot", "archive", b.PBMPrefix, ".."},
		{"nested snapshot", "archive", b.PBMPrefix, "snapshot/other"},
		{"backslash snapshot", "archive", b.PBMPrefix, "snapshot\\other"},
	} {
		t.Run(change.name, func(t *testing.T) {
			candidate := domain.Backup{Storage: change.storage, PBMPrefix: change.prefix, PBMName: change.snapshot}
			if _, _, err := SnapshotKeys(s, candidate); err == nil {
				t.Fatal("unsafe snapshot namespace accepted", candidate)
			}
		})
	}
}
