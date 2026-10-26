// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestDeleteNormalizesLegacyRecordedBindingPrefix(t *testing.T) {
	for _, prefix := range []string{"/x/y", "x/y/", "/x/y/", "/"} {
		t.Run(prefix, func(t *testing.T) {
			p, rc, backend, agents := backupCleanupSetup(t)
			s := retryState(t, rc)
			st := rc.Store.(*store.Store)
			storage, err := st.GetBackupStorage(t.Context(), rc.Cluster.Ref().Namespace, "archive")
			if err != nil {
				t.Fatal(err)
			}
			storage.Prefix = prefix
			if err := st.UpdateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "owned", Cluster: rc.Cluster.Name, Storage: "archive", PBMName: "snapshot", PBMPrefix: strings.TrimRight(prefix, "/") + "/" + s.PBMPrefix}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			calls := 0
			p.deleteClusterObjects = func(_ context.Context, binding domain.BackupStorage, cluster, uid string) error {
				calls++
				if backend.services[s.Members[0].Name] != nil {
					t.Fatal("runtime can still write backups")
				}
				directory, err := backupstorage.ClusterDirectory(binding, cluster, uid)
				if err != nil || directory != domain.BackupStoragePrefix(b.PBMPrefix)+"/" {
					t.Fatal(directory, err)
				}
				return nil
			}
			finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
			if calls != 1 || agents.calls != 0 {
				t.Fatal("cleanup skipped or contacted a native runtime", calls, agents.calls)
			}
			if _, err := st.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
				t.Fatal("success retained backup inventory", err)
			}
		})
	}
}
