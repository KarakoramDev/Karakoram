// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRepositoryBindingPrefixNormalization(t *testing.T) {
	for _, prefix := range []string{"x/y", "/x/y", "x/y/", "/x/y/", "/", ""} {
		storage := domain.BackupStorage{Prefix: prefix, EndpointURL: "https://s3.example.test"}
		cluster := domain.Cluster{Name: "demo"}
		base := repoPrefix(&storage, &cluster, &state{UID: "uid"})
		repo, err := repository(&storage, base)
		if err != nil || repo.Path != "/"+base {
			t.Fatal(prefix, repo.Path, base, err)
		}
		directory, err := backupstorage.PostgresDirectory(storage, cluster.Ref().Namespace, cluster.Name, base)
		if err != nil || directory != base+"/" {
			t.Fatal(prefix, directory, base, err)
		}
	}
}

func TestPersistedBindingKeepsRepositoryAndPITRSelection(t *testing.T) {
	for _, test := range []struct{ prefix, canonical string }{
		{"a//b", "a/b"}, {"a/./b", "a/b"}, {"a/../b", "b"},
		{"/backups///nested/", "backups/nested"},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			p, rc, s := backupFixture(t, "single")
			st := rc.Store.(*store.Store)
			storage, err := st.GetBackupStorage(t.Context(), rc.Cluster.Ref().Namespace, "archive")
			if err != nil {
				t.Fatal(err)
			}
			storage.Prefix = test.prefix // An already-persisted, pre-validation binding.
			if err := st.UpdateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			want := test.canonical + "/postgres/default/demo-incarnation"
			if base := repoPrefix(storage, rc.Cluster, s); base != want {
				t.Fatalf("repository moved: %q != %q", base, want)
			}
			directory, err := backupstorage.PostgresDirectory(*storage, "default", "demo", want)
			if err != nil || directory != want+"/" {
				t.Fatal("cleanup differs from historical writer", directory, err)
			}
			start := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			target := start.Add(time.Minute)
			b := &domain.Backup{Namespace: "default", Cluster: rc.Cluster.Name, Name: "base", Storage: storage.Name, Provider: "postgres", Phase: domain.PhaseSucceeded, CompletedAt: &start, SourceVersion: rc.Cluster.Spec.Version, Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: want, SystemID: "system"}}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/postgres/status" {
					t.Error("PITR selection mutated an agent", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", SystemID: "system", RecoverableUntil: target.Format(time.RFC3339Nano)})
			})
			selected, restore, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: "default", Cluster: rc.Cluster.Name, Time: target.Format(time.RFC3339Nano)})
			if err != nil {
				t.Fatal("historical backup excluded from PITR selection", err)
			}
			if selected.Name != b.Name || restore.Source.Path != "/"+want || restore.Set != b.Postgres.Label {
				t.Fatal("restore repository changed", selected.Name, restore)
			}
		})
	}
}
