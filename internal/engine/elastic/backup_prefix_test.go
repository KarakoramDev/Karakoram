// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestPersistedBindingKeepsSnapshotRepositoryAcrossCheckpoint(t *testing.T) {
	for _, test := range []struct{ prefix, canonical string }{
		{"a//b", "a/b"}, {"/backups///nested/", "backups/nested"},
		{"/x/y/", "x/y"}, {"/", ""},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			var bases []string
			snapshotReads := 0
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/snapshot-one") {
					snapshotReads++
					opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{Snapshot: "snapshot-one", State: "IN_PROGRESS"}}})
					return
				}
				if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_snapshot/") {
					var repository esapi.Repository
					if err := json.NewDecoder(r.Body).Decode(&repository); err != nil {
						t.Fatal(err)
					}
					bases = append(bases, repository.Settings["base_path"].(string))
				}
				opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
			})
			st.storage.Prefix = test.prefix // Existing bindings bypass new-registration validation.
			want := rc.Cluster.Ref().Namespace + "/" + rc.Cluster.Name + "/" + s.UID
			if test.canonical != "" {
				want = test.canonical + "/" + want
			}
			if got := clusterBasePath(&st.storage, rc.Cluster, s); got != want {
				t.Fatal("historical repository moved", got, want)
			}
			op := &domain.Operation{ID: "prefix-continuity", Kind: domain.OperationBackup}
			result, err := p.runBackup(t.Context(), rc, s, op, operationState{}, st.backup.Name)
			if err != nil {
				t.Fatal(err)
			}
			if cp := opsCheckpoint(t, result); cp.Stage != "start" {
				t.Fatal("location checkpoint not persisted", cp)
			}
			base, _, _, err := recordedSnapshot(&st.storage, &st.backup)
			if err != nil || base != want {
				t.Fatal("persisted location cannot be resumed", base, err)
			}
			directory, err := backupstorage.ElasticsearchDirectory(st.storage, rc.Cluster.Ref().Namespace, rc.Cluster.Name, s.UID, base)
			if err != nil || directory != want+"/" {
				t.Fatal("cleanup differs from writer", directory, err)
			}
			result, err = p.runBackup(t.Context(), rc, s, op, opsCheckpoint(t, result), st.backup.Name)
			if err != nil || opsCheckpoint(t, result).Stage != "poll" || snapshotReads != 1 || st.backup.Phase != domain.PhaseRunning {
				t.Fatal("backup failed past persisted-location checkpoint", result, snapshotReads, err)
			}
			if len(bases) == 0 {
				t.Fatal("native repository was not configured")
			}
			for _, base := range bases {
				if base != want {
					t.Fatal("native repository reconfigured to another namespace", base, want)
				}
			}
		})
	}
}
