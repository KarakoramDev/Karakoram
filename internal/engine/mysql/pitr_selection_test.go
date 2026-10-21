// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestResolvePITRSelectsCoveredChain(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "incremental"}[incremental], func(t *testing.T) {
			p, rc, backend := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			older := inventoryBackup(t, rc, "older", "full", "", 10, time.Unix(10, 0))
			newer := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(20, 0))
			newer.MySQL.PhysicalIdentity = "other-source"
			if err := rc.Store.UpdateBackup(t.Context(), &newer); err != nil {
				t.Fatal(err)
			}
			leaf := inventoryBackup(t, rc, "newer-leaf", "incremental", "base", 20, time.Unix(30, 0))
			leaf.MySQL.PhysicalIdentity = newer.MySQL.PhysicalIdentity
			if err := rc.Store.UpdateBackup(t.Context(), &leaf); err != nil {
				t.Fatal(err)
			}
			want := older.Name
			if incremental {
				leaf := inventoryBackup(t, rc, "older-leaf", "incremental", older.Name, 20, time.Unix(25, 0))
				leaf.MySQL.ChainRootLabel = older.MySQL.Label
				if err := rc.Store.UpdateBackup(t.Context(), &leaf); err != nil {
					t.Fatal(err)
				}
				want = leaf.Name
			}
			calls := map[string]int{}
			backend.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
				if r.URL.Path != "/v1/mysql/pitr-coverage" {
					return false
				}
				var request myspec.PITRCoverageRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ObjectPaths) != 1 {
					t.Error("invalid coverage request", err)
					http.Error(w, "invalid request", http.StatusBadRequest)
					return true
				}
				path := request.ObjectPaths[0]
				calls[path]++
				start, end := time.Unix(20, 0), time.Unix(35, 0)
				if path == older.MySQL.ObjectPaths[0] {
					start, end = time.Unix(10, 0), time.Unix(50, 0)
				}
				json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{Windows: []myspec.PITRWindow{{Start: start, End: end}}})
				return true
			}
			selected, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Time: time.Unix(40, 0).UTC().Format(time.RFC3339Nano)})
			if err != nil {
				t.Fatal(err)
			}
			if selected.Name != want {
				t.Fatalf("selected %s, want covered chain %s", selected.Name, want)
			}
			if len(calls) != 2 || calls[older.MySQL.ObjectPaths[0]] != 1 || calls[newer.MySQL.ObjectPaths[0]] != 1 {
				t.Fatalf("coverage must be shared by admission and selection: %v", calls)
			}
		})
	}
}

func TestResolvePITRRememberedCoverageSelection(t *testing.T) {
	for _, offline := range []bool{false, true} {
		t.Run(map[bool]string{false: "live-lineage-required", true: "offline-newest-safe"}[offline], func(t *testing.T) {
			p, rc, backend := setupPXC(t)
			converge(t, p, rc, backend)
			storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Backup.Storage, rc.Cluster.Spec.Backup.PITR.Enabled = storage.Name, true
			if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			inventoryBackup(t, rc, "older", "full", "", 10, time.Unix(10, 0))
			newer := inventoryBackup(t, rc, "newer", "full", "", 10, time.Unix(20, 0))
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.GTIDWindows = []domain.TimeWindow{{Start: time.Unix(10, 0), End: time.Unix(50, 0)}}
			if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			oldAction := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/pitr-coverage" {
					json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{})
					return true
				}
				return oldAction(w, r, name)
			}
			if offline {
				rc.Agents = nil
			}
			selected, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Time: time.Unix(40, 0).UTC().Format(time.RFC3339Nano)})
			if offline {
				if err != nil || selected == nil || selected.Name != newer.Name {
					t.Fatalf("remembered disaster recovery selection: %v, %v", selected, err)
				}
			} else if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "fresh full backup from the current archive writer") {
				t.Fatalf("no live lineage covers admitted remembered target: %v", err)
			}
		})
	}
}
