// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPITRReanchorCachesVerifiedBaseline(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "verified", true: "pending-backoff"}[pending], func(t *testing.T) {
			p, rc, b := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			calls := 0
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/pitr-coverage" {
					calls++
					response := myspec.PITRCoverageResponse{SourceUUID: b.native[name].ServerUUID, Pending: pending && calls == 1}
					json.NewEncoder(w).Encode(response)
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			start := time.Now().UTC()
			pass := func(elapsed time.Duration, want int) {
				t.Helper()
				rc.Clock = trafficRenewalClock{at: start.Add(elapsed)}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if err = p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
				if err = save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if calls != want {
					t.Fatalf("at %s: coverage calls=%d, want %d", elapsed, calls, want)
				}
			}
			pass(0, 0)
			pass(4*time.Minute, 0)
			pass(5*time.Minute, 1)
			pass(6*time.Minute, 1)
			count := 1
			if pending {
				count = 2
			}
			pass(10*time.Minute, count)
			pass(20*time.Minute, count)
			inventoryBackup(t, rc, "new-base", "full", "", 20, time.Unix(20, 0))
			pass(21*time.Minute, count+1)
			pass(30*time.Minute, count+1)
		})
	}
}

func TestPITRReanchorInvalidatesReusedBaselineName(t *testing.T) {
	for _, observeGap := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "missing-baseline"}[observeGap], func(t *testing.T) {
			p, rc, backend := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			calls := 0
			foreign := false
			previous := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/pitr-coverage" {
					calls++
					source := backend.native[name].ServerUUID
					if foreign {
						source = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
					}
					json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{SourceUUID: source})
					return true
				}
				return previous != nil && previous(w, r, name)
			}
			start := time.Now().UTC()
			pass := func(elapsed time.Duration) *state {
				t.Helper()
				rc.Clock = trafficRenewalClock{at: start.Add(elapsed)}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			pass(0)
			pass(5 * time.Minute)
			if calls != 1 {
				t.Fatal("initial verification count", calls)
			}
			if err := rc.Store.DeleteBackup(t.Context(), rc.Cluster.Ref(), "base"); err != nil {
				t.Fatal(err)
			}
			if observeGap {
				// Even a cheap pending-backup gate must not preserve a removed
				// baseline's source identity across the durable round trip.
				blocker := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "manual", Phase: domain.PhasePending}
				if err := rc.Store.CreateBackup(t.Context(), blocker); err != nil {
					t.Fatal(err)
				}
				s := pass(6 * time.Minute)
				if s.PITRReanchor.Baseline != "" || s.PITRReanchor.SourceUUID != "" || !s.PITRReanchor.VerifyAfter.IsZero() {
					t.Fatal("missing baseline retained cached verification", s.PITRReanchor)
				}
				if err := rc.Store.DeleteBackup(t.Context(), rc.Cluster.Ref(), "manual"); err != nil {
					t.Fatal(err)
				}
			}
			bs, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			bs.Generation++
			if err := saveBackupState(rc.Cluster, bs); err != nil {
				t.Fatal(err)
			}
			replacement := inventoryBackup(t, rc, "base", "full", "", 20, time.Unix(20, 0))
			replacement.OperationID = "replacement-full"
			replacement.RecoveryIdentity = "fixture-replacement"
			replacement.CreatedAt = start.Add(6 * time.Minute)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			storage, err := p.backupStorage(t.Context(), rc, "", rc.Cluster.Ref().Namespace)
			if err != nil {
				t.Fatal(err)
			}
			replacement.MySQL.Generation = bs.Generation
			replacement.MySQL.Repository = repoPrefix(storage, rc.Cluster, s, bs.Generation)
			replacement.MySQL.ObjectPaths = []string{replacement.MySQL.Repository + "/base/replacement"}
			if err := rc.Store.UpdateBackup(t.Context(), &replacement); err != nil {
				t.Fatal(err)
			}
			if err := captureFixtureBackupAccess(t.Context(), rc, &replacement); err != nil {
				t.Fatal(err)
			}
			foreign = true
			pass(7 * time.Minute)
			if calls != 2 {
				t.Fatalf("same-name replacement reused old source: calls=%d", calls)
			}
			items, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, b := range items {
				found = found || b.Trigger == domain.BackupTriggerPITRReanchor
			}
			if !found {
				t.Fatal("replacement's foreign source did not reanchor")
			}
		})
	}
}
