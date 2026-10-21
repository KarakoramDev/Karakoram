// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPITRBackupUsesArchiveWriterAndPinsIncrementalSource(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "group")
			}
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			for name, status := range b.native {
				status.PhysicalIdentity = "physical-" + name
				b.native[name] = status
			}
			if provider == "mysql" {
				member, err := backupMember(t.Context(), rc, s)
				if err != nil || member == b.leader {
					t.Fatalf("non-PITR secondary preference: %s err=%v", member, err)
				}
			}
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			member, err := backupMember(t.Context(), rc, s)
			if err != nil || member != b.leader {
				t.Fatalf("PITR baseline did not use archive writer: %s writer=%s err=%v", member, b.leader, err)
			}
			for _, m := range s.data() {
				if m.Name != member {
					if _, err := backupMemberIdentity(t.Context(), rc, s, b.native[m.Name].PhysicalIdentity); err == nil {
						t.Fatal("incremental silently switched physical archive source")
					}
					break
				}
			}
		})
	}
}

func TestRestorePreflightCompletesBeforeAnyStop(t *testing.T) {
	for _, outcome := range []string{"failed", "prepared", "failed-recheck", "failed-recheck-poll", "digest-changed"} {
		t.Run(outcome, func(t *testing.T) {
			p, rc, b := backupFixture(t, "group")
			base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			base.RecoveryIdentity = "sealed-preflight-source"
			if err := captureFixtureBackupAccess(t.Context(), rc, &base); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.UpdateBackup(t.Context(), &base); err != nil {
				t.Fatal(err)
			}
			restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "recover", Source: domain.RestoreSource{Backup: base.Name}, Phase: domain.PhasePending}
			if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			stops, preflights, polls := 0, 0, 0
			var recheckID string
			b.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
				switch r.URL.Path {
				case "/v1/mysql/restore-preflight":
					var request myspec.RestoreRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						return true
					}
					recheckID = request.RecheckID
					preflights++
					if stops != 0 {
						t.Error("STOP preceded proof")
					}
					if preflights == 1 {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "running", Stage: "prepare"})
					} else if outcome == "failed-recheck" {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "failed", Stage: "ready", Error: "replay_plan_changed"})
					} else if outcome == "digest-changed" {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "prepared", Stage: "ready", PlanDigest: strings.Repeat("b", 64)})
					} else {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "running", Stage: "rechecking", PlanDigest: fixtureManifestDigest, RecheckID: recheckID})
					}
					return true
				case "/v1/mysql/restore-status":
					polls++
					if stops != 0 {
						t.Error("STOP preceded completed proof")
					}
					if outcome == "failed" {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "failed", Stage: "prepare", Error: "pitr_preflight_unavailable_take_fresh_full_from_current_writer"})
					} else if outcome == "failed-recheck-poll" && recheckID != "" {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "failed", Stage: "rechecking", Error: "replay_plan_changed", RecheckID: recheckID})
					} else if recheckID != "" && polls == 2 {
						// A stale prepared proof must not authorize this attempt.
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "prepared", Stage: "ready", PlanDigest: fixtureManifestDigest, RecheckID: recheckID, VerifiedRecheckID: "earlier-attempt"})
					} else {
						json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "prepared", Stage: "ready", PlanDigest: fixtureManifestDigest, RecheckID: recheckID, VerifiedRecheckID: recheckID})
					}
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/") && strings.HasSuffix(r.URL.Path, "/stop") {
					stops++
				}
				return false
			}
			op := &domain.Operation{ID: "preflight-operation", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
			var operationErr error
			for range 12 {
				result, err := p.RunOperation(t.Context(), rc, op)
				op.Checkpoint = result.Checkpoint
				if err != nil {
					operationErr = err
					break
				}
				if stops > 0 {
					break
				}
			}
			wantPreflights := 2
			if outcome == "failed" {
				wantPreflights = 1
			}
			wantPolls := 1
			if outcome == "prepared" {
				wantPolls = 3
			}
			if outcome == "failed-recheck-poll" {
				wantPolls = 2
			}
			if preflights != wantPreflights || polls != wantPolls {
				t.Fatalf("preflights=%d polls=%d", preflights, polls)
			}
			if outcome != "prepared" {
				if operationErr == nil || stops != 0 {
					t.Fatalf("unsafe failure: stops=%d err=%v", stops, operationErr)
				}
				s, _ := load(rc.Cluster)
				for _, m := range s.data() {
					if m.Offline || b.native[m.Name].MemberState != "ONLINE" {
						t.Fatalf("preflight failure changed serving member %s", m.Name)
					}
				}
				cleanupAt := time.Now()
				op.FailureCleanupStartedAt = &cleanupAt
				if result, err := p.CancelOperation(t.Context(), rc, op); err != nil || !result.Done || result.Step != "restore-preflight-aborted" {
					t.Fatalf("safe preflight failure did not release recovery ownership: %+v err=%v", result, err)
				}
				bs, err := loadBackupState(rc.Cluster)
				if err != nil || bs.RestoreOperation != nil {
					t.Fatalf("failed preflight blocks a fresh restore request: %+v err=%v", bs, err)
				}
			} else if operationErr != nil || stops == 0 {
				t.Fatalf("prepared proof did not permit fence: stops=%d err=%v", stops, operationErr)
			}
		})
	}
}
