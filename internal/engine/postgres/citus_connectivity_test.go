// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestCitusTopologyRejectsCorrectMetadataWithBrokenWorkerAuthentication(t *testing.T) {
	for _, failure := range []string{"transport", "worker-auth", "omitted-worker", "duplicate-group", "source-host", "healthy"} {
		t.Run(failure, func(t *testing.T) {
			_, rc, s := citusOpsFixture(t)
			observations := map[string]pgspec.StatusResponse{}
			var nodes []pgspec.CitusNode
			for _, m := range s.data() {
				observations[m.Name] = pgspec.StatusResponse{Running: true, Role: "primary", Leader: m.Name}
				nodes = append(nodes, pgspec.CitusNode{GroupID: m.Group, Host: m.Name, Port: 5432, Role: "primary", Active: true})
			}
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				status := pgspec.CitusStatusResponse{IsCoordinator: true, Nodes: append([]pgspec.CitusNode(nil), nodes...)}
				switch r.URL.Path {
				case "/v1/postgres/citus-status":
				case "/v1/postgres/citus-connectivity":
					switch failure {
					case "transport":
						http.Error(w, "worker authentication unavailable", http.StatusServiceUnavailable)
						return
					case "worker-auth":
						status.Nodes[1].Active = false
					case "omitted-worker":
						status.Nodes = status.Nodes[:1]
					case "duplicate-group":
						status.Nodes[1] = status.Nodes[0]
					case "source-host":
						status.Nodes[1].Host = "source-worker"
					}
				default:
					t.Error("unexpected action", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(status)
			})
			condition := citusTopology(t.Context(), rc, s, observations)
			if failure == "healthy" {
				if condition.Status != "True" {
					t.Fatal(condition)
				}
			} else if condition.Status == "True" || condition.Reason != "ConnectivityFailed" {
				t.Fatal("correct node metadata falsely certifies readiness while coordinator-to-worker authentication fails", condition)
			}
		})
	}
}

func TestDistributedWorkerAuthenticationFailureWithdrawsReady(t *testing.T) {
	p, rc, _, fake := setupDistributed(t, 1)
	converge(t, p, rc)
	fake.connectivityFailed = true
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("failed worker authentication remained Ready")
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			t.Fatal("failed worker authentication retained Ready=True")
		}
		found = found || condition.Type == "CitusTopology" && condition.Reason == "ConnectivityFailed" && condition.Status == "False"
	}
	if !found {
		t.Fatal("missing connectivity failure condition", status.Conditions)
	}
	fake.connectivityFailed = false
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatal("successful worker SQL did not recover readiness", status, err)
	}
}

func TestCitusReadinessDefersRemoteSQLToRestoreOwner(t *testing.T) {
	for _, owner := range []string{"client-hold", "release-journal", "restore-recovery", "backup-recovery"} {
		t.Run(owner, func(t *testing.T) {
			_, rc, s := citusOpsFixture(t)
			var backup backupState
			switch owner {
			case "client-hold":
				backup.ManagedRolesHeld = true
			case "release-journal":
				// Desired holds have cleared, but some members may still have
				// the old fenced configuration after a partial release reload.
				backup.RestoreOperation = "restore"
			case "restore-recovery":
				s.RestoreRecovery = &domain.Operation{ID: "restore", Kind: domain.OperationRestore}
			case "backup-recovery":
				s.RestoreRecovery = &domain.Operation{ID: "backup", Kind: domain.OperationBackup}
			}
			if err := saveBackupState(rc.Cluster, backup); err != nil {
				t.Fatal(err)
			}
			observations := map[string]pgspec.StatusResponse{}
			var nodes []pgspec.CitusNode
			for _, m := range s.data() {
				observations[m.Name] = pgspec.StatusResponse{Running: true, Role: "primary", Leader: m.Name}
				nodes = append(nodes, pgspec.CitusNode{GroupID: m.Group, Host: m.Name, Port: 5432, Role: "primary", Active: true})
			}
			probes := 0
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/citus-status":
				case "/v1/postgres/citus-connectivity":
					probes++
				default:
					t.Error("unexpected action", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(pgspec.CitusStatusResponse{IsCoordinator: true, Nodes: nodes})
			})
			condition := citusTopology(t.Context(), rc, s, observations)
			if owner == "backup-recovery" {
				if condition.Status != "True" || probes != 1 {
					t.Fatal("backup cleanup incorrectly suppressed ordinary connectivity readiness", condition, probes)
				}
			} else if condition.Status != "False" || condition.Reason != "RestoreInProgress" || probes != 0 {
				t.Fatal("ordinary readiness probed before restore ownership released", condition, probes)
			}
		})
	}
}
