// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestAdministratorRotationJournalRecovery(t *testing.T) {
	for _, outcome := range []string{"applied", "unknown", "rejected"} {
		t.Run(outcome, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			proxy := rc.Cluster.Spec.Components.Extra["proxy"]
			proxy.Replicas = 0
			rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			converge(t, p, rc)
			rc.Cluster.Spec.Backup.Storage = ""
			old, err := secret(t.Context(), rc, "users/postgres")
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			var firstVerifier string
			loseResponse := outcome == "unknown"
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				s, _ := load(rc.Cluster)
				switch r.URL.Path {
				case "/v1/status":
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", AdminRotation: true, Version: rc.Cluster.Spec.Version})
				case "/v1/reload":
					w.Write([]byte(`{}`))
				case "/v1/postgres/roles":
					var req pgspec.AccessRequest
					json.NewDecoder(r.Body).Decode(&req)
					if req.Operation != "admin-rotation" {
						t.Errorf("unexpected operation %s", req.Operation)
						w.WriteHeader(400)
						return
					}
					calls++
					if firstVerifier == "" {
						firstVerifier = *req.Role.Password
					} else if firstVerifier != *req.Role.Password {
						t.Error("replay replaced stable verifier")
					}
					if loseResponse {
						loseResponse = false
						json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "unknown"})
						return
					}
					reply := outcome
					if reply == "unknown" {
						reply = "applied"
					}
					json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: reply, Code: "transaction_rejected", RoleOID: 10})
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			op := &domain.Operation{ID: "admin-journal-test", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil || result.Step != "transition" {
				t.Fatal(result, err)
			}
			j, err := readAdminRotation(t.Context(), rc)
			if err != nil || j.Password == string(old) || j.Previous != string(old) {
				t.Fatal("credentials not safely sealed", err)
			}
			selected := j.Password
			cfg, err := p.MemberConfig(t.Context(), rc, "demop0")
			if err != nil {
				t.Fatal(err)
			}
			var sp pgspec.Spec
			json.Unmarshal(cfg.EngineSpec, &sp)
			if sp.AlternateSuperuser == nil || sp.AlternateSuperuser.Password != selected || sp.Superuser.Password != string(old) {
				t.Fatal("transition configuration lost a working credential")
			}
			// Lose the operation checkpoint and restart the provider at every step.
			var done bool
			for range 60 {
				p = New(Options{})
				result, err = p.RunOperation(t.Context(), rc, op)
				if outcome == "rejected" && err != nil {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(result.Checkpoint), selected) {
					t.Fatal("checkpoint leaked password")
				}
				if result.Done {
					done = true
					break
				}
			}
			active, readErr := secret(t.Context(), rc, "users/postgres")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if outcome == "rejected" {
				if err == nil || string(active) != string(old) || calls != 1 {
					t.Fatal("definitive rejection changed administrator")
				}
				j, _ = readAdminRotation(t.Context(), rc)
				if j.Password != "" || j.Previous != "" || j.Stage != "rejected" {
					t.Fatal("rejection retained pending credentials")
				}
			} else {
				if !done || string(active) != selected {
					t.Fatalf("rotation did not converge: step=%s message=%s calls=%d", result.Step, result.Message, calls)
				}
				cfg, err = p.MemberConfig(t.Context(), rc, "demop0")
				if err != nil {
					t.Fatal(err)
				}
				json.Unmarshal(cfg.EngineSpec, &sp)
				// Decode into a fresh value: omitted fields intentionally preserve rolling compatibility.
				var clean pgspec.Spec
				json.Unmarshal(cfg.EngineSpec, &clean)
				if clean.AlternateSuperuser != nil {
					t.Fatal("alternate survived final acknowledgement")
				}
			}
		})
	}
}

func TestAdministratorRotationCrashAfterNativeCommit(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	j := &adminRotation{Operation: "native-crash", Stage: "native", Previous: "old-synthetic", Password: "new-synthetic", Verifier: "stable-synthetic", Groups: []int{0}}
	if err := saveAdminRotation(t.Context(), rc, j); err != nil {
		t.Fatal(err)
	}
	calls := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
			return
		}
		calls++
		json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 10})
	})
	original := rc.Secrets
	// The intent write succeeds; fail only the post-commit journal write.
	rc.Secrets = &failAdminAcknowledgement{SecretStore: original}
	op := &domain.Operation{ID: j.Operation, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("post-commit crash not injected")
	}
	rc.Secrets = original
	p = New(Options{})
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	recovered, _ := readAdminRotation(t.Context(), rc)
	if recovered.Completed != 1 || calls != 2 || recovered.Password != j.Password {
		t.Fatal("receipt replay did not recover unchanged staged credential")
	}
}

type failAdminAcknowledgement struct{ engine.SecretStore }

func (s *failAdminAcknowledgement) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, data []byte) error {
	var j adminRotation
	if key == adminRotationKey && json.Unmarshal(data, &j) == nil && j.Completed == 1 {
		return errors.New("sealed acknowledgement unavailable")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, data)
}

func TestAdministratorRotationEveryCitusGroupPrimary(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	worker := *s.data()[0]
	worker.Name, worker.Group = "demow1p0", 1
	s.Members = append(s.Members, &worker)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	j := &adminRotation{Operation: "citus-admin", Stage: "native", Previous: "old-synthetic", Password: "new-synthetic", Verifier: "stable-synthetic", Groups: []int{0, 1}}
	if err := saveAdminRotation(t.Context(), rc, j); err != nil {
		t.Fatal(err)
	}
	calls := 0
	rejectSecond := true
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary"})
			return
		}
		calls++
		outcome := "applied"
		if calls == 2 && rejectSecond {
			outcome = "rejected"
			rejectSecond = false
		}
		json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: outcome, Code: "transaction_rejected", RoleOID: 10})
	})
	op := &domain.Operation{ID: j.Operation, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
	for range 4 {
		if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := readAdminRotation(t.Context(), rc)
	if err != nil || recovered.Completed != 2 || recovered.Stage != "publish" || calls != 3 || recovered.Password != j.Password {
		t.Fatal("Citus group failure did not recover forward with the same credential", err)
	}
}

func TestAdministratorRotationProxyPublicationBarrier(t *testing.T) {
	p, rc, _, s, _, late := replicatedProxyFixture(t)
	late.applied(s.ConfigVersion - 1)
	pending, err := p.adminConfigurationPending(t.Context(), rc)
	if err == nil && pending == "" {
		t.Fatal("administrator completion ignored stale proxy acknowledgement")
	}
}

func TestAdministratorRotationFailureBeforeDispatchDiscardsPending(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	old, err := secret(t.Context(), rc, "users/postgres")
	if err != nil {
		t.Fatal(err)
	}
	j := &adminRotation{Operation: "pre-dispatch-failure", Stage: "native", Previous: string(old), Password: "unused-synthetic", Verifier: "unused-verifier", Groups: []int{0}}
	if err := saveAdminRotation(t.Context(), rc, j); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: j.Operation, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
	done := false
	for range 50 {
		result, err := p.CancelOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Done {
			done = true
			break
		}
	}
	current, err := secret(t.Context(), rc, "users/postgres")
	if err != nil || !done || string(current) != string(old) {
		t.Fatal("failure cleanup changed the only working credential", err)
	}
	j, err = readAdminRotation(t.Context(), rc)
	if err != nil || j.Stage != "rejected" || j.Password != "" || j.Previous != "" {
		t.Fatal("failure cleanup retained pending credential", err)
	}
}
