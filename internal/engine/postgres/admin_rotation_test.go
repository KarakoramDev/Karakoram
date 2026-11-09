// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
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

func TestAdministratorRotationCitusConfigurationPublication(t *testing.T) {
	p, rc, _, agents := setupDistributed(t, 3)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	// Replicated multi-group formation needs more steps than the default fixture.
	for range 150 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	converge(t, p, rc)
	rc.Cluster.Spec.Backup.Storage = ""
	op := &domain.Operation{ID: "citus-publication", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
	published := map[string]bool{}
	var selected string
	for range 150 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatalf("rotation failed during %s: %v", op.Step, err)
		}
		j, err := readAdminRotation(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if selected == "" {
			selected = j.Password
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range s.data() {
			if m.Stage == "deployed" {
				published[j.Stage] = true
				if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
					t.Fatalf("configuration publication admitted unrelated work: %t %v", allowed, err)
				}
			}
		}
		active, err := secret(t.Context(), rc, "users/postgres")
		if err != nil {
			t.Fatal(err)
		}
		if len(agents.adminRotations) < 3 && string(active) == selected {
			t.Fatal("published administrator credential before every group receipt")
		}
		op.Checkpoint, op.Step = result.Checkpoint, result.Step
		if result.Done {
			active, err := secret(t.Context(), rc, "users/postgres")
			if err != nil || string(active) != selected {
				t.Fatal("rotation did not publish the selected credential", err)
			}
			if !published["transition"] || !published["publish"] || !published["cleanup"] {
				t.Fatalf("did not exercise every configuration publication barrier: %v", published)
			}
			if len(agents.adminRotations) != 3 {
				t.Fatalf("expected one native transaction per group, got %v", agents.adminRotations)
			}
			for i, name := range agents.adminRotations {
				if m := s.find(name); m == nil || m.Group != []int{1, 2, 0}[i] {
					t.Fatalf("native transaction missed worker-first group %d", i)
				}
			}
			return
		}
	}
	t.Fatal("Citus administrator rotation did not converge")
}

func TestAdministratorRotationCitusPublicationRetainsMaintenanceFences(t *testing.T) {
	for _, name := range []string{
		"pending", "rolling", "upgrade", "replacement", "scale", "rebalance",
		"restore", "backup", "import", "unformed", "member-count", "new-member", "unjoined",
		"wrong-user", "completed-journal",
	} {
		t.Run(name, func(t *testing.T) {
			p, rc, _, _ := setupDistributed(t, 3)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.Topology = "distributed"
			s.allocate(rc.Cluster)
			s.Formed = true
			s.GroupFormed = map[int]bool{1: true, 2: true}
			for _, m := range s.data() {
				m.Stage, m.ProxyReady = "joined", true
			}
			m := s.data()[0]
			m.Stage = "deployed"
			j := &adminRotation{Operation: "publication-owner", Stage: "transition", Groups: []int{0, 1, 2}}
			op := &domain.Operation{ID: j.Operation, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
			switch name {
			case "pending":
				s.Pending = &pending{}
			case "rolling":
				s.Rolling = &rollingState{}
			case "upgrade":
				s.Upgrade = &upgradeState{}
			case "replacement":
				s.Replacement = &replacementState{Recovery: "active"}
			case "scale":
				s.CitusScale = &citusScaleState{}
			case "rebalance":
				s.Rebalance = &citusRebalanceCheckpoint{}
			case "restore":
				s.RestoreRecovery = &domain.Operation{ID: "foreign-restore"}
			case "backup":
				s.CitusOperation = "foreign-backup"
			case "import":
				m.ImportBatch = &engine.ImportPlan{}
			case "unformed":
				s.GroupFormed[2] = false
			case "member-count":
				s.Members = s.Members[1:]
			case "new-member":
				m.ProxyReady = false
			case "unjoined":
				m.Stage = "imported"
			case "wrong-user":
				op.Payload = json.RawMessage(`{"user":"application"}`)
			case "completed-journal":
				j.Stage = "rotated"
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := saveAdminRotation(t.Context(), rc, j); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "maintenance owns the cluster") {
				t.Fatalf("rotation bypassed %s fence: %v", name, err)
			}
		})
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

func TestAdministratorLocalPeerConfigurationCapability(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	for _, capable := range []bool{false, true, false} {
		rc.AgentConfigCapabilities = nil
		if capable {
			rc.AgentConfigCapabilities = []string{agentapi.ConfigCapabilityPostgresLocalPeer}
		}
		cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec pgspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.LocalPeerAuth != capable {
			t.Fatal("peer auth capability was not request-scoped")
		}
		if !capable && strings.Contains(string(cfg.EngineSpec), "localPeerAuth") {
			t.Fatal("old strict-decoder agent received a new configuration field")
		}
	}
}

type rotationRecoveryBackend struct {
	*fakeBackend
	agents   *distributedAgents
	upgraded []string
}

func (b *rotationRecoveryBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.fakeBackend.Deploy(ctx, name, d)
	if err == nil {
		b.agents.mu.Lock()
		delete(b.agents.legacyReceipts, name)
		b.agents.mu.Unlock()
		b.upgraded = append(b.upgraded, name)
	}
	return op, err
}

func TestAdministratorRotationRescuesLegacyHalfAppliedCitus(t *testing.T) {
	t.Parallel()
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			t.Parallel()
			p, rc, backend, agents := setupDistributed(t, replicas)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
			for range 150 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s := converge(t, p, rc)
			rc.Cluster.Spec.Backup.Storage = ""
			old, err := secret(t.Context(), rc, "users/postgres")
			if err != nil {
				t.Fatal(err)
			}
			// Exact pre-upgrade journal: group 0 accepted, workers have no
			// receipt and old published proxy logins get backend 28P01.
			j := &adminRotation{Operation: "recover-half-applied", Stage: "native", Previous: string(old), Password: "new-synthetic", Verifier: "stable-synthetic", Groups: []int{0, 1, 2}, Completed: 1}
			agents.legacyReceipts = map[string]bool{}
			for _, m := range s.data() {
				if m.Group != 0 {
					agents.legacyReceipts[m.Name] = true
				}
			}
			if err := saveAdminRotation(t.Context(), rc, j); err != nil {
				t.Fatal(err)
			}
			owner := &domain.Operation{ID: j.Operation, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
			if err := rc.Store.EnqueueOperation(t.Context(), owner); err != nil {
				t.Fatal(err)
			}
			// Model the native coordinator's single verifier against the real
			// rendered proxy backend login, rather than only inspecting secrets.
			proxyBackendCode := func() string {
				for _, m := range s.Members {
					if m.Role != "proxy" {
						continue
					}
					config, err := p.MemberConfig(t.Context(), rc, m.Name)
					if err != nil {
						t.Fatal(err)
					}
					var spec pgspec.Spec
					if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
						t.Fatal(err)
					}
					if spec.Proxy == nil || len(spec.Proxy.Users) == 0 {
						t.Fatal("proxy fixture lacks an administrator backend login")
					}
					if spec.Proxy.Users[0].Password != j.Password {
						return "28P01"
					}
					return ""
				}
				t.Fatal("proxy fixture missing")
				return ""
			}
			if proxyBackendCode() != "28P01" {
				t.Fatal("fixture did not reproduce the half-rotation proxy outage")
			}
			recovery := &rotationRecoveryBackend{fakeBackend: backend, agents: agents}
			rc.Backend = recovery
			for range 100 {
				// Ordinary Sync resumes the same owner after CP upgrade. No
				// separately admitted upgrade-agents operation or SQL is used.
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				current, err := readAdminRotation(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				active, _ := secret(t.Context(), rc, "users/postgres")
				if current.Completed < 3 && string(active) != string(old) {
					t.Fatal("proxy credential published before worker receipts")
				}
				if current.Completed < 3 && proxyBackendCode() != "28P01" {
					t.Fatal("proxy login changed before all worker receipts")
				}
				if current.Stage == "rotated" {
					if string(active) != "new-synthetic" {
						t.Fatal("forward recovery did not publish the new proxy credential")
					}
					if proxyBackendCode() != "" {
						t.Fatal("proxy backend login did not recover after publication")
					}
					if len(recovery.upgraded) != 2 || len(agents.adminRotations) != 2 {
						t.Fatalf("did not refresh and resume both worker primaries: %v %v", recovery.upgraded, agents.adminRotations)
					}
					return
				}
			}
			t.Fatal("half-applied Citus rotation did not converge forward")
		})
	}
}

func TestAdministratorRotationReportsBlockedReceipt(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	j := &adminRotation{Operation: "blocked-receipt", Stage: "native", Previous: "old-synthetic", Password: "new-synthetic", Verifier: "stable-synthetic", Groups: []int{0}}
	if err := saveAdminRotation(t.Context(), rc, j); err != nil {
		t.Fatal(err)
	}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary"})
		} else {
			json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "unknown", Code: "transaction_unconfirmed"})
		}
	})
	op := &domain.Operation{ID: j.Operation, Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"postgres"}`)}
	for range 29 {
		if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "blocked at group 0 after 30 attempts") {
		t.Fatalf("missing actionable bounded recovery failure: %v", err)
	}
	current, _ := readAdminRotation(t.Context(), rc)
	if current.Password != j.Password || current.Completed != 0 {
		t.Fatal("blocked recovery destroyed the sealed forward intent")
	}
}
