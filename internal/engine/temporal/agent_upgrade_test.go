// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeBackend struct {
	*testBackend
	order []string
}

func (b *agentUpgradeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.order = append(b.order, name)
	return b.testBackend.Deploy(ctx, name, d)
}

func TestAgentUpgradeOrderReplayAndGates(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "outdated", true: "include-current"}[include], func(t *testing.T) {
			p, rc, _, agents := fixture(t, "split")
			converge(t, p, rc)
			backend := &agentUpgradeBackend{testBackend: rc.Backend.(*testBackend)}
			rc.Backend = backend
			agents.builds = map[string]string{}
			s, _ := loadState(rc.Cluster)
			for _, m := range s.Members {
				if include {
					agents.builds[m.Name] = engine.CurrentAgentBuild().Build
				}
			}
			before := append([]byte(nil), rc.Cluster.Internal...)
			configs := map[string][]byte{}
			for k, v := range rc.Secrets.(*memorySecrets).values {
				configs[k] = append([]byte(nil), v...)
			}
			if !slices.Contains(p.Describe().Operations, domain.OperationUpgradeAgents) {
				t.Fatal("operation not advertised")
			}
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
			op := &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: payload}
			step := func() engine.OpResult { t.Helper(); p = New(p.options); return stepOperation(t, p, rc, op) }
			step()
			var cp agentUpgradeCheckpoint
			json.Unmarshal(op.Checkpoint, &cp)
			if !reflect.DeepEqual(cp.Order, []string{"history", "matching", "frontend", "worker", "ui"}) {
				t.Fatal(cp.Order)
			}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
			step()
			if len(backend.order) != 0 {
				t.Fatal("window bypassed")
			}
			rc.Cluster.Spec.Maintenance = nil
			step()
			if len(backend.order) != 0 {
				t.Fatal("missing durable intent")
			}
			step()
			if len(backend.order) != 1 {
				t.Fatal("deployment missing")
			}
			if !include {
				step()
				json.Unmarshal(op.Checkpoint, &cp)
				if cp.Index != 0 {
					t.Fatal("ignored exact target build")
				}
			}
			agents.unready = "history"
			if step().Done {
				t.Fatal("ignored native readiness")
			}
			agents.builds[s.Members["history"].Name] = engine.CurrentAgentBuild().Build
			step()
			json.Unmarshal(op.Checkpoint, &cp)
			if cp.Index != 0 {
				t.Fatal("ignored native readiness")
			}
			agents.unready = ""
			// Finish an in-flight component even after the maintenance window closes.
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
			step()
			json.Unmarshal(op.Checkpoint, &cp)
			if cp.Index != 1 {
				t.Fatal(cp)
			}
			if cp.SettleUntil.IsZero() {
				t.Fatal("missing history settle checkpoint")
			}
			step()
			if len(backend.order) != 1 {
				t.Fatal("next component bypassed window")
			}
			rc.Cluster.Spec.Maintenance = nil
			done := false
			for range 100 {
				out := step()
				cp = agentUpgradeCheckpoint{}
				json.Unmarshal(op.Checkpoint, &cp)
				if cp.Waiting && cp.Submitted {
					agents.builds[s.Members[cp.Order[cp.Index]].Name] = engine.CurrentAgentBuild().Build
				}
				if out.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("did not complete")
			}
			want := []string{}
			for _, name := range cp.Order {
				want = append(want, s.Members[name].Name)
			}
			if !reflect.DeepEqual(backend.order, want) {
				t.Fatal(backend.order, want)
			}
			if !bytes.Equal(before, rc.Cluster.Internal) || !reflect.DeepEqual(configs, rc.Secrets.(*memorySecrets).values) {
				t.Fatal("rollout changed version/configuration")
			}
			step()
			if !reflect.DeepEqual(backend.order, want) {
				t.Fatal("replay redeployed completed members")
			}
		})
	}
}

func TestAgentUpgradeBuildFenceAndCurrentSkip(t *testing.T) {
	p, rc, _, agents := fixture(t, "combined")
	converge(t, p, rc)
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: "different"})
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}); err == nil {
		t.Fatal("missing build fence")
	}
	agents.builds = map[string]string{}
	s, _ := loadState(rc.Cluster)
	for _, m := range s.Members {
		agents.builds[m.Name] = engine.CurrentAgentBuild().Build
	}
	backend := &agentUpgradeBackend{testBackend: rc.Backend.(*testBackend)}
	rc.Backend = backend
	payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	finishOperation(t, p, rc, op)
	if len(backend.order) != 0 {
		t.Fatal("current agents redeployed")
	}
}

func TestAgentUpgradeCancellation(t *testing.T) {
	for _, phase := range []string{"window", "intent", "deploy", "readiness", "between"} {
		t.Run(phase, func(t *testing.T) {
			p, rc, _, agents := fixture(t, "split")
			converge(t, p, rc)
			backend := &agentUpgradeBackend{testBackend: rc.Backend.(*testBackend)}
			rc.Backend = backend
			agents.builds = map[string]string{}
			s, _ := loadState(rc.Cluster)
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
			op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
			step := func(cancel bool) engine.OpResult {
				t.Helper()
				var out engine.OpResult
				var err error
				if cancel {
					out, err = p.CancelOperation(t.Context(), rc, op)
				} else {
					out, err = p.RunOperation(t.Context(), rc, op)
				}
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = out.Checkpoint
				return out
			}
			if err := p.CanCancelOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			step(false)
			if phase == "window" {
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
			}
			step(false)
			if phase == "deploy" || phase == "readiness" || phase == "between" {
				step(false)
				agents.unready = "history"
				if phase == "readiness" {
					step(false)
				}
				if phase != "between" {
					if step(true).Done {
						t.Fatal("cancel abandoned disrupted component")
					}
					agents.unready = ""
					if step(true).Done {
						t.Fatal("cancel ignored exact build")
					}
				}
				agents.unready = ""
				agents.builds[s.Members["history"].Name] = engine.CurrentAgentBuild().Build
				if phase == "between" {
					step(false)
				}
				if !step(true).Done {
					t.Fatal("cancel did not complete safely")
				}
				if len(backend.order) != 1 {
					t.Fatal("cancel started another component", backend.order)
				}
			} else {
				if !step(true).Done || len(backend.order) != 0 {
					t.Fatal("cancel disrupted component")
				}
			}
		})
	}
}

func TestAgentUpgradeRecoveryAdmission(t *testing.T) {
	for _, action := range []string{"recovering", "import"} {
		t.Run(action, func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			converge(t, p, rc)
			s, _ := loadState(rc.Cluster)
			for _, m := range s.Members {
				if action == "recovering" {
					m.Recovering = true
				} else {
					m.ImportBatch = &engine.ImportPlan{}
				}
				break
			}
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatalf("recovery claim allowed: %v %v", allowed, err)
			}
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
			if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}); err == nil {
				t.Fatal("recovery passed defensive admission")
			}
		})
	}
}

type uncertainUpgradeBackend struct {
	*agentUpgradeBackend
	lose bool
}

func (b *uncertainUpgradeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.agentUpgradeBackend.Deploy(ctx, name, d)
	if b.lose {
		return infra.Op{}, io.ErrUnexpectedEOF
	}
	return op, err
}

func TestAgentUpgradeAmbiguousSubmission(t *testing.T) {
	for _, mode := range []string{"cancel-old", "cancel-target", "replay-target", "replay-current", "retry-readiness"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, _, agents := fixture(t, "split")
			converge(t, p, rc)
			backend := &uncertainUpgradeBackend{agentUpgradeBackend: &agentUpgradeBackend{testBackend: rc.Backend.(*testBackend)}}
			rc.Backend = backend
			agents.builds = map[string]string{}
			s, _ := loadState(rc.Cluster)
			if mode == "replay-current" {
				for _, m := range s.Members {
					agents.builds[m.Name] = engine.CurrentAgentBuild().Build
				}
			}
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: mode == "replay-current"})
			op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
			for range 2 {
				stepOperation(t, p, rc, op)
			}
			backend.lose = true
			if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
				t.Fatal("lost response not exercised")
			}
			p = New(p.options)
			if mode == "replay-current" {
				backend.lose = false
				out, err := p.RunOperation(t.Context(), rc, op)
				if err != nil || out.Done || len(backend.order) != 2 {
					t.Fatal("same-build replay policy changed", err)
				}
				return
			}
			if mode == "replay-target" {
				agents.builds[s.Members["history"].Name] = engine.CurrentAgentBuild().Build
				out, err := p.RunOperation(t.Context(), rc, op)
				var cp agentUpgradeCheckpoint
				json.Unmarshal(out.Checkpoint, &cp)
				if err != nil || cp.Index != 1 || len(backend.order) != 1 {
					t.Fatal("replay failed to adopt target", err, cp)
				}
				return
			}
			agents.unready = "history"
			if mode == "retry-readiness" {
				out, err := p.RunOperation(t.Context(), rc, op)
				if err != nil || out.Done || len(backend.order) != 1 {
					t.Fatal("retry bypassed readiness gate", err)
				}
				return
			}
			out, err := p.CancelOperation(t.Context(), rc, op)
			if err != nil || out.Done {
				t.Fatal("cancel abandoned ambiguous deployment", err)
			}
			agents.unready = ""
			if mode == "cancel-target" {
				agents.builds[s.Members["history"].Name] = engine.CurrentAgentBuild().Build
			}
			out, err = p.CancelOperation(t.Context(), rc, op)
			if err != nil || !out.Done || len(backend.order) != 1 {
				t.Fatal("cancel failed to settle ambiguous deployment", err)
			}
		})
	}
}

func TestAgentUpgradeFrozenTargetAdoptionChecksEveryInstance(t *testing.T) {
	p, rc, _, agents := fixture(t, "split")
	history := rc.Cluster.Spec.Components.Extra["history"]
	history.Replicas = 2
	rc.Cluster.Spec.Components.Extra["history"] = history
	converge(t, p, rc)
	s, _ := loadState(rc.Cluster)
	if s.Members["history"].Replicas != 2 {
		t.Fatal("test requires two instances")
	}
	// Simulate restart under a different control-plane executable: neither
	// frozen-target-b nor other-build-c is the current executable identity.
	var calls atomic.Int64
	agents.buildForStatus = func(string) string {
		if calls.Add(1)%2 == 0 {
			return "other-build-c"
		}
		return "frozen-target-b"
	}
	cp := agentUpgradeCheckpoint{
		operationCheckpoint: operationCheckpoint{Stage: "roll", Order: []string{"history"}, Waiting: true},
		Target:              engine.AgentUpgradePayload{TargetBuild: "frozen-target-b"},
	}
	raw, _ := json.Marshal(cp)
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	out, err := p.RunOperation(t.Context(), rc, op)
	if err == nil {
		t.Fatal("mixed frozen-target instances bypassed changed-build fence")
	}
	var observed agentUpgradeCheckpoint
	json.Unmarshal(out.Checkpoint, &observed)
	if observed.Index != 0 || len(observed.Done) != 0 {
		t.Fatal("mixed instances adopted", observed)
	}
	agents.buildForStatus = func(string) string { return "frozen-target-b" }
	out, err = p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	observed = agentUpgradeCheckpoint{}
	json.Unmarshal(out.Checkpoint, &observed)
	if observed.Index != 1 || len(observed.Done) != 1 {
		t.Fatal("exact frozen target not adopted", observed)
	}
}
