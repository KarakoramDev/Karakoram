// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"
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
