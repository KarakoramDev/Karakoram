// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentRolloutBackend struct {
	*testBackend
	names []string
}

func (b *agentRolloutBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, e := b.testBackend.Deploy(ctx, name, d)
	if e == nil {
		b.names = append(b.names, name)
		b.agentBuilds[name] = engine.CurrentAgentBuild().Build
	}
	return op, e
}
func TestAgentRolloutCoreBeforeComponentsAndResume(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "include"}[include], func(t *testing.T) {
			p, rc, b := setupProvider(t)
			convergeProvider(t, p, rc)
			s, e := loadState(rc.Cluster)
			if e != nil {
				t.Fatal(e)
			}
			b.agentBuilds = map[string]string{}
			for name := range b.deployed {
				b.agentBuilds[name] = "old"
			}
			b.agentBuilds[s.Members["auth"].Name] = engine.CurrentAgentBuild().Build
			before := map[string]string{}
			components := map[string]bool{}
			for _, m := range s.Members {
				before[m.Name] = m.Version
				components[m.Name] = true
			}
			wrapped := &agentRolloutBackend{testBackend: b}
			rc.Backend = wrapped
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
			op := &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: payload}
			done := false
			for range 100 {
				p = New(p.options)
				r, e := p.RunOperation(t.Context(), rc, op)
				if e != nil {
					t.Fatal(e)
				}
				if r.Checkpoint != nil {
					op.Checkpoint = r.Checkpoint
				}
				if r.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("rollout did not finish")
			}
			optional := false
			seen := map[string]int{}
			for _, name := range wrapped.names {
				seen[name]++
				if components[name] {
					optional = true
				} else if optional {
					t.Fatal("database rolled after optional component", wrapped.names)
				}
			}
			for name := range b.deployed {
				want := 1
				if name == s.Members["auth"].Name && !include {
					want = 0
				}
				if seen[name] != want {
					t.Fatalf("%s deployments %d, want %d: %v", name, seen[name], want, wrapped.names)
				}
			}
			after, _ := loadState(rc.Cluster)
			versions := map[string]string{}
			for _, m := range after.Members {
				versions[m.Name] = m.Version
			}
			if !reflect.DeepEqual(before, versions) {
				t.Fatal("component version changed during agent rollout")
			}
		})
	}
}
func TestComponentMaintenanceStartOnlyAndHealing(t *testing.T) {
	for _, change := range []string{"config", "resources", "remove"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b := setupProvider(t)
			convergeProvider(t, p, rc)
			s, _ := loadState(rc.Cluster)
			originalOptions := append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
			// Keep freshly issued PKI valid while freezing a closed window.
			at := time.Now().UTC()
			rc.Clock = importReplayClock{at: at}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + at.Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
			switch change {
			case "config":
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"siteUrl":"https://app.example.com"}`)
			case "resources":
				c := rc.Cluster.Spec.Components.Extra["auth"]
				c.Resources.MaxCPU++
				rc.Cluster.Spec.Components.Extra["auth"] = c
			case "remove":
				c := rc.Cluster.Spec.Components.Extra["auth"]
				c.Replicas = 0
				rc.Cluster.Spec.Components.Extra["auth"] = c
			}
			handled, e := p.syncComponents(t.Context(), rc)
			if e != nil || !handled {
				t.Fatal(handled, e)
			}
			after, _ := loadState(rc.Cluster)
			for component, before := range s.Members {
				current := after.Members[component]
				if current == nil || current.Stage != before.Stage || current.ServiceID != before.ServiceID ||
					current.Version != before.Version || current.ConfigVersion != before.ConfigVersion ||
					current.ConfigHash != before.ConfigHash || current.Replicas != before.Replicas ||
					!reflect.DeepEqual(current.Resources, before.Resources) {
					t.Fatalf("closed window disrupted component %s", component)
				}
			}
			// A deployment already beyond its boundary must finish even while closed.
			after.Members["auth"].Stage = engine.ComponentDeployed
			if e := saveState(rc.Cluster, after); e != nil {
				t.Fatal(e)
			}
			// Remove new desired intent: only in-flight readiness is under test.
			rc.Cluster.Spec.EngineOptions = originalOptions
			c := rc.Cluster.Spec.Components.Extra["auth"]
			c.Replicas = 1
			c.Resources = after.Members["auth"].Resources
			rc.Cluster.Spec.Components.Extra["auth"] = c
			for range 5 {
				if _, e := p.syncComponents(t.Context(), rc); e != nil {
					t.Fatal(e)
				}
			}
			after, _ = loadState(rc.Cluster)
			if after.Members["auth"].Stage != engine.ComponentJoined {
				t.Fatal("window blocked readiness")
			}
			delete(b.services, after.Members["auth"].Name)
			b.deployed[after.Members["auth"].Name] = false
			for range 15 {
				if _, e := p.syncComponents(t.Context(), rc); e != nil {
					t.Fatal(e)
				}
			}
			if !b.deployed[after.Members["auth"].Name] {
				t.Fatal("component healing waited for window")
			}
		})
	}
}
func TestSupabaseAgentCancellationBetweenMembers(t *testing.T) {
	p, rc, b := setupProvider(t)
	convergeProvider(t, p, rc)
	b.agentBuilds = map[string]string{}
	for name := range b.deployed {
		b.agentBuilds[name] = engine.CurrentAgentBuild().Build
	}
	s, _ := loadState(rc.Cluster)
	for _, m := range s.Members {
		b.agentBuilds[m.Name] = "old"
	}
	cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}, CoreDone: true, Order: []string{"gateway", "auth", "rest"}}
	raw, _ := json.Marshal(cp)
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	r, e := p.CancelOperation(t.Context(), rc, op)
	if e != nil || !r.Done {
		t.Fatal("cancellation did not stop between components", r, e)
	}
}

func TestOptionalAgentRolloutWindowClosesBetweenMembers(t *testing.T) {
	p, rc, b := setupProvider(t)
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	b.agentBuilds = map[string]string{}
	for _, m := range s.Members {
		b.agentBuilds[m.Name] = "old"
	}
	wrapped := &agentRolloutBackend{testBackend: b}
	rc.Backend = wrapped
	cp := agentUpgradeCheckpoint{
		Target:   engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build},
		CoreDone: true,
		Order:    []string{"gateway", "auth"},
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	step := func() engine.OpResult {
		t.Helper()
		p = New(p.options)
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if r.Checkpoint != nil {
			op.Checkpoint = r.Checkpoint
		}
		return r
	}
	step()
	first := s.Members["gateway"].Name
	if !reflect.DeepEqual(wrapped.names, []string{first}) {
		t.Fatal("first optional member did not deploy", wrapped.names)
	}
	at := time.Now().UTC()
	rc.Clock = importReplayClock{at: at}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + at.Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	b.agentBuilds[first] = "old"
	for range 3 {
		step()
	}
	if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if !cp.Waiting || cp.Index != 0 {
		t.Fatal("closed window bypassed recovering member build fence", cp)
	}
	b.agentBuilds[first] = engine.CurrentAgentBuild().Build
	for range 5 {
		step()
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			t.Fatal(err)
		}
		if cp.Index == 1 {
			break
		}
	}
	if cp.Waiting || cp.Index != 1 {
		t.Fatal("closed window stranded recovering optional member", cp)
	}
	r := step()
	if len(wrapped.names) != 1 || r.Message != "Waiting for maintenance window" || r.RequeueAfter <= 0 {
		t.Fatal("next optional member redeployed outside window", wrapped.names, r)
	}
	after, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if after.Pending != nil || after.Members["auth"].ConfigVersion != s.Members["auth"].ConfigVersion {
		t.Fatal("waiting changed next optional member configuration or deployment")
	}
	op.IgnoreMaintenanceWindow = true
	raw, err = json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	op = &domain.Operation{}
	if err := json.Unmarshal(raw, op); err != nil {
		t.Fatal(err)
	}
	step()
	if !reflect.DeepEqual(wrapped.names, []string{first, s.Members["auth"].Name}) {
		t.Fatal("persisted override did not permit optional deployment", wrapped.names)
	}
	for range 8 {
		r = step()
		if r.Done {
			break
		}
	}
	if !r.Done || rc.Operation != nil {
		t.Fatal("optional rollout failed to complete or leaked operation context", r)
	}
}

func TestDisabledComponentMaintenanceThroughSync(t *testing.T) {
	for _, component := range []string{"auth", "gateway"} {
		t.Run(component, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			public := &publicBackend{testBackend: backend}
			rc.Backend = public
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			member := state.Members[component]
			at := time.Now().UTC()
			rc.Clock = importReplayClock{at: at}
			window := &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + at.Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
			rc.Cluster.Spec.Maintenance = window
			desired := rc.Cluster.Spec.Components.Extra[component]
			desired.Replicas = 0
			rc.Cluster.Spec.Components.Extra[component] = desired
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				state, err = loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if state.Members[component] == nil || backend.services[member.Name] == nil || state.Pending != nil {
					// Gateway revocation has its own receipt on the first pass.
					if component != "gateway" || state.Pending == nil || state.Pending.Action != "public" || backend.services[member.Name] == nil {
						t.Fatalf("closed window started component removal: %+v", state)
					}
				}
			}
			if component == "gateway" && public.enabled {
				t.Fatal("closed window delayed gateway exposure revocation")
			}
			rc.Cluster.Spec.Maintenance = nil
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			state, err = loadState(rc.Cluster)
			if err != nil || state.Pending == nil || state.Pending.Action != "delete" {
				t.Fatalf("open window did not submit deletion: %+v %v", state, err)
			}
			rc.Cluster.Spec.Maintenance = window
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			state, err = loadState(rc.Cluster)
			if err != nil || state.Members[component] != nil || state.Pending != nil || backend.services[member.Name] != nil {
				t.Fatalf("closed window blocked accepted deletion completion: %+v %v", state, err)
			}
		})
	}
}
