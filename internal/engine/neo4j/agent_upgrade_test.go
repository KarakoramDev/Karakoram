// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type upgradeBackend struct {
	*fake.Backend
	names []string
	specs []infra.RuntimeSpec
}

func (b *upgradeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.names = append(b.names, name)
	b.specs = append(b.specs, d.Runtime)
	return b.Backend.Deploy(ctx, name, d)
}
func upgradeOperation(include bool) *domain.Operation {
	raw, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
	return &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: raw}
}
func upgradeAgent(t *testing.T, rc *engine.Context, a *agentState) (*string, *bool) {
	t.Helper()
	build := "old"
	ready := true
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: build}, ConfigVersion: a.config.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"neo4j": {State: "running"}}})
		case "/v1/neo4j/readiness":
			json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: ready, ConfigVersion: a.config.ConfigVersion, Version: a.spec.Version, AppliedRestartToken: a.spec.RestartToken})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = testAgents{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return &build, &ready
}
func TestAgentUpgradeCatalogReplayAndIncludeCurrent(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "include"}[include], func(t *testing.T) {
			p, rc, b, a := setup(t)
			converge(t, p, rc)
			if !slices.Contains(p.Describe().Operations, domain.OperationUpgradeAgents) {
				t.Fatal("missing operation")
			}
			build, _ := upgradeAgent(t, rc, a)
			*build = engine.CurrentAgentBuild().Build
			s, _ := load(rc.Cluster)
			before, _ := json.Marshal(s)
			expected, err := p.runtimeSpec(rc, s)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &upgradeBackend{Backend: b}
			rc.Backend = wrapped
			op := upgradeOperation(include)
			done := false
			for range 12 {
				p = New(p.options)
				r, e := p.RunOperation(t.Context(), rc, op)
				if e != nil {
					t.Fatal(e)
				}
				op.Checkpoint = r.Checkpoint
				if r.Done {
					done = true
					break
				}
			}
			if !done || len(wrapped.names) != map[bool]int{false: 0, true: 1}[include] {
				t.Fatalf("done=%v deploys=%v", done, wrapped.names)
			}
			if include && !reflect.DeepEqual(wrapped.specs[0], expected) {
				t.Fatal("changed deployed runtime")
			}
			after, _ := load(rc.Cluster)
			raw, _ := json.Marshal(after)
			if !reflect.DeepEqual(before, raw) {
				t.Fatal("changed applied version, resources or volume")
			}
		})
	}
}
func TestAgentUpgradeBuildReadinessWindowAndCancel(t *testing.T) {
	p, rc, b, a := setup(t)
	converge(t, p, rc)
	build, ready := upgradeAgent(t, rc, a)
	wrapped := &upgradeBackend{Backend: b}
	rc.Backend = wrapped
	bad := upgradeOperation(true)
	bad.Payload = json.RawMessage(`{"targetBuild":"other"}`)
	if _, e := p.RunOperation(t.Context(), rc, bad); e == nil {
		t.Fatal("missing build fence")
	}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	op := upgradeOperation(true)
	for range 3 {
		r, e := p.RunOperation(t.Context(), rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
	}
	if len(wrapped.names) != 0 {
		t.Fatal("closed window deployed")
	}
	rc.Cluster.Spec.Maintenance = nil
	for len(wrapped.names) == 0 {
		r, e := p.RunOperation(t.Context(), rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
	}
	for range 3 {
		r, e := p.CancelOperation(t.Context(), rc, op)
		if e != nil || r.Done {
			t.Fatal("abandoned old build", r, e)
		}
		op.Checkpoint = r.Checkpoint
	}
	*build = engine.CurrentAgentBuild().Build
	*ready = false
	r, e := p.CancelOperation(t.Context(), rc, op)
	if e != nil || r.Done {
		t.Fatal("native readiness bypassed", r, e)
	}
	op.Checkpoint = r.Checkpoint
	*ready = true
	r, e = p.CancelOperation(t.Context(), rc, op)
	if e != nil || !r.Done || len(wrapped.names) != 1 {
		t.Fatal("did not safely finish", r, e, wrapped.names)
	}
}
func TestAgentUpgradeRejectsPendingWork(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	s.Member.Pending = &infra.Op{ID: "other"}
	save(rc.Cluster, s)
	if _, e := p.RunOperation(t.Context(), rc, upgradeOperation(true)); e == nil {
		t.Fatal("pending work accepted")
	}
}

func TestAgentUpgradeRejectsRecoveryJournals(t *testing.T) {
	for _, journal := range []string{"receipt", "import", "lost-volume", "restore"} {
		t.Run(journal, func(t *testing.T) {
			p, rc, b, _ := setup(t)
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			switch journal {
			case "receipt":
				s.Member.RuntimeRecovery.Op = &infra.Op{ID: "recovery"}
			case "import":
				s.Member.RuntimeRecovery.ImportPlan = &engine.ImportPlan{}
			case "lost-volume":
				s.Member.RuntimeRecovery.VolumeLost = true
			case "restore":
				s.RestoreRecovery = &domain.Operation{ID: "restore"}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			wrapped := &upgradeBackend{Backend: b}
			rc.Backend = wrapped
			if _, err := p.RunOperation(t.Context(), rc, upgradeOperation(true)); err == nil || len(wrapped.names) != 0 {
				t.Fatal("recovery work did not block agent rollout", err, wrapped.names)
			}
		})
	}
}

func TestAgentUpgradeCancelsWhileWaitingForWindow(t *testing.T) {
	p, rc, b, _ := setup(t)
	converge(t, p, rc)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	wrapped := &upgradeBackend{Backend: b}
	rc.Backend = wrapped
	op := upgradeOperation(true)
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	var canceller engine.OperationCanceller = p
	if err := canceller.CanCancelOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	result, err = canceller.CancelOperation(t.Context(), rc, op)
	if err != nil || !result.Done || len(wrapped.names) != 0 {
		t.Fatal("window wait cancellation did not stop safely", result, err)
	}
}
