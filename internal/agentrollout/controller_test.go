// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentrollout

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type clock struct{ at time.Time }

func (c *clock) Now() time.Time { return c.at }

type provider struct {
	engine.Provider
	supported bool
}

func (provider) Name() string { return "fake" }
func (p provider) Describe() domain.ProviderInfo {
	info := domain.ProviderInfo{Name: "fake"}
	if p.supported {
		info.Operations = []string{domain.OperationUpgradeAgents}
	}
	return info
}

func setup(t *testing.T, automatic bool, cap int) (*Controller, *clock) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	registry, err := engine.NewRegistry(provider{supported: true})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(domain.AgentRolloutSettings{Automatic: automatic, MaxConcurrentClusters: cap})
	if err = st.SetSetting(t.Context(), SettingsKey, raw); err != nil {
		t.Fatal(err)
	}
	clock := &clock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return New(Options{Store: st, Registry: registry, Clock: clock, Target: func() domain.AgentBuild { return domain.AgentBuild{Build: "target", Version: "v2"} }}), clock
}
func cluster(t *testing.T, c *Controller, name string, change func(*domain.Cluster)) domain.Cluster {
	t.Helper()
	value := domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: c.opts.Clock.Now(), Members: []domain.MemberStatus{{Name: name + "m0", Health: true, Agent: domain.AgentStatus{Healthy: true, Build: "old", LastSeen: c.opts.Clock.Now()}}}}}
	if change != nil {
		change(&value)
	}
	if err := c.opts.Store.CreateCluster(t.Context(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func TestAgentRolloutConcurrencyAndFailureFence(t *testing.T) {
	c, clock := setup(t, true, 1)
	a := cluster(t, c, "alpha", nil)
	b := cluster(t, c, "beta", nil)
	if err := c.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	op, err := c.opts.Store.GetOperation(t.Context(), OperationID(a, "target"))
	if err != nil {
		t.Fatal(err)
	}
	if op.Actor != "system:agent-rollout" || op.Phase != domain.PhasePending {
		t.Fatal(op)
	}
	if _, err = c.opts.Store.GetOperation(t.Context(), OperationID(b, "target")); err != store.ErrNotFound {
		t.Fatal("cap exceeded", err)
	}
	op.Phase = domain.PhaseFailed
	if err = c.opts.Store.UpdateOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	clock.at = clock.at.Add(time.Second)
	if err = c.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	again, err := c.opts.Store.GetOperation(t.Context(), op.ID)
	if err != nil || again.Phase != domain.PhaseFailed {
		t.Fatal("failure rearmed", again, err)
	}
	if _, err = c.opts.Store.GetOperation(t.Context(), OperationID(b, "target")); err != nil {
		t.Fatal("next cluster not admitted", err)
	}
	if err = c.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	events, err := c.opts.Store.ListEvents(t.Context(), store.EventFilter{})
	if err != nil || len(events) != 2 {
		t.Fatal("warning dedupe", events, err)
	}
	// A controller restart must retain event and operation fences.
	restarted := New(c.opts)
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	events, err = c.opts.Store.ListEvents(t.Context(), store.EventFilter{})
	if err != nil || len(events) != 2 {
		t.Fatal(events, err)
	}
}
func TestAgentRolloutEligibilityAndWarningsWhenDisabled(t *testing.T) {
	c, _ := setup(t, true, 5)
	skipped := []domain.Cluster{
		cluster(t, c, "managed", func(v *domain.Cluster) { v.ManagedBy = &domain.ManagedBy{Kind: "backup-drill", ID: "drill"} }),
		cluster(t, c, "deleting", func(v *domain.Cluster) { v.Deleting = true }),
		cluster(t, c, "suspended", func(v *domain.Cluster) {
			v.Status.Phase = domain.PhaseSuspended
			v.Status.Members[0].Agent.Healthy = false
		}),
		cluster(t, c, "unknown", func(v *domain.Cluster) { v.Status.Members[0].Agent.Build = "" }),
	}
	if err := c.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, v := range skipped {
		if _, err := c.opts.Store.GetOperation(t.Context(), OperationID(v, "target")); err != store.ErrNotFound {
			t.Fatal(v.Name, err)
		}
	}
	raw := json.RawMessage(`{"automatic":false,"maxConcurrentClusters":1}`)
	if err := c.opts.Store.SetSetting(t.Context(), SettingsKey, raw); err != nil {
		t.Fatal(err)
	}
	manual := cluster(t, c, "manual", nil)
	if err := c.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.opts.Store.GetOperation(t.Context(), OperationID(manual, "target")); err != store.ErrNotFound {
		t.Fatal(err)
	}
	events, err := c.opts.Store.ListEvents(t.Context(), store.EventFilter{Cluster: "manual"})
	if err != nil || len(events) != 1 || events[0].Reason != "AgentsOutdated" {
		t.Fatal(events, err)
	}
}
func TestAgentRolloutRunInjectedTicks(t *testing.T) {
	c, _ := setup(t, false, 1)
	cluster(t, c, "demo", nil)
	ticks := make(chan time.Time)
	close(ticks)
	c.opts.Tick = ticks
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	events, err := c.opts.Store.ListEvents(t.Context(), store.EventFilter{})
	if err != nil || len(events) != 1 {
		t.Fatal(events, err)
	}
}
func TestAgentRolloutIDIncludesIncarnationAndBuild(t *testing.T) {
	c := domain.Cluster{Name: "demo", Namespace: "default", CreatedAt: time.Unix(1, 0)}
	id := OperationID(c, "a")
	if len(id) != len("agents-")+16 || id != OperationID(c, "a") || id == OperationID(c, "b") {
		t.Fatal(id)
	}
	c.CreatedAt = c.CreatedAt.Add(time.Second)
	if id == OperationID(c, "a") {
		t.Fatal("incarnation ignored")
	}
}
