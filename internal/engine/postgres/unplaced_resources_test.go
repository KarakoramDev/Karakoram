// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type unplacedResizeBackend struct{ *unplacedScaleBackend }

func (*unplacedResizeBackend) ResourceUpdatesDeployRuntime() bool { return true }

func (b *unplacedResizeBackend) ResizeUnplacedRuntime(ctx context.Context, name string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	op, err := b.SetResources(ctx, name, desired)
	return op, err == nil, err
}

func TestBlockedProxyResizeSupersedesInitialDeployment(t *testing.T) {
	p, rc, f := setup(t, "single")
	s := converge(t, p, rc)
	var proxy *member
	for _, m := range s.Members {
		if m.Role == "proxy" {
			proxy = m
		}
	}
	if proxy == nil {
		t.Fatal("missing proxy")
	}
	proxy.Stage = "deploying"
	proxy.RuntimeRecovery.Initialized = false
	proxy.Resources.MinCPU, proxy.Resources.MaxCPU = 4, 4
	proxy.DeployOp = &infra.Op{ID: "blocked", Kind: "deploy"}
	f.services[proxy.Name].Resources = proxy.Resources
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Resources.MinCPU, desired.Resources.MaxCPU = 1, 1
	desired.Resources.MinRAM, desired.Resources.MaxRAM = .25, .25
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &unplacedResizeBackend{&unplacedScaleBackend{fakeBackend: f}}
	rc.Backend = b
	agents := rc.Agents
	rc.Agents = &unplacedScaleAgents{AgentDialer: agents, targets: map[string]bool{proxy.Name: true}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	proxy = s.find(proxy.Name)
	if b.deployPolls != 0 || proxy.DeployOp.ID == "blocked" || proxy.Resources.MaxCPU != 1 || f.services[proxy.Name].Resources.MaxRAM != .25 {
		t.Fatalf("obsolete deployment retained: polls=%d proxy=%+v", b.deployPolls, proxy)
	}
	rc.Agents = agents
	converge(t, p, rc)
}

type racedResizeBackend struct {
	*unplacedResizeBackend
	failure *infra.DeploymentExhaustedError
	later   bool
	target  string
	deploys int
}

func (b *racedResizeBackend) ResizeUnplacedRuntime(ctx context.Context, name string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	b.target = name
	if b.later {
		if _, err := b.SetResources(ctx, name, desired); err != nil {
			return infra.Op{}, false, err
		}
		return infra.Op{ID: "corrected", Kind: "resources"}, true, nil
	}
	return infra.Op{}, false, b.failure
}

func (b *racedResizeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "corrected" {
		b.services[b.target].Resources = *b.failure.PreviousResources
		return infra.OpState{Failed: true}, b.failure
	}
	return b.unplacedResizeBackend.OpStatus(ctx, op)
}

func (b *racedResizeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.deploys++
	return b.fakeBackend.Deploy(ctx, name, d)
}

func TestUnplacedResizeFailureIsDurableAndRearmConverges(t *testing.T) {
	for _, later := range []bool{false, true} {
		t.Run(map[bool]string{false: "admission", true: "later-poll"}[later], func(t *testing.T) {
			p, rc, f := setup(t, "single")
			s := converge(t, p, rc)
			var proxy *member
			for _, m := range s.Members {
				if m.Role == "proxy" {
					proxy = m
				}
			}
			proxy.Stage, proxy.RuntimeRecovery.Initialized = "deploying", false
			proxy.Resources.MinRAM, proxy.Resources.MaxRAM = 4, 4
			previous := proxy.Resources
			f.services[proxy.Name].Resources = previous
			proxy.DeployOp = &infra.Op{ID: "original", Kind: "deploy"}
			desired := rc.Cluster.Spec.Components.Extra["proxy"]
			desired.Resources.MinRAM, desired.Resources.MaxRAM = 1, 1
			rc.Cluster.Spec.Components.Extra["proxy"] = desired
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b := &racedResizeBackend{unplacedResizeBackend: &unplacedResizeBackend{&unplacedScaleBackend{fakeBackend: f}}, later: later,
				failure: &infra.DeploymentExhaustedError{Service: proxy.Name, LastError: "UnplacedResizeDeferred: placement raced bootstrap stop", PreviousResources: &previous}}
			rc.Backend = b
			agents := rc.Agents
			rc.Agents = &unplacedScaleAgents{AgentDialer: agents, targets: map[string]bool{proxy.Name: true}}
			for i := range 8 {
				if later && i == 1 {
					current, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					current.find(proxy.Name).RuntimeRecovery.Initialized = true
					if err := save(rc.Cluster, current); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
					t.Fatal(err)
				}
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			proxy = s.find(proxy.Name)
			if b.deploys != 0 || proxy.DeployOp != nil || proxy.Resources.MaxRAM != 4 || proxy.Stage != "imported" || len(rc.Cluster.Status.DeploymentFailures) != 1 {
				t.Fatalf("failure escaped durable fence: deploys=%d proxy=%+v failures=%+v", b.deploys, proxy, rc.Cluster.Status.DeploymentFailures)
			}
			rc.Cluster.Spec.RetryEpoch++
			rc.Agents = agents
			converge(t, p, rc)
			s, _ = load(rc.Cluster)
			if b.deploys != 1 || s.find(proxy.Name).Resources.MaxRAM != 1 || f.services[proxy.Name].Resources.MaxRAM != 1 {
				t.Fatal("explicit re-arm did not deploy corrected resources")
			}
		})
	}
}
