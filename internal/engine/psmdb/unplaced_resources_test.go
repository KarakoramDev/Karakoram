// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type unplacedResizeBackend struct {
	*supersedingCleanupBackend
	resizes int
}

func (*unplacedResizeBackend) ResourceUpdatesDeployRuntime() bool { return true }
func (b *unplacedResizeBackend) ResizeUnplacedRuntime(context.Context, string, infra.Op, infra.Resources) (infra.Op, bool, error) {
	b.resizes++
	return infra.Op{ID: "replacement", Kind: "resources"}, true, nil
}

func TestUnchangedBlockedDeploymentDoesNotResizeRuntimeDiskBounds(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	m := s.Members[0]
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk = 5
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = 10
	m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
	s.Pending = &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	b := &unplacedResizeBackend{supersedingCleanupBackend: &supersedingCleanupBackend{fakeInfra: f}}
	rc.Backend = b
	if busy, err := p.observePending(t.Context(), rc, s); !busy || err != nil {
		t.Fatalf("busy=%v err=%v", busy, err)
	}
	if b.resizes != 0 || b.deployPolls != 1 || s.Pending.Op.ID != "blocked" {
		t.Fatal("unchanged runtime resources superseded deployment")
	}
}

func TestNeverPlacedRAMCorrectionRefreshesFirstMemberConfig(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 4
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	m := s.Members[0]
	m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
	if m.CacheGB != 1.75 {
		t.Fatalf("original cache=%v", m.CacheGB)
	}
	s.Pending = &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	b := &unplacedResizeBackend{supersedingCleanupBackend: &supersedingCleanupBackend{fakeInfra: f}}
	rc.Backend = b
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 1
	if busy, err := p.observePending(t.Context(), rc, s); !busy || err != nil {
		t.Fatalf("busy=%v err=%v", busy, err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if b.resizes != 1 || config.WiredTigerCacheSizeGB != .25 || config.PBMGoMemoryLimitBytes != 384<<20 {
		t.Fatalf("corrected first config: resizes=%d cache=%v pbm=%d", b.resizes, config.WiredTigerCacheSizeGB, config.PBMGoMemoryLimitBytes)
	}
}

type racedResizeBackend struct {
	*unplacedResizeBackend
	failure *infra.DeploymentExhaustedError
	later   bool
}

func (b *racedResizeBackend) ResizeUnplacedRuntime(context.Context, string, infra.Op, infra.Resources) (infra.Op, bool, error) {
	if b.later {
		return infra.Op{ID: "corrected", Kind: "resources"}, true, nil
	}
	return infra.Op{}, false, b.failure
}

func (b *racedResizeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "corrected" {
		return infra.OpState{Failed: true}, b.failure
	}
	return b.unplacedResizeBackend.OpStatus(ctx, op)
}

func TestUnplacedResizeFailureRetiresReceiptAndRearmRefreshesCache(t *testing.T) {
	for _, later := range []bool{false, true} {
		t.Run(map[bool]string{false: "admission", true: "later-poll"}[later], func(t *testing.T) {
			p, rc, f, _ := setup(t)
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 4
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
			s, _ := load(rc.Cluster)
			plan(rc.Cluster, s)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			m.Stage = "imported"
			m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
			previous := m.Resources
			s.Pending = &pending{Action: "deploy", Member: m.Name, Version: rc.Cluster.Spec.Version, Op: infra.Op{ID: "original", Kind: "deploy"}}
			b := &racedResizeBackend{unplacedResizeBackend: &unplacedResizeBackend{supersedingCleanupBackend: &supersedingCleanupBackend{fakeInfra: f}}, later: later,
				failure: &infra.DeploymentExhaustedError{Service: m.Name, LastError: "UnplacedResizeDeferred: allocation raced bootstrap stop", PreviousResources: &previous}}
			rc.Backend = b
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 1
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 1
			_, err := p.observePending(t.Context(), rc, s)
			if later && err == nil {
				m.RuntimeRecovery.Initialized = true
				_, err = p.observePending(t.Context(), rc, s)
			}
			if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || s.Pending != nil || m.Resources.MaxRAM != 4 || m.CacheGB != 1.75 {
				t.Fatalf("failure not rolled back/retired: err=%v pending=%+v resources=%+v cache=%v", err, s.Pending, m.Resources, m.CacheGB)
			}
			before := f.mutations
			for range 5 {
				if err := p.deploy(t.Context(), rc, s, m, "deploy", rc.Cluster.Spec.Version); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
					t.Fatalf("durable fence missing: %v", err)
				}
			}
			if f.mutations != before || len(f.deployments) != 0 {
				t.Fatal("ordinary deployment bypassed conditional failure fence")
			}
			rc.Cluster.Spec.RetryEpoch++
			if err := p.deploy(t.Context(), rc, s, m, "deploy", rc.Cluster.Spec.Version); err != nil {
				t.Fatal(err)
			}
			if s.Pending == nil || s.Pending.Action != "resources" {
				t.Fatal("re-arm did not synchronize corrected capacity before deploy")
			}
			if _, err := p.observePending(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := p.deploy(t.Context(), rc, s, m, "deploy", rc.Cluster.Spec.Version); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil || len(f.deployments) != 1 || m.Resources.MaxRAM != 1 || config.WiredTigerCacheSizeGB != .25 || config.PBMGoMemoryLimitBytes != 384<<20 {
				t.Fatalf("re-arm failed corrected runtime/config: err=%v deployments=%d ram=%v cache=%v pbm=%d", err, len(f.deployments), m.Resources.MaxRAM, config.WiredTigerCacheSizeGB, config.PBMGoMemoryLimitBytes)
			}
		})
	}
}
