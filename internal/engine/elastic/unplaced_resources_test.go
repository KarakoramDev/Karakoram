// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type unplacedResizeBackend struct{ *unplacedScaleBackend }

func (*unplacedResizeBackend) ResourceUpdatesDeployRuntime() bool { return true }

func (b *unplacedResizeBackend) ResizeUnplacedRuntime(ctx context.Context, name string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	op, err := b.SetResources(ctx, name, desired)
	return op, err == nil, err
}

func TestBlockedDeploymentResizePrecedesPendingObservation(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	m := s.Members[0]
	m.RuntimeRecovery.Initialized = false
	m.Stage = stageImported
	m.Resources.MinCPU, m.Resources.MaxCPU = 4, 4
	f.services[m.Name].Resources = m.Resources
	s.Pending = &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	b := &unplacedResizeBackend{&unplacedScaleBackend{&supersedingCleanupBackend{coreInfra: f}}}
	rc.Backend = b
	busy, err := p.observePending(t.Context(), rc, s)
	if !busy || err != nil {
		t.Fatalf("busy=%v err=%v", busy, err)
	}
	if b.deployPolls != 0 || s.Pending.Op.ID == "blocked" || m.Resources.MaxCPU != rc.Cluster.Spec.Components.Engine.Resources.MaxCPU {
		t.Fatalf("blocked deployment retained: polls=%d pending=%+v resources=%+v", b.deployPolls, s.Pending, m.Resources)
	}
}

func TestUnchangedBlockedDeploymentDoesNotResizeRuntimeDiskBounds(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	m := s.Members[0]
	m.RuntimeRecovery.Initialized = false
	m.Stage = stageImported
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk = 5
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = 10
	m.Resources = runtimeResources(m, rc.Cluster.Spec.Components.Engine.Resources)
	s.Pending = &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	b := &unplacedResizeBackend{&unplacedScaleBackend{&supersedingCleanupBackend{coreInfra: f}}}
	rc.Backend = b
	before := f.mutations
	if busy, err := p.observePending(t.Context(), rc, s); !busy || err != nil {
		t.Fatalf("busy=%v err=%v", busy, err)
	}
	if b.deployPolls != 1 || f.mutations != before || s.Pending.Op.ID != "blocked" {
		t.Fatal("unchanged runtime resources superseded initial deployment")
	}
}
