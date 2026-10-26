// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type correctedResizeBackend struct {
	*testBackend
	blocked bool
	calls   int
	polls   int
}

func (*correctedResizeBackend) ResourceUpdatesDeployRuntime() bool { return true }
func (b *correctedResizeBackend) ResizeUnplacedRuntime(_ context.Context, _ string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	b.calls++
	if expected.ID != "blocked-resize" || desired.MinRAM != 1 || desired.MaxRAM != 1 {
		panic("incorrect conditional resize target")
	}
	return infra.Op{ID: "corrected-resize", Kind: "resources"}, b.blocked, nil
}
func (b *correctedResizeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "blocked-resize" {
		b.polls++
		return infra.OpState{Status: "starting"}, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestComponentCorrectedResizeReplacesPendingTarget(t *testing.T) {
	for _, blocked := range []bool{true, false} {
		t.Run(map[bool]string{true: "blocked", false: "progressing"}[blocked], func(t *testing.T) {
			p, rc, base := setupProvider(t)
			backend := &correctedResizeBackend{testBackend: base, blocked: blocked}
			rc.Backend = backend
			desired := domain.ComponentSpec{Replicas: 1, Resources: domain.Resources{MinRAM: 1, MaxRAM: 1}}
			rc.Cluster.Spec.Components.Extra["storage"] = desired
			pendingResources := desired.Resources
			pendingResources.MinRAM, pendingResources.MaxRAM = 2, 2
			s := &componentState{
				Members: map[string]*componentMember{"storage": {Name: "storage", Component: "storage", Stage: engine.ComponentJoined, Replicas: 1, Resources: desired.Resources}},
				Pending: &componentPending{Component: "storage", Action: "resize", Op: infra.Op{ID: "blocked-resize"}, Resources: pendingResources, Target: domain.ComponentSpec{Replicas: 1, Resources: pendingResources}},
			}
			busy, err := p.observeComponent(t.Context(), rc, s)
			if err != nil || !busy || backend.calls != 1 {
				t.Fatalf("correction busy=%v calls=%d err=%v", busy, backend.calls, err)
			}
			if blocked {
				if s.Pending.Op.ID != "corrected-resize" || s.Pending.Resources.MaxRAM != 1 || s.Pending.Target.Resources.MaxRAM != 1 || backend.polls != 0 {
					t.Fatalf("obsolete receipt retained: %+v polls=%d", s.Pending, backend.polls)
				}
			} else if s.Pending.Op.ID != "blocked-resize" || s.Pending.Resources.MaxRAM != 2 || backend.polls != 1 {
				t.Fatalf("genuinely progressing receipt replaced: %+v polls=%d", s.Pending, backend.polls)
			}
		})
	}
}

func TestStorageCorrectedResizeConvergesThroughSync(t *testing.T) {
	p, rc, deployed := storageDeployFailureFixture(t, infra.OpState{Done: true})
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members["storage"]
	oversized := m.Resources
	oversized.MinRAM, oversized.MaxRAM = 2, 2
	s.Pending = &componentPending{
		Component: "storage", Action: "resize", Op: infra.Op{ID: "blocked-resize"},
		Resources: oversized, Target: domain.ComponentSpec{Replicas: m.Replicas, Resources: oversized},
	}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	backend := &correctedResizeBackend{testBackend: deployed.testBackend, blocked: true}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = loadState(rc.Cluster)
	if err != nil || s.Pending == nil || s.Pending.Op.ID != "corrected-resize" || backend.polls != 0 {
		t.Fatalf("public Sync retained obsolete resize: %+v polls=%d err=%v", s, backend.polls, err)
	}
	convergeProvider(t, p, rc)
	if backend.calls != 1 {
		t.Fatalf("corrected deployment resubmitted %d times", backend.calls)
	}
	s, err = loadState(rc.Cluster)
	if err != nil || s.Pending != nil || s.Members["storage"].Resources.MaxRAM != 1 || s.Members["storage"].Stage != engine.ComponentJoined {
		t.Fatalf("corrected component did not converge: %+v %v", s, err)
	}
}
