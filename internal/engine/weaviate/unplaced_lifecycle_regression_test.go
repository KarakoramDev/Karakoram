// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type abandonedBackend struct {
	*fake.Backend
	abandoned, volume             string
	obsoletePolls, deploys, stops int
	runtimeReceipt                string
	absence                       bool
	hold                          bool
}

func (*abandonedBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *abandonedBackend) RuntimeNeverPlaced(_ context.Context, name string) (bool, error) {
	return name == b.abandoned, nil
}
func (b *abandonedBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name == b.abandoned {
		b.deploys++
	}
	return b.Backend.Deploy(ctx, name, d)
}
func (b *abandonedBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	if name == b.abandoned {
		b.stops++
	}
	return b.Backend.Stop(ctx, name)
}
func (b *abandonedBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	if name == b.volume && !b.absence {
		panic("volume deleted without authoritative runtime absence")
	}
	op, err := b.Backend.DeleteServiceByID(ctx, name, id)
	if name == b.abandoned && err == nil {
		b.runtimeReceipt = op.ID
	}
	return op, err
}
func (b *abandonedBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "never-placed" {
		b.obsoletePolls++
		return infra.OpState{}, nil
	}
	if op.ID == b.runtimeReceipt && op.ID != "" && b.hold {
		return infra.OpState{}, nil
	}
	st, err := b.Backend.OpStatus(ctx, op)
	if op.ID == b.runtimeReceipt && st.Done && err == nil {
		b.absence = true
	}
	return st, err
}
func TestUnplacedScaleBackCheckpointLifecycle(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	coreConverge(t, f)
	f.rc.Cluster.Spec.Components.Engine.Replicas = 4
	// Run real scale-out through import, but never let the fourth deploy place.
	var s *state
	for range 80 {
		if _, err := f.p.Sync(ctx, f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if len(s.Members) == 4 && s.Pending != nil && s.Pending.Action == "deploy" {
			break
		}
	}
	if len(s.Members) != 4 || s.Pending == nil {
		t.Fatal("scale-out did not reach deployment")
	}
	m := s.find(s.Pending.Member)
	retiredPort, err := f.p.allocatePublicPort(ctx, f.rc, s, m, 8080)
	if err != nil {
		t.Fatal(err)
	}
	m.PublicHTTPPort = retiredPort
	if err := f.backend.EnsurePublicPort(ctx, m.Name, infra.PublicPort{Port: retiredPort, TargetPort: 8080, Protocol: "tcp"}); err != nil {
		t.Fatal(err)
	}
	s.Pending.Op = &infra.Op{ID: "never-placed", Kind: "deploy", Hostname: m.Name}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	b := &abandonedBackend{Backend: f.backend, abandoned: m.Name, volume: m.Volume, hold: true}
	f.rc.Backend = b
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	sawIntent, sawRuntimeWait, sawVolume := false, false, false
	for step := range 80 {
		f.p = New(Options{Store: f.rc.Store})
		if _, err := f.p.Sync(ctx, f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.ScaleDown != nil {
			t.Fatal("unplaced member entered native drain")
		}
		if abandoned := s.find(m.Name); abandoned != nil && abandoned.UnplacedRetirement != nil {
			sawIntent = true
			q := abandoned.UnplacedRetirement
			if q.Phase == "runtime" && q.Op != nil {
				sawRuntimeWait = true
				if step > 5 {
					b.hold = false
				}
			}
			if q.Phase == "volume" {
				sawVolume = true
			}
		}
		if s.find(m.Name) == nil {
			if !sawIntent || !sawRuntimeWait || !sawVolume || !b.absence || b.deploys != 0 || b.stops != 0 || b.obsoletePolls != 0 {
				t.Fatalf("unsafe retirement: intent=%v runtime=%v volume=%v backend=%+v", sawIntent, sawRuntimeWait, sawVolume, b)
			}
			for _, name := range []string{m.Name, m.Volume} {
				service, err := b.GetService(ctx, name)
				if err != nil || service != nil {
					t.Fatal("abandoned resource retained", name, err)
				}
			}
			reclaimed, err := f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
			if err != nil || reclaimed != retiredPort {
				t.Fatal("completed retirement did not reclaim public listener", reclaimed, err)
			}
			coreConverge(t, f)
			return
		}
	}
	t.Fatal("unplaced scale-back failed to converge")
}
