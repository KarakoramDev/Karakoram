// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type pendingProxyScaleBackend struct {
	*fakeBackend
	count   int
	calls   int
	fail    error
	busy    bool
	targets []int
}

func (b *pendingProxyScaleBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: op.ID == "superseding-scale" && !b.busy, Status: "starting"}, nil
}

func (b *pendingProxyScaleBackend) SetContainerCount(_ context.Context, name string, min, max int) (infra.Op, error) {
	b.calls++
	if b.fail != nil {
		return infra.Op{}, b.fail
	}
	b.count = max
	b.targets = append(b.targets, max)
	return infra.Op{ID: "superseding-scale", Hostname: name}, nil
}

func TestPendingProxyScaleAcceptsSupersedingReduction(t *testing.T) {
	p, rc, base := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demopx")
	m.Replicas = 2
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	s.Pending = &pending{Action: "scale", Member: m.Name, Replicas: 3, Op: infra.Op{ID: "stalled-scale"}}
	b := &pendingProxyScaleBackend{fakeBackend: base, count: 3}
	rc.Backend = b
	busy, err := p.observePending(t.Context(), rc, s)
	if err != nil || !busy || b.calls != 1 || b.count != 2 || s.Pending == nil || s.Pending.Op.ID != "superseding-scale" || s.Pending.Replicas != 2 {
		t.Fatalf("superseding count stayed behind stalled scale: busy=%t err=%v calls=%d count=%d pending=%+v", busy, err, b.calls, b.count, s.Pending)
	}
	// A reconstructed CP observes only the new operation, never the old target.
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.observePending(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Pending != nil || s.find(m.Name).Replicas != 2 || b.calls != 1 {
		t.Fatalf("superseding scale did not complete: %+v calls=%d", s.Pending, b.calls)
	}
}

func TestPendingProxyScalePreservesFenceOnRejectedSupersession(t *testing.T) {
	p, rc, base := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demopx")
	m.Replicas = 2
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	old := &pending{Action: "scale", Member: m.Name, Replicas: 3, Op: infra.Op{ID: "stalled-scale"}}
	s.Pending = old
	b := &pendingProxyScaleBackend{fakeBackend: base, fail: infra.ErrConflict}
	rc.Backend = b
	busy, err := p.observePending(t.Context(), rc, s)
	if !busy || !errors.Is(err, infra.ErrConflict) || b.calls != 1 || s.Pending != old {
		t.Fatalf("rejected supersession discarded old fence: busy=%t err=%v calls=%d pending=%+v", busy, err, b.calls, s.Pending)
	}
}

func TestPendingProxyScaleDoesNotBypassIncreaseBudgetOrStatefulFence(t *testing.T) {
	for _, target := range []string{"unchanged", "increase", "disable", "data"} {
		t.Run(target, func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demopx")
			replicas := 3
			switch target {
			case "increase":
				replicas = 4
			case "disable":
				replicas = 0
			case "data":
				m = s.data()[0]
				replicas = 2
			}
			desired := rc.Cluster.Spec.Components.Extra["proxy"]
			desired.Replicas = replicas
			rc.Cluster.Spec.Components.Extra["proxy"] = desired
			old := &pending{Action: "scale", Member: m.Name, Replicas: 3, Op: infra.Op{ID: "stalled-scale"}}
			s.Pending = old
			b := &pendingProxyScaleBackend{fakeBackend: base}
			rc.Backend = b
			busy, err := p.observePending(t.Context(), rc, s)
			if err != nil || !busy || b.calls != 0 || s.Pending != old {
				t.Fatalf("bypassed existing barrier: busy=%t err=%v calls=%d", busy, err, b.calls)
			}
		})
	}
}

func TestPendingProxyScaleSuccessiveReductionsAndInterruptedPersistence(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "successive", true: "interrupted-save"}[interrupt], func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demopx")
			m.Replicas = 2
			desired := rc.Cluster.Spec.Components.Extra["proxy"]
			desired.Replicas = 2
			rc.Cluster.Spec.Components.Extra["proxy"] = desired
			s.Pending = &pending{Action: "scale", Member: m.Name, Replicas: 3, Op: infra.Op{ID: "stalled-scale"}}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b := &pendingProxyScaleBackend{fakeBackend: base, count: 3, busy: true}
			rc.Backend = b
			if busy, err := p.observePending(t.Context(), rc, s); err != nil || !busy || b.count != 2 {
				t.Fatalf("first reduction rejected: busy=%t count=%d err=%v", busy, b.count, err)
			}
			if interrupt {
				// Drop the unsaved provider state after backend acceptance, as
				// after a CP crash. Backend desired count two survives.
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if s.Pending.Replicas != 3 || b.count != 2 {
					t.Fatal("fixture did not preserve the acceptance/save gap")
				}
				if _, err := p.observePending(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if s.Pending.Replicas != 2 || b.count != 2 {
					t.Fatal("reconstructed CP re-admitted the obsolete count")
				}
			}
			desired.Replicas = 1
			rc.Cluster.Spec.Components.Extra["proxy"] = desired
			if busy, err := p.observePending(t.Context(), rc, s); err != nil || !busy || b.count != 1 || s.Pending.Replicas != 1 {
				t.Fatalf("second reduction waited behind count two: pending=%+v count=%d err=%v", s.Pending, b.count, err)
			}
			for _, target := range b.targets {
				if target == 3 {
					t.Fatal("obsolete larger count was re-admitted")
				}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			var err error
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			b.busy = false
			if _, err := p.observePending(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if s.Pending != nil || s.find(m.Name).Replicas != 1 {
				t.Fatalf("latest reduction did not complete after reconstruction: %+v", s.Pending)
			}
		})
	}
}
