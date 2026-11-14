// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type tombstoneStopBackend struct {
	*fakeBackend
	rc    *engine.Context
	t     *testing.T
	stops int
}

func (b *tombstoneStopBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	persisted, err := b.rc.Store.GetCluster(ctx, b.rc.Cluster.Ref())
	if err != nil {
		b.t.Fatal(err)
	}
	journal, err := load(persisted)
	if err != nil {
		b.t.Fatal(err)
	}
	if len(journal.ResolutionTombstones) != 1 || !journal.ResolutionTombstones[0].Unprotected {
		b.t.Fatal("Stop preceded durable tombstone and decision", journal.ResolutionTombstones)
	}
	b.stops++
	return b.fakeBackend.Stop(ctx, name)
}

func TestResolutionTombstoneJournalBeforeStop(t *testing.T) {
	p, rc, b, s, lost := recoveryFixture(t)
	lost.RuntimeRecovery.VolumeLost = true
	lost.ConfirmedVolumeLost = true
	lost.Removing = true
	lost.Retirement = "runtime-stop"
	backend := &tombstoneStopBackend{fakeBackend: b, rc: rc, t: t}
	rc.Backend = backend
	events := &placementSafetyEvents{}
	rc.Events = events
	persist := func() {
		t.Helper()
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if err := rc.Store.(interface {
			UpdateClusterState(context.Context, *domain.Cluster) error
		}).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
			t.Fatal(err)
		}
		current, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster = current
		s, err = load(current)
		if err != nil {
			t.Fatal(err)
		}
		lost = s.find(lost.Name)
		b.mu.Lock()
		for _, peer := range s.Members {
			if peer.Role == "proxy" {
				b.applied[peer.Name] = peer.ConfigVersion
			}
		}
		b.mu.Unlock()
	}
	for tick := range 3 {
		if _, err := p.retireMember(t.Context(), rc, s, lost); err != nil {
			t.Fatal(err)
		}
		if tick < 2 && backend.stops != 0 {
			t.Fatal("Stop before checkpoint", tick)
		}
		persist()
	}
	if backend.stops != 1 || len(events.events) != 1 || events.events[0].Type != "Warning" || events.events[0].Reason != "RetirementUnprotected" {
		t.Fatal(backend.stops, events.events)
	}
}
