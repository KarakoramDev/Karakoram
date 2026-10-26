// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type growthRecoveryBackend struct{ *fakeBackend }

func (b growthRecoveryBackend) Capabilities() infra.Capabilities {
	c := b.fakeBackend.Capabilities()
	c.DiskGrowth = true
	return c
}

func TestStoppedDatabaseAllowsVolumeGrowth(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setup(t, topology)
			s := converge(t, p, rc)
			rc.Backend = growthRecoveryBackend{b}
			target := s.data()[len(s.data())-1]
			rc.Cluster.Spec.Components.Engine.Resources.MinDisk = target.Disk.MinDisk + 10
			rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = target.Disk.MaxDisk + 10
			rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: s.data()[0].Name}
			b.actions = nil
			if _, err := p.Sync(context.Background(), rc); err != nil {
				t.Fatal(err)
			}
			if len(b.actions) != 1 || b.actions[0] != "resize:"+s.data()[0].Volume {
				t.Fatalf("growth blocked by stopped SQL: %v", b.actions)
			}
			current, _ := load(rc.Cluster)
			if current.Pending == nil || current.Pending.Action != "resize-volume" {
				t.Fatal("growth has no durable ownership")
			}
		})
	}
}

func TestMaintenanceAllowsFailedRuntimeResourceCorrection(t *testing.T) {
	for _, journal := range []string{"rolling", "upgrade"} {
		t.Run(journal, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			target := s.data()[1]
			if journal == "rolling" {
				s.Rolling = &rollingState{}
			} else {
				s.Upgrade = &upgradeState{}
			}
			previous := target.Resources
			wanted := previous
			wanted.MinRAM += 1
			wanted.MaxRAM += 1
			rc.Cluster.Spec.Components.Engine.Resources = wanted
			b.services[target.Name].Status = infra.StatusFailed
			b.services[target.Name].Resources = previous
			rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: target.Name}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b.actions = nil
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(b.actions, []string{"resize:" + target.Name}) {
				t.Fatalf("maintenance blocked failed resource correction: %v", b.actions)
			}
			current, _ := load(rc.Cluster)
			if (journal == "rolling" && current.Rolling == nil) || (journal == "upgrade" && current.Upgrade == nil) {
				t.Fatal("maintenance ownership was cleared")
			}
			if current.Pending == nil || current.Pending.Resources != (domain.Resources(wanted)) {
				t.Fatal("resource receipt missing")
			}
		})
	}
}

func TestMaintenanceCorrectsStoppedReplicaOnRunningRuntime(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	target := s.data()[1]
	s.Rolling = &rollingState{Phases: []rollingPhase{{Direction: "increase", Options: []byte("{}")}}, Version: s.ConfigVersion}
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM += 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM += 1
	b.statusOverrides = map[string]pgspec.StatusResponse{target.Name: {Role: "replica", Running: false}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.actions = nil
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.actions, []string{"resize:" + target.Name}) {
		t.Fatalf("stopped replica cannot correct memory while runtime lives: %v", b.actions)
	}
	current, _ := load(rc.Cluster)
	if current.Rolling == nil {
		t.Fatal("resource correction discarded the directional journal")
	}
}

func TestUpgradeReplicaFailureDoesNotStrandLaterReplicas(t *testing.T) {
	p, rc, b := upgradeFenceFixture(t)
	s, _ := load(rc.Cluster)
	u := s.Upgrade
	u.Stage, u.Index, u.Waiting = "replicas", 1, true
	s.find(u.Primary).Version = u.To
	first, later := s.data()[1], s.data()[2]
	first.Version, first.Offline, first.Stage = u.To, false, "deployed"
	later.Offline = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: first.Name}
	if _, err := p.reconcileUpgrade(t.Context(), rc, s); err != nil {
		t.Fatal("failed first replica blocked independent rebuild", err)
	}
	if later.Version != u.To || later.Offline {
		t.Fatal("healthy later replica remained fenced on old major")
	}
	if u.Stage != "replicas" {
		t.Fatal("upgrade completed without the unavailable replica")
	}
	want := "/v1/postgres/upgrade-run:" + later.Name
	if !reflect.DeepEqual(b.actions, []string{want}) {
		t.Fatalf("unexpected rebuild actions: %v", b.actions)
	}
}

func TestMaintenanceDoesNotResizeStoppedMemberWithoutWriterProof(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	s.Rolling = &rollingState{Phases: []rollingPhase{{Direction: "increase", Options: []byte("{}")}}, Version: s.ConfigVersion}
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM += 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM += 1
	b.statusOverrides = map[string]pgspec.StatusResponse{}
	for _, m := range s.data() {
		b.statusOverrides[m.Name] = pgspec.StatusResponse{Running: false}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.actions = nil
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(b.actions) != 0 {
		t.Fatalf("capacity correction bypassed missing writer proof: %v", b.actions)
	}
}

func TestUpgradeReplicaRebuildRequiresNewMajorWriter(t *testing.T) {
	for _, native := range []pgspec.StatusResponse{{Role: "starting", Running: false}, {Role: "primary", Running: true, Version: "16.15.1"}} {
		p, rc, b := upgradeFenceFixture(t)
		s, _ := load(rc.Cluster)
		s.Upgrade.Stage = "replicas"
		s.find(s.Upgrade.Primary).Version = s.Upgrade.To
		b.statusOverrides = map[string]pgspec.StatusResponse{s.Upgrade.Primary: native}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if _, err := p.reconcileUpgrade(t.Context(), rc, s); err == nil {
			t.Fatal("unsafe replica rebuild accepted without an upgraded writer")
		}
		if len(b.actions) != 0 {
			t.Fatalf("old replica data touched without upgraded writer: %v", b.actions)
		}
	}
}
