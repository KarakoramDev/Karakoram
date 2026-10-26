// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
)

func healingCluster(t *testing.T, replicas int) (*Provider, *engine.Context) {
	t.Helper()
	p, rc, agents, _ := renewalCluster(t, replicas)
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error { return nil }
	s, _ := load(rc.Cluster)
	f := rc.Backend.(*fakeInfra)
	if _, err := reconcileConfig(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		m.ServiceID, m.VolumeID, m.Stage = m.Name, m.Volume, "active"
		f.services[m.Name] = &infra.Service{Hostname: m.Name, ID: m.ServiceID, Kind: infra.KindRuntime, Status: infra.StatusRunning}
		f.services[m.Volume] = &infra.Service{Hostname: m.Volume, ID: m.VolumeID, Kind: infra.KindVolume, Status: infra.StatusActive}
		activateConfig(rc.Cluster, s, m)
		agents[m.Name].version.Store(m.ConfigVersion)
	}
	s.Phase = "updating"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	return p, rc
}

func TestSyncRefusesReadyWithoutPrimary(t *testing.T) {
	p, rc := healingCluster(t, 3)
	rc.Cluster.Status.Phase = domain.PhaseUpdating
	probe := func(_ context.Context, _ *engine.Context, s *internalState, m *member) (mongoadmin.Status, error) {
		rs := mongoadmin.Status{Set: m.Set}
		for _, n := range s.setMembers(m.Set) {
			rs.Members = append(rs.Members, mongoadmin.Member{Name: n.Name + ":27017", State: "SECONDARY", Health: 1, OptimeDate: time.Now()})
		}
		return rs, nil
	}
	if _, err := p.sync(t.Context(), rc, func(context.Context, *engine.Context, *internalState, *member) (bool, error) { return false, nil }, probe); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if s.Phase == "ready" || rc.Cluster.Status.Phase == domain.PhaseReady {
		t.Fatal("secondary-only set declared Ready")
	}
}

func TestFormedLostDataNeverInvokesBootstrap(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	// Persisted set identity survives losing all native state. The replica-set
	// observation fails; no agent bootstrap may be issued for this old identity.
	s.Phase = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	result, err := p.sync(t.Context(), rc, func(context.Context, *engine.Context, *internalState, *member) (bool, error) { return false, nil }, func(context.Context, *engine.Context, *internalState, *member) (mongoadmin.Status, error) {
		return mongoadmin.Status{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Sets["rs0"].Stage != "ready" || s.Phase == "bootstrap" || rc.Cluster.Status.Phase == domain.PhaseReady {
		t.Fatalf("lost data accepted or reinitialized: state=%s result=%+v", rc.Cluster.Internal, result)
	}
}

func TestRouterRunningRequiresObservedMetadataHealth(t *testing.T) {
	st := agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongos": {State: "running"}}}
	if routerHealthy(st) {
		t.Fatal("PID alone declared router ready")
	}
	ready := true
	st.MongoReady = &ready
	if !routerHealthy(st) {
		t.Fatal("observed healthy router rejected")
	}
	ready = false
	if routerHealthy(st) {
		t.Fatal("router without config majority declared ready")
	}
}

func TestManagedUserRepairPrecedesMonitoringCredentialUse(t *testing.T) {
	p, rc := healingCluster(t, 3)
	repaired := false
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error {
		repaired = true
		return nil
	}
	probe := func(context.Context, *engine.Context, *internalState, *member) (mongoadmin.Status, error) {
		if !repaired {
			t.Fatal("monitoring probe blocked the credential repair that it requires")
		}
		return mongoadmin.Status{}, nil
	}
	if _, err := p.sync(t.Context(), rc, func(context.Context, *engine.Context, *internalState, *member) (bool, error) { return false, nil }, probe); err != nil {
		t.Fatal(err)
	}
	if !repaired {
		t.Fatal("ready-state managed user repair was skipped")
	}
}
