// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type coldReadinessAgents struct{ engine.AgentDialer }

func (d coldReadinessAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	// Account for a 1.5s cold namespace/TLS establishment and 1s agent
	// response before returning the otherwise healthy fixture client.
	timer := time.NewTimer(2500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return d.AgentDialer.Dial(ctx, ref, name)
	}
}

func TestObservationColdAgentAndSlowPlatformFitsSweep(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	rc.Clock = engine.RealClock{}
	rc.Agents = coldReadinessAgents{rc.Agents}
	rc.Backend = &delayedStatusBackend{Backend: rc.Backend}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatalf("healthy cold MongoDB member lost fresh readiness: %+v, %v", status, err)
	}
}

type readinessAgentDialer struct {
	base         engine.AgentDialer
	failed       map[string]bool
	hung         bool
	transient    atomic.Int32
	peerObserved atomic.Bool
}

func (d *readinessAgentDialer) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if d.failed[name] {
		if d.hung {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, errors.New("runtime unreachable")
	}
	if name == "demom0" && d.transient.CompareAndSwap(1, 0) {
		return nil, errors.New("single transient read failure")
	}
	d.peerObserved.Store(true)
	return d.base.Dial(ctx, ref, name)
}

func writableReadinessFixture(t *testing.T) (*Provider, *engine.Context, *replacementMongo) {
	t.Helper()
	p, rc := healingCluster(t, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.TLSMode, s.Phase = "disabled", "ready"
	for _, set := range s.Sets {
		set.Stage = "ready"
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.command = func(command bson.Raw) bson.M {
		return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
	}
	return p, rc, server
}

func assertNotWritableReady(t *testing.T, status domain.ClusterStatus) {
	t.Helper()
	if status.Phase == domain.PhaseReady {
		t.Fatal("unreachable replica set remained Ready")
	}
	for _, condition := range status.Conditions {
		if condition.Type == "Healthy" && condition.Status == "True" || condition.Reason == "WritablePrimary" && condition.Status == "True" {
			t.Fatal("stale positive readiness survived", condition)
		}
	}
}

func TestReadinessWithdrawsPrimaryWithStalePeerView(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	baseline, err := p.Status(t.Context(), rc)
	if err != nil || baseline.Phase != domain.PhaseReady {
		t.Fatal("fixture was not writable and healthy", baseline, err)
	}
	// The wire fixture deliberately keeps advertising m1 as PRIMARY and even
	// returns a successful hello. Independent member reachability must veto it.
	rc.Agents = &readinessAgentDialer{base: rc.Agents, failed: map[string]bool{"demom0": true, "demom1": true}}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	assertNotWritableReady(t, status)
	for _, member := range status.Members {
		if member.Name == "demom2" && (!member.Agent.Healthy || member.Agent.LastSeen.IsZero()) {
			t.Fatal("surviving secondary was hidden", member)
		}
	}
}

func TestReadinessHungMembersDoNotSerializePeerObservation(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	dialer := &readinessAgentDialer{base: rc.Agents, failed: map[string]bool{"demom0": true, "demom1": true}, hung: true}
	rc.Agents = dialer
	start := time.Now()
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= 2*engine.ObservationTimeout+time.Second {
		t.Fatalf("two unavailable members serialized timeout budgets: %s", elapsed)
	}
	if !dialer.peerObserved.Load() {
		t.Fatal("healthy peer was not probed")
	}
	assertNotWritableReady(t, status)
}

func TestReadinessSingleTransientProbeDoesNotFlap(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	dialer := &readinessAgentDialer{base: rc.Agents}
	dialer.transient.Store(1)
	rc.Agents = dialer
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() || dialer.transient.Load() != 0 {
		t.Fatal("one failed probe was not absorbed by the fresh retry", status, err)
	}
}

func TestReadinessRequiresFreshWritableHello(t *testing.T) {
	p, rc, server := writableReadinessFixture(t)
	server.hello = bson.M{"ok": 1, "isWritablePrimary": false}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	assertNotWritableReady(t, status)
	for _, condition := range status.Conditions {
		if condition.Type == "Replication/rs0" && condition.Reason == "NoWritablePrimary" && condition.Status == "False" {
			return
		}
	}
	t.Fatal("readiness was not withdrawn by the authenticated writable hello", status.Conditions)
}

type delayedStatusBackend struct {
	infra.Backend
	containers atomic.Int32
	inventory  atomic.Int32
}

func waitStatusBackend(ctx context.Context) error {
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *delayedStatusBackend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	if err := waitStatusBackend(ctx); err != nil {
		return infra.Project{}, err
	}
	return b.Backend.ProjectInfo(ctx)
}

func (b *delayedStatusBackend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	b.inventory.Add(1)
	if err := waitStatusBackend(ctx); err != nil {
		return nil, err
	}
	return b.Backend.ListServices(ctx, prefix)
}

func (b *delayedStatusBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	b.containers.Add(1)
	if err := waitStatusBackend(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Containers(ctx, name)
}

func TestObservationSlowPlatformInventoryFitsSweep(t *testing.T) {
	p, rc := healingCluster(t, 5)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.TLSMode, s.Phase = "disabled", "ready"
	for _, set := range s.Sets {
		set.Stage = "ready"
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.command = func(bson.Raw) bson.M {
		return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
	}
	backend := &delayedStatusBackend{Backend: rc.Backend}
	rc.Backend = backend
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy slow inventory exhausted the sweep", status, err)
	}
	if backend.inventory.Load() != 1 || backend.containers.Load() != 5 {
		t.Fatal("platform inventory was duplicated", backend.inventory.Load(), backend.containers.Load())
	}
}

func TestObservationFCVFallsBackAcrossSortedReplicaSets(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.TLSMode, s.Phase = "disabled", "ready"
	s.Sets = map[string]*setState{"a": {Stage: "ready"}, "b": {Stage: "ready"}}
	s.Members[0].Set = "a"
	for _, m := range s.Members[1:] {
		m.Set = "b"
	}
	var attempts [2]atomic.Int32
	native := legacyNativeDialer{}
	for i, set := range []string{"a", "b"} {
		server := nativeHealingFixture(t, rc, s)
		members := s.setMembers(set)
		observed, err := healthyTestReplication(t.Context(), rc, s, members[0])
		if err != nil {
			t.Fatal(err)
		}
		server.observed = &observed
		primary := members[0].Name + ":27017"
		server.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "setName": set, "hosts": bson.A{primary}, "primary": primary, "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
		server.command = func(command bson.Raw) bson.M {
			if command.Lookup("getParameter").Type != 0 {
				attempts[i].Add(1)
				if i == 0 {
					return bson.M{"ok": 0, "code": 13, "errmsg": "FCV observation unavailable"}
				}
				return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
			}
			return bson.M{"ok": 1}
		}
		for _, m := range members {
			native[m.Name+":27017"] = server
		}
	}
	rc.Dialer = native
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.FCV != "8.0" || attempts[0].Load() != 1 || attempts[1].Load() != 1 {
		t.Fatal("FCV did not fall back from the first sorted set", status.FCV, attempts[0].Load(), attempts[1].Load(), err)
	}
}
