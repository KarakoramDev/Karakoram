// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type legacyNativeDialer map[string]*replacementMongo

func (d legacyNativeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d[address].DialContext(ctx, network, address)
}

func TestLegacyShardedAgentsConvergeSyncAndStatusWithoutRollout(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
	rc.Cluster.Spec.TLS.Mode = "disabled"
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Components.Router.Replicas = 2
	s, _ := load(rc.Cluster)
	s.UID = "legacy-sharded"
	s.Phase = "ready"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
		m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
		m.Disk = component(rc.Cluster.Spec, m).Resources
		m.Containers = 1
		if m.Role == agentapi.RoleRouter {
			m.Containers = 2
		}
		m.PublicHash = hash(rc.Cluster.Spec.Exposure)
	}
	for _, set := range s.Sets {
		set.Stage = "ready"
		set.Added = true
	}
	seedFormedServices(t, p, rc, f, s)
	if _, err := reconcileConfig(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		activateConfig(rc.Cluster, s, m)
	}
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error { return nil }
	native := legacyNativeDialer{}
	for set := range s.Sets {
		server := nativeHealingFixture(t, rc, s)
		members := s.setMembers(set)
		server.config = mongoadmin.Config{ID: set, Version: 1}
		observed, _ := healthyTestReplication(t.Context(), rc, s, members[0])
		server.observed = &observed
		primary := members[0].Name + ":27017"
		server.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "setName": set, "hosts": bson.A{primary}, "primary": primary, "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
		server.command = func(command bson.Raw) bson.M {
			return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": major(rc.Cluster.Spec.Version)}}
		}
		for _, m := range members {
			native[m.Name+":27017"] = server
		}
	}
	rc.Dialer = native
	var partition atomic.Bool
	var reads atomic.Int64
	routerA := nativeHealingFixture(t, rc, s)
	routerA.command = func(command bson.Raw) bson.M { reads.Add(1); return bson.M{"ok": 1} }
	routerB := nativeHealingFixture(t, rc, s)
	routerB.command = func(command bson.Raw) bson.M {
		reads.Add(1)
		if partition.Load() {
			return bson.M{"ok": 0, "code": 64, "errmsg": "config majority unavailable"}
		}
		return bson.M{"ok": 1}
	}
	rc.Dialer = native
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: rc.Cluster.Spec.Version, ConfigVersion: 999, HostFingerprint: "retained-host", Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "mongos": {State: "running"}}})
	})
	a := httptest.NewTLSServer(handler)
	defer a.Close()
	b := httptest.NewUnstartedServer(handler)
	b.TLS = a.TLS.Clone()
	b.StartTLS()
	defer b.Close()
	rc.Backend = &routerInventory{fakeInfra: f, instances: []infra.Container{{ID: "a", Hostname: "router-a", Status: infra.ContainerRunning}, {ID: "b", Hostname: "router-b", Status: infra.ContainerRunning}}}
	rc.Agents = routerInstanceDialer{servers: map[string]*httptest.Server{"router-a": a, "router-b": b}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw}), native: map[string]*replacementMongo{"router-a": routerA, "router-b": routerB}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before := len(f.deployments)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	current, _ := load(rc.Cluster)
	if current.Phase != "ready" || len(f.deployments) != before {
		t.Fatal("legacy agents require a rollout", string(rc.Cluster.Internal))
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || reads.Load() < 4 {
		t.Fatal("legacy full status did not converge", status, err, reads.Load())
	}
	partition.Store(true)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded {
		t.Fatal("legacy partition did not degrade full status", status, err)
	}
	for _, m := range status.Members {
		if m.Role == "router" && m.Health {
			t.Fatal("partitioned router declared healthy")
		}
	}
	if len(f.deployments) != before {
		t.Fatal("legacy readiness forced a redeploy")
	}
}
