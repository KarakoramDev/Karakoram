// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type routerInventory struct {
	*fakeInfra
	instances []infra.Container
}

func (r *routerInventory) Containers(context.Context, string) ([]infra.Container, error) {
	return r.instances, nil
}

type routerInstanceDialer struct {
	servers map[string]*httptest.Server
	ca      []byte
	native  map[string]*replacementMongo
}

func (d routerInstanceDialer) Dial(_ context.Context, _ domain.ClusterRef, member string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.servers["router-a"].URL, Token: member, CAPEM: d.ca, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if host == "127.0.0.1" {
			host = "router-a"
		}
		server := d.servers[host]
		if server == nil {
			return nil, errors.New("instance unreachable")
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}, DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
		_, port, err := net.SplitHostPort(logical)
		if err != nil {
			return nil, err
		}
		if port == "27017" && d.native != nil {
			if server := d.native[instance]; server != nil {
				return server.DialContext(ctx, network, logical)
			}
			return nil, errors.New("native instance unreachable")
		}
		server := d.servers[instance]
		if server == nil {
			return nil, errors.New("agent instance unreachable")
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}})
}

func TestRouterReadinessRequiresEveryInstanceMetadataAndConfig(t *testing.T) {
	_, rc, f, _ := setup(t)
	var ready atomic.Bool
	ready.Store(false)
	var version atomic.Int64
	version.Store(7)
	handler := func(second bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer demor" {
				t.Error("logical authentication lost")
			}
			healthy := !second || ready.Load()
			v := int64(7)
			if second {
				v = version.Load()
			}
			status := agentapi.Status{ConfigVersion: v, Processes: map[string]agentapi.ProcessStatus{"mongos": {State: "running"}}, MongoReady: &healthy}
			if !healthy {
				status.MongoError = "config-server majority unavailable"
			}
			_ = json.NewEncoder(w).Encode(status)
		}
	}
	a := httptest.NewTLSServer(handler(false))
	defer a.Close()
	b := httptest.NewUnstartedServer(handler(true))
	b.TLS = a.TLS.Clone()
	b.StartTLS()
	defer b.Close()
	rc.Backend = &routerInventory{fakeInfra: f, instances: []infra.Container{{ID: "instance-a", Hostname: "router-a", Status: infra.ContainerRunning}, {ID: "instance-b", Hostname: "router-b", Status: infra.ContainerRunning}}}
	rc.Agents = routerInstanceDialer{servers: map[string]*httptest.Server{"router-a": a, "router-b": b}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw})}
	m := &member{Name: "demor", Role: agentapi.RoleRouter, Containers: 2}
	ok, _, err := routerInstancesHealthy(t.Context(), rc, m, 7)
	if ok || err == nil || !strings.Contains(err.Error(), "instance-b") || !strings.Contains(err.Error(), "config-server majority") {
		t.Fatalf("healthy logical DNS masked failed router: %v %v", ok, err)
	}
	ready.Store(true)
	version.Store(6)
	if ok, _, err = routerInstancesHealthy(t.Context(), rc, m, 7); ok || err == nil {
		t.Fatalf("stale router accepted: %v %v", ok, err)
	}
	version.Store(7)
	if ok, _, err = routerInstancesHealthy(t.Context(), rc, m, 7); !ok || err != nil {
		t.Fatalf("recovered routers rejected: %v %v", ok, err)
	}
}

func TestLegacyRouterMetadataProbePinsEveryInstance(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	s.UID, s.TLSMode = "legacy-router", "disabled"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var partition, modern, ready atomic.Bool
	ready.Store(true)
	var nativeReads atomic.Int64
	nativeA := nativeHealingFixture(t, rc, s)
	nativeA.command = func(command bson.Raw) bson.M {
		if command.Lookup("find").StringValue() != "shards" || command.Lookup("$db").StringValue() != "config" || command.Lookup("readConcern").Document().Lookup("level").StringValue() != "majority" {
			t.Error("legacy probe did not use config majority metadata")
		}
		elements, err := command.Elements()
		if err != nil {
			t.Error(err)
			return bson.M{"ok": 0, "code": 2, "errmsg": "invalid BSON"}
		}
		seen := make(map[string]bool, len(elements))
		for _, element := range elements {
			if seen[element.Key()] {
				t.Errorf("duplicate metadata wire field %s", element.Key())
				return bson.M{"ok": 0, "code": 40413, "codeName": "IDLDuplicateField", "errmsg": "duplicate field"}
			}
			seen[element.Key()] = true
		}
		maxTime, ok := command.Lookup("maxTimeMS").AsInt64OK()
		if !ok || maxTime <= 0 || maxTime > 2000 {
			t.Errorf("metadata wire maxTimeMS outside command budget: %v", command.Lookup("maxTimeMS"))
		}
		nativeReads.Add(1)
		return bson.M{"ok": 1}
	}
	nativeB := nativeHealingFixture(t, rc, s)
	nativeB.command = func(command bson.Raw) bson.M {
		nativeReads.Add(1)
		if partition.Load() {
			return bson.M{"ok": 0, "code": 64, "errmsg": "config majority unavailable"}
		}
		return bson.M{"ok": 1}
	}
	handler := func(second bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			status := agentapi.Status{ConfigVersion: 7, Processes: map[string]agentapi.ProcessStatus{"mongos": {State: "running"}}}
			if second && modern.Load() {
				healthy := ready.Load()
				status.MongoReady = &healthy
			}
			if err := json.NewEncoder(w).Encode(status); err != nil {
				t.Error(err)
			}
		}
	}
	a := httptest.NewTLSServer(handler(false))
	defer a.Close()
	b := httptest.NewUnstartedServer(handler(true))
	b.TLS = a.TLS.Clone()
	b.StartTLS()
	defer b.Close()
	rc.Backend = &routerInventory{fakeInfra: f, instances: []infra.Container{{ID: "a", Hostname: "router-a", Status: infra.ContainerRunning}, {ID: "b", Hostname: "router-b", Status: infra.ContainerRunning}}}
	rc.Agents = routerInstanceDialer{servers: map[string]*httptest.Server{"router-a": a, "router-b": b}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw}), native: map[string]*replacementMongo{"router-a": nativeA, "router-b": nativeB}}
	m := &member{Name: "demor", Role: agentapi.RoleRouter, Containers: 2}
	if ok, _, err := routerInstancesHealthy(t.Context(), rc, m, 7); !ok || err != nil || nativeReads.Load() != 2 {
		t.Fatal("legacy healthy routers did not converge", ok, err, nativeReads.Load())
	}
	partition.Store(true)
	if ok, _, err := routerInstancesHealthy(t.Context(), rc, m, 7); ok || err == nil || !strings.Contains(err.Error(), "instance b") {
		t.Fatal("legacy partition masked by other router", ok, err)
	}
	modern.Store(true)
	ready.Store(false)
	partition.Store(false)
	before := nativeReads.Load()
	if ok, _, err := routerInstancesHealthy(t.Context(), rc, m, 7); ok || err == nil {
		t.Fatal("negative modern observation ignored", ok, err)
	}
	if nativeReads.Load() != before+1 {
		t.Fatal("negative modern observation replaced with legacy probe")
	}
	ready.Store(true)
	if ok, _, err := routerInstancesHealthy(t.Context(), rc, m, 7); !ok || err != nil {
		t.Fatal("mixed-version routers failed to recover", ok, err)
	}
}

func TestStatusPausedRouterInstanceCannotHideBehindHealthyLogicalEndpoint(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Components.Router.Replicas = 2
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "mixed-paused"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var paused atomic.Bool
	paused.Store(true)
	handler := func(second bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ready := true
			status := agentapi.Status{ConfigVersion: 100, MongoReady: &ready, Processes: map[string]agentapi.ProcessStatus{
				"mongos": {State: "running"}, "mongod": {State: "running"},
			}}
			if second && paused.Load() {
				ready = false
				status.Processes["mongos"] = agentapi.ProcessStatus{State: "paused", Error: "investigate before SIGCONT"}
				status.MongoError = "investigate before SIGCONT"
			}
			_ = json.NewEncoder(w).Encode(status)
		}
	}
	a := httptest.NewTLSServer(handler(false))
	defer a.Close()
	b := httptest.NewUnstartedServer(handler(true))
	b.TLS = a.TLS.Clone()
	b.StartTLS()
	defer b.Close()
	rc.Backend = &routerInventory{fakeInfra: f, instances: []infra.Container{
		{ID: "instance-a", Hostname: "router-a", Status: infra.ContainerRunning},
		{ID: "instance-b", Hostname: "router-b", Status: infra.ContainerRunning},
	}}
	rc.Agents = routerInstanceDialer{servers: map[string]*httptest.Server{"router-a": a, "router-b": b}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw})}
	check := func(wantPaused bool) {
		t.Helper()
		got, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, member := range got.Members {
			if member.Role != "router" {
				continue
			}
			found = true
			if !member.Agent.Healthy || (member.State == "paused") != wantPaused || member.Health == wantPaused {
				t.Fatalf("mixed-instance health/connection mismatch: %+v", member)
			}
			conditionFound := false
			for _, condition := range got.Conditions {
				if condition.Type == "Process/"+member.Name+"/mongos/instance-b" {
					conditionFound = condition.Reason == "ProcessPaused" && strings.Contains(condition.Message, "SIGCONT") && strings.Contains(condition.Message, "instance-b")
				}
			}
			if conditionFound != wantPaused {
				t.Fatalf("mixed-instance diagnostic missing/stale: %+v", got.Conditions)
			}
		}
		if !found {
			t.Fatal("router missing")
		}
	}
	check(true)
	paused.Store(false)
	check(false)
}
