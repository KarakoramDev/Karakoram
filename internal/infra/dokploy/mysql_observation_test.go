// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Retain the relay's per-connection inventory authorization, including separate
// metadata/native transports. Only the tunnel and healthy native SQL are fake.
type mysqlObservationAgents struct {
	backend        *Backend
	endpoint       string
	ca             []byte
	authorizations atomic.Int32
}

func (a *mysqlObservationAgents) Dial(_ context.Context, _ model.ClusterRef, member string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(a.endpoint)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}
	return agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: a.endpoint, Token: member, CAPEM: a.ca, DialContext: dial,
		DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
			containers, err := a.backend.Containers(infra.WithFreshTaskInventory(ctx), member)
			if err != nil {
				return nil, err
			}
			for _, c := range containers {
				if c.Hostname == instance && c.Status == infra.ContainerRunning {
					a.authorizations.Add(1)
					return dial(ctx, network, logical)
				}
			}
			return nil, fmt.Errorf("instance is not in current inventory")
		},
	})
}

func TestMySQLGroupObservationRequestBudget(t *testing.T) {
	testMySQLGroupObservation(t, 3, 0)
}

func TestMySQLGroupObservationWithThreeProxyTasksAndSlowPlatform(t *testing.T) {
	testMySQLGroupObservation(t, 3, 300*time.Millisecond)
}

func TestMySQLGroupObservationWithMeasuredInventoryLatency(t *testing.T) {
	// Sequential authenticated inventory GETs measured 147–723ms. Exercise
	// the representative latency and rounded-up tail for both proxy fleets.
	for _, proxyTasks := range []int{2, 3} {
		for _, latency := range []time.Duration{400 * time.Millisecond, 750 * time.Millisecond} {
			t.Run(fmt.Sprintf("proxies%d/%s", proxyTasks, latency), func(t *testing.T) {
				testMySQLGroupObservation(t, proxyTasks, latency)
			})
		}
	}
}

func testMySQLGroupObservation(t *testing.T, proxyTasks int, latency time.Duration) {
	t.Helper()
	f := newFakeAPI(t)
	var getCount atomic.Int64
	f.requestHook = func(_ http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.Method == http.MethodGet {
			getCount.Add(1)
		}
		return false, nil
	}
	b := fakeBackend(t, f, nil)
	provider := mysql.New(mysql.Options{Provider: "mysql"})
	c := &model.Cluster{Name: "sample", Generation: 2, Spec: model.ClusterSpec{Provider: "mysql", Topology: model.Topology{Type: "cluster"}}}
	provider.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Replicas = 3
	proxy := c.Spec.Components.Extra["proxy"]
	proxy.Replicas = proxyTasks
	c.Spec.Components.Extra["proxy"] = proxy
	members := make([]map[string]any, 0, 4)
	for i := range 4 {
		name, role, volume, replicas := fmt.Sprintf("samplem%d", i), "data", fmt.Sprintf("samplem%dv", i), 1
		if i == 3 {
			name, role, volume, replicas = "samplemx", "proxy", "", proxyTasks
		}
		spec := runtimeSpec(name, volume)
		spec.MinContainers, spec.MaxContainers = replicas, replicas
		var specs []infra.ServiceSpec
		if volume != "" {
			specs = append(specs, volumeSpec(volume))
		}
		specs = append(specs, spec)
		imported := importFake(t, b, specs...)
		deployFake(t, b, name, volume)
		members = append(members, map[string]any{
			"name": name, "role": role, "index": i, "stage": "joined", "serviceId": imported.Services[name],
			"replicas": replicas, "configVersion": 1, "version": c.Spec.Version,
		})
	}
	var err error
	c.Internal, err = json.Marshal(map[string]any{"mysql": map[string]any{
		"uid": "sample-identity", "configVersion": 1, "topology": "cluster", "members": members,
		"formation": map[string]any{"state": "formed", "formationEpoch": 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
		case "/v1/mysql/status":
			_ = json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY", GroupMembers: 3})
		case "/v1/mysql/proxy-status":
			_ = json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: 10, Status: "ONLINE"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	setupGETs := getCount.Load()
	agents := &mysqlObservationAgents{backend: b, endpoint: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	f.mu.Lock()
	before := make(map[string]int, len(f.calls))
	for procedure, count := range f.calls {
		before[procedure] = count
	}
	// Every individual read is responsive; the pass must account for the
	// complete dependency graph, including fresh per-transport authorization.
	f.latency = latency
	f.mu.Unlock()
	rc := &engine.Context{Cluster: c, Backend: b, Agents: agents, Clock: engine.RealClock{}, ObservationEpoch: "test-process"}
	started := time.Now()
	status, err := engine.ObserveStatus(engine.WithObservationPass(t.Context()), provider, rc)
	if err != nil || status.Phase != model.PhaseReady {
		t.Fatalf("healthy group/proxies failed bounded observation after %s: phase=%s err=%v", time.Since(started), status.Phase, err)
	}
	t.Logf("healthy sweep completed in %s with unchanged four-second read budget", time.Since(started))
	want := map[string]int{
		"environment.one": 1, "application.one": 7, "cluster.getNodes": 1,
		"swarm.getNodeApps": 1, "docker.getConfig": 4, "deployment.all": 4,
		"docker.getServiceContainersByAppName": 4 + 2*proxyTasks,
	}
	f.mu.Lock()
	for procedure, count := range f.calls {
		if delta := count - before[procedure]; delta != 0 {
			t.Logf("platform procedure %s: %d reads", procedure, delta)
			if delta != want[procedure] {
				t.Errorf("platform request budget %s: got %d, want %d", procedure, delta, want[procedure])
			}
			delete(want, procedure)
		}
	}
	f.mu.Unlock()
	for procedure, count := range want {
		t.Errorf("platform request budget %s: got 0, want %d", procedure, count)
	}
	if agents.authorizations.Load() != int32(2*proxyTasks) {
		t.Fatalf("expected fresh authorization for both transports of all %d proxies, got %d", proxyTasks, agents.authorizations.Load())
	}
	for _, m := range status.Members {
		if !m.Health || !m.Agent.Healthy {
			t.Fatalf("positive member proof missing: %s", m.Name)
		}
	}
	if latency == 0 {
		// Replay twelve independent five-second observation slots, without
		// sleeping: this is cadence-normalized status work, not a wall-clock
		// Manager/reconcile benchmark. Mutation reads remain outside snapshots.
		t.Logf("create/import/deploy fixture GETs: %d", setupGETs)
		measureMinute := func(label string, tasks int) {
			t.Helper()
			before := getCount.Load()
			for range 12 {
				passStart := getCount.Load()
				status, err := engine.ObserveStatus(engine.WithObservationPass(t.Context()), provider, rc)
				if err != nil || status.Phase != model.PhaseReady {
					t.Fatalf("%s observation: phase=%s err=%v", label, status.Phase, err)
				}
				// The per-pass bound includes both pinned transports per proxy;
				// assert every slot independently, including the scaled fleet.
				if got, max := getCount.Load()-passStart, int64(22+2*tasks); got > max {
					t.Errorf("%s pass GETs=%d exceeds %d", label, got, max)
				}
				rc.Cluster.Status = status
			}
			t.Logf("%s GETs per twelve status passes: %d", label, getCount.Load()-before)
		}
		measureMinute("idle", proxyTasks)
		beforeCount := getCount.Load()
		op, err := b.SetContainerCount(t.Context(), "samplemx", proxyTasks+1, proxyTasks+1)
		if err != nil {
			t.Fatal(err)
		}
		awaitFake(t, b, op)
		t.Logf("proxy count-change operation GETs: %d", getCount.Load()-beforeCount)
		measureMinute("after proxy count change", proxyTasks+1)
	}
	// No cross-pass inventory or positive health may survive a stopped runtime.
	f.mu.Lock()
	f.latency = 0
	f.mu.Unlock()
	op, err := b.Stop(t.Context(), "samplem0")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	status, _ = engine.ObserveStatus(engine.WithObservationPass(t.Context()), provider, rc)
	if status.Phase == model.PhaseReady {
		t.Fatal("stopped member retained positive availability")
	}
}
