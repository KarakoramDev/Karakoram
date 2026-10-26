// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Model the real proxy contract: an agent/process can be healthy while native
// readiness waits for an ONLINE writer from the CP-published backend roster.
func proxyFromPublishedBackends(t *testing.T, p *Core, rc *engine.Context, backend *fakeBackend) func() bool {
	t.Helper()
	var mu sync.Mutex
	waitedForWriter := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer demopx" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		config, err := p.MemberConfig(r.Context(), rc, "demopx")
		if err != nil {
			http.Error(w, "proxy configuration unavailable", http.StatusServiceUnavailable)
			return
		}
		var spec pgspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			http.Error(w, "invalid proxy configuration", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: config.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"proxysql": {State: "running", PID: 42}}})
		case "/v1/postgres/status":
			status := pgspec.StatusResponse{Role: "proxy"}
			for _, host := range spec.Proxy.Servers {
				group := 20
				if host == backend.leader {
					group = 10
					status.Running = true
				}
				status.ProxyBackends = append(status.ProxyBackends, pgspec.ProxyBackend{Host: host, Hostgroup: group, Status: "ONLINE"})
			}
			waitedForWriter = waitedForWriter || !status.Running
			json.NewEncoder(w).Encode(status)
		case "/v1/reload":
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := (fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}).Dial(t.Context(), rc.Cluster.Ref(), "demopx")
	if err != nil {
		t.Fatal(err)
	}
	rc.Agents = relayProxyAgents{AgentDialer: rc.Agents, proxy: client}
	return func() bool {
		mu.Lock()
		defer mu.Unlock()
		return waitedForWriter
	}
}

func TestDataJoinPrecedesProxyReadiness(t *testing.T) {
	for _, scenario := range []string{"resume", "bootstrap", "conversion", "runtime-recreation"} {
		t.Run(scenario, func(t *testing.T) {
			initial := "single"
			if scenario == "bootstrap" {
				initial = "ha"
			}
			p, rc, backend := setup(t, initial)
			var s *state
			if scenario == "bootstrap" {
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				s.allocate(rc.Cluster)
				// This is the durable allocation order of a single cluster later
				// grown to HA; do not sort the identity inventory to fix readiness.
				s.Members = []*member{s.find("demop0"), s.find("demopx"), s.find("demop1"), s.find("demop2")}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			} else {
				converge(t, p, rc)
				rc.Cluster.Spec.Topology.Type = "ha"
				rc.Cluster.Spec.Components.Engine.Replicas = 3
				rc.Cluster.Generation++
				if scenario != "conversion" {
					s = converge(t, p, rc)
				}
			}
			volumes := map[string]string{}
			uid := ""
			if scenario == "resume" || scenario == "runtime-recreation" {
				uid = s.UID
				for _, m := range s.data() {
					volumes[m.Name] = m.VolumeID
				}
			}
			if scenario == "resume" {
				done := false
				for range 50 {
					var err error
					done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				if !done {
					t.Fatal("retained-volume suspension did not complete")
				}
				rc.Cluster.Generation++
			}
			if scenario == "runtime-recreation" {
				for _, m := range s.Members {
					delete(backend.services, m.Name)
				}
				rc.Cluster.Generation++
			}
			backend.leader = "demop2"
			waitedForWriter := proxyFromPublishedBackends(t, p, rc, backend)
			finished := converge(t, p, rc)
			var order []string
			for _, m := range finished.Members {
				order = append(order, m.Name)
				if m.Stage != "joined" {
					t.Fatalf("member %s never joined: %s", m.Name, m.Stage)
				}
			}
			if !reflect.DeepEqual(order, []string{"demop0", "demopx", "demop1", "demop2"}) {
				t.Fatalf("readiness changed durable allocation order: %v", order)
			}
			if !waitedForWriter() {
				t.Fatal("proxy never exercised its missing-writer readiness gate")
			}
			config, err := p.MemberConfig(t.Context(), rc, "demopx")
			if err != nil {
				t.Fatal(err)
			}
			var proxy pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &proxy); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(proxy.Proxy.Servers, backend.leader) || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("current primary was not published before resume completion: servers=%v generation=%d/%d", proxy.Proxy.Servers, rc.Cluster.Status.ObservedGeneration, rc.Cluster.Generation)
			}
			if uid != "" {
				if finished.UID != uid {
					t.Fatal("readiness recovery replaced the cluster identity")
				}
				for _, m := range finished.data() {
					if m.VolumeID != volumes[m.Name] {
						t.Fatal("readiness recovery replaced a retained data volume")
					}
				}
			}
		})
	}
}
