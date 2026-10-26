// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type proxyClockTestRuntime struct {
	mu          sync.Mutex
	native      pgspec.StatusResponse
	maintenance bool
	stopped     bool
	loseReply   bool
	nativeError bool
	statusError bool
	requests    []string
	restarted   map[string]bool
}

func proxyClockFixture(t *testing.T) (*Core, *engine.Context, *fakeBackend, *state, *backupCleanupClock, *proxyClockTestRuntime) {
	t.Helper()
	p, rc, backend := setup(t, "ha")
	s := converge(t, p, rc)
	clock := &backupCleanupClock{at: time.Now().UTC().Add(time.Hour)}
	rc.Clock = clock
	runtime := &proxyClockTestRuntime{restarted: map[string]bool{}, native: pgspec.StatusResponse{
		Role: "proxy", ProxyBackends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 20, Status: "SHUNNED", Diagnostics: &pgspec.ProxyDiagnostics{
			Connect: &pgspec.ProxyMonitorError{Class: "refused", TimeUnixMicro: clock.at.Add(8 * time.Hour).UnixMicro()},
		}}},
	}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer demopx" {
			t.Error("instance request lost logical proxy identity")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			if runtime.statusError {
				http.Error(w, "generic observation unavailable", http.StatusServiceUnavailable)
				return
			}
			state := "running"
			if runtime.stopped {
				state = "stopped"
			}
			// Deliberately no new capabilities: model the existing generic API.
			json.NewEncoder(w).Encode(agentapi.Status{Maintenance: runtime.maintenance, Processes: map[string]agentapi.ProcessStatus{"proxysql": {State: state, PID: 42}}})
		case "/v1/postgres/status":
			if runtime.nativeError {
				http.Error(w, "native observation unavailable", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(runtime.native)
		case "/v1/processes/proxysql/restart":
			var request agentapi.ProcessAction
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			id := r.Header.Get("Idempotency-Key")
			if id == "" || id != request.RequestID || request.Process != "proxysql" || request.Action != "restart" {
				t.Error("proxy reset did not use the existing identified restart protocol")
			}
			runtime.requests = append(runtime.requests, id)
			runtime.restarted[id] = true // Native agent restart receipts deduplicate retries.
			if runtime.loseReply {
				runtime.loseReply = false
				http.Error(w, "restart response lost", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	proxy, err := (fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}).Dial(t.Context(), rc.Cluster.Ref(), "demopx")
	if err != nil {
		t.Fatal(err)
	}
	rc.Agents = relayProxyAgents{AgentDialer: rc.Agents, proxy: proxy}
	return p, rc, backend, s, clock, runtime
}

func persistAndReloadProxyClock(t *testing.T, rc *engine.Context, s *state) *state {
	t.Helper()
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster = cluster
	restored, err := load(cluster)
	if err != nil {
		t.Fatal(err)
	}
	return restored
}

func TestProxyClockRecoveryDurableAndBounded(t *testing.T) {
	p, rc, _, s, clock, runtime := proxyClockFixture(t)
	events := &deploymentGuardEvents{}
	rc.Events = events
	if handled, err := p.recoverProxyClock(t.Context(), rc, s); err != nil || !handled {
		t.Fatal("future monitor clock was not handled", err)
	}
	if len(runtime.requests) != 0 {
		t.Fatal("restart happened before durable intent could be stored")
	}
	receipt := s.find("demopx").ProxyClockRecovery["demopx-instance"]
	if receipt == nil || receipt.RequestID == "" || receipt.Completed || !strings.Contains(receipt.Message, "restarting proxy to clear it") {
		t.Fatal("missing truthful identified recovery intent")
	}
	if len(events.events) != 1 || events.events[0].Reason != "ProxyMonitorClockAhead" {
		t.Fatal("missing clock-discontinuity event")
	}
	requestID := receipt.RequestID
	s = persistAndReloadProxyClock(t, rc, s) // CP restarts before submitting.
	runtime.loseReply = true
	if handled, err := p.recoverProxyClock(t.Context(), rc, s); err == nil || !handled {
		t.Fatal("lost restart response was not retained as retryable work")
	}
	s = persistAndReloadProxyClock(t, rc, s) // CP restarts after the lost reply.
	if handled, err := p.recoverProxyClock(t.Context(), rc, s); err != nil || !handled {
		t.Fatal("identified restart did not resume", err)
	}
	if len(runtime.requests) != 2 || runtime.requests[0] != requestID || runtime.requests[1] != requestID || len(runtime.restarted) != 1 {
		t.Fatal("CP restart or lost response changed the native restart receipt")
	}
	s = persistAndReloadProxyClock(t, rc, s)
	for range 3 {
		if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
	}
	clock.at = clock.at.Add(proxyClockCooldown + time.Minute)
	if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(runtime.requests) != 2 {
		t.Fatal("unchanged future evidence caused a restart loop after cooldown")
	}
	condition := proxyClockCondition(rc, s)
	if condition == nil || condition.Status != "False" || !strings.Contains(condition.Message, "already restarted") {
		t.Fatal("persistent future evidence was not truthfully surfaced")
	}
	// Different evidence cannot bypass the cooldown either.
	runtime.native.ProxyBackends[0].Diagnostics.Connect.TimeUnixMicro++
	clock.at = clock.at.Add(-proxyClockCooldown)
	if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(runtime.requests) != 2 {
		t.Fatal("changed evidence bypassed cooldown")
	}
	runtime.native.Running = true
	runtime.native.ProxyBackends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}
	if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if proxyClockCondition(rc, s) != nil {
		t.Fatal("healthy proxy retained obsolete clock recovery condition")
	}
}

func TestProxyClockRecoveryAdmission(t *testing.T) {
	for _, scenario := range []string{"healthy-proxy", "online-writer", "current-timestamps", "within-margin", "no-diagnostics", "unhealthy-primary", "maintenance", "stopped", "replacement-instance"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, backend, s, clock, runtime := proxyClockFixture(t)
			switch scenario {
			case "healthy-proxy":
				runtime.native.Running = true
			case "online-writer":
				runtime.native.ProxyBackends = append(runtime.native.ProxyBackends, pgspec.ProxyBackend{Host: "demop1", Hostgroup: 10, Status: "ONLINE"})
			case "current-timestamps":
				runtime.native.ProxyBackends[0].Diagnostics.Connect.TimeUnixMicro = clock.at.UnixMicro()
			case "within-margin":
				runtime.native.ProxyBackends[0].Diagnostics.Connect.TimeUnixMicro = clock.at.Add(proxyClockMargin).UnixMicro()
			case "no-diagnostics":
				runtime.native.ProxyBackends[0].Diagnostics = nil
			case "unhealthy-primary":
				backend.statusOverrides = map[string]pgspec.StatusResponse{"demop0": {Role: "primary", Running: false}}
			case "maintenance":
				runtime.maintenance = true
			case "stopped":
				runtime.stopped = true
			case "replacement-instance":
				if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				prior := runtime.requests[0]
				rc.Backend = &proxyInventoryBackend{fakeBackend: backend, instances: []infra.Container{{ID: "replacement", Hostname: "replacement-task", Status: infra.ContainerRunning}}}
				if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if len(runtime.requests) != 2 || runtime.requests[1] == prior || s.find("demopx").ProxyClockRecovery["demopx-instance"] != nil {
					t.Fatal("replacement instance inherited an obsolete restart receipt")
				}
				return
			}
			for range 2 {
				if _, err := p.recoverProxyClock(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
			}
			if len(runtime.requests) != 0 {
				t.Fatal("ineligible proxy/primary/control state caused a restart")
			}
		})
	}
}

func TestProxyClockObservationFailuresDoNotBlockRecovery(t *testing.T) {
	for _, failure := range []string{"inventory", "partial-inventory", "transport", "native-status", "generic-status"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend, s, _, runtime := proxyClockFixture(t)
			proxy := s.find("demopx")
			proxy.ProxyClockRecovery = map[string]*proxyClockRecovery{
				"demopx-instance": {RequestID: "preserve-current", EvidenceMicros: 42, Completed: true},
				"unobserved":      {RequestID: "preserve-unobserved", EvidenceMicros: 43, Completed: true},
			}
			switch failure {
			case "inventory", "partial-inventory":
				inventory := &proxyInventoryBackend{fakeBackend: backend, err: engine.ErrUnavailable}
				if failure == "partial-inventory" {
					inventory.instances = []infra.Container{{ID: "demopx-instance", Hostname: "demopx-instance", Status: infra.ContainerRunning}}
				}
				rc.Backend = inventory
			case "transport":
				rc.Agents = &unplacedScaleAgents{AgentDialer: rc.Agents, targets: map[string]bool{"demopx": true}}
			case "native-status":
				runtime.nativeError = true
			case "generic-status":
				runtime.statusError = true
			}
			if handled, err := p.recoverProxyClock(t.Context(), rc, s); handled || err != nil {
				t.Fatalf("optional clock observation preempted ordinary recovery: handled=%v err=%v", handled, err)
			}
			if len(runtime.requests) != 0 || len(proxy.ProxyClockRecovery) != 2 ||
				proxy.ProxyClockRecovery["demopx-instance"].RequestID != "preserve-current" ||
				proxy.ProxyClockRecovery["unobserved"].RequestID != "preserve-unobserved" {
				t.Fatal("incomplete observations restarted a proxy or pruned durable receipts")
			}
		})
	}
}

func TestProxyClockUnreachableDeploymentStillAdvances(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		name := "failed"
		if exhausted {
			name = "exhausted"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			proxy := s.find("demopx")
			proxy.Stage = "deploying"
			proxy.DeployOp = &infra.Op{ID: "failed-deployment"}
			proxy.ProxyClockRecovery = map[string]*proxyClockRecovery{"unobserved": {RequestID: "keep", Completed: true}}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Agents = &unplacedScaleAgents{AgentDialer: rc.Agents, targets: map[string]bool{"demopx": true}}
			if exhausted {
				rc.Backend = &deploymentGuardBackend{Backend: backend, target: proxy.Name, pendingID: "failed-deployment", observed: map[string]bool{}}
			} else {
				rc.Backend = failedDeploymentBackend{backend}
			}
			_, err := p.Sync(t.Context(), rc)
			if exhausted {
				if err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "deployment failed for demopx") {
				t.Fatalf("ordinary failed deployment observation was bypassed: %v", err)
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			proxy = s.find("demopx")
			if proxy.Stage != "imported" || proxy.DeployOp != nil {
				t.Fatalf("unreachable proxy prevented ordinary deployment recovery: stage=%s op=%v", proxy.Stage, proxy.DeployOp)
			}
			if exhausted {
				if _, recorded := rc.Cluster.Status.DeploymentFailures[proxy.Name]; !recorded {
					t.Fatal("deployment exhaustion was not recorded")
				}
			}
			if len(proxy.ProxyClockRecovery) != 1 || proxy.ProxyClockRecovery["unobserved"].RequestID != "keep" {
				t.Fatal("unreachable deployment pruned its clock recovery receipt")
			}
		})
	}
}
