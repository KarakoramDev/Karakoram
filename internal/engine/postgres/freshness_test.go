// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestFreshnessHungFormerPrimaryDoesNotHideNewWriter(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse, len(s.Members))
	for _, m := range s.Members {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		st.Role = "replica"
		st.Streaming = true
		st.Leader = "demop1"
		if m.Name == "demop1" {
			st.Role = "primary"
		}
		if m.Role == "proxy" {
			st.Role = "proxy"
		}
		statuses[m.Name] = st
	}
	var firstPeerProbe atomic.Int64
	start := time.Now()
	releaseHungRequests := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if name == "demop0" {
			// A POST whose body is deliberately unread need not observe its
			// peer's close until another read. Explicit teardown releases it.
			select {
			case <-r.Context().Done():
			case <-releaseHungRequests:
			}
			return
		}
		firstPeerProbe.CompareAndSwap(0, time.Since(start).Nanoseconds())
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(statuses[name])
		default:
			t.Errorf("unexpected mutating call %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer func() {
		close(releaseHungRequests)
		server.Close()
	}()
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// Exercise the real order: Sync must finish before the reconciler asks for
	// Status. A client's 180-second engine timeout must never be the bound.
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*engine.ObservationTimeout {
		t.Fatalf("dead member delayed publication for %s", elapsed)
	}
	if elapsed := time.Duration(firstPeerProbe.Load()); elapsed == 0 || elapsed >= 2*engine.ObservationTimeout {
		t.Fatalf("healthy peer waited behind the dead member: %s", elapsed)
	}
	found := false
	for _, m := range status.Members {
		found = found || m.Name == "demop1" && m.Role == "primary" && m.Health
	}
	if !found || status.Phase == domain.PhaseReady {
		t.Fatalf("new writer was not published with unhealthy former primary: %+v", status)
	}
}

func TestProxyBackendCapacityBudget(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	proxy := s.find("demopx")
	for _, tc := range []struct {
		name                   string
		old, next              string
		current, desired, want int
	}{
		{"default", "", "", 1, 1, 43},
		{"two-proxies", "", "", 2, 2, 20},
		{"scale-up", "", "", 1, 2, 27},
		{"scale-down", "", "", 2, 1, 27},
		{"increase", "100", "200", 1, 1, 43},
		{"decrease", "200", "100", 1, 1, 43},
		{"larger", "200", "200", 1, 1, 93},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := func(value string) json.RawMessage {
				if value == "" {
					return nil
				}
				return parameterOptions(map[string]string{"max_connections": value})
			}
			rc.Cluster.Spec.EngineOptions = raw(tc.next)
			for _, m := range s.data() {
				m.Options = raw(tc.old)
			}
			proxy.Replicas = tc.current
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = tc.desired
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, proxy.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Proxy.MaxConnections != tc.want {
				t.Fatalf("per-instance backend pool=%d want=%d", spec.Proxy.MaxConnections, tc.want)
			}
		})
	}
}

func TestHealthProxyWithoutOnlineWriterIsDegraded(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demopx": {Role: "proxy", Running: false, ProxyBackends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "SHUNNED", Diagnostics: &pgspec.ProxyDiagnostics{Ping: &pgspec.ProxyMonitorError{Class: "timeout", TimeUnixMicro: 1792300090000000}, ConnectionErrors: 17, PoolObserved: true}}}},
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseDegraded {
		t.Fatalf("SHUNNED writer hidden by healthy data members: phase=%s", status.Phase)
	}
	if condition := healthCondition(status, "ProxyWriter"); condition.Status != "False" || condition.Reason != "WriterRouteUnavailable" {
		t.Fatalf("writer-route condition missing: %+v", condition)
	}
	message := healthCondition(status, "ProxyWriter").Message
	if !strings.Contains(message, "SHUNNED") || !strings.Contains(message, "ConnERR=17") || !strings.Contains(message, "ping=timeout@") {
		t.Fatalf("native writer failure attribution missing: %s", message)
	}
	for _, m := range status.Members {
		if m.Set == "proxy" && m.Health {
			t.Fatal("proxy reported healthy without an ONLINE writer")
		}
	}
}

func TestProxyCapacityDecreaseWaitsForEveryProxyAck(t *testing.T) {
	p, rc, b, _, first, late := replicatedProxyFixture(t)
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "50"})
	b.actions = nil
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil || s.Rolling == nil || s.Rolling.Version == 0 {
		t.Fatal("capacity decrease did not publish its phase", err)
	}
	first.applied(s.Rolling.Version)
	for range 5 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	for _, action := range b.actions {
		if strings.HasPrefix(action, "/v1/postgres/restart:") {
			t.Fatal("PostgreSQL shrank before proxy pool acknowledgement", b.actions)
		}
	}
	late.mu.Lock()
	reloads := late.reloads
	late.mu.Unlock()
	if reloads == 0 {
		t.Fatal("smaller budget was not requested from the lagging proxy")
	}
	late.applied(s.Rolling.Version)
	for range 5 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(strings.Join(b.actions, "\n"), "/v1/postgres/restart:demop0") {
		t.Fatal("acknowledged smaller pool did not release the writer restart", b.actions)
	}
}

type proxyPlacementStatusBackend struct {
	*proxyInventoryBackend
}

func (b proxyPlacementStatusBackend) PlacementFailures(context.Context, []string) ([]infra.PlacementFailure, error) {
	return []infra.PlacementFailure{{Member: "demopx", Message: `placement blocked: dimension "cpu" exhausted; reserved port collision`}}, nil
}

func TestProxyUnplacedInstanceDoesNotMislabelServingWriterRoutes(t *testing.T) {
	p, rc, b, s, _, late := replicatedProxyFixture(t)
	component := rc.Cluster.Spec.Components.Extra["proxy"]
	component.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = component
	s.find("demopx").Replicas = 3
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.services["demopx"].Status = infra.StatusCreating
	rc.Backend = proxyPlacementStatusBackend{b}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	ready := healthCondition(status, "Ready")
	if ready.Status != "False" || ready.Reason != "ProxyInstanceUnplaced" || status.Phase != domain.PhaseDegraded {
		t.Fatalf("unplaced proxy was not attributed to placement: %+v", status)
	}
	if !strings.Contains(ready.Message, "2 of 3") || !strings.Contains(ready.Message, "cpu") || !strings.Contains(ready.Message, "reserved port collision") {
		t.Fatalf("placement reason missing: %+v", ready)
	}
	if condition := healthCondition(status, "ProxyWriter"); condition.Status == "False" {
		t.Fatalf("healthy current-primary writer routes mislabeled: %+v", condition)
	}
	// A real serving-proxy failure still takes priority over placement and
	// continues to enforce the every-instance current-primary writer rule.
	late.mu.Lock()
	late.backends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "SHUNNED"}}
	late.mu.Unlock()
	status, err = p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if ready := healthCondition(status, "Ready"); ready.Status != "False" || ready.Reason != "ProxyUnavailable" {
		t.Fatalf("serving writer failure hidden by placement: %+v", ready)
	}
	if writer := healthCondition(status, "ProxyWriter"); writer.Status != "False" || writer.Reason != "WriterRouteUnavailable" {
		t.Fatalf("serving writer rule was weakened: %+v", writer)
	}
}

type failedProxyScaleBackend struct {
	*fakeBackend
	canRecover     bool
	submitted      int
	ordinaryScales int
}

func (b *failedProxyScaleBackend) RecoverFailedScale(_ context.Context, name string, min, max int) (infra.Op, bool, error) {
	if !b.canRecover {
		return infra.Op{}, false, nil
	}
	b.submitted++
	b.services[name].Status = infra.StatusCreating
	return infra.Op{ID: "recovered-count", Kind: "scale", Hostname: name}, true, nil
}

func (b *failedProxyScaleBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.ordinaryScales++
	return b.fakeBackend.SetContainerCount(ctx, name, min, max)
}

func (b *failedProxyScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	switch op.ID {
	case "failed-count":
		return infra.OpState{Failed: true, Status: "failed", Message: "native deployment failed"}, nil
	case "recovered-count":
		b.services[op.Hostname].Status = infra.StatusRunning
		return infra.OpState{Done: true, Status: "done"}, nil
	default:
		return b.fakeBackend.OpStatus(ctx, op)
	}
}

func TestProxyTerminalScaleFailureRecoversOnlyWithPlacementFreeProof(t *testing.T) {
	for _, canRecover := range []bool{false, true} {
		name := "unplaceable"
		if canRecover {
			name = "already-healthy"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = 2
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			s := converge(t, p, rc)
			rc.Cluster.Generation++
			b := &failedProxyScaleBackend{fakeBackend: base, canRecover: canRecover}
			rc.Backend = b
			m := s.find("demopx")
			m.Replicas = 1
			s.Pending = &pending{Op: infra.Op{ID: "failed-count", Kind: "scale"}, Action: "scale", Member: m.Name, Replicas: 2}
			b.services[m.Name].Status = infra.StatusFailed
			b.services[m.Name].MinContainers, b.services[m.Name].MaxContainers = 2, 2
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if ready := healthCondition(status, "Ready"); ready.Status != "False" || ready.Reason != "ProxyRolloutFailed" {
				t.Fatalf("terminal failure described as pending: %+v", ready)
			}
			if writer := healthCondition(status, "ProxyWriter"); writer.Status == "False" {
				t.Fatalf("terminal infrastructure failure mislabeled healthy serving routes: %+v", writer)
			}
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("terminal old handle did not surface failure")
			}
			if !canRecover {
				for range 4 {
					if _, err := p.Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "infrastructure runtime failed") {
						t.Fatalf("failed runtime became a successful endless wait: %v", err)
					}
				}
				if b.submitted != 0 || b.ordinaryScales != 0 {
					t.Fatalf("unplaceable failure resubmitted operations: recovery=%d ordinary=%d", b.submitted, b.ordinaryScales)
				}
				return
			}
			for range 10 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				status, err = p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				rc.Cluster.Status = status
				if status.Phase == domain.PhaseReady && status.ObservedGeneration == rc.Cluster.Generation {
					break
				}
			}
			if b.submitted != 1 || b.ordinaryScales != 0 || status.Phase != domain.PhaseReady || status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("new bounded scale operation did not converge: submitted=%d status=%+v", b.submitted, status)
			}
		})
	}
}

func TestProxyTerminalFailureSurvivesUnavailableObservations(t *testing.T) {
	for _, observation := range []string{"not-running", "invalid-observation"} {
		for _, corrective := range []bool{false, true} {
			name := observation + "/unchanged"
			if corrective {
				name = observation + "/corrective-resources"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, b := setup(t, "ha")
				converge(t, p, rc)
				b.services["demopx"].Status = infra.StatusFailed
				if observation == "not-running" {
					b.statusOverrides = map[string]pgspec.StatusResponse{"demopx": {Role: "proxy", Running: false}}
				} else {
					b.statusWireOverrides = map[string]json.RawMessage{"demopx": json.RawMessage(`{"running":"invalid"}`)}
				}
				desired := rc.Cluster.Spec.Components.Extra["proxy"]
				if corrective {
					desired.Resources.MinRAM++
					desired.Resources.MaxRAM++
					rc.Cluster.Spec.Components.Extra["proxy"] = desired
				}
				for range 8 {
					_, err := p.Sync(t.Context(), rc)
					if !corrective {
						if err == nil || !strings.Contains(err.Error(), "infrastructure runtime failed") {
							t.Fatalf("unavailable observation swallowed terminal failure: %v", err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("terminal failure blocked corrective resources: %v", err)
					}
					if b.services["demopx"].Resources == desired.Resources {
						return
					}
				}
				if corrective {
					t.Fatal("corrective resize was not submitted")
				}
			})
		}
	}
}

func TestFailedProxyScaleAppliesResourceCorrectionBeforeRetryingCount(t *testing.T) {
	p, rc, base := setup(t, "ha")
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	s := converge(t, p, rc)
	m := s.find("demopx")
	desired.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	base.services[m.Name].MinContainers, base.services[m.Name].MaxContainers = 3, 3
	base.services[m.Name].Status = infra.StatusFailed
	s.Pending = &pending{Op: infra.Op{ID: "failed-count", Kind: "scale"}, Action: "scale", Member: m.Name, Replicas: 3}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	backend := &failedProxyScaleBackend{fakeBackend: base}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed scale was not observed")
	}
	desired.Resources.MinRAM /= 2
	desired.Resources.MaxRAM /= 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	for range 20 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("resource correction was blocked: %v", err)
		}
		if reflect.DeepEqual(base.services[m.Name].Resources, desired.Resources) {
			break
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if backend.ordinaryScales != 0 || backend.submitted != 0 || s.Pending == nil || s.Pending.Action != "resize" ||
		!reflect.DeepEqual(s.Pending.Resources, desired.Resources) {
		t.Fatalf("count retry preceded resource correction: scales=%d recovery=%d pending=%+v", backend.ordinaryScales, backend.submitted, s.Pending)
	}
}
