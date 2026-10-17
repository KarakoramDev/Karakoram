// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type proxyInventoryBackend struct {
	*fakeBackend
	instances []infra.Container
	err       error
}

func (b *proxyInventoryBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if strings.HasSuffix(name, "px") {
		return b.instances, b.err
	}
	return b.fakeBackend.Containers(ctx, name)
}

type proxyInstanceFixture struct {
	mu      sync.Mutex
	version int64
	reloads int
	running bool
	server  *httptest.Server
}

func (f *proxyInstanceFixture) applied(version int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version = version
}

func newProxyInstanceFixture(t *testing.T, version int64) *proxyInstanceFixture {
	return newProxyInstanceFixtureTLS(t, version, nil)
}

func newProxyInstanceFixtureTLS(t *testing.T, version int64, tlsConfig *tls.Config) *proxyInstanceFixture {
	t.Helper()
	f := &proxyInstanceFixture{version: version, running: true}
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer demopx" {
			t.Error("instance request lost logical member authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: f.version})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: f.running, Role: "proxy"})
		case "/v1/reload":
			f.reloads++ // Acceptance alone intentionally does not acknowledge Apply.
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	f.server.TLS = tlsConfig
	f.server.StartTLS()
	t.Cleanup(f.server.Close)
	return f
}

type proxyInstanceAgents struct {
	engine.AgentDialer
	instances map[string]*proxyInstanceFixture
}

func (a proxyInstanceAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name != "demopx" {
		return a.AgentDialer.Dial(ctx, ref, name)
	}
	first := a.instances["proxy-a"]
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first.server.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: first.server.URL, Token: name, CAPEM: ca,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			instance := a.instances[host]
			if instance == nil && host == "127.0.0.1" {
				instance = first // Model logical-service DNS always selecting replica A.
			}
			if instance == nil {
				return nil, errors.New("instance unreachable")
			}
			return (&net.Dialer{}).DialContext(ctx, network, instance.server.Listener.Addr().String())
		},
	})
}

func replicatedProxyFixture(t *testing.T) (*Core, *engine.Context, *proxyInventoryBackend, *state, *proxyInstanceFixture, *proxyInstanceFixture) {
	t.Helper()
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	a := newProxyInstanceFixture(t, s.ConfigVersion)
	late := newProxyInstanceFixture(t, s.ConfigVersion)
	inventory := &proxyInventoryBackend{fakeBackend: b, instances: []infra.Container{
		{ID: "allocation-a", Hostname: "proxy-a", Status: infra.ContainerRunning},
		{ID: "allocation-b", Hostname: "proxy-b", Status: infra.ContainerActive},
	}}
	rc.Backend = inventory
	rc.Agents = proxyInstanceAgents{AgentDialer: rc.Agents, instances: map[string]*proxyInstanceFixture{"proxy-a": a, "proxy-b": late}}
	return p, rc, inventory, s, a, late
}

func TestProxyDrainRequiresEveryLiveInstance(t *testing.T) {
	for _, finish := range []string{"late-ack", "disappeared", "stopped", "single", "all-gone", "service-gone"} {
		t.Run(finish, func(t *testing.T) {
			p, rc, b, s, a, late := replicatedProxyFixture(t)
			if finish == "single" {
				b.instances = b.instances[:1]
			}
			m := s.find("demop2")
			if _, err := p.retireMember(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			version := m.ProxyDrainVersion
			// Reconstruct the durable checkpoint as a restarted CP would.
			var err error
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m = s.find(m.Name)
			if finish != "single" {
				a.applied(version)
			}
			if _, err := p.retireMember(t.Context(), rc, s, m); err != nil || b.services[m.Name] == nil {
				t.Fatalf("runtime removed before every live instance applied: %v", err)
			}
			lagging := late
			if finish == "single" {
				lagging = a
			}
			lagging.mu.Lock()
			reloads := lagging.reloads
			lagging.mu.Unlock()
			if reloads != 1 {
				t.Fatalf("lagging instance not reloaded directly: %d", reloads)
			}
			// Repeated successful reload responses still cannot authorize deletion.
			if _, err := p.retireMember(t.Context(), rc, s, m); err != nil || b.services[m.Name] == nil {
				t.Fatalf("reload response treated as Apply acknowledgement: %v", err)
			}
			switch finish {
			case "late-ack":
				late.applied(version)
			case "disappeared":
				b.instances = b.instances[:1]
			case "stopped":
				b.instances[1].Status = infra.StatusStopped
			case "single":
				a.applied(version)
			case "all-gone":
				b.instances = nil
			case "service-gone":
				b.err = infra.ErrNotFound
				delete(b.services, "demopx")
			}
			if _, err := p.retireMember(t.Context(), rc, s, m); err != nil || b.services[m.Name] != nil {
				t.Fatalf("acknowledged/disappeared instances blocked runtime deletion: %v", err)
			}
			if m.ProxyDrainVersion != version || s.ConfigVersion != version {
				t.Fatal("retry republished the durable drain version")
			}
		})
	}
}

func TestProxyDrainFailsClosedOnUnknownInstance(t *testing.T) {
	for _, failure := range []string{"inventory", "missing-inventory", "address", "unreachable", "new-instance"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, b, s, a, late := replicatedProxyFixture(t)
			m := s.find("demop2")
			if _, err := p.retireMember(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			a.applied(m.ProxyDrainVersion)
			late.applied(m.ProxyDrainVersion)
			switch failure {
			case "inventory":
				b.err = errors.New("inventory unavailable")
			case "missing-inventory":
				b.err = infra.ErrNotFound
			case "address":
				b.instances[1].Hostname = "demopx"
			case "unreachable":
				b.instances[1].Hostname = "missing-instance"
			case "new-instance":
				// A new live allocation cannot inherit an old allocation's ack.
				b.instances[1].ID = "replacement-allocation"
				late.applied(m.ProxyDrainVersion - 1)
			}
			_, err := p.retireMember(t.Context(), rc, s, m)
			if failure != "new-instance" && err == nil {
				t.Fatal("unobservable live instance was treated as acknowledged")
			}
			if b.services[m.Name] == nil {
				t.Fatal("runtime removed without all live-instance acknowledgements")
			}
		})
	}
}

func TestProxyReadyAndStatusRequireEveryLiveInstance(t *testing.T) {
	p, rc, b, s, _, late := replicatedProxyFixture(t)
	m := s.find("demopx")
	late.applied(m.ConfigVersion - 1)
	if ok, err := ready(t.Context(), rc, m); err != nil || ok {
		t.Fatalf("one instance acknowledged readiness for the service: ok=%v err=%v", ok, err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatalf("status hid an unacknowledged proxy instance: %+v %v", status, err)
	}
	for _, member := range status.Members {
		if member.Name == m.Name && member.Agent.ConfigVersion != m.ConfigVersion-1 {
			t.Fatal("proxy status did not report the minimum applied version")
		}
	}
	late.applied(m.ConfigVersion)
	if ok, err := ready(t.Context(), rc, m); err != nil || !ok {
		t.Fatalf("fully acknowledged proxies did not become ready: %v", err)
	}
	late.mu.Lock()
	late.running = false
	late.mu.Unlock()
	if ok, err := ready(t.Context(), rc, m); err != nil || ok {
		t.Fatalf("stopped ProxySQL instance counted as ready: %v", err)
	}
	b.instances = nil
	if ok, err := ready(t.Context(), rc, m); err != nil || ok {
		t.Fatalf("zero proxy instances counted as ready: %v", err)
	}
}
