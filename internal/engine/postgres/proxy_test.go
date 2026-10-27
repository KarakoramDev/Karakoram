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
	mu          sync.Mutex
	version     int64
	reloads     int
	running     bool
	backends    []pgspec.ProxyBackend
	processes   map[string]agentapi.ProcessStatus
	agent       agentapi.AgentBuild
	nativeError bool
	server      *httptest.Server
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
	f := &proxyInstanceFixture{version: version, running: true, backends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}}
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
			json.NewEncoder(w).Encode(agentapi.Status{Agent: f.agent, ConfigVersion: f.version, Processes: f.processes})
		case "/v1/postgres/status":
			if f.nativeError {
				http.Error(w, "native observation unavailable", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: f.running, Role: "proxy", ProxyBackends: f.backends})
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

func TestCredentialsPublishedRequiresCurrentRevisionOnEveryProxy(t *testing.T) {
	p, rc, b, s, a, late := replicatedProxyFixture(t)
	assertPublished := func(want string) {
		t.Helper()
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if got := healthCondition(status, "CredentialsPublished"); got.Status != want {
			t.Fatalf("CredentialsPublished = %+v, want %s", got, want)
		}
	}
	assertPublished("True")
	for _, mutation := range []string{"create", "rotate", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			cached, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.bumpAccessRevision(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			projected := p.ProjectCredentialsStatus(t.Context(), rc, cached)
			if healthCondition(projected, "CredentialsPublished").Status != "False" {
				t.Fatal("cached publication survived a newer access revision")
			}
			if healthCondition(cached, "CredentialsPublished").Status != "True" {
				t.Fatal("projection mutated the persisted observation")
			}
			// Old config acknowledgements and Ready must not authorize use
			// before reconciliation has even seen the access mutation.
			assertPublished("False")
			revision, err := p.accessRevision(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			s.ConfigHash, err = desiredConfigHash(rc, revision)
			if err != nil {
				t.Fatal(err)
			}
			s.ConfigVersion++
			for _, m := range s.Members {
				m.ConfigVersion = s.ConfigVersion
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			a.applied(s.ConfigVersion)
			assertPublished("False")
			late.applied(s.ConfigVersion)
			assertPublished("True")
			late.mu.Lock()
			late.running = false
			late.mu.Unlock()
			assertPublished("False")
			late.mu.Lock()
			late.running = true
			late.mu.Unlock()
			assertPublished("True")
		})
	}
	b.instances = b.instances[:1]
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	assertPublished("False")
	b.instances = nil
	assertPublished("False")
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

func TestProxyStatusRequiresCurrentWriterInEveryFreshObservation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		backends  []pgspec.ProxyBackend
		wantReady bool
	}{
		{name: "legacy-running-without-rows"},
		{name: "shunned", backends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "SHUNNED"}}},
		{name: "wrong-writer", backends: []pgspec.ProxyBackend{{Host: "demop1", Hostgroup: 10, Status: "ONLINE"}}},
		{name: "other-online-current-shunned", backends: []pgspec.ProxyBackend{
			{Host: "demop0", Hostgroup: 10, Status: "SHUNNED"},
			{Host: "demop1", Hostgroup: 10, Status: "ONLINE"},
		}},
		{name: "reader-only", backends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 20, Status: "ONLINE"}}},
		{name: "old-shunned-current-online", backends: []pgspec.ProxyBackend{
			{Host: "demop1", Hostgroup: 10, Status: "SHUNNED"},
			{Host: "demop0", Hostgroup: 10, Status: "ONLINE"},
		}, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, _, _, late := replicatedProxyFixture(t)
			// Establish Ready first: the next call must not reuse this
			// successful observation or the logical service's healthy peer.
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("baseline not Ready: %+v %v", status, err)
			}
			late.mu.Lock()
			late.backends = tc.backends
			late.mu.Unlock()
			status, err = p.Status(t.Context(), rc)
			if err != nil || (status.Phase == domain.PhaseReady) != tc.wantReady {
				t.Fatalf("current writer readiness: phase=%s wantReady=%t err=%v", status.Phase, tc.wantReady, err)
			}
			for _, m := range status.Members {
				if m.Set == "proxy" && m.Health != tc.wantReady {
					t.Fatalf("proxy health=%t want=%t", m.Health, tc.wantReady)
				}
			}
			late.mu.Lock()
			late.backends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}
			late.mu.Unlock()
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("fresh ONLINE did not recover readiness: %+v %v", status, err)
			}
		})
	}
}

func TestProxyCurrentWriterRequiresKnownPrimary(t *testing.T) {
	if proxyHasCurrentWriter("", []pgspec.ProxyBackend{{Host: "", Hostgroup: 10, Status: "ONLINE"}}) {
		t.Fatal("unknown primary accepted as an ONLINE writer")
	}
}

type proxyExposureRolloutBackend struct {
	*proxyInventoryBackend
	public  bool
	rolling bool
	member  string
}

func (b *proxyExposureRolloutBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv4: "192.0.2.1"}, nil
}

func (b *proxyExposureRolloutBackend) EnsurePublicPort(_ context.Context, name string, _ infra.PublicPort) error {
	if !b.public {
		b.public = true
		b.rolling = true
		b.services[name].Status = infra.StatusCreating
	}
	return nil
}

func (b *proxyExposureRolloutBackend) DeletePublicPorts(_ context.Context, name string) error {
	if b.public {
		b.public = false
		b.rolling = true
		b.services[name].Status = infra.StatusCreating
	}
	return nil
}

func (b *proxyExposureRolloutBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	if !b.public {
		return nil, nil
	}
	name := b.member
	if name == "" {
		name = "demopx"
	}
	return []infra.PublicPortRoute{{Hostname: name, PublicPort: infra.PublicPort{
		Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4",
	}}}, nil
}

func TestProxyExposureWaitsForRolloutAndFreshInstances(t *testing.T) {
	for _, public := range []bool{true, false} {
		t.Run(map[bool]string{true: "enable", false: "disable"}[public], func(t *testing.T) {
			p, rc, inventory, s, first, late := replicatedProxyFixture(t)
			b := &proxyExposureRolloutBackend{proxyInventoryBackend: inventory, public: !public}
			rc.Backend = b
			s.find("demopx").Public = !public
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: public, IPType: "IPV4"}
			rc.Cluster.Generation++
			previous := rc.Cluster.Status.ObservedGeneration
			for range 50 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				first.applied(s.ConfigVersion)
				late.applied(s.ConfigVersion)
				if b.rolling {
					break
				}
			}
			if !b.rolling {
				t.Fatal("exposure change never started its infrastructure rollout")
			}
			assertPending := func() {
				t.Helper()
				if rc.Cluster.Status.ObservedGeneration != previous {
					t.Fatalf("exposure generation completed before rollout/all-instance health: observed=%d want=%d",
						rc.Cluster.Status.ObservedGeneration, previous)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase == domain.PhaseReady {
					t.Fatalf("unready exposure reported Ready: %+v %v", status, err)
				}
			}
			assertPending()
			// Healthy old agents must not bypass a scheduler canary/serial barrier.
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			assertPending()
			b.services["demopx"].Status = infra.StatusRunning
			late.mu.Lock()
			late.running = false
			late.mu.Unlock()
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			assertPending()
			late.mu.Lock()
			late.running = true
			late.version = s.ConfigVersion - 1
			late.mu.Unlock()
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			assertPending()
			late.applied(s.ConfigVersion)
			for range 50 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				first.applied(s.ConfigVersion)
				late.applied(s.ConfigVersion)
				if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					break
				}
			}
			if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("healthy rollout did not complete generation: %+v", rc.Cluster.Status)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("healthy rollout did not become Ready: %+v %v", status, err)
			}
		})
	}
}

func TestProxyFailedRolloutDoesNotBlockCorrectiveScale(t *testing.T) {
	p, rc, inventory, s, first, late := replicatedProxyFixture(t)
	b := &proxyExposureRolloutBackend{proxyInventoryBackend: inventory, public: true, rolling: true}
	rc.Backend = b
	s.find("demopx").Public = true
	b.services["demopx"].Status = infra.StatusFailed
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// An exposure canary failed/reverted but its predecessors still serve.
	// Correcting the requested count must remain possible without first
	// declaring that failed rollout healthy.
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas++
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	rc.Cluster.Generation++
	previous := rc.Cluster.Status.ObservedGeneration
	for range 50 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		var err error
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		first.applied(s.ConfigVersion)
		late.applied(s.ConfigVersion)
		if rc.Cluster.Status.ObservedGeneration != previous {
			t.Fatal("failed rollout completed the corrective generation")
		}
		if b.services["demopx"].MaxContainers == proxy.Replicas {
			break
		}
	}
	if b.services["demopx"].MaxContainers != proxy.Replicas {
		t.Fatal("failed rollout blocked the corrective proxy count")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatalf("corrective scale bypassed infrastructure readiness: %+v %v", status, err)
	}
	// Only the infrastructure's later success releases generation readiness.
	b.services["demopx"].Status = infra.StatusRunning
	for range 50 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		first.applied(s.ConfigVersion)
		late.applied(s.ConfigVersion)
		if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			return
		}
	}
	t.Fatal("corrected rollout did not complete after infrastructure recovery")
}

func TestDirectExposureWaitsForInfrastructureRollout(t *testing.T) {
	for _, public := range []bool{true, false} {
		t.Run(map[bool]string{true: "enable", false: "disable"}[public], func(t *testing.T) {
			p, rc, base := setup(t, "single")
			proxy := rc.Cluster.Spec.Components.Extra["proxy"]
			proxy.Replicas = 0
			rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			s := converge(t, p, rc)
			target := connectionMember(rc, s)
			b := &proxyExposureRolloutBackend{
				proxyInventoryBackend: &proxyInventoryBackend{fakeBackend: base},
				public:                !public, member: target,
			}
			rc.Backend = b
			s.find(target).Public = !public
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: public, IPType: "IPV4"}
			rc.Cluster.Generation++
			previous := rc.Cluster.Status.ObservedGeneration
			for range 50 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if b.rolling {
					break
				}
			}
			if !b.rolling {
				t.Fatal("direct exposure did not initiate its runtime change")
			}
			for range 20 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if rc.Cluster.Status.ObservedGeneration != previous {
				t.Errorf("direct exposure completed before infrastructure rollout: observed=%d want=%d",
					rc.Cluster.Status.ObservedGeneration, previous)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase == domain.PhaseReady {
				t.Errorf("direct exposure reported Ready during infrastructure rollout: %+v %v", status, err)
			}
			b.services[target].Status = infra.StatusRunning
			converge(t, p, rc)
			if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("direct exposure did not converge after infrastructure success")
			}
		})
	}
}
