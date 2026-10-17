// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

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
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Joining all enabled members while final DNS is unresolved must be possible:
// the gateway is first, and Studio's discovery record is created last.
func joinComponentsWithoutFinalReadiness(t *testing.T, p *Provider, rc *engine.Context, phase domain.Phase) {
	t.Helper()
	observed := rc.Cluster.Status.ObservedGeneration
	for range 150 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != observed {
			t.Fatal("stack became Ready before Envoy discovered every enabled upstream")
		}
		state, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		joined := state.Pending == nil
		for _, name := range stackComponents.Enabled(rc.Cluster.Spec.Components) {
			member := state.Members[name]
			joined = joined && member != nil && member.Stage == engine.ComponentJoined
		}
		if !joined {
			continue
		}
		result, err := p.Sync(t.Context(), rc)
		if err != nil || result.RequeueAfter != 2*time.Second {
			t.Fatal("final discovery wait must be polled after component joins", result, err)
		}
		status, err = p.Status(t.Context(), rc)
		if err != nil || status.Phase != phase || status.ObservedGeneration != observed {
			t.Fatalf("final discovery wait: phase=%s observed=%d, want %s/%d, err=%v", status.Phase, status.ObservedGeneration, phase, observed, err)
		}
		for _, member := range status.Members {
			if member.Set == "gateway" && !member.Health {
				t.Fatal("unresolved upstreams must not revoke gateway bootstrap health")
			}
		}
		return
	}
	t.Fatal("sequential component startup deadlocked on gateway DNS")
}

func TestFinalReadinessRequiresEveryEnabledEnvoyCluster(t *testing.T) {
	p, rc, backend := setupProvider(t)
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &domain.BackupStorage{
		Name: "objects", Namespace: "default", Type: "s3", EndpointURL: "https://objects.example.test",
		Bucket: "objects", Region: "test", VerifyTLS: true,
		Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"},
	}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
	for _, name := range sbspec.GatewayUpstreams {
		component := rc.Cluster.Spec.Components.Extra[name]
		component.Replicas = 1
		rc.Cluster.Spec.Components.Extra[name] = component
	}
	backend.gatewayHosts["studio"] = 0
	joinComponentsWithoutFinalReadiness(t, p, rc, domain.PhaseCreating)
	backend.gatewayHosts["studio"] = 1
	convergeProvider(t, p, rc)
	for _, name := range sbspec.GatewayUpstreams {
		t.Run(name, func(t *testing.T) {
			for _, omitted := range []bool{false, true} {
				backend.gatewayHosts[name] = 0
				if omitted {
					delete(backend.gatewayHosts, name)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase != domain.PhaseUpdating {
					t.Fatal("zero/missing upstream hosts accepted as Ready", status.Phase, err)
				}
			}
			backend.gatewayHosts[name] = 1
		})
	}
	counts := backend.gatewayHosts
	backend.gatewayHosts = nil // A failed admin read or older agent fails closed.
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseUpdating {
		t.Fatal("missing gateway observation accepted as Ready", status.Phase, err)
	}
	backend.gatewayHosts = counts
	convergeProvider(t, p, rc)
}

func TestFinalGatewayReadinessGatesAdditionsAndReenables(t *testing.T) {
	p, rc, backend := setupProvider(t)
	backend.gatewayHosts["studio"] = 0 // Disabled upstream must not block minimal.
	convergeProvider(t, p, rc)
	for _, action := range []string{"add", "re-enable"} {
		t.Run(action, func(t *testing.T) {
			for _, name := range []string{"meta", "studio"} {
				component := rc.Cluster.Spec.Components.Extra[name]
				component.Replicas = 1
				rc.Cluster.Spec.Components.Extra[name] = component
			}
			rc.Cluster.Generation++
			backend.gatewayHosts["studio"] = 0
			joinComponentsWithoutFinalReadiness(t, p, rc, domain.PhaseUpdating)
			backend.gatewayHosts["studio"] = 1
			convergeProvider(t, p, rc)
			studio := rc.Cluster.Spec.Components.Extra["studio"]
			studio.Replicas = 0
			rc.Cluster.Spec.Components.Extra["studio"] = studio
			rc.Cluster.Generation++
			backend.gatewayHosts["studio"] = 0
			convergeProvider(t, p, rc)
		})
	}
}

func TestGatewayBootstrapUpgradeChangesConfigWithoutSettingsChange(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	member := state.Members["gateway"]
	current, err := p.desiredSpec(t.Context(), rc, member)
	if err != nil {
		t.Fatal(err)
	}
	legacy := current
	legacy.RestartToken = ""
	legacy.RestartToken = configDigest(legacy)
	if legacy.RestartToken == current.RestartToken {
		t.Fatal("existing gateway bootstrap would not be refreshed on upgrade")
	}
	if err := p.publishConfig(t.Context(), rc, member, legacy); err != nil {
		t.Fatal(err)
	}
	if err := saveState(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	convergeProvider(t, p, rc)
	state, err = loadState(rc.Cluster)
	if err != nil || state.Members["gateway"].ConfigHash != current.RestartToken {
		t.Fatal("gateway did not migrate to the observable DNS bootstrap", err)
	}
	runtime, err := p.runtimeSpec(rc, state.Members["gateway"])
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range runtime.Ports {
		if port.Port != 8000 && port.Port != 9443 {
			t.Fatal("gateway admin must never be exposed as an infrastructure port", port)
		}
	}
}

type gatewayInventoryBackend struct {
	*testBackend
	instances []infra.Container
	err       error
}

func (b *gatewayInventoryBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if strings.HasSuffix(name, codes["gateway"]) {
		return b.instances, b.err
	}
	return b.testBackend.Containers(ctx, name)
}

type gatewayInstanceFixture struct {
	mu           sync.Mutex
	version      int64
	hosts        map[string]int
	ready        bool
	unobservable bool
	observations int
	server       *httptest.Server
}

func newGatewayInstanceFixture(t *testing.T, member string, version int64, tlsConfig *tls.Config) *gatewayInstanceFixture {
	t.Helper()
	f := &gatewayInstanceFixture{version: version, ready: true, hosts: map[string]int{}}
	for _, upstream := range sbspec.GatewayUpstreams {
		f.hosts[upstream] = 1
	}
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+member {
			t.Error("instance probe lost logical member authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: f.version})
		case "/v1/supabase/status":
			f.observations++
			if f.unobservable {
				http.Error(w, "observation unavailable", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(sbspec.Status{Ready: f.ready, GatewayUpstreamHosts: f.hosts})
		default:
			http.NotFound(w, r)
		}
	}))
	f.server.TLS = tlsConfig
	f.server.StartTLS()
	t.Cleanup(f.server.Close)
	return f
}

type gatewayInstanceAgents struct {
	engine.AgentDialer
	member    string
	instances map[string]*gatewayInstanceFixture
}

func (a gatewayInstanceAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name != a.member {
		return a.AgentDialer.Dial(ctx, ref, name)
	}
	first := a.instances["gateway-a"]
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first.server.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: first.server.URL, Token: name, CAPEM: ca,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			instance := a.instances[host]
			if host == "127.0.0.1" {
				instance = first // Logical service DNS always selects healthy A.
			}
			if instance == nil {
				return nil, errors.New("instance unreachable")
			}
			return (&net.Dialer{}).DialContext(ctx, network, instance.server.Listener.Addr().String())
		},
	})
}

func TestFinalGatewayReadinessRequiresEveryLiveInstance(t *testing.T) {
	for _, failure := range []string{
		"unresolved", "missing-upstream", "unobservable", "unreachable", "older-config",
		"inventory-error", "missing-inventory", "empty-inventory", "shared-address", "no-address", "listener-down",
	} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			gateway := rc.Cluster.Spec.Components.Extra["gateway"]
			gateway.Replicas = 2
			rc.Cluster.Spec.Components.Extra["gateway"] = gateway
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := state.Members["gateway"]
			a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			b := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, a.server.TLS.Clone())
			inventory := &gatewayInventoryBackend{testBackend: backend, instances: []infra.Container{
				{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
				{ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerActive},
			}}
			rc.Backend = inventory
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name,
				instances: map[string]*gatewayInstanceFixture{"gateway-a": a, "gateway-b": b}}
			observed := rc.Cluster.Status.ObservedGeneration
			rc.Cluster.Generation++
			b.mu.Lock()
			switch failure {
			case "unresolved":
				b.hosts["rest"] = 0
			case "missing-upstream":
				delete(b.hosts, "rest")
			case "unobservable":
				b.unobservable = true
			case "unreachable":
				inventory.instances[1].Hostname = "unreachable"
			case "older-config":
				b.version--
			case "inventory-error":
				inventory.err = errors.New("inventory unavailable")
			case "missing-inventory":
				inventory.err = infra.ErrNotFound
			case "empty-inventory":
				inventory.instances = nil
			case "shared-address":
				inventory.instances[1].Hostname = m.Name
			case "no-address":
				inventory.instances[1].Hostname = ""
			case "listener-down":
				b.ready = false
			}
			b.mu.Unlock()
			for range 2 {
				result, err := p.Sync(t.Context(), rc)
				if err != nil || result.RequeueAfter != 2*time.Second {
					t.Fatalf("final readiness wait must requeue: %+v, %v", result, err)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase != domain.PhaseUpdating ||
					status.ObservedGeneration != observed || rc.Cluster.Status.ObservedGeneration != observed {
					t.Fatalf("one healthy gateway acknowledged the stack: phase=%s observed=%d/%d err=%v",
						status.Phase, status.ObservedGeneration, rc.Cluster.Status.ObservedGeneration, err)
				}
			}
			b.mu.Lock()
			b.version, b.hosts["rest"], b.unobservable, b.ready = m.ConfigVersion, 1, false, true
			b.mu.Unlock()
			inventory.err = nil
			inventory.instances = []infra.Container{
				{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
				{ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerActive},
			}
			convergeProvider(t, p, rc)
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.observations == 0 {
				t.Fatal("readiness never observed gateway B directly")
			}
		})
	}
}

func TestFinalGatewayReadinessRefreshesLiveInventory(t *testing.T) {
	for _, gone := range []string{"disappeared", "stopped"} {
		t.Run(gone, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := state.Members["gateway"]
			a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			inventory := &gatewayInventoryBackend{testBackend: backend, instances: []infra.Container{
				{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
				{ID: "allocation-b", Hostname: "unreachable", Status: infra.ContainerRunning},
			}}
			rc.Backend = inventory
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name,
				instances: map[string]*gatewayInstanceFixture{"gateway-a": a}}
			if gatewayInstancesReady(t.Context(), rc, m) {
				t.Fatal("unreachable live sibling was ignored")
			}
			if gone == "disappeared" {
				inventory.instances = inventory.instances[:1]
			} else {
				inventory.instances[1].Status = infra.StatusStopped
			}
			if !gatewayInstancesReady(t.Context(), rc, m) {
				t.Fatal("gone sibling still blocked fresh readiness")
			}
			// A newly listed process cannot inherit the gone instance's result.
			inventory.instances = append(inventory.instances, infra.Container{
				ID: "replacement", Hostname: "unreachable", Status: infra.ContainerRunning,
			})
			if gatewayInstancesReady(t.Context(), rc, m) {
				t.Fatal("replacement inherited another instance's acknowledgement")
			}
		})
	}
}
