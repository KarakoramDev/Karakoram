// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestPublicExposureCapturedHealthyStateAcknowledgesGeneration(t *testing.T) {
	p, rc, base := setup(t, "single")
	b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
	rc.Backend = b
	converge(t, p, rc)
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	rc.Cluster.Generation++
	for range 30 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			if rc.SyncWait != "" {
				t.Fatalf("applied generation retained blocker: %s", rc.SyncWait)
			}
			return
		}
		if rc.SyncWait == "" {
			t.Fatal("silent generation wait")
		}
	}
	t.Fatalf("healthy formed singleton/proxy did not acknowledge public enable: %s", rc.SyncWait)
}

func TestMemberReadinessWaitReasons(t *testing.T) {
	for _, tc := range []struct {
		name    string
		native  pgspec.StatusResponse
		generic *agentapi.Status
		want    string
	}{
		{name: "native", want: "native process to run"},
		{name: "role", native: pgspec.StatusResponse{Running: true, Role: "replica"}, want: "primary or streaming replica"},
		{name: "process", native: pgspec.StatusResponse{Running: true, Role: "primary"}, generic: &agentapi.Status{Error: "failed"}, want: "healthy supervised processes"},
		{name: "config", native: pgspec.StatusResponse{Running: true, Role: "primary"}, generic: &agentapi.Status{ConfigVersion: -1}, want: "configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demop0")
			b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: tc.native}
			if tc.generic != nil {
				b.genericOverrides = map[string]agentapi.Status{m.Name: *tc.generic}
			}
			ok, err := ready(t.Context(), rc, m)
			if err != nil || ok || !strings.Contains(rc.SyncWait, tc.want) {
				t.Fatalf("ready=%v err=%v reason=%q want=%q", ok, err, rc.SyncWait, tc.want)
			}
		})
	}
}

type waitInventoryBackend struct {
	*fakeBackend
}

func (*waitInventoryBackend) Containers(context.Context, string) ([]infra.Container, error) {
	return nil, nil
}

func TestProxyReadinessEmptyInventoryReason(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	rc.Backend = &waitInventoryBackend{fakeBackend: b}
	ok, err := ready(t.Context(), rc, s.find("demopx"))
	if err != nil || ok || !strings.Contains(rc.SyncWait, "live instance inventory") {
		t.Fatalf("ready=%v err=%v reason=%q", ok, err, rc.SyncWait)
	}
}

type exposureInstanceBackend struct {
	*exposureBackend
	instances []infra.Container
	family    string
}

func (b *exposureInstanceBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name == "demopx" {
		return b.instances, nil
	}
	return b.fakeBackend.Containers(ctx, name)
}

func (b *exposureInstanceBackend) Layout() infra.Layout {
	layout := b.fakeBackend.Layout()
	layout.PrivateAddressFamily = b.family
	return layout
}

type exposureInstanceAgents struct {
	engine.AgentDialer
	instances   map[string]*proxyInstanceFixture
	logical     *proxyInstanceFixture
	unreachable bool
}

func (a exposureInstanceAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name != "demopx" {
		return a.AgentDialer.Dial(ctx, ref, name)
	}
	first := a.logical
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first.server.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: first.server.URL, Token: name, CAPEM: ca,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			if a.unreachable {
				return nil, errors.New("private endpoint unavailable password=private")
			}
			return (&net.Dialer{}).DialContext(ctx, network, first.server.Listener.Addr().String())
		},
		DialInstanceContext: func(ctx context.Context, network, _, instance string) (net.Conn, error) {
			f := a.instances[instance]
			if f == nil {
				return nil, errors.New("instance unreachable")
			}
			return (&net.Dialer{}).DialContext(ctx, network, f.server.Listener.Addr().String())
		},
	})
}

func TestProxyPrivateEndpointHealthAndGeneration(t *testing.T) {
	for _, backend := range []string{"dokploy", "nomad", "zerops"} {
		for _, failure := range []string{"healthy", "unreachable", "native-stopped", "native-error", "instance-stopped"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				p, rc, base := setup(t, "single")
				exposure := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
				rc.Backend = exposure
				// Finish public enable with working service discovery, then model
				// the published service's independent task/endpoint health.
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
				s := converge(t, p, rc)
				a := newProxyInstanceFixture(t, s.ConfigVersion)
				logical := newProxyInstanceFixtureTLS(t, s.ConfigVersion, a.server.TLS)
				instances := []infra.Container{{ID: "allocation-a", Hostname: "proxy-a", Status: infra.ContainerRunning}}
				target := "proxy-a"
				family := infra.AddressFamilyIPv4
				switch backend {
				case "nomad":
					target = "192.0.2.10"
					instances[0].Hostname = "demopx"
					instances[0].IPv4 = target
				case "zerops":
					target = "2001:db8::10"
					instances[0].IPv6 = target
					family = infra.AddressFamilyIPv6
				}
				rc.Backend = &exposureInstanceBackend{exposureBackend: exposure, instances: instances, family: family}
				agents := exposureInstanceAgents{AgentDialer: rc.Agents, instances: map[string]*proxyInstanceFixture{target: a}, logical: logical}
				switch failure {
				case "unreachable":
					agents.unreachable = true
				case "native-stopped":
					logical.running = false
				case "native-error":
					logical.nativeError = true
				case "instance-stopped":
					a.running = false
				}
				rc.Agents = agents
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				ready, published := "", ""
				readyMessage := ""
				for _, condition := range status.Conditions {
					switch condition.Type {
					case "Ready":
						ready, readyMessage = condition.Status, condition.Message
					case "CredentialsPublished":
						published = condition.Status
					}
				}
				if failure == "healthy" {
					if ready != "True" || published != "True" || status.Phase != domain.PhaseReady {
						t.Fatalf("healthy %s endpoint regressed: %+v", backend, status)
					}
					return
				}
				if ready != "False" || published != "False" || status.Phase != domain.PhaseDegraded {
					t.Fatalf("unhealthy endpoint/task falsely ready: %+v", status)
				}
				if failure != "instance-stopped" && (!strings.Contains(readyMessage, "Private service endpoint demopx") || !strings.Contains(readyMessage, "DNS alias/VIP") || strings.Contains(readyMessage, "password=private")) {
					t.Fatalf("missing/unsafe private endpoint guidance: %q", readyMessage)
				}
				if failure == "unreachable" || failure == "native-stopped" || failure == "native-error" {
					rc.Cluster.Generation++
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation || !strings.Contains(rc.SyncWait, "member demopx native running observation:") || strings.Contains(rc.SyncWait, "password=private") {
						t.Fatalf("logical endpoint failure bypassed or hidden: observed=%d desired=%d wait=%q", rc.Cluster.Status.ObservedGeneration, rc.Cluster.Generation, rc.SyncWait)
					}
				}
			})
		}
	}
}

func TestNativeObservationWaitBoundsAndRedactsErrors(t *testing.T) {
	reason := nativeObservationWait("proxy", errors.New("password=private\n"+strings.Repeat("x", 1000)))
	if strings.Contains(reason, "private") || strings.ContainsAny(reason, "\r\n") || len(reason) > 600 {
		t.Fatalf("unsafe observation diagnostic: %q", reason)
	}
}
