// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type componentInstanceBackend struct {
	*coreInfra
	member    string
	instances []infra.Container
}

func (b *componentInstanceBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name == b.member {
		return b.instances, nil
	}
	return b.coreInfra.Containers(ctx, name)
}

type componentInstanceDialer struct {
	engine.AgentDialer
	member string
	first  *httptest.Server
	second *httptest.Server
}

func (d componentInstanceDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if member != d.member {
		return d.AgentDialer.Dial(ctx, ref, member)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d.first.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.first.URL, Token: "test-token", CAPEM: ca, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		server := d.first
		switch host {
		case "instance-b":
			server = d.second
		case "instance-a", "127.0.0.1":
		default:
			return nil, errors.New("instance unavailable")
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}})
}

func TestStatelessComponentReadinessRequiresEveryInstance(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		for _, failure := range []string{"native-unhealthy", "stale-config", "missing-instance", "desired-increase", "extra-live-unhealthy", "unreachable", "paused", "process-error"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				desired := rc.Cluster.Spec.Components.Extra[kind]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[kind] = desired
				s := coreConverge(t, p, rc, f)
				var m *member
				for _, candidate := range s.componentMembers() {
					if candidate.Kind == kind {
						m = candidate
					}
				}
				if m == nil {
					t.Fatal("component missing")
				}
				m.Containers = 2
				desired.Replicas = 2
				if failure == "desired-increase" {
					m.Containers = 1
				}
				if failure == "extra-live-unhealthy" {
					m.Containers, desired.Replicas = 1, 1
				}
				rc.Cluster.Spec.Components.Extra[kind] = desired
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				handler := func(bad bool) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						version := int64(1 << 30)
						if bad && failure == "stale-config" {
							version = 0
						}
						switch r.URL.Path {
						case "/v1/status":
							process := agentapi.ProcessStatus{State: "running"}
							if bad && failure == "paused" {
								process.State = "paused"
							}
							if bad && failure == "process-error" {
								process.Error = "child failed"
							}
							json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version, Processes: map[string]agentapi.ProcessStatus{product(m): process}})
						case "/v1/elasticsearch/readiness":
							json.NewEncoder(w).Encode(agentspec.Readiness{Ready: !bad || (failure != "native-unhealthy" && failure != "extra-live-unhealthy"), ConfigVersion: version, Message: "component native health unavailable"})
						default:
							http.NotFound(w, r)
						}
					})
				}
				a := httptest.NewTLSServer(handler(false))
				defer a.Close()
				b := httptest.NewUnstartedServer(handler(true))
				b.TLS = a.TLS.Clone()
				b.StartTLS()
				defer b.Close()
				inventory := &componentInstanceBackend{coreInfra: f, member: m.Name, instances: []infra.Container{{ID: "allocation-a", Hostname: "instance-a", Status: infra.ContainerRunning}, {ID: "allocation-b", Hostname: "instance-b", Status: infra.ContainerRunning}}}
				switch failure {
				case "missing-instance", "desired-increase":
					inventory.instances = inventory.instances[:1]
				case "unreachable":
					inventory.instances[1].Hostname = "unreachable"
				}
				rc.Backend = inventory
				rc.Agents = componentInstanceDialer{AgentDialer: rc.Agents, member: m.Name, first: a, second: b}
				if p.componentReady(t.Context(), rc, m) {
					t.Fatal("healthy logical service hid unhealthy sibling")
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase == domain.PhaseReady {
					t.Fatal("cluster ready with unhealthy component instance")
				}
				for _, observed := range status.Members {
					if observed.Name == m.Name && (observed.Health || !strings.Contains(observed.Agent.Message, m.Name)) {
						t.Fatalf("component diagnosis missing: %+v", observed)
					}
					if observed.Name == m.Name && (failure == "paused" || failure == "process-error") {
						wantState, reason := "paused", "ProcessPaused"
						if failure == "process-error" {
							wantState, reason = "error", "ProcessError"
						}
						if observed.State != wantState || !observed.Agent.Healthy || observed.Role != kind {
							t.Fatalf("child state lost or agent disconnected: %+v", observed)
						}
						found := false
						for _, condition := range status.Conditions {
							if condition.Type == "Process/"+m.Name+"/"+product(m)+"/allocation-b" && condition.Reason == reason {
								found = true
							}
						}
						if !found {
							t.Fatalf("pinned instance condition missing: %+v", status.Conditions)
						}
					}
				}
			})
		}
	}
}
