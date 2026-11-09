// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

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
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type admissionBackend struct {
	infra.Backend
	tasks        []infra.Container
	failed       bool
	polls        int
	inventoryErr error
}

func (b *admissionBackend) Containers(ctx context.Context, member string) ([]infra.Container, error) {
	if !infra.FreshTaskInventory(ctx) || member != "proxy" {
		return nil, errors.New("fresh owned inventory required")
	}
	return b.tasks, b.inventoryErr
}
func (*admissionBackend) Layout() infra.Layout { return infra.Layout{} }
func (*admissionBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{ProxyDeploymentPreservesServing: true}
}
func (*admissionBackend) ProxyUpdatesPreserveServing(context.Context, string) (bool, error) {
	return true, nil
}
func (b *admissionBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	b.polls++
	return infra.OpState{Failed: b.failed}, nil
}

func TestProxyAdmissionWatcher(t *testing.T) {
	for _, scenario := range []string{"unready", "unapplied", "unready-wrong-task", "unready-wrong-member", "ready", "stale-revision", "wrong-task", "wrong-member", "legacy", "foreign", "failed", "unauthorized", "missing-address", "not-opted-in"} {
		t.Run(scenario, func(t *testing.T) {
			proof := agentapi.ProxyServingProof{TaskID: "new", Member: "proxy", ConfigVersion: 7, Ready: true}
			switch scenario {
			case "unready":
				proof.Ready = false
			case "unapplied":
				proof.ConfigVersion, proof.Ready = 0, false
			case "unready-wrong-task":
				proof.TaskID, proof.Ready = "other", false
			case "unready-wrong-member":
				proof.Member, proof.Ready = "other", false
			case "stale-revision":
				proof.ConfigVersion = 6
			case "wrong-task":
				proof.TaskID = "other"
			case "wrong-member":
				proof.Member = "other"
			}
			posts, gets, dials := 0, 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/proxy-serving" {
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if r.Method == http.MethodGet {
					gets++
					if scenario == "legacy" {
						http.NotFound(w, r)
						return
					}
					if scenario == "unauthorized" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					json.NewEncoder(w).Encode(proof)
					return
				}
				var admit agentapi.ProxyServingAdmission
				if err := json.NewDecoder(r.Body).Decode(&admit); err != nil {
					t.Error(err)
				}
				if admit.TaskID != "new" || admit.ConfigVersion != 7 {
					t.Errorf("unbound admission: %+v", admit)
				}
				posts++
				proof.Admitted = true
				json.NewEncoder(w).Encode(map[string]bool{"success": true})
			}))
			defer server.Close()
			b := &admissionBackend{tasks: []infra.Container{{ID: "incumbent", Hostname: "incumbent.test", Status: infra.ContainerRunning}, {ID: "new", Hostname: "unpublished.invalid", IPv4: "127.0.0.1", Status: infra.ContainerStarting, ProxyServingAdmission: true}}}
			if scenario == "missing-address" {
				b.tasks[1].IPv4 = ""
			}
			if scenario == "not-opted-in" {
				b.tasks[1].ProxyServingAdmission = false
			}
			if scenario == "foreign" {
				b.tasks = b.tasks[:1]
				b.inventoryErr = errors.New("unowned service")
			}
			b.failed = scenario == "failed"
			_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: b, Agents: inventoryAgentDialer{agentapi.ClientOptions{BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Token: "test", DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
				dials++
				if instance != "127.0.0.1" {
					t.Errorf("incumbent or foreign task dialled: %s", instance)
				}
				return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(instance, port))
			}}}}
			for range 2 {
				status, err := ObserveProxyRollout(t.Context(), rc, infra.Op{}, "proxy", 7, true)
				wantErr := scenario == "stale-revision" || scenario == "wrong-task" || scenario == "wrong-member" || scenario == "unready-wrong-task" || scenario == "unready-wrong-member" || scenario == "foreign" || scenario == "unauthorized" || scenario == "missing-address"
				if (err != nil) != wantErr {
					t.Fatalf("error=%v expected error=%v", err, wantErr)
				}
				if status.Done || status.Failed != b.failed {
					t.Fatalf("receipt bypassed: %+v", status)
				}
			}
			wantPosts := 0
			if scenario == "ready" {
				wantPosts = 1
			}
			if posts != wantPosts {
				t.Fatalf("admitted %d times; want %d", posts, wantPosts)
			}
			if (scenario == "foreign" || scenario == "failed" || scenario == "not-opted-in" || scenario == "missing-address") && (dials != 0 || gets != 0) {
				t.Fatal("unowned/failed rollout dialled")
			}
			if b.polls != 2 {
				t.Fatal("receipt budget not polled")
			}
			if b.tasks[0].Status != infra.ContainerRunning {
				t.Fatal("incumbent changed")
			}
		})
	}
}

func TestProxyAdmissionLegacyPreflight(t *testing.T) {
	for name, legacy := range map[string]bool{"fixed": false, "legacy": true} {
		t.Run(name, func(t *testing.T) {
			posts := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					posts++
				}
				if legacy {
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(agentapi.ProxyServingProof{Member: "proxy"})
			}))
			defer server.Close()
			backend := &admissionBackend{tasks: []infra.Container{{ID: "incumbent", Hostname: "old.test", Status: infra.ContainerRunning}}}
			rc := &Context{Cluster: &domain.Cluster{Name: "demo"}, Backend: backend, Agents: inventoryAgentDialer{agentapi.ClientOptions{
				BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Token: "test",
				DialInstanceContext: func(ctx context.Context, network, _, instance string) (net.Conn, error) {
					if instance != "old.test" {
						t.Errorf("unexpected preflight instance %s", instance)
					}
					return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
				},
			}}}
			err := RequireProxyAdmissionProtocol(t.Context(), rc, "proxy")
			if legacy {
				if err == nil || !strings.Contains(err.Error(), "run `clusters upgrade-agents` first") {
					t.Fatalf("legacy proxy not rejected with upgrade guidance: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if posts != 0 || backend.polls != 0 || backend.tasks[0].Status != infra.ContainerRunning {
				t.Fatal("preflight mutated or admitted the incumbent")
			}
		})
	}
}
