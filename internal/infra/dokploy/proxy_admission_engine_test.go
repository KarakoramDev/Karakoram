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
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type admissionAgents struct {
	options agentapi.ClientOptions
}

func (a admissionAgents) Dial(context.Context, model.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(a.options)
}

func TestProxyAdmissionEngineRetainsIncumbent(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			op, err := b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.holdTasks = true
			a.neverHealthyProxy = !ready
			taskID := fakeString(a.proxyRollout.next[0], "containerId")
			f.mu.Unlock()
			posts := 0
			admitted := false
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.TLS.ServerName != "example.com" {
					t.Error("pinned transport lost the logical TLS identity")
				}
				if r.URL.Path != "/v1/proxy-serving" {
					http.NotFound(w, r)
					return
				}
				if r.Method == http.MethodGet {
					json.NewEncoder(w).Encode(agentapi.ProxyServingProof{TaskID: taskID, Member: "proxy", ConfigVersion: 9, Ready: ready, Admitted: admitted})
					return
				}
				var pair agentapi.ProxyServingAdmission
				if err := json.NewDecoder(r.Body).Decode(&pair); err != nil {
					t.Error(err)
				}
				if pair.TaskID != taskID || pair.ConfigVersion != 9 {
					t.Errorf("wrong admission: %+v", pair)
				}
				admitted = true
				posts++
				json.NewEncoder(w).Encode(map[string]bool{"success": true})
			}))
			defer server.Close()
			host, port, err := net.SplitHostPort(server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.proxyRollout.next[0]["networkAddresses"] = []string{host + "/32"}
			f.mu.Unlock()
			options := agentapi.ClientOptions{BaseURL: "https://example.com:" + port, Token: "test", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
				if logical != "example.com:"+port {
					return nil, fmt.Errorf("logical member address changed")
				}
				tasks, err := b.Containers(infra.WithFreshTaskInventory(ctx), "proxy")
				if err != nil {
					return nil, err
				}
				for _, task := range tasks {
					if task.ID == taskID && task.IPv4 == instance && task.Status == infra.ContainerStarting {
						// Dial the address returned by owned platform inventory,
						// never the server's known address or pre-health DNS.
						return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(instance, port))
					}
				}
				return nil, fmt.Errorf("not an owned starting task")
			}}
			rc := &engine.Context{Cluster: &model.Cluster{Name: "demo"}, Backend: b, Agents: admissionAgents{options: options}}
			for range 2 {
				state, err := engine.ObserveProxyRollout(t.Context(), rc, op, "proxy", 9, true)
				if err != nil || state.Done || state.Failed {
					t.Fatalf("in-flight admission: %+v %v", state, err)
				}
			}
			if ready && posts != 1 || !ready && posts != 0 {
				t.Fatalf("unexpected admission count %d", posts)
			}
			f.mu.Lock()
			a.holdTasks = false
			f.mu.Unlock()
			if ready {
				awaitFake(t, b, op)
			} else {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || !state.Failed {
					t.Fatalf("unready rollout did not pause: %+v %v", state, err)
				}
			}
			assertProxyNeverAllDown(t, f, a)
			if ready {
				// A completed rollout leaves its opt-in in the service template.
				// Swarm then replaces a lost task with no new engine receipt.
				f.mu.Lock()
				f.applyReplicaSpec(a, 1, false)
				replacement := a.tasks[0]
				replacement["currentState"] = "Starting 1 second ago"
				replacement["networkAddresses"] = []string{host + "/32"}
				a.holdTasks = true
				taskID = fakeString(replacement, "containerId")
				f.mu.Unlock()
				admitted = false
				if err := engine.ReconcileProxyAdmission(t.Context(), rc, "proxy", 9); err != nil {
					t.Fatal(err)
				}
				if posts != 2 || !admitted {
					t.Fatal("automatic replacement was not admitted after the receipt completed")
				}
			}
		})
	}
}
