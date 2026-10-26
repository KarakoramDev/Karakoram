// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type restartInventory struct {
	*testBackend
	instances map[string][]infra.Container
}

func (b restartInventory) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if instances, ok := b.instances[name]; ok {
		return instances, nil
	}
	return b.testBackend.Containers(ctx, name)
}

type restartFixture struct {
	mu                         sync.Mutex
	server                     *httptest.Server
	process                    agentapi.ProcessStatus
	requests                   map[string]bool
	launches                   int
	noOp, unhealthy, lostReply bool
	version                    int64
	statusError, nativeError   bool
	nativeMessage              string
}

func newRestartFixture(t *testing.T, component string, version int64) *restartFixture {
	f := &restartFixture{process: agentapi.ProcessStatus{State: "running", PID: 100, SupervisorSession: "session", Launch: 1}, requests: map[string]bool{}, version: version}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/v1/status":
			if f.statusError && f.launches > 0 {
				http.Error(w, "active agent observation unavailable", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: f.version, Processes: map[string]agentapi.ProcessStatus{component: f.process}})
		case "/v1/supabase/status":
			if f.nativeError && f.launches > 0 {
				http.Error(w, "active native observation unavailable", http.StatusServiceUnavailable)
				return
			}
			hosts := map[string]int{}
			for _, name := range sbspec.GatewayUpstreams {
				hosts[name] = 1
			}
			json.NewEncoder(w).Encode(sbspec.Status{Ready: !f.unhealthy || f.launches == 0, Message: f.nativeMessage, GatewayUpstreamHosts: hosts})
		case "/v1/processes/" + component + "/restart":
			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				t.Error("missing stable restart request identity")
			}
			if !f.requests[key] {
				f.requests[key] = true
				if !f.noOp {
					f.launches++
					f.process.PID++
					f.process.Restarts++
					f.process.Launch++
				}
				if f.lostReply {
					http.Error(w, "lost accepted reply", http.StatusServiceUnavailable)
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

type restartAgents struct {
	engine.AgentDialer
	fixtures map[string]*restartFixture
	members  map[string]string
}

func (a restartAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	first := a.fixtures[a.members[name]]
	if first == nil {
		return a.AgentDialer.Dial(ctx, ref, name)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first.server.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: first.server.URL, CAPEM: ca, Token: name, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		target := a.fixtures[host]
		if host == "127.0.0.1" {
			target = first
		}
		if target == nil {
			return nil, fmt.Errorf("unknown instance %s", host)
		}
		return (&net.Dialer{}).DialContext(ctx, network, target.server.Listener.Addr().String())
	}})
}
func TestSupabaseRestartRequiresEveryComponentInstance(t *testing.T) {
	for _, scenario := range []string{"success", "rollout-extra", "no-op", "unhealthy", "stale-config", "lost-reply", "disappeared", "legacy-checkpoint", "reordered-unready", "active-status-error", "active-native-error", "active-stopped", "completed-replaced", "completed-extra", "completed-removed"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			for name, count := range map[string]int{"auth": 2, "realtime": 1} {
				spec := rc.Cluster.Spec.Components.Extra[name]
				spec.Replicas = count
				rc.Cluster.Spec.Components.Extra[name] = spec
			}
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			inventory := restartInventory{testBackend: backend, instances: map[string][]infra.Container{}}
			agents := restartAgents{AgentDialer: rc.Agents, fixtures: map[string]*restartFixture{}, members: map[string]string{}}
			for component, m := range state.Members {
				count := m.Replicas
				if component == "auth" && scenario == "rollout-extra" {
					count++
				}
				for i := range count {
					id := fmt.Sprintf("%s-instance-%d", m.Name, i)
					f := newRestartFixture(t, component, m.ConfigVersion)
					agents.fixtures[id] = f
					agents.members[m.Name] = id
					inventory.instances[m.Name] = append(inventory.instances[m.Name], infra.Container{ID: id, Hostname: id, Status: infra.ContainerRunning})
				}
			}
			target := agents.fixtures["demoau-instance-1"]
			activeFence := scenario == "reordered-unready" || scenario == "active-status-error" || scenario == "active-native-error" || scenario == "active-stopped"
			completedChange := strings.HasPrefix(scenario, "completed-")
			if activeFence {
				target = agents.fixtures["demoau-instance-0"]
			}
			target.noOp = scenario == "no-op"
			target.unhealthy = scenario == "unhealthy" || scenario == "reordered-unready"
			target.statusError = scenario == "active-status-error"
			target.nativeError = scenario == "active-native-error"
			target.nativeMessage = "upstream connection unavailable"
			target.lostReply = scenario == "lost-reply"
			if scenario == "stale-config" {
				target.version--
			}
			rc.Backend = inventory
			rc.Agents = agents
			op := &domain.Operation{ID: "restart-stack", Kind: domain.OperationRestart}
			if scenario == "legacy-checkpoint" {
				op.Checkpoint = json.RawMessage(`{"order":["demop0","demopx"],"index":2}`)
			}
			done := false
			disappeared := false
			sawError := false
			changed := false
			sawWait := false
			for range 100 {
				// Only persisted cluster/operation state survives a control-plane restart.
				p = New(p.options)
				faultWasChanged := changed
				result, err := p.RunOperation(t.Context(), rc, op)
				if len(result.Checkpoint) > 0 {
					op.Checkpoint = result.Checkpoint
				}
				target.mu.Lock()
				launched := target.launches > 0
				if launched && activeFence && !changed {
					instances := inventory.instances["demoau"]
					instances[0], instances[1] = instances[1], instances[0]
					if scenario == "active-stopped" {
						target.process.State = "stopped"
					}
					changed = true
				}
				target.mu.Unlock()
				if activeFence && launched {
					sibling := agents.fixtures["demoau-instance-1"]
					sibling.mu.Lock()
					launches := sibling.launches
					sibling.mu.Unlock()
					if launches != 0 {
						t.Fatalf("restarted sibling while active target was incomplete: %s", scenario)
					}
				}
				if completedChange && !changed {
					var cp restartCheckpoint
					if e := json.Unmarshal(op.Checkpoint, &cp); e != nil {
						t.Fatal(e)
					}
					if len(cp.Completed["gateway"]) > 0 {
						switch scenario {
						case "completed-removed":
							inventory.instances["demogw"] = nil
						default:
							id := "demogw-replacement"
							f := newRestartFixture(t, "gateway", state.Members["gateway"].ConfigVersion)
							agents.fixtures[id] = f
							instance := infra.Container{ID: id, Hostname: id, Status: infra.ContainerRunning}
							if scenario == "completed-extra" {
								inventory.instances["demogw"] = append(inventory.instances["demogw"], instance)
							} else {
								inventory.instances["demogw"] = []infra.Container{instance}
							}
						}
						changed = true
					}
				}
				if err != nil {
					var wait *engine.PlatformRetryWait
					if errors.As(err, &wait) {
						sawWait = true
						if activeFence && launched && faultWasChanged {
							reason := map[string]string{
								"reordered-unready":   "upstream connection unavailable",
								"active-status-error": "agent status observation failed",
								"active-native-error": "native status observation failed",
								"active-stopped":      "not running",
							}[scenario]
							if !strings.Contains(wait.Message, "demoau-instance-0") || !strings.Contains(wait.Message, reason) {
								t.Fatalf("wait lost active target/reason: %s", wait.Message)
							}
						}
						if scenario == "stale-config" && strings.Contains(wait.Message, "configuration version") && !strings.Contains(wait.Message, "require") {
							t.Fatalf("wait lost config version expectation: %s", wait.Message)
						}
						continue
					}
					sawError = true
					if completedChange && changed && (strings.Contains(err.Error(), "inventory changed after restart verification") || strings.Contains(err.Error(), "disappeared before restart completion")) {
						break
					}
					if scenario == "disappeared" && strings.Contains(err.Error(), "disappeared during restart") {
						break
					}
					if scenario != "lost-reply" {
						t.Fatal(err)
					}
					continue
				}
				if len(result.Checkpoint) > 0 {
					op.Checkpoint = result.Checkpoint
				}
				if scenario == "disappeared" && !disappeared {
					var cp restartCheckpoint
					json.Unmarshal(op.Checkpoint, &cp)
					if _, ok := cp.Instances["demoau-instance-1"]; ok {
						inventory.instances["demoau"] = inventory.instances["demoau"][:1]
						// Keep the desired count satisfied so the explicit missing-target fence is exercised.
						spec := rc.Cluster.Spec.Components.Extra["auth"]
						spec.Replicas = 1
						rc.Cluster.Spec.Components.Extra["auth"] = spec
						disappeared = true
					}
				}
				if result.Done {
					done = true
					break
				}
			}
			shouldSucceed := scenario == "success" || scenario == "rollout-extra" || scenario == "lost-reply" || scenario == "legacy-checkpoint"
			if done != shouldSucceed {
				t.Fatalf("restart Done=%v, want %v (must not succeed without verified component restarts)", done, shouldSucceed)
			}
			if scenario == "disappeared" && (!disappeared || !sawError) {
				t.Fatal("missing instance was not diagnosed")
			}
			if scenario == "lost-reply" && !sawError {
				t.Fatal("accepted-response loss was not exercised")
			}
			if (activeFence || completedChange) && !changed {
				t.Fatal("review regression fault was not exercised")
			}
			if activeFence && !sawWait {
				t.Fatal("active-target blocker did not produce an accounted wait")
			}
			if completedChange && !sawError {
				t.Fatal("completed component inventory change was not rejected")
			}
			if shouldSucceed {
				for id, f := range agents.fixtures {
					f.mu.Lock()
					launches := f.launches
					f.mu.Unlock()
					if launches != 1 {
						t.Errorf("%s restarted %d times, want exactly once", id, launches)
					}
				}
			}
		})
	}
}
