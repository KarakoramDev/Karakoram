// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type proxyAdmissionSyncBackend struct {
	*fakeBackend
	proxy     string
	admitted  atomic.Bool
	fenced    atomic.Bool // Durable provider authority, outside the reconstructed CP.
	retried   atomic.Bool
	receipt   infra.OpState
	incumbent bool
}

func (b *proxyAdmissionSyncBackend) Capabilities() infra.Capabilities {
	c := b.fakeBackend.Capabilities()
	c.ProxyDeploymentPreservesServing = true
	return c
}
func (*proxyAdmissionSyncBackend) ProxyUpdatesPreserveServing(context.Context, string) (bool, error) {
	return true, nil
}
func (b *proxyAdmissionSyncBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name != b.proxy {
		return b.fakeBackend.Containers(ctx, name)
	}
	status := infra.ContainerStarting
	if b.admitted.Load() {
		status = infra.ContainerRunning
	}
	tasks := []infra.Container{{ID: b.taskID(), Hostname: "unpublished.invalid", IPv4: "127.0.0.1", Status: status, ProxyServingAdmission: !b.fenced.Load()}}
	if b.incumbent {
		tasks = append(tasks, infra.Container{ID: "incumbent", IPv4: "127.0.0.2", Status: infra.ContainerRunning})
	}
	return tasks, nil
}
func (b *proxyAdmissionSyncBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "proxy-receipt" {
		if b.receipt.Failed {
			b.fenced.Store(true)
		}
		return b.receipt, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func (b *proxyAdmissionSyncBackend) taskID() string {
	if b.retried.Load() {
		return "retry"
	}
	return "replacement"
}

func (b *proxyAdmissionSyncBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	op, err := b.fakeBackend.SetContainerCount(ctx, name, min, max)
	if err == nil {
		b.retried.Store(true)
		b.fenced.Store(false)
		b.admitted.Store(false)
	}
	return op, err
}

type proxyAdmissionSyncAgents struct {
	engine.AgentDialer
	member  string
	options agentapi.ClientOptions
}

func (a proxyAdmissionSyncAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == a.member {
		return agentapi.NewClient(a.options)
	}
	return a.AgentDialer.Dial(ctx, ref, name)
}

func TestProxyAdmissionThroughCoreSync(t *testing.T) {
	for _, scenario := range []string{"upgrade-agents", "upgrade-agents-paused", "joined", "deployed", "completed-scale", "completed-resize", "completed-deploy", "failed-scale", "failed-resize", "failed-deploy"} {
		t.Run(scenario, func(t *testing.T) {
			topology := "single"
			if strings.HasPrefix(scenario, "upgrade-agents") {
				topology = "group"
			}
			p, rc, base := setup(t, topology)
			setProxyScaleTarget(rc, 1, false)
			converge(t, p, rc, base)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			var m *member
			for _, candidate := range s.Members {
				if candidate.Role == "proxy" {
					m = candidate
				}
			}
			if m == nil {
				t.Fatal("missing proxy")
			}
			backend := &proxyAdmissionSyncBackend{fakeBackend: base, proxy: m.Name, receipt: infra.OpState{Done: true}}
			observe := func() error {
				_, err := p.Sync(t.Context(), rc)
				return err
			}
			var upgrade *domain.Operation
			if strings.HasPrefix(scenario, "upgrade-agents") {
				serving := true
				checkpoint, err := json.Marshal(agentUpgradeCheckpoint{
					Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}, Order: []string{m.Name},
					Member: m.Name, Stage: "waiting", BeforeBuilds: map[string]string{"incumbent": "7873aa42"},
					ProxyServing: &serving, Deploy: &infra.Op{ID: "proxy-receipt"},
				})
				if err != nil {
					t.Fatal(err)
				}
				backend.receipt.Done = false
				backend.incumbent = true
				upgrade = &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name,
					Kind: domain.OperationUpgradeAgents, Checkpoint: checkpoint}
				observe = func() error {
					result, err := p.RunOperation(t.Context(), rc, upgrade)
					if result.Checkpoint != nil {
						upgrade.Checkpoint = result.Checkpoint
					}
					if err == nil && (result.RequeueAfter <= 0 || result.RequeueAfter > 2*time.Second) {
						t.Errorf("pending upgrade lost bounded admission cadence: %s", result.RequeueAfter)
					}
					return err
				}
			}
			switch scenario {
			case "deployed":
				m.Stage = "deployed"
			case "completed-scale", "completed-resize", "failed-scale", "failed-resize":
				stage := "scaled"
				if scenario == "completed-resize" || scenario == "failed-resize" {
					stage = "resource-scaled"
				}
				s.Rolling = &rollingState{From: m.Version, To: m.Version, Order: []string{m.Name}, Stage: stage, Targets: map[string]domain.ComponentSpec{m.Name: desiredComponent(rc, m)}, Op: &infra.Op{ID: "proxy-receipt"}}
			case "completed-deploy", "failed-deploy":
				m.Stage = "deploying"
				m.DeployOp = &infra.Op{ID: "proxy-receipt"}
			}
			if strings.HasPrefix(scenario, "failed-") {
				backend.receipt = infra.OpState{Failed: true, Message: "rollout paused"}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			access, err := accessRevision(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			revision := max(m.ConfigVersion, access)
			var posts atomic.Int32
			var applied atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/proxy-serving":
					if r.Method == http.MethodGet {
						version := int64(0)
						if applied.Load() {
							version = revision
						}
						json.NewEncoder(w).Encode(agentapi.ProxyServingProof{TaskID: backend.taskID(), Member: m.Name, ConfigVersion: version, Ready: applied.Load(), Admitted: backend.admitted.Load()})
						return
					}
					var pair agentapi.ProxyServingAdmission
					if err := json.NewDecoder(r.Body).Decode(&pair); err != nil {
						t.Error(err)
					}
					if pair.TaskID != backend.taskID() || pair.ConfigVersion != revision {
						t.Errorf("wrong admission %+v", pair)
					}
					posts.Add(1)
					backend.admitted.Store(true)
					w.WriteHeader(http.StatusNoContent)
				case "/v1/status":
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: revision, EngineVersion: m.Version})
				case "/v1/mysql/proxy-status":
					json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{admitted(s.data()[0].Name, myspec.ProxyWriterGroup, "ONLINE")}})
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer server.Close()
			_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			options := agentapi.ClientOptions{BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Token: m.Name, DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
				if instance != "127.0.0.1" {
					t.Errorf("serving pre-D006 incumbent was probed for admission: %s", instance)
				}
				return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(instance, port))
			}}
			rc.Backend = backend
			rc.Agents = proxyAdmissionSyncAgents{AgentDialer: rc.Agents, member: m.Name, options: options}
			// The real agent publishes TLS before a successful first engine apply.
			// A cold ProxySQL admin connection race therefore exposes revision 0,
			// not the desired revision synthesized by the original fixture.
			if !strings.HasPrefix(scenario, "failed-") {
				if err := observe(); err != nil {
					t.Fatalf("unapplied startup entered error backoff: %v", err)
				}
				if posts.Load() != 0 {
					t.Fatal("unapplied revision authorized")
				}
			}
			if scenario == "upgrade-agents-paused" {
				backend.receipt = infra.OpState{Failed: true, Message: "proxy rollout paused"}
				stops := len(base.stopped)
				runPausedProxyUpgrade(t, p, rc, upgrade)
				if posts.Load() != 0 || backend.admitted.Load() || applied.Load() {
					t.Fatal("paused unapplied upgrade admitted a replacement")
				}
				tasks, err := backend.Containers(t.Context(), m.Name)
				if err != nil || len(tasks) != 2 || tasks[1].ID != "incumbent" || tasks[1].Status != infra.ContainerRunning || len(base.stopped) != stops {
					t.Fatalf("paused upgrade did not preserve its serving incumbent: tasks=%+v err=%v", tasks, err)
				}
				return
			}
			applied.Store(true)
			err = observe()
			if strings.HasPrefix(scenario, "failed-") {
				if (scenario == "failed-deploy") != (err != nil) || posts.Load() != 0 {
					t.Fatalf("failed receipt bypassed: err=%v posts=%d", err, posts.Load())
				}
				for range 2 {
					raw, err := json.Marshal(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					var restored domain.Cluster
					if err := json.Unmarshal(raw, &restored); err != nil {
						t.Fatal(err)
					}
					// Internal is stored separately from the public cluster JSON.
					restored.Internal = append(json.RawMessage(nil), rc.Cluster.Internal...)
					rc.Cluster = &restored
					p = New(p.options)
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if posts.Load() != 0 {
						t.Fatal("failed roll admitted after cleanup/CP reconstruction")
					}
				}
				op, err := backend.SetContainerCount(t.Context(), m.Name, 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				proxy := current.find(m.Name)
				proxy.Stage, proxy.DeployOp = "joined", nil
				revision = max(proxy.ConfigVersion, access)
				current.Rolling = &rollingState{From: rc.Cluster.Spec.Version, To: rc.Cluster.Spec.Version, Order: []string{proxy.Name}, Stage: "scaled", Targets: map[string]domain.ComponentSpec{proxy.Name: desiredComponent(rc, proxy)}, Op: &op}
				if err := save(rc.Cluster, current); err != nil {
					t.Fatal(err)
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if posts.Load() != 1 {
					t.Fatal("accepted retry did not admit its new task")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if posts.Load() != 1 {
				t.Fatalf("Core.Sync did not admit %s replacement: posts=%d", scenario, posts.Load())
			}
			current, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if current.Rolling != nil && (current.Rolling.Stage == "scaled" || current.Rolling.Stage == "resource-scaled") {
				t.Fatal("completed receipt remained blocked on Starting task")
			}
			if scenario == "deployed" && current.find(m.Name).Stage != "joined" {
				t.Fatal("configuration readiness wait did not release after admission")
			}
		})
	}
}

func runPausedProxyUpgrade(t *testing.T, p engine.Provider, rc *engine.Context, op *domain.Operation) {
	t.Helper()
	st := rc.Store.(*store.Store)
	if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	op.Phase = domain.PhaseRunning
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := reconciler.New(reconciler.Options{Store: st, Registry: registry,
		Context: *rc, ResyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for {
		current, err := st.GetOperation(t.Context(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Terminal() {
			if current.Phase != domain.PhaseFailed || current.CompletedAt == nil || !strings.Contains(current.Message, "proxy rollout paused") || !strings.Contains(current.Message, "manual recovery") {
				t.Fatalf("paused receipt did not fail the operation: %+v", current)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("paused upgrade remained %s: %s", current.Phase, current.Message)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
