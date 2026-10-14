// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type cleanupAgentDialer struct {
	url string
	ca  []byte
}

func (d cleanupAgentDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test", CAPEM: d.ca})
}

type cleanupBackend struct {
	infra.Backend
	starts int
	polls  int
}

func (b *cleanupBackend) Start(ctx context.Context, _ string) (infra.Op, error) {
	if err := ctx.Err(); err != nil {
		return infra.Op{}, err
	}
	b.starts++
	return infra.Op{ID: "router-start"}, nil
}
func (b *cleanupBackend) OpStatus(ctx context.Context, _ infra.Op) (infra.OpState, error) {
	if err := ctx.Err(); err != nil {
		return infra.OpState{}, err
	}
	b.polls++
	return infra.OpState{Done: true}, nil
}

func TestExpiredRestoreRunsCompensationBeforeFailure(t *testing.T) {
	for _, expiry := range []string{"deadline", "retry budget"} {
		for _, step := range []string{"poll", "routers-off", "maintenance-on"} {
			t.Run(expiry+"/"+step, func(t *testing.T) {
				ctx := context.Background()
				m, st := newTestManager(t, &psmdb.Provider{})
				m.opts.OperationRetryLimit = 2
				backend := &cleanupBackend{}
				m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
				failures, starts, maintenanceOff, configs, restorePolls := 0, 0, 0, 0, 0
				agent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/status":
						_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
						return
					case "/v1/processes/mongod/start", "/v1/processes/pbm-agent/start":
						// Cleanup must outlive the exhausted retry budget too.
						if failures < 3 {
							failures++
							http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
							return
						}
						starts++
					case "/v1/maintenance":
						maintenanceOff++
					case "/v1/pbm":
						var req agentapi.PBMRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
							return
						}
						if req.Command == "config" {
							configs++
						}
						if req.Command == "describe-restore" {
							restorePolls++
						}
						_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{"running":{},"snapshots":[]}`)})
						return
					}
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{})
				}))
				t.Cleanup(agent.Close)
				m.opts.Context.Agents = cleanupAgentDialer{agent.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agent.Certificate().Raw})}
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}, Internal: json.RawMessage(`{"phase":"restore","members":[{"name":"m0","set":"rs0","role":"mongod","stage":"active"},{"name":"router","role":"mongos","stage":"active"}],"pbmMaintenance":{"m0":true},"pbmStopped":{"router":["mongos"]}}`)}
				if err := st.CreateCluster(ctx, c); err != nil {
					t.Fatal(err)
				}
				if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "storage", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
					t.Fatal(err)
				}
				if err := st.CreateBackup(ctx, &domain.Backup{Name: "base", Cluster: c.Name, Storage: "storage", Phase: domain.PhaseSucceeded}); err != nil {
					t.Fatal(err)
				}
				if err := st.CreateRestore(ctx, &domain.Restore{Name: "recover", Cluster: c.Name, Source: domain.RestoreSource{Backup: "base"}, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
				cp := map[string]any{"step": step, "member": "m0", "physical": true, "pbmName": "restore-task"}
				if step == "routers-off" {
					cp["routerOp"] = infra.Op{ID: "router-stop"}
				}
				raw, _ := json.Marshal(cp)
				op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Step: step, Checkpoint: raw, Payload: json.RawMessage(`{"restore":"recover"}`), CreatedAt: time.Now(), LastError: "PBM unavailable"}
				if expiry == "deadline" {
					op.CreatedAt = time.Now().Add(-7 * time.Hour)
				} else {
					op.Failures = m.opts.OperationRetryLimit
				}
				if err := st.EnqueueOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
				for attempt := range 60 {
					_, _ = m.reconcile(ctx, c.Ref())
					got, err := st.GetOperation(ctx, op.ID)
					if err != nil {
						t.Fatal(err)
					}
					if got.Phase == domain.PhaseFailed {
						if !strings.Contains(got.Message, expiry) || !strings.Contains(got.Message, "PBM unavailable") || got.CancelRequested || got.CompletedAt == nil {
							t.Fatal(got)
						}
						break
					}
					if got.Phase != domain.PhaseRunning || got.Step != failureCleanupStep || got.CompletedAt != nil {
						t.Fatal(got)
					}
					if attempt == 59 {
						t.Fatal("cleanup never completed", got)
					}
				}
				got, err := st.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				var state struct {
					Maintenance map[string]bool     `json:"pbmMaintenance"`
					Stopped     map[string][]string `json:"pbmStopped"`
				}
				if err := json.Unmarshal(got.Internal, &state); err != nil {
					t.Fatal(err)
				}
				if len(state.Maintenance) != 0 || len(state.Stopped) != 0 || starts != 2 || maintenanceOff != 1 || backend.starts != 1 || configs == 0 || restorePolls != 0 {
					t.Fatalf("incomplete compensation: state=%s starts=%d maintenance=%d routerStarts=%d configs=%d restorePolls=%d", got.Internal, starts, maintenanceOff, backend.starts, configs, restorePolls)
				}
				r, err := st.GetRestore(ctx, c.Ref(), "recover")
				if err != nil || r.Phase != domain.PhaseFailed || !strings.Contains(r.Error, expiry) {
					t.Fatal(r, err)
				}
			})
		}
	}
}

func TestExpiredReplaceMemberReleasesConvergenceGate(t *testing.T) {
	for _, expiry := range []string{"deadline", "retry budget"} {
		t.Run(expiry, func(t *testing.T) {
			ctx := context.Background()
			m, st := newTestManager(t, &psmdb.Provider{})
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}, Internal: json.RawMessage(`{"replaceActive":true,"members":[{"name":"old","role":"mongod"},{"name":"replacement","role":"mongod","stage":"planned"}]}`)}
			if err := st.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Step: "replacement-sync", Checkpoint: json.RawMessage(`{"stage":"join","newMember":"replacement"}`), Payload: json.RawMessage(`{"member":"old"}`), CreatedAt: time.Now(), LastError: "platform unavailable"}
			if expiry == "deadline" {
				op.CreatedAt = time.Now().Add(-3 * time.Hour)
			} else {
				op.Failures = m.opts.OperationRetryLimit
			}
			if err := st.EnqueueOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(ctx, op.ID)
			if err != nil || got.Phase != domain.PhaseFailed || !strings.Contains(got.Message, expiry) || got.CancelRequested {
				t.Fatal(got, err)
			}
			current, err := st.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				ReplaceActive bool `json:"replaceActive"`
			}
			if err := json.Unmarshal(current.Internal, &state); err != nil || state.ReplaceActive {
				t.Fatal(string(current.Internal), err)
			}
		})
	}
}

func TestSuccessfulOperationStepResetsFailureBudget(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	calls := 0
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		calls++
		if calls%2 == 1 {
			return engine.OpResult{}, errors.New("temporary failure")
		}
		return engine.OpResult{Done: calls == 6, Step: "progress"}, nil
	}
	m, st := newTestManager(t, p)
	m.opts.OperationRetryLimit = 2
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		_, _ = m.reconcile(ctx, c.Ref())
	}
	got, err := st.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.Failures != 0 || calls != 6 {
		t.Fatal(got, err, calls)
	}
}
