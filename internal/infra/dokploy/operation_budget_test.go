// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type operationBudgetRequestKey struct{}

type operationBudgetUnit struct {
	pass   *sync.Map
	caller string
}

type operationBudgetReceipt struct {
	app, action string
	issued      time.Time
}

type operationBudgetRecovery struct {
	pass      *sync.Map
	receipt   operationBudgetReceipt
	endpoints map[string]int
	completed bool
	wrote     bool
}

// Recovery's initial application reload runs on the caller's goroutine; later
// reads may run in ObserveParallel workers with the same stripped context.
// Recognize that context before applying its separate transaction budget. Other
// uncached availability reads keep their zero allowance. Resolve the full name
// from the method expression so a rename cannot silently disable attribution;
// CallersFrames handles inlining.
func operationBudgetRecoveryRead() bool {
	want := runtime.FuncForPC(reflect.ValueOf((*Backend).reconcileObservedService).Pointer()).Name()
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if frame.Function == want {
			return true
		}
		if !more {
			return false
		}
	}
}

// Exercise the real worker, queue, provider and backend; only platform task
// progression and native agent responses are simulated. Holding a rollout for a
// fixed interval detects repeated pending sweeps, not just backend call cost.
// Attribute observation GETs by the attempt-local cache, which is shared by
// concurrent member probes but never reused by a later sweep.
func TestManagerOperationRequestBudget(t *testing.T) {
	const hold = 20 * time.Second
	// A sweep can include native connection authorization and reloads, so task
	// and config reads include those costs. Bound descriptive inventory reads
	// separately: spare authorization capacity must not hide per-member reads.
	postgresBudget := map[string]int{
		"environment.one": 1, "application.one": 3, "cluster.getNodes": 1,
		"swarm.getNodeApps": 1, "deployment.all": 2, "docker.getConfig": 5,
		"docker.getServiceContainersByAppName": 8,
	}
	mysqlBudget := map[string]int{
		"environment.one": 3, "application.one": 7, "cluster.getNodes": 1,
		"swarm.getNodeApps": 2, "deployment.all": 4, "docker.getConfig": 9,
		"docker.getServiceContainersByAppName": 14,
	}
	for _, scenario := range []struct {
		kind, operation string
		budget          map[string]int
		reconcileBudget int
	}{
		{"postgres", "proxy-count", postgresBudget, 128}, {"mysql", "proxy-count", mysqlBudget, 80},
		{"postgres", "upgrade-agents", postgresBudget, 128},
	} {
		t.Run(scenario.kind+"-"+scenario.operation, func(t *testing.T) {
			kind := scenario.kind
			// Serialized mutation reads cannot use observation snapshots. Bound
			// each reconcile/operation attempt and each inbound config request,
			// rather than their wall-clock-dependent sum.
			uncachedBudget := map[string]int{
				infra.CallerReconcile: scenario.reconcileBudget,
				infra.CallerOperation: 40, infra.CallerAgentConfig: 8,
			}
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			x := newSteadyBudgetFixture(t, kind, b)
			x.rc.Cluster.Spec.Provider = kind
			x.rc.Cluster.Spec.DataMcp = nil
			x.rc.Artifact = budgetArtifact(renderTestArtifact(t))
			st := x.rc.Store.(*store.Store)
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			var connections sync.Map
			agents := x.rc.Agents.(*providerBudgetAgents)
			agents.connected = func(address, task string) { connections.Store(address, task) }
			old := x.handler
			x.handler = func(w http.ResponseWriter, r *http.Request) {
				c, err := st.GetCluster(r.Context(), x.rc.Cluster.Ref())
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				rc := *x.rc
				rc.Cluster = c
				var root map[string]any
				_ = json.Unmarshal(c.Internal, &root)
				version := int64(root[kind].(map[string]any)["configVersion"].(float64))
				member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/mysql/proxy-status":
					_ = json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Host: "samplem0", Hostgroup: myspec.ProxyWriterGroup, Status: "ONLINE"}}})
					return
				case "/v1/mysql/status":
					role := "SECONDARY"
					if member == "samplem0" {
						role = "PRIMARY"
					}
					_ = json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: role, GroupMembers: 3})
					return
				case "/v1/status":
					build := engine.CurrentAgentBuild()
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version, Agent: agentapi.AgentBuild{Build: build.Build, Version: build.Version, Commit: build.Commit}})
					return
				case "/v1/proxy-serving":
					task, _ := connections.Load(r.RemoteAddr)
					_ = json.NewEncoder(w).Encode(agentapi.ProxyServingProof{TaskID: fmt.Sprint(task), Member: member, ConfigVersion: version, Ready: true, Admitted: true})
					return
				case "/v1/reload":
					configCtx := context.WithValue(r.Context(), operationBudgetRequestKey{}, new(sync.Map))
					_, err := x.p.(engine.AgentConfigSource).MemberConfig(infra.WithRequestCaller(configCtx, infra.CallerAgentConfig), &rc, member)
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{})
					return
				}
				rec := httptest.NewRecorder()
				old(rec, r)
				for key, values := range rec.Header() {
					w.Header()[key] = values
				}
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
			}
			// Replace the server only for this lifecycle fixture. Status-only budget
			// fixtures retain their frozen revision and native response handlers.
			providerBudgetServer(t, x.rc, x.handler)
			x.rc.Agents.(*providerBudgetAgents).connected = agents.connected
			for _, member := range x.members {
				if err := st.SetAgentToken(ctx, &model.AgentToken{Cluster: "sample", Member: member, Token: member}); err != nil {
					t.Fatal(err)
				}
			}
			var root map[string]any
			_ = json.Unmarshal(x.rc.Cluster.Internal, &root)
			for _, raw := range root[kind].(map[string]any)["members"].([]any) {
				member := raw.(map[string]any)
				name := member["name"].(string)
				resources := x.rc.Cluster.Spec.Components.Engine.Resources
				if member["role"] == "proxy" {
					resources = x.rc.Cluster.Spec.Components.Extra["proxy"].Resources
					if kind == "mysql" {
						// Let the real provider deploy and freeze its runtime
						// fingerprint instead of inventing one in the fixture.
						member["stage"] = "imported"
					}
				}
				member["resources"] = resources
				if member["role"] == "data" {
					volume := name + "v"
					service, err := b.GetService(ctx, volume)
					if err != nil {
						t.Fatal(err)
					}
					member["volume"], member["volumeId"], member["disk"], member["initialized"] = volume, service.ID, resources, true
				}
			}
			x.rc.Cluster.Internal, _ = json.Marshal(root)
			if err := st.UpdateClusterSpec(ctx, x.rc.Cluster.Ref(), x.rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			c, err := st.GetCluster(ctx, x.rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			c.Internal = x.rc.Cluster.Internal
			c.Status = model.ClusterStatus{Phase: model.PhaseReady, ObservedGeneration: c.Generation}
			if err := st.UpdateClusterState(ctx, c); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			for _, app := range f.apps {
				for _, task := range app.tasks {
					if task["state"] == "running" {
						task["health"] = "healthy"
					}
				}
			}
			f.mu.Unlock()
			var sweepsMu sync.Mutex
			sweeps := make(map[*sync.Map]map[string]int)
			receipts := make(map[*sync.Map]map[string]operationBudgetReceipt)
			recoveries := make(map[context.Context]*operationBudgetRecovery)
			uncached := make(map[operationBudgetUnit]map[string]int)
			uncachedCallers := make(map[string]int)
			rolloutHeld, heldSweeps := false, 0
			transport := b.c.http.Transport
			if transport == nil {
				transport = http.DefaultTransport
			}
			b.c.http.Transport = clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				endpoint := strings.TrimPrefix(r.URL.Path, "/api/")
				pass := engine.ObservationPassCache(r.Context())
				cache := infra.ObservationCache(r.Context())
				availability := infra.RequestCaller(r.Context()) == infra.CallerAvailability
				sweepsMu.Lock()
				recovery := recoveries[r.Context()]
				if recovery != nil && r.Method != http.MethodGet {
					recovery.wrote = true
				}
				if r.Method == http.MethodGet {
					if availability && cache == nil && recovery == nil && endpoint == "application.one" && operationBudgetRecoveryRead() {
						if receipt, ok := receipts[pass][r.URL.Query().Get("applicationId")]; ok {
							recovery = &operationBudgetRecovery{pass: pass, receipt: receipt, endpoints: make(map[string]int)}
							recoveries[r.Context()] = recovery
						}
					}
					if recovery != nil {
						recovery.endpoints[endpoint]++
					} else if cache != nil {
						if sweeps[cache] == nil {
							sweeps[cache] = make(map[string]int)
							if rolloutHeld {
								heldSweeps++
							}
						}
						sweeps[cache][endpoint]++
					} else {
						if pass == nil {
							pass, _ = r.Context().Value(operationBudgetRequestKey{}).(*sync.Map)
						}
						unit := operationBudgetUnit{pass, infra.RequestCaller(r.Context())}
						if uncached[unit] == nil {
							uncached[unit] = make(map[string]int)
						}
						uncached[unit][endpoint]++
						uncachedCallers[unit.caller]++
					}
				}
				sweepsMu.Unlock()
				response, err := transport.RoundTrip(r)
				if err != nil || !availability || endpoint != "application.one" || response.StatusCode != http.StatusOK {
					return response, err
				}
				// Inspect only synthetic application metadata, then replay the body
				// unchanged. Recovery must be justified by a receipt this availability
				// pass actually observed, not merely by the recovery function's name.
				data, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(data))
				var app application
				if json.Unmarshal(data, &app) == nil {
					if metadata, ok := parseMetadata(app.Description); ok {
						sweepsMu.Lock()
						if cache != nil && metadata.Pending != nil {
							if receipts[pass] == nil {
								receipts[pass] = make(map[string]operationBudgetReceipt)
							}
							pending := metadata.Pending
							receipts[pass][app.ID] = operationBudgetReceipt{app.ID, pending.Action, pending.Issued}
						}
						if recovery != nil && app.ID == recovery.receipt.app && metadata.Pending == nil {
							recovery.completed = true
						}
						sweepsMu.Unlock()
					}
				}
				return response, nil
			})
			reg, err := engine.NewRegistry(x.p)
			if err != nil {
				t.Fatal(err)
			}
			manager, err := reconciler.New(reconciler.Options{Store: st, Registry: reg, Context: *x.rc})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- manager.Run(ctx) }()
			stop := sync.OnceFunc(func() { cancel(); <-done })
			defer stop()
			deadline := time.Now().Add(20 * time.Second)
			for {
				c, err = st.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if manager.SteadyConfigBaseline(ctx, c) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("baseline did not converge: %+v", c.Status)
				}
				time.Sleep(25 * time.Millisecond)
			}
			counts := countProviderGETs(f)
			sweepsMu.Lock()
			clear(sweeps)
			clear(uncached)
			clear(uncachedCallers)
			clear(receipts)
			clear(recoveries)
			rolloutHeld = true
			sweepsMu.Unlock()
			f.mu.Lock()
			for _, app := range f.apps {
				metadata, _ := parseMetadata(fakeString(app.row, "description"))
				if metadata.Hostname == x.proxy {
					app.holdTasks = true
				}
			}
			f.mu.Unlock()
			op := &model.Operation{Kind: model.OperationUpdate}
			started := time.Now()
			if scenario.operation == "proxy-count" {
				proxy := c.Spec.Components.Extra["proxy"]
				proxy.Replicas++
				c.Spec.Components.Extra["proxy"] = proxy
				if err := st.UpdateClusterSpec(store.WithOperation(ctx, op), c.Ref(), c.Spec); err != nil {
					t.Fatal(err)
				}
			} else {
				op.Kind, op.Cluster = model.OperationUpgradeAgents, c.Name
				op.Payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
				if err := st.EnqueueOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
			}
			manager.Trigger(c.Ref())
			release := time.AfterFunc(hold, func() {
				sweepsMu.Lock()
				rolloutHeld = false
				sweepsMu.Unlock()
				f.mu.Lock()
				defer f.mu.Unlock()
				for _, app := range f.apps {
					metadata, _ := parseMetadata(fakeString(app.row, "description"))
					if metadata.Hostname == x.proxy {
						app.holdTasks = false
					}
				}
			})
			defer release.Stop()
			for {
				got, err := st.GetOperation(ctx, op.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.CompletedAt != nil {
					if !operationBudgetCompletedWithin(started, *got.CompletedAt, 55*time.Second) {
						t.Fatalf("operation completed outside its wall ceiling: started=%s completed=%s", started, *got.CompletedAt)
					}
					if got.Phase != model.PhaseSucceeded || got.Failures != 0 {
						t.Fatalf("operation: %+v", got)
					}
					t.Logf("attempts=%d failures=%d", got.Attempts, got.Failures)
					break
				}
				if time.Since(started) > 55*time.Second {
					t.Fatalf("operation did not finish: %+v", got)
				}
				time.Sleep(25 * time.Millisecond)
			}
			if time.Since(started) < hold {
				t.Fatal("operation bypassed the held platform rollout")
			}
			// Completion wakes an immediate ordinary reconcile. Do not cancel it
			// at an arbitrary HTTP request while the 25 ms store poll catches up:
			// include that whole pass, ending at the same steady fence as setup.
			deadline = time.Now().Add(20 * time.Second)
			for {
				c, err = st.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if manager.SteadyConfigBaseline(ctx, c) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("completed operation did not reach steady baseline: %+v", c.Status)
				}
				time.Sleep(25 * time.Millisecond)
			}
			stop()
			counts.mu.Lock()
			total := requestTotal(counts.endpoints)
			deployments := counts.requests["POST application.dropDeployment"]
			counts.mu.Unlock()
			if scenario.operation == "upgrade-agents" && deployments < 2 {
				t.Fatalf("agent roll did not deploy both data and proxy runtimes: deployments=%d", deployments)
			}
			// A worker pass has pre/post sweeps; availability has one sweep per
			// ReadinessInterval. Receipt polling ramps through 2, 3, 4 seconds
			// to that interval. Three startup passes cover the ramp and setup.
			workerPasses := 3 + int(hold/engine.ReadinessInterval)
			sweepsPerPass := 2
			if scenario.operation == "upgrade-agents" {
				// Agent upgrades have one post-attempt observation and intentionally
				// retain their native 2-second cadence, unlike proxy-count receipts.
				workerPasses = 3 + int(hold/(2*time.Second))
				sweepsPerPass = 1
			}
			maxHeldSweeps := sweepsPerPass*workerPasses + int(hold/engine.ReadinessInterval)
			if heldSweeps < 2 || heldSweeps > maxHeldSweeps {
				t.Errorf("held rollout sweeps=%d, want 2..%d", heldSweeps, maxHeldSweeps)
			}
			histogram := make(map[int]int)
			peak := make(map[string]int)
			for _, endpoints := range sweeps {
				histogram[requestTotal(endpoints)]++
				for endpoint, n := range endpoints {
					peak[endpoint] = max(peak[endpoint], n)
					if n > scenario.budget[endpoint] {
						t.Errorf("sweep GET %s=%d exceeds %d (sweep GETs=%d endpoints=%v)", endpoint, n, scenario.budget[endpoint], requestTotal(endpoints), endpoints)
					}
				}
			}
			t.Logf("GETs=%d elapsed=%s sweeps=%d held=%d per-sweep GETs:count=%v", total, time.Since(started), len(sweeps), heldSweeps, histogram)
			// The engine pass identifies one availability renewal tick, including
			// its retry. Repeated ticks may legitimately poll a still-pending
			// receipt; a tick may recover it once pending and once on completion.
			type recoveryTick struct {
				pass    *sync.Map
				receipt operationBudgetReceipt
			}
			ticks := make(map[recoveryTick][2]int)
			distinctReceipts := make(map[operationBudgetReceipt]bool)
			recoveryGETs := 0
			for _, recovery := range recoveries {
				key := recoveryTick{recovery.pass, recovery.receipt}
				counts := ticks[key]
				stage := 0
				if recovery.completed {
					stage = 1
				}
				counts[stage]++
				ticks[key] = counts
				if counts[stage] > 1 {
					t.Errorf("availability receipt recovery contexts in one tick=%d exceeds 1 (completed=%t)", counts[stage], recovery.completed)
				}
				distinctReceipts[recovery.receipt] = true
				endpoints := recovery.endpoints
				// Pending receipt: reload + receipt application, tasks, inventory,
				// inspect. A concurrently cleared receipt instead reads deployments.
				budget := map[string]int{"application.one": 2, "deployment.all": 1,
					"docker.getServiceContainersByAppName": 1, "swarm.getNodeApps": 1, "docker.getConfig": 1}
				limit := 5
				if recovery.wrote {
					// Completing our own receipt also reloads/persists metadata,
					// inspects the applied image and then observes the runtime fresh.
					budget["application.one"], budget["deployment.all"] = 4, 2
					budget["swarm.getNodeApps"], budget["docker.getConfig"] = 3, 3
					budget["docker.getServiceContainersByAppName"] = 2
					limit = 14
					if scenario.operation == "upgrade-agents" {
						// A data runtime additionally validates its retained volume.
						budget["application.one"], budget["environment.one"], budget["cluster.getNodes"] = 5, 1, 1
						limit = 17
					}
				}
				if !recovery.wrote && recovery.completed && scenario.operation == "upgrade-agents" {
					budget["application.one"], budget["environment.one"], budget["cluster.getNodes"] = 3, 1, 1
					limit = 8
				}
				n := requestTotal(endpoints)
				recoveryGETs += n
				if n > limit {
					t.Errorf("availability recovery GETs=%d exceeds %d: %v", n, limit, endpoints)
				}
				for endpoint, count := range endpoints {
					if count > budget[endpoint] {
						t.Errorf("availability recovery GET %s=%d exceeds %d: %v", endpoint, count, budget[endpoint], endpoints)
					}
				}
			}
			maxReceipts := 1
			if scenario.operation == "upgrade-agents" {
				maxReceipts = 2 // One data runtime and one proxy runtime.
			}
			if len(distinctReceipts) > maxReceipts {
				t.Errorf("availability recovery receipts=%d exceeds %d", len(distinctReceipts), maxReceipts)
			}
			t.Logf("availability recovery passes=%d receipts=%d GETs=%d (separately bounded)", len(recoveries), len(distinctReceipts), recoveryGETs)
			t.Logf("per-sweep peak=%v budget=%v", peak, scenario.budget)
			uncachedTotal := 0
			uncachedPeak := make(map[string]int)
			uncachedHistogram := make(map[int]int)
			uncachedCallerPeak := make(map[string]int)
			for unit, endpoints := range uncached {
				n := requestTotal(endpoints)
				uncachedCallerPeak[unit.caller] = max(uncachedCallerPeak[unit.caller], n)
				if unit.pass == nil {
					t.Errorf("unattributed uncached GETs: caller=%s endpoints=%v", unit.caller, endpoints)
				}
				if n > uncachedBudget[unit.caller] {
					t.Errorf("uncached %s pass GETs=%d exceeds %d: %v", unit.caller, n, uncachedBudget[unit.caller], endpoints)
				}
				uncachedTotal += n
				uncachedHistogram[n]++
				for endpoint, n := range endpoints {
					uncachedPeak[endpoint] = max(uncachedPeak[endpoint], n)
				}
			}
			t.Logf("uncached GETs=%d callers=%v per-pass GETs:count=%v endpoint peak=%v", uncachedTotal, uncachedCallers, uncachedHistogram, uncachedPeak)
			t.Logf("uncached per-pass peak=%v budget=%v held sweep ceiling=%d", uncachedCallerPeak, uncachedBudget, maxHeldSweeps)
		})
	}
}

func operationBudgetCompletedWithin(started, completed time.Time, limit time.Duration) bool {
	elapsed := completed.Sub(started)
	return elapsed >= 0 && elapsed <= limit
}

func TestOperationBudgetCompletionWallCeiling(t *testing.T) {
	started := time.Unix(1, 0)
	for _, tc := range []struct {
		elapsed time.Duration
		want    bool
	}{
		{-time.Nanosecond, false}, {0, true}, {55 * time.Second, true},
		{55*time.Second + time.Nanosecond, false}, {90 * time.Second, false},
	} {
		got := operationBudgetCompletedWithin(started, started.Add(tc.elapsed), 55*time.Second)
		if got != tc.want {
			t.Fatalf("elapsed=%s accepted=%v, want %v", tc.elapsed, got, tc.want)
		}
	}
}
