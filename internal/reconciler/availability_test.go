// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"modernc.org/sqlite"
)

type blockedSyncProvider struct {
	*fakeProvider
	started      chan struct{}
	startOnce    sync.Once
	duration     time.Duration
	unavailable  atomic.Bool
	observations atomic.Int32
}

func (p *blockedSyncProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	// This state is not checkpointed. The observer must load its own durable
	// snapshot, not copy this concurrently mutated lifecycle context.
	rc.Cluster.Internal = json.RawMessage(`{"sync":"unpublished"}`)
	p.startOnce.Do(func() { close(p.started) })
	duration := p.duration
	if duration == 0 {
		duration = 20 * time.Second
	}
	select {
	case <-ctx.Done():
		return engine.Result{}, ctx.Err()
	case <-time.After(duration):
	}
	return engine.Result{RequeueAfter: time.Minute}, nil
}

func (p *blockedSyncProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	p.observations.Add(1)
	if p.unavailable.Load() {
		return domain.ClusterStatus{}, errors.New("agent unreachable")
	}
	if rc.AvailabilityOnly {
		if string(rc.Cluster.Internal) != `{"checkpoint":true}` {
			return domain.ClusterStatus{}, errors.New("observer inherited uncheckpointed Sync state")
		}
		// Status may calculate provider drift. An independent availability
		// observer must never save that provider-owned state over Sync.
		rc.Cluster.Internal = json.RawMessage(`{"observation":"must-not-persist"}`)
	}
	return domain.ClusterStatus{
		Phase:      domain.PhaseReady,
		Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}},
		Members:    []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}},
	}, nil
}

func TestAvailabilityRemainsIndependentOfTwentySecondSync(t *testing.T) {
	for _, dead := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "dead-agent"}[dead], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &blockedSyncProvider{fakeProvider: &fakeProvider{}, started: make(chan struct{})}
				m, st := newTestManager(t, p)
				m.opts.Context.ObservationEpoch = "current-process"
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
					Internal: json.RawMessage(`{"checkpoint":true}`),
					Status:   domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				w := &worker{trigger: make(chan struct{}, 1)}
				m.wg.Add(1)
				go m.work(ctx, c.Ref(), w)
				defer func() { cancel(); m.wg.Wait() }()
				<-p.started
				p.unavailable.Store(dead)
				for second := 1; second <= 19; second++ {
					time.Sleep(time.Second)
					synctest.Wait()
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					fresh := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
					if !dead && (fresh.Phase != domain.PhaseReady || !fresh.Members[0].Health || !fresh.Members[0].Agent.Healthy || fresh.ObservationEpoch != "current-process") {
						t.Fatalf("healthy availability starved at %ds: %+v", second, fresh)
					}
					if dead && second > int(engine.ReadinessMaxAge/time.Second) && (fresh.Phase == domain.PhaseReady || !fresh.ObservedAt.IsZero() || fresh.Members[0].Agent.Healthy) {
						t.Fatalf("unreachable agent renewed its lease at %ds: %+v", second, fresh)
					}
					if string(stored.Internal) != `{"checkpoint":true}` || stored.Status.ObservedGeneration != 1 {
						t.Fatal("availability changed lifecycle checkpoint or applied generation", stored)
					}
				}
				if p.observations.Load() < 4 {
					t.Fatal("independent observations did not run while Sync was blocked")
				}
				// Let the twenty-second Sync finish. Its final checkpoint, not the
				// observer's provider-local drift, must remain authoritative.
				time.Sleep(2 * time.Second)
				synctest.Wait()
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil || string(stored.Internal) != `{"sync":"unpublished"}` {
					t.Fatal("Sync checkpoint was overwritten by availability", stored, err)
				}
			})
		})
	}
}

func TestStageTimingUsesMonotonicDurationAndSanitizedFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _ := newTestManager(t, &fakeProvider{})
		var logs bytes.Buffer
		m.opts.Context.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		ref := domain.ClusterRef{Name: "demo"}
		m.logStage(ref, "provider.sync", time.Now())
		if !strings.Contains(logs.String(), `"level":"DEBUG"`) || !strings.Contains(logs.String(), `"stage":"provider.sync"`) {
			t.Fatal("short stages lack debug attribution", logs.String())
		}
		logs.Reset()
		started := time.Now()
		time.Sleep(20 * time.Second)
		m.logStage(ref, "provider.sync", started)
		var record map[string]any
		if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["level"] != "WARN" || record["duration"] != float64(20*time.Second) || record["stage"] != "provider.sync" {
			t.Fatal("long stage did not report monotonic lease overrun", record)
		}
		for key := range record {
			switch key {
			case "time", "level", "msg", "cluster", "stage", "duration":
			default:
				t.Fatal("stage timing exposed non-timing data", key)
			}
		}
	})
}

func TestIndependentAvailabilityRetainsLifecycleOwnership(t *testing.T) {
	for _, scenario := range []string{"creating-without-members", "restoring", "suspended", "deleting", "running-operation", "operation-starts-during-probe", "missing-namespace"} {
		t.Run(scenario, func(t *testing.T) {
			p := &operationProvider{fakeProvider: &fakeProvider{}}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":true}`),
				Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
			switch scenario {
			case "creating-without-members":
				c.Status.Phase = domain.PhaseCreating
			case "restoring":
				c.Status.Phase = domain.PhaseRestoring
			case "suspended":
				c.Status.Phase = domain.PhaseSuspended
			case "deleting":
				c.Deleting = true
			case "missing-namespace":
				c.Namespace = "broken"
				if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: c.Namespace, Phase: "Ready"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if scenario == "running-operation" {
				if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			probes := 0
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				probes++
				if scenario != "operation-starts-during-probe" {
					t.Fatal("independent observation bypassed a lifecycle/dependency fence")
				}
				if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
				return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
			}
			err = m.renewAvailability(t.Context(), c.Ref())
			if (err != nil) != (scenario == "missing-namespace") {
				t.Fatal("unexpected independent observation error", err)
			}
			if probes != map[bool]int{false: 0, true: 1}[scenario == "operation-starts-during-probe"] {
				t.Fatal("unexpected probe count", probes)
			}
			after, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || after.Status.Phase != before.Status.Phase || !after.UpdatedAt.Equal(before.UpdatedAt) || string(after.Internal) != string(before.Internal) || !after.Status.ObservedAt.IsZero() {
				t.Fatal("independent observer changed lifecycle-owned state", after, err)
			}
		})
	}
}

func TestObservationAndPublicationAreArbitratedInBothDirections(t *testing.T) {
	for _, renewalFirst := range []bool{false, true} {
		for _, firstHealthy := range []bool{false, true} {
			name := map[bool]string{false: "ordinary-first", true: "renewal-first"}[renewalFirst] +
				map[bool]string{false: "/failure-before-success", true: "/success-before-failure"}[firstHealthy]
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := &operationProvider{fakeProvider: &fakeProvider{}}
					m, st := newTestManager(t, p)
					m.opts.Context.ObservationEpoch = "current-process"
					c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":true}`),
						Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
					if err := st.CreateCluster(t.Context(), c); err != nil {
						t.Fatal(err)
					}
					gate := make(chan struct{}, 1)
					normalCtx := withObservationPublication(t.Context(), gate)
					renewalCtx := withObservationPublication(t.Context(), gate)
					entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
					release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
					var renewalOnce sync.Once
					var probes [2]atomic.Int32
					healthy := func(renewal bool) bool {
						if renewal == renewalFirst {
							return firstHealthy
						}
						return !firstHealthy
					}
					p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
						index := 0
						if rc.AvailabilityOnly {
							index = 1
							renewalOnce.Do(func() {
								close(entered[1])
								<-release[1]
							})
						}
						probes[index].Add(1)
						if !healthy(rc.AvailabilityOnly) {
							return domain.ClusterStatus{}, errors.New("agent unavailable")
						}
						return domain.ClusterStatus{Phase: domain.PhaseReady,
							Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
							Members:    []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}}}, nil
					}
					done := [2]chan error{make(chan error, 1), make(chan error, 1)}
					normal := func() {
						if err := holdObservation(normalCtx); err != nil {
							done[0] <- err
							return
						}
						defer releaseObservation(normalCtx)
						current, err := st.GetCluster(normalCtx, c.Ref())
						if err != nil {
							done[0] <- err
							return
						}
						rc := m.opts.Context
						rc.Cluster = current
						status, _ := m.observeStatus(normalCtx, p, &rc)
						// Reproduce the reviewed gap AFTER a complete ordinary
						// sample but BEFORE its unconditional lifecycle write.
						close(entered[0])
						<-release[0]
						status.ObservedGeneration = current.Status.ObservedGeneration
						current.Status = status
						current.Internal = json.RawMessage(`{"ordinary":"owns-checkpoint"}`)
						done[0] <- m.state(normalCtx, current, domain.PhaseReady)
					}
					renewal := func() { done[1] <- m.renewAvailability(renewalCtx, c.Ref()) }
					first, second := 0, 1
					if renewalFirst {
						first, second = 1, 0
					}
					start := [2]func(){normal, renewal}
					go start[first]()
					<-entered[first]
					go start[second]()
					synctest.Wait()
					if probes[second].Load() != 0 {
						t.Fatal("second path observed before the first path published")
					}
					select {
					case <-entered[second]:
						t.Fatal("second publication overtook the held sample")
					default:
					}
					close(release[first])
					<-entered[second]
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil || (stored.Status.Phase == domain.PhaseReady) != firstHealthy {
						t.Fatal("first sample was not published before the next observation", stored, err)
					}
					close(release[second])
					for _, result := range done {
						if err := <-result; err != nil {
							t.Fatal(err)
						}
					}
					stored, err = st.GetCluster(t.Context(), c.Ref())
					if err != nil || (stored.Status.Phase == domain.PhaseReady) == firstHealthy ||
						stored.Status.Members[0].Agent.Healthy == firstHealthy || stored.Status.ObservedGeneration != 1 ||
						string(stored.Internal) != `{"ordinary":"owns-checkpoint"}` {
						t.Fatal("observation ordering changed health or lifecycle ownership", stored, err)
					}
				})
			})
		}
	}
}

func TestPostSyncDependencyFailureCannotRepublishCachedAvailability(t *testing.T) {
	m, st := newTestManager(t, &fakeProvider{})
	c := &domain.Cluster{Namespace: "broken", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
		Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1, ObservedAt: time.Now(), ObservationEpoch: "current-process",
			Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
			Members:    []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: time.Now()}}}}}
	if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: c.Namespace, Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	rc := m.opts.Context
	rc.Cluster = c
	if err := m.resolve(t.Context(), &rc); err == nil {
		t.Fatal("unresolved namespace did not fail")
	}
	stored, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || stored.Status.Phase == domain.PhaseReady || !stored.Status.ObservedAt.IsZero() ||
		stored.Status.ObservationEpoch != "" || stored.Status.Members[0].Health || stored.Status.Members[0].Agent.Healthy ||
		stored.Status.ObservedGeneration != 1 {
		t.Fatal("dependency failure republished pre-Sync positives", stored, err)
	}
}

var preliminaryPublicationFaultID atomic.Uint64

func TestPreliminaryPublicationFailureRetainsGateThroughFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var writes atomic.Int32
		function := fmt.Sprintf("d040_preliminary_write_%d", preliminaryPublicationFaultID.Add(1))
		if err := sqlite.RegisterScalarFunction(function, 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
			if writes.Add(1) == 1 {
				return nil, errors.New("transient preliminary publication failure")
			}
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		key, err := secrets.New(strings.Repeat("k", 32))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "publication.db")
		st, err := store.Open(path, key)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		p := &operationProvider{fakeProvider: &fakeProvider{}}
		registry, err := engine.NewRegistry(p)
		if err != nil {
			t.Fatal(err)
		}
		m, err := New(Options{Store: st, Registry: registry, Context: engine.Context{ObservationEpoch: "current-process"}})
		if err != nil {
			t.Fatal(err)
		}
		c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":true}`),
			Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
		if err := st.CreateCluster(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		admin, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()
		if _, err := admin.ExecContext(t.Context(), `CREATE TRIGGER fail_preliminary BEFORE UPDATE OF status ON clusters WHEN NEW.name='demo' BEGIN SELECT `+function+`(); END`); err != nil {
			t.Fatal(err)
		}
		gate := make(chan struct{}, 1)
		normalCtx := withObservationPublication(t.Context(), gate)
		renewalCtx := withObservationPublication(t.Context(), gate)
		renewalEntered, releaseRenewal := make(chan struct{}), make(chan struct{})
		releaseFallback := make(chan struct{})
		var renewalOnce sync.Once
		var renewalProbes atomic.Int32
		p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
			if rc.AvailabilityOnly {
				renewalProbes.Add(1)
				renewalOnce.Do(func() {
					close(renewalEntered)
					<-releaseRenewal
				})
				return domain.ClusterStatus{}, errors.New("newer agent observation failed")
			}
			return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
				Members: []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}}}, nil
		}
		p.syncFn = func(*engine.Context) (engine.Result, error) {
			t.Error("Sync ran after preliminary publication failed")
			return engine.Result{}, nil
		}
		failedPublication := make(chan error, 1)
		normalDone, renewalDone := make(chan error, 1), make(chan error, 1)
		go func() {
			defer releaseObservation(normalCtx)
			current, err := st.GetCluster(normalCtx, c.Ref())
			if err != nil {
				failedPublication <- err
				normalDone <- err
				return
			}
			rc := m.opts.Context
			rc.Cluster = current
			_, err = m.synchronize(normalCtx, p, &rc, nil)
			failedPublication <- err
			<-releaseFallback
			// Match reconcile's unconditional m.state even when synchronize
			// reports a preliminary publication failure.
			normalDone <- m.state(normalCtx, current, domain.PhaseReady)
		}()
		if err := <-failedPublication; err == nil || !strings.Contains(err.Error(), "transient preliminary publication failure") {
			t.Fatal("did not exercise the failed preliminary state write", err)
		}
		go func() { renewalDone <- m.renewAvailability(renewalCtx, c.Ref()) }()
		synctest.Wait()
		if renewalProbes.Load() != 0 {
			t.Fatal("failed preliminary publication released ownership before the caller's fallback write")
		}
		close(releaseFallback)
		<-renewalEntered
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || stored.Status.Phase != domain.PhaseReady || stored.Status.ObservationEpoch != "current-process" {
			t.Fatal("fallback did not publish before the subsequent observation", stored, err)
		}
		close(releaseRenewal)
		if err := <-normalDone; err != nil {
			t.Fatal(err)
		}
		if err := <-renewalDone; err != nil {
			t.Fatal(err)
		}
		stored, err = st.GetCluster(t.Context(), c.Ref())
		if err != nil || stored.Status.Phase == domain.PhaseReady || !stored.Status.ObservedAt.IsZero() ||
			stored.Status.Members[0].Agent.Healthy || stored.Status.ObservedGeneration != 1 || string(stored.Internal) != `{"checkpoint":true}` {
			t.Fatal("older fallback restored positive health over the subsequent failure", stored, err)
		}
		if writes.Load() != 3 {
			t.Fatal("expected one failed preliminary write, one fallback and one failed-health publication", writes.Load())
		}
	})
}
