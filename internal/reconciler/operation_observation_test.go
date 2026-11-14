// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestRunningOperationsRefreshAvailabilityWithoutSync(t *testing.T) {
	for _, kind := range []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestart, domain.OperationRestore, domain.OperationReplaceMember} {
		for _, unavailable := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/healthy", true: "/unavailable"}[unavailable], func(t *testing.T) {
				p := &operationProvider{fakeProvider: &fakeProvider{}}
				m, st := newTestManager(t, p)
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "running", Cluster: c.Name, Kind: kind, Phase: domain.PhaseRunning, Step: "poll"}
				if kind == domain.OperationRestore {
					op.Payload = json.RawMessage(`{"restore":"recover"}`)
					if err := st.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: c.Name}); err != nil {
						t.Fatal(err)
					}
				}
				if err := st.EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				p.syncFn = func(*engine.Context) (engine.Result, error) {
					t.Fatal("ordinary Sync ran during imperative operation")
					return engine.Result{}, nil
				}
				observations := 0
				p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
					observations++
					if unavailable {
						return domain.ClusterStatus{}, errors.New("agent unavailable")
					}
					return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}}}, nil
				}
				p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
					return engine.OpResult{RequeueAfter: time.Minute}, nil
				}
				delay, err := m.reconcile(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if observations == 0 || delay > engine.ReadinessInterval {
					t.Fatalf("operation starved bounded observations: samples=%d delay=%s", observations, delay)
				}
				if stored.Status.ObservedGeneration != 1 {
					t.Fatal("health changed applied generation")
				}
				if kind == domain.OperationRestore {
					if stored.Status.Phase != domain.PhaseRestoring {
						t.Fatal("observation hid restore phase", stored.Status)
					}
					found := false
					for _, condition := range stored.Status.Conditions {
						if condition.Type == "Ready" {
							found = condition.Status == "False" && condition.Reason == "Restoring"
						}
					}
					if !found {
						t.Fatal("restore did not withdraw Ready", stored.Status)
					}
				} else if unavailable {
					if stored.Status.Phase == domain.PhaseReady {
						t.Fatal("unavailable observation retained Ready")
					}
				} else if stored.Status.ObservedAt.IsZero() {
					t.Fatal("operation failed to renew readiness sample")
				}
			})
		}
	}
}

func TestRunningOperationDoesNotObserveUnresolvedNamespace(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	defaultBackend := fake.New(fake.Options{})
	m.opts.Context.Backend = defaultBackend
	m.opts.Namespaces = &testNamespaces{StaticNamespaces: engine.StaticNamespaces{DefaultBackend: defaultBackend}, calls: map[string]int{}}
	if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: "broken", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Namespace: "broken", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: time.Now(), Conditions: []domain.Condition{{Type: "Healthy", Status: "True"}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationBackup, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	probes := 0
	p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
		probes++
		if rc.Backend != defaultBackend {
			t.Fatal("unexpected dependency")
		}
		return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err == nil {
		t.Fatal("missing namespace error")
	}
	stored, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if probes != 0 || !stored.Status.ObservedAt.IsZero() || stored.Status.Phase == domain.PhaseReady {
		t.Fatal("unresolved default-project probe renewed health", probes, stored.Status)
	}
	found := false
	for _, condition := range stored.Status.Conditions {
		found = found || condition.Type == "NamespaceReady" && condition.Reason == "NamespaceUnavailable" && condition.Status == "False"
	}
	if !found {
		t.Fatal("namespace failure overwritten", stored.Status)
	}
}

func TestOperationErrorBackoffKeepsAvailabilityFresh(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	m.opts.BackoffMin, m.opts.BackoffMax = time.Minute, time.Minute
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "fenced", Cluster: c.Name, Kind: domain.OperationBackup, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		attempts.Add(1)
		return engine.OpResult{}, errors.New("PBMReceiptUnknown")
	}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		t.Error("backoff observer ran ordinary Sync")
		return engine.Result{}, nil
	}
	samples := make(chan time.Time, 8)
	p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
		if !rc.AvailabilityOnly {
			t.Error("backoff probe was not availability-only")
		}
		samples <- time.Now()
		return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	w := &worker{trigger: make(chan struct{}, 1)}
	m.wg.Add(1)
	go m.work(ctx, c.Ref(), w)
	defer func() { cancel(); m.wg.Wait() }()
	select {
	case <-samples:
	case <-time.After(time.Second):
		t.Fatal("initial operation observation missing")
	}
	select {
	case <-samples:
	case <-time.After(engine.ReadinessInterval + time.Second):
		t.Fatal("operation error backoff starved availability observation")
	}
	eventually(t, func() bool {
		stored, err := st.GetCluster(t.Context(), c.Ref())
		return err == nil && !stored.Status.ObservedAt.IsZero() && engine.FreshStatus(stored.Status, time.Now()).Phase == domain.PhaseReady
	})
	if attempts.Load() != 1 {
		t.Fatal("availability sampling bypassed operation retry backoff", attempts.Load())
	}
}

func TestBackoffObservationPreservesLifecycleAndWithdrawsFailedHealth(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseReady, domain.PhaseRestoring, domain.PhaseSuspended} {
		for _, unavailable := range []bool{false, true} {
			t.Run(string(phase)+map[bool]string{false: "/healthy", true: "/unavailable"}[unavailable], func(t *testing.T) {
				p := &operationProvider{fakeProvider: &fakeProvider{}}
				m, st := newTestManager(t, p)
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: phase, ObservedGeneration: 1}}
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				probes := 0
				p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
					probes++
					if !rc.AvailabilityOnly {
						t.Error("not availability-only")
					}
					if unavailable {
						return domain.ClusterStatus{}, errors.New("unreachable")
					}
					return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}}, nil
				}
				if err := m.observeDuringBackoff(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if stored.Status.ObservedGeneration != 1 {
					t.Fatal("observation changed applied generation")
				}
				if phase == domain.PhaseSuspended {
					if probes != 0 || stored.Status.Phase != phase {
						t.Fatal("observed suspended runtime")
					}
				} else if phase == domain.PhaseRestoring {
					if stored.Status.Phase != phase {
						t.Fatal("observation hid restore")
					}
					for _, condition := range stored.Status.Conditions {
						if condition.Type == "Ready" && condition.Status == "True" {
							t.Fatal("old processes certified restore Ready")
						}
					}
				} else if unavailable && stored.Status.Phase == domain.PhaseReady {
					t.Fatal("failed probe retained Ready")
				}
			})
		}
	}
}

func TestBackoffObservationFencesRunningRestoreAcrossNamespaceFailure(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: "broken", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Namespace: "broken", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseRestoring}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	m.opts.Namespaces = &testNamespaces{calls: map[string]int{}}
	if err := m.observeDuringBackoff(t.Context(), c.Ref()); err == nil {
		t.Fatal("expected namespace failure")
	}
	stored, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != domain.PhaseRestoring {
		t.Errorf("namespace failure removed restore phase: %s", stored.Status.Phase)
	}
	// Simulate both recovered dependencies and a previously degraded snapshot:
	// running-operation ownership, not the last phase, must fence old processes.
	stored.Status.Phase = domain.PhaseDegraded
	if err := st.UpdateClusterState(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	m.opts.Namespaces = recoveredBackoffNamespace{engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{})}}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}}, nil
	}
	if err := m.observeDuringBackoff(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	stored, err = st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != domain.PhaseRestoring {
		t.Errorf("healthy old processes hid running restore: %s", stored.Status.Phase)
	}
	found := false
	for _, condition := range stored.Status.Conditions {
		if condition.Type == "Ready" {
			found = condition.Status == "False" && condition.Reason == "Restoring"
		}
	}
	if !found {
		t.Fatal("running restore lost Ready=False fence")
	}
}

func TestBackoffObservationPreservesDeploymentWait(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	wait := domain.Condition{Type: "PlatformDeploymentWaiting", Status: "True", Reason: "ProjectAdmission", Key: "slot/project/member", Message: "waiting for deployment slot", LastTransitionTime: time.Now().Add(-time.Minute).UTC()}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseDegraded, Conditions: []domain.Condition{wait}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
		if rc.DeploymentWait != wait.Message || rc.DeploymentWaitReason != wait.Reason || rc.DeploymentWaitKey != wait.Key {
			t.Error("backoff observation dropped deployment wait context")
		}
		return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
	}
	if err := m.observeDuringBackoff(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range stored.Status.Conditions {
		if condition.Type == wait.Type {
			if condition.Reason != wait.Reason || condition.Key != wait.Key || condition.Message != wait.Message || !condition.LastTransitionTime.Equal(wait.LastTransitionTime) {
				t.Fatal("deployment wait dedup evidence changed", condition)
			}
			return
		}
	}
	t.Fatal("backoff observation erased deployment wait")
}

type recoveredBackoffNamespace struct {
	engine.StaticNamespaces
}

func (n recoveredBackoffNamespace) Backend(string) (infra.Backend, error) {
	return n.DefaultBackend, nil
}

func operationHealth(rc *engine.Context) domain.ClusterStatus {
	return domain.ClusterStatus{Phase: domain.PhaseReady,
		Conditions: []domain.Condition{{Type: "Ready", Status: "True"}, {Type: "Healthy", Status: "True"}},
		Members: []domain.MemberStatus{{Name: "member", Health: true,
			Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now(), Build: "previous-build"}}}}
}

type sampleCachingOperationProvider struct{ *operationProvider }

func (p *sampleCachingOperationProvider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	engine.ObservationPassStart(ctx, rc.Clock.Now())
	return p.operationProvider.RunOperation(ctx, rc, op)
}

func TestBlockedOperationRenewsAvailabilityBeyondLease(t *testing.T) {
	for _, dead := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "agent-failure"}[dead], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &sampleCachingOperationProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
				m, st := newTestManager(t, p)
				m.opts.Context.ObservationEpoch = "current-process"
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
					Internal: json.RawMessage(`{"checkpoint":true}`),
					Status:   domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
				c.Status = operationHealth(&engine.Context{Clock: m.opts.Context.Clock})
				c.Status.ObservedGeneration = 1
				c.Status.ObservedAt, c.Status.ObservationEpoch = time.Now(), "current-process"
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "rollout", Cluster: c.Name, Kind: domain.OperationUpgradeAgents,
					Phase: domain.PhaseRunning, Step: "waiting"}
				if err := st.EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				started := make(chan struct{})
				var once sync.Once
				var unavailable atomic.Bool
				p.runFn = func(rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
					rc.Cluster.Internal = json.RawMessage(`{"operation":"progress"}`)
					once.Do(func() { close(started) })
					time.Sleep(25 * time.Second) // Virtual time, not a synchronization sleep.
					return engine.OpResult{Step: "next", Message: "waiting for placement",
						Checkpoint: json.RawMessage(`{"receipt":"pending"}`), RequeueAfter: time.Minute}, nil
				}
				p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
					if unavailable.Load() {
						return domain.ClusterStatus{}, errors.New("agent unavailable")
					}
					return operationHealth(rc), nil
				}
				ctx, cancel := context.WithCancel(t.Context())
				w := &worker{trigger: make(chan struct{}, 1)}
				m.wg.Add(1)
				go m.work(ctx, c.Ref(), w)
				defer func() { cancel(); m.wg.Wait() }()
				<-started
				unavailable.Store(dead)
				var last time.Time
				advances := 0
				for second := 1; second <= 24; second++ {
					time.Sleep(time.Second)
					synctest.Wait()
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					fresh := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
					if !dead && second >= 5 {
						if fresh.Phase == domain.PhaseDegraded || fresh.ObservedAt.IsZero() ||
							len(fresh.Members) != 1 || !fresh.Members[0].Health || !fresh.Members[0].Agent.Healthy {
							t.Fatalf("healthy operation lost availability at %ds: %+v", second, fresh)
						}
						for _, condition := range fresh.Conditions {
							if condition.Type == "Healthy" && condition.Status != "True" ||
								condition.Type == "AgentsCurrent" && (condition.Status != "False" || condition.Reason != "Outdated") {
								t.Fatal("health or agent inventory withdrawn", condition)
							}
						}
						if fresh.ObservedAt.After(last) {
							advances++
							last = fresh.ObservedAt
						}
					}
					if dead && second > int(engine.ReadinessMaxAge/time.Second) &&
						(!fresh.ObservedAt.IsZero() || fresh.Phase == domain.PhaseReady) {
						t.Fatal("failed observation kept health alive", fresh)
					}
					if string(stored.Internal) != `{"checkpoint":true}` {
						t.Fatal("renewal published uncheckpointed operation state", stored.Internal)
					}
				}
				if !dead && advances < 4 {
					t.Fatal("operation did not advance its availability lease", advances)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil || string(stored.Internal) != `{"operation":"progress"}` {
					t.Fatal("operation state lost to renewal", stored, err)
				}
				if !dead && time.Since(stored.Status.ObservedAt) >= engine.ReadinessMaxAge {
					t.Fatal("post-attempt observation replayed pre-operation evidence", stored.Status)
				}
				tracked, err := st.GetOperation(t.Context(), op.ID)
				if err != nil || tracked.Step != "next" || string(tracked.Checkpoint) != `{"receipt":"pending"}` ||
					tracked.Message != "waiting for placement" || tracked.Phase != domain.PhaseRunning {
					t.Fatal("operation receipt lost to renewal", tracked, err)
				}
			})
		})
	}
}

func TestOperationPersistenceRebasesConcurrentRenewal(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer-failure", true: "newer-success"}[healthy], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &operationProvider{fakeProvider: &fakeProvider{}}
				m, st := newTestManager(t, p)
				m.opts.Context.ObservationEpoch = "current-process"
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
					Internal: json.RawMessage(`{"checkpoint":true}`), Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
				c.Status = operationHealth(&engine.Context{Clock: m.opts.Context.Clock})
				c.Status.ObservedAt, c.Status.ObservationEpoch = time.Now(), "current-process"
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "rollout", Cluster: c.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhaseRunning}
				if err := st.EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				operationEntered, finishOperation := make(chan struct{}), make(chan struct{})
				renewalEntered, finishRenewal := make(chan struct{}), make(chan struct{})
				var renewalOnce sync.Once
				p.runFn = func(rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
					rc.Cluster.Internal = json.RawMessage(`{"operation":"complete"}`)
					rc.Cluster.Status.Message = "operation-owned message"
					for i := range rc.Cluster.Status.Conditions {
						if rc.Cluster.Status.Conditions[i].Type == "Healthy" {
							rc.Cluster.Status.Conditions[i].Reason = "EarlierOperationSafetyRead"
						}
					}
					rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions,
						domain.Condition{Type: "OperationReceipt", Status: "True", Reason: "Recorded"})
					close(operationEntered)
					<-finishOperation
					return engine.OpResult{Done: true, Step: "complete", Checkpoint: json.RawMessage(`{"done":true}`)}, nil
				}
				p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
					renewalOnce.Do(func() { close(renewalEntered); <-finishRenewal })
					if !healthy {
						return domain.ClusterStatus{}, errors.New("newer failed observation")
					}
					return operationHealth(rc), nil
				}
				gate := make(chan struct{}, 1)
				operationCtx := withObservationPublication(t.Context(), gate)
				renewalCtx := withObservationPublication(t.Context(), gate)
				operationDone, renewalDone := make(chan error, 1), make(chan error, 1)
				go func() { _, err := m.reconcile(operationCtx, c.Ref()); operationDone <- err }()
				<-operationEntered
				time.Sleep(time.Second)
				go func() { renewalDone <- m.renewAvailability(renewalCtx, c.Ref()) }()
				<-renewalEntered
				close(finishOperation)
				synctest.Wait()
				select {
				case err := <-operationDone:
					t.Fatal("operation published through an in-flight renewal", err)
				default:
				}
				close(finishRenewal)
				if err := <-renewalDone; err != nil {
					t.Fatal(err)
				}
				if err := <-operationDone; err != nil {
					t.Fatal(err)
				}
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil || string(stored.Internal) != `{"operation":"complete"}` ||
					stored.Status.Message != "operation-owned message" || stored.Status.Members[0].Health != healthy {
					t.Fatal("operation and health failed to rebase", stored, err)
				}
				if healthy && !stored.Status.ObservedAt.After(c.Status.ObservedAt) ||
					!healthy && !stored.Status.ObservedAt.IsZero() {
					t.Fatal("operation replayed an older observation", stored.Status)
				}
				found := false
				for _, condition := range stored.Status.Conditions {
					found = found || condition.Type == "OperationReceipt" && condition.Status == "True"
					if !healthy && condition.Type == "Healthy" && condition.Status == "True" {
						t.Fatal("operation replayed an earlier positive condition", condition)
					}
				}
				tracked, err := st.GetOperation(t.Context(), op.ID)
				if !found || err != nil || tracked.Phase != domain.PhaseSucceeded ||
					tracked.Step != "complete" || string(tracked.Checkpoint) != `{"done":true}` {
					t.Fatal("renewal erased operation fields", stored.Status, tracked, err)
				}
				// Renewal after the checkpoint must not mutate provider state.
				if err := m.renewAvailability(renewalCtx, c.Ref()); err != nil {
					t.Fatal(err)
				}
				stored, err = st.GetCluster(t.Context(), c.Ref())
				if err != nil || string(stored.Internal) != `{"operation":"complete"}` {
					t.Fatal("later renewal erased operation state", stored, err)
				}
			})
		})
	}
}

type isolatedOperationProvider struct{ *operationProvider }

func (*isolatedOperationProvider) ObserveDuringOperation(string) bool { return false }

func TestUnsafeOperationsExcludeIndependentObservation(t *testing.T) {
	for _, kind := range []string{domain.OperationRestore, domain.OperationRotateTLS,
		domain.OperationRotateSystemUser, "recover-quorum", "future-operation", domain.OperationUpgradeAgents} {
		t.Run(kind, func(t *testing.T) {
			base := &operationProvider{fakeProvider: &fakeProvider{}}
			var p engine.Provider = base
			if kind == domain.OperationUpgradeAgents {
				p = &isolatedOperationProvider{base}
			}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
				Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: time.Now()}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: kind, Phase: domain.PhaseRunning}); err != nil {
				t.Fatal(err)
			}
			base.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				t.Fatal("unsafe operation admitted independent observation")
				return domain.ClusterStatus{}, nil
			}
			if canObserveDuringOperation(p, kind) {
				t.Fatal("unsafe operation released observation ownership")
			}
			if err := m.renewAvailability(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			stored, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || !stored.Status.ObservedAt.Equal(c.Status.ObservedAt) {
				t.Fatal("excluded operation changed availability", stored, err)
			}
		})
	}
}
