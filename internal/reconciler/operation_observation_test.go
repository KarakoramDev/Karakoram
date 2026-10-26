// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
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
