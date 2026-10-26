// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type exposureSupportBackend struct {
	infra.Backend
	supported bool
	err       error
}

func (b *exposureSupportBackend) SupportsPublicIP(context.Context, string) (bool, error) {
	return b.supported, b.err
}

func TestUnsupportedExposureTerminatesGeneration(t *testing.T) {
	for _, platformRefusal := range []bool{false, true} {
		t.Run(map[bool]string{false: "project capability", true: "platform refusal"}[platformRefusal], func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			syncCalls := 0
			p.syncFn = func(rc *engine.Context) (engine.Result, error) {
				syncCalls++
				if rc.Cluster.Spec.Exposure.IPType == "IPV6" {
					return engine.Result{}, nil
				}
				return engine.Result{}, infra.ErrPublicIPTypeUnsupported
			}
			p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
				if rc.Cluster.Spec.Exposure.IPType == "IPV6" {
					return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}}, nil
				}
				return domain.ClusterStatus{Phase: domain.PhaseCreating, Conditions: []domain.Condition{{Type: "Ready", Status: "False", Reason: "MembersStarting"}}}, nil
			}
			m, st := newTestManager(t, p)
			b := &exposureSupportBackend{}
			if platformRefusal {
				b.err = errors.New("lookup unavailable")
			}
			m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: b}
			op := &domain.Operation{Kind: domain.OperationCreate}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Exposure: domain.Exposure{Public: true, IPType: "IPV4"}}}
			if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(t.Context(), c.Ref())
			got, err := st.GetOperation(t.Context(), op.ID)
			if err != nil || got.Phase != domain.PhaseFailed {
				t.Fatalf("operation=%+v err=%v", got, err)
			}
			current, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || current.Status.Phase != domain.PhaseDegraded || current.Status.ObservedGeneration != 0 {
				t.Fatalf("cluster=%+v err=%v", current, err)
			}
			found := false
			for _, condition := range current.Status.Conditions {
				if condition.Type == "ExposureSupported" && condition.Status == "False" && condition.Reason == "PublicIPTypeUnsupported" {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing precise condition: %+v", current.Status.Conditions)
			}
			if !platformRefusal && syncCalls != 0 {
				t.Fatal("unsupported exposure still mutated backend")
			}
			current.Spec.Exposure.IPType = "IPV6"
			b.err, b.supported = nil, true
			update := &domain.Operation{Kind: domain.OperationUpdate}
			if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), update), current.Ref(), current.Spec); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), current.Ref()); err != nil {
				t.Fatal(err)
			}
			recovered, err := st.GetCluster(t.Context(), current.Ref())
			if err != nil || recovered.Status.Phase != domain.PhaseReady || recovered.Status.ObservedGeneration != recovered.Generation {
				t.Fatalf("corrected exposure did not recover: %+v %v", recovered, err)
			}
			finished, err := st.GetOperation(t.Context(), update.ID)
			if err != nil || finished.Phase != domain.PhaseSucceeded {
				t.Fatalf("corrected update did not succeed: %+v %v", finished, err)
			}
		})
	}
}

func TestPublicPortConflictTerminatesGenerationAndRecoversPrivate(t *testing.T) {
	testPublicPortFailure(t, &infra.PublicPortConflictError{Port: 5432}, "PublicPortInUse")
}

func TestHiddenPublicPortReloadFailureTerminatesGeneration(t *testing.T) {
	testPublicPortFailure(t, &infra.PublicPortPublishError{Port: 5432}, "PublicPortPublishFailed")
}

func TestEndpointRepairExhaustionTerminatesGenerationAndRecoversPrivate(t *testing.T) {
	testPublicPortFailure(t, &infra.EndpointRepairExhaustedError{Service: "proxy", Attempts: 3}, "PublishedEndpointRepairExhausted")
}

func testPublicPortFailure(t *testing.T, failure error, reason string) {
	t.Helper()
	expectEndpointFailure := false
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		if expectEndpointFailure {
			found := false
			for _, condition := range rc.Cluster.Status.Conditions {
				found = found || condition.Type == "PublishedEndpoint" && condition.Reason == "PublishedEndpointRepairExhausted"
			}
			if !found {
				t.Fatal("preliminary health observation discarded endpoint exhaustion")
			}
		}
		if rc.Cluster.Spec.Exposure.Public {
			return engine.Result{}, failure
		}
		return engine.Result{}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}}, nil
	}
	m, st := newTestManager(t, p)
	b := &exposureSupportBackend{supported: true}
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: b}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	c, _ = st.GetCluster(t.Context(), c.Ref())
	previousGeneration := c.Status.ObservedGeneration
	c.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	op := &domain.Operation{Kind: domain.OperationUpdate}
	if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), op), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	_, _ = m.reconcile(t.Context(), c.Ref())
	failed, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || failed.Phase != domain.PhaseFailed || failed.CompletedAt == nil {
		t.Fatalf("conflicting update not immediately terminal: %+v %v", failed, err)
	}
	current, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || !current.Spec.Exposure.Public || current.Status.Phase != domain.PhaseDegraded || current.Status.ObservedGeneration != previousGeneration {
		t.Fatalf("conflict changed intent or acknowledged generation: %+v %v", current, err)
	}
	found := false
	conditionType := "ExposureSupported"
	var exhausted *infra.EndpointRepairExhaustedError
	if errors.As(failure, &exhausted) {
		conditionType = "PublishedEndpoint"
	}
	for _, condition := range current.Status.Conditions {
		if condition.Type == conditionType && condition.Status == "False" && condition.Reason == reason && condition.Message == failure.Error() {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing actionable conflict condition: %+v", current.Status.Conditions)
	}
	if exhausted != nil {
		expectEndpointFailure = true
		rc := m.opts.Context
		rc.Cluster = current
		for range 2 {
			if _, err := m.synchronize(t.Context(), p, &rc, nil); !errors.As(err, &exhausted) {
				t.Fatalf("repeated exhaustion was not reported: %v", err)
			}
		}
		if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
			t.Fatal(err)
		}
		if err := m.observeDuringBackoff(t.Context(), current.Ref()); err != nil {
			t.Fatal(err)
		}
		current, err = st.GetCluster(t.Context(), current.Ref())
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, condition := range current.Status.Conditions {
			found = found || condition.Type == "PublishedEndpoint" && condition.Reason == reason && condition.Message == failure.Error()
		}
		if !found || current.Status.Phase != domain.PhaseDegraded {
			t.Fatalf("backoff health observation discarded endpoint exhaustion: %+v", current.Status)
		}
		expectEndpointFailure = false
	}
	current.Spec.Exposure.Public = false
	update := &domain.Operation{Kind: domain.OperationUpdate}
	if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), update), current.Ref(), current.Spec); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), current.Ref()); err != nil {
		t.Fatal(err)
	}
	recovered, err := st.GetCluster(t.Context(), current.Ref())
	if err != nil || recovered.Status.Phase != domain.PhaseReady || recovered.Status.ObservedGeneration != recovered.Generation {
		t.Fatalf("private correction did not recover: %+v %v", recovered, err)
	}
	finished, err := st.GetOperation(t.Context(), update.ID)
	if err != nil || finished.Phase != domain.PhaseSucceeded {
		t.Fatalf("private correction operation: %+v %v", finished, err)
	}
	if exhausted != nil {
		for _, condition := range recovered.Status.Conditions {
			if condition.Type == "PublishedEndpoint" {
				t.Fatalf("successful disable retained endpoint exhaustion: %+v", condition)
			}
		}
		// A successful fresh publication must not revive the previous incident.
		failure = nil
		recovered.Spec.Exposure.Public = true
		enable := &domain.Operation{Kind: domain.OperationUpdate}
		if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), enable), recovered.Ref(), recovered.Spec); err != nil {
			t.Fatal(err)
		}
		if _, err := m.reconcile(t.Context(), recovered.Ref()); err != nil {
			t.Fatal(err)
		}
		reEnabled, err := st.GetCluster(t.Context(), recovered.Ref())
		if err != nil || reEnabled.Status.Phase != domain.PhaseReady || reEnabled.Status.ObservedGeneration != reEnabled.Generation {
			t.Fatalf("fresh publication did not recover: %+v %v", reEnabled, err)
		}
		for _, condition := range reEnabled.Status.Conditions {
			if condition.Type == "PublishedEndpoint" {
				t.Fatalf("fresh publication revived endpoint exhaustion: %+v", condition)
			}
		}
	}
}

func TestExposureLookupFailureDoesNotDegrade(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	calls := 0
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		calls++
		return engine.Result{}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}}, nil
	}
	m, st := newTestManager(t, p)
	b := &exposureSupportBackend{err: errors.New("temporary project lookup failure")}
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: b}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Exposure: domain.Exposure{Public: true, IPType: "IPV4"}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || got.Status.Phase != domain.PhaseReady || calls != 1 {
		t.Fatalf("unknown support blocked convergence: %+v %v calls=%d", got, err, calls)
	}
	found := false
	for _, condition := range got.Status.Conditions {
		if condition.Type == "ExposureSupported" && condition.Status == "Unknown" && condition.Reason == "PublicIPSupportUnknown" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing unknown support condition: %+v", got.Status.Conditions)
	}
}
