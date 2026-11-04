// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package reconciler

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func providerOverwriteTransitionStatus(at time.Time, nativeStatus, nativeReason, message string, healthy bool) domain.ClusterStatus {
	phase, readyStatus, readyReason := domain.PhaseReady, "True", "MembersReady"
	if !healthy {
		phase, readyStatus, readyReason = domain.PhaseDegraded, "False", "MembersUnhealthy"
	}
	return domain.ClusterStatus{
		Phase: phase, Message: "provider status: " + message,
		Conditions: []domain.Condition{
			{Type: "NativeHealth", Status: nativeStatus, Reason: nativeReason, Message: message, LastTransitionTime: at},
			{Type: "Configuration", Status: "True", Reason: "ConfigApplied", Message: "latest configuration: " + message, LastTransitionTime: at},
			{Type: "Ready", Status: readyStatus, Reason: readyReason, Message: "latest readiness: " + message, LastTransitionTime: at},
		},
		Members: []domain.MemberStatus{{Name: "member", Health: healthy, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
	}
}

func transitionCondition(t *testing.T, status domain.ClusterStatus, name string) domain.Condition {
	t.Helper()
	for _, condition := range status.Conditions {
		if condition.Type == name {
			return condition
		}
	}
	t.Fatal("persisted status lost condition", name)
	return domain.Condition{}
}

func TestManagerPreservesStableTransitionsWhenSyncOverwritesClusterStatus(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Millisecond)
	original := base.Add(-time.Hour)
	clock := &maintenanceStepClock{now: base}
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	m, st := newTestManager(t, p)
	m.opts.Context.Clock = clock
	m.opts.Context.ObservationEpoch = "condition-transition-test"
	message := "first native observation"
	snapshot := func(rc *engine.Context) domain.ClusterStatus {
		status := providerOverwriteTransitionStatus(rc.Clock.Now(), "True", "NativeAvailable", message, true)
		status.ObservedGeneration = rc.Cluster.Generation
		return status
	}
	p.statusFn = snapshotStatusFunction(snapshot)
	syncs := 0
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		syncs++
		// Model a generic provider that publishes its own complete fresh
		// status during convergence. This is a replacement slice, not a
		// timestamp mutation through an alias to the original conditions.
		rc.Cluster.Status = snapshot(rc)
		if transitionCondition(t, rc.Cluster.Status, "NativeHealth").LastTransitionTime.Equal(original) {
			t.Fatal("fixture did not overwrite the original transition timestamp")
		}
		return engine.Result{RequeueAfter: time.Minute}, nil
	}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: providerOverwriteTransitionStatus(base, "True", "NativeAvailable", "original content", true)}
	for i := range c.Status.Conditions {
		c.Status.Conditions[i].LastTransitionTime = original
	}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status.ObservedGeneration = c.Generation
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	for _, nextMessage := range []string{"first native observation", "second native observation"} {
		message = nextMessage
		clock.now = clock.now.Add(5 * time.Second)
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
		saved, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"NativeHealth", "Configuration", "Ready"} {
			condition := transitionCondition(t, saved.Status, name)
			if !condition.LastTransitionTime.Equal(original) {
				t.Fatal("stable condition inherited a provider Sync observation time instead of its immutable transition", name, condition.LastTransitionTime, original)
			}
		}
		if transitionCondition(t, saved.Status, "NativeHealth").Message != message || transitionCondition(t, saved.Status, "Configuration").Message != "latest configuration: "+message || transitionCondition(t, saved.Status, "Ready").Message != "latest readiness: "+message {
			t.Fatal("preserving transition time restored stale condition content", saved.Status.Conditions)
		}
		if saved.Status.Phase != domain.PhaseReady || saved.Status.ObservedGeneration != c.Generation || len(saved.Status.Members) != 1 || !saved.Status.Members[0].Health || !saved.Status.Members[0].Agent.Healthy || !saved.Status.Members[0].Agent.LastSeen.Equal(clock.now) {
			t.Fatal("timestamp preservation changed fresh Ready/member/agent health evidence", saved.Status)
		}
	}
	if syncs != 2 {
		t.Fatal("regression did not execute two full stable Ready Manager passes", syncs)
	}
}

// Keep provider hooks identical to normal operationProvider.Status: this helper
// only adapts the error-free native observation callback, not Manager results.
func snapshotStatusFunction(snapshot func(*engine.Context) domain.ClusterStatus) func(*engine.Context) (domain.ClusterStatus, error) {
	return func(rc *engine.Context) (domain.ClusterStatus, error) {
		return snapshot(rc), nil
	}
}

func TestManagerKeepsNewTransitionWhenSyncChangesConditionStatusOrReason(t *testing.T) {
	for _, change := range []string{"status", "reason"} {
		t.Run(change, func(t *testing.T) {
			base := time.Now().UTC().Truncate(time.Millisecond)
			original := base.Add(-time.Hour)
			clock := &maintenanceStepClock{now: base}
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			m, st := newTestManager(t, p)
			m.opts.Context.Clock = clock
			m.opts.Context.ObservationEpoch = "condition-change-test"
			nativeStatus, nativeReason, message, healthy := "True", "NativeAvailable", "stable native observation", true
			snapshot := func(rc *engine.Context) domain.ClusterStatus {
				status := providerOverwriteTransitionStatus(rc.Clock.Now(), nativeStatus, nativeReason, message, healthy)
				status.ObservedGeneration = rc.Cluster.Generation
				return status
			}
			p.statusFn = snapshotStatusFunction(snapshot)
			syncs := 0
			p.syncFn = func(rc *engine.Context) (engine.Result, error) {
				syncs++
				if syncs == 2 {
					message = "new native condition content"
					if change == "status" {
						nativeStatus, healthy = "False", false
					} else {
						nativeReason = "NativeLeaderChanged"
					}
				}
				// The pre-Sync health sample still has the original fact. Only
				// this real convergence boundary changes Status or Reason.
				rc.Cluster.Status = snapshot(rc)
				return engine.Result{RequeueAfter: time.Minute}, nil
			}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: providerOverwriteTransitionStatus(base, nativeStatus, nativeReason, "original content", true)}
			for i := range c.Status.Conditions {
				c.Status.Conditions[i].LastTransitionTime = original
			}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			c.Status.ObservedGeneration = c.Generation
			if err := st.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				clock.now = clock.now.Add(5 * time.Second)
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			condition := transitionCondition(t, saved.Status, "NativeHealth")
			if syncs != 2 || condition.Status != nativeStatus || condition.Reason != nativeReason || condition.Message != message || !condition.LastTransitionTime.Equal(clock.now) {
				t.Fatal("real Status/Reason transition lost its new timestamp or native content", syncs, condition, clock.now)
			}
			configuration := transitionCondition(t, saved.Status, "Configuration")
			if !configuration.LastTransitionTime.Equal(original) || configuration.Message != "latest configuration: "+message {
				t.Fatal("message-only refresh was treated as a transition or its content was lost", configuration)
			}
			wantPhase := domain.PhaseReady
			if !healthy {
				wantPhase = domain.PhaseDegraded
			}
			if saved.Status.Phase != wantPhase || saved.Status.ObservedGeneration != c.Generation || len(saved.Status.Members) != 1 || saved.Status.Members[0].Health != healthy || !saved.Status.Members[0].Agent.Healthy {
				t.Fatal("transition preservation changed native health gating", saved.Status)
			}
			ready := transitionCondition(t, saved.Status, "Ready")
			wantTransition := original
			if !healthy {
				wantTransition = clock.now
			}
			if !ready.LastTransitionTime.Equal(wantTransition) {
				t.Fatal("Ready transition did not follow its actual health gate change", ready)
			}
		})
	}
}
