// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRestoreReadinessLifecycle(t *testing.T) {
	for _, syncDuring := range []bool{false, true} {
		name := "observation"
		if syncDuring {
			name = "synchronization"
		}
		t.Run(name, func(t *testing.T) {
			base := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p := &operationSyncProvider{operationProvider: base, policy: func(*domain.Operation) bool { return syncDuring }}
			m, st := newTestManager(t, p)
			readyStatus := func() domain.ClusterStatus {
				return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}}}
			}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: readyStatus()}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: c.Name}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			base.statusFn = func(*engine.Context) (domain.ClusterStatus, error) { return readyStatus(), nil }
			base.syncFn = func(*engine.Context) (engine.Result, error) { return engine.Result{}, nil }
			completed := false
			base.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				persisted, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				assertRestoreReady(t, persisted.Status, domain.PhaseRestoring, "False", "Restoring")
				return engine.OpResult{Done: completed, Step: "restore-archive", Message: "Recovering data"}, nil
			}
			var transition time.Time
			for range 2 {
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				current, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				stamp := assertRestoreReady(t, current.Status, domain.PhaseRestoring, "False", "Restoring")
				if !transition.IsZero() && !transition.Equal(stamp) {
					t.Fatal("restore readiness transition flapped", transition, stamp)
				}
				transition = stamp
			}
			completed = true
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			current, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			assertRestoreReady(t, current.Status, domain.PhaseUpdating, "False", "RestoreFinished")
			for range 2 {
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				current, err = st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				assertRestoreReady(t, current.Status, domain.PhaseReady, "True", "MembersReady")
			}
		})
	}
}

func assertRestoreReady(t *testing.T, status domain.ClusterStatus, phase domain.Phase, value, reason string) time.Time {
	t.Helper()
	if status.Phase != phase {
		t.Fatalf("phase=%s, want %s", status.Phase, phase)
	}
	for _, condition := range status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status != value || condition.Reason != reason {
				t.Fatalf("Ready=%+v, want %s/%s", condition, value, reason)
			}
			return condition.LastTransitionTime
		}
	}
	t.Fatal("missing Ready condition", status)
	return time.Time{}
}
