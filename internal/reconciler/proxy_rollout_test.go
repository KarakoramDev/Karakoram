// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestOrdinaryProxyRolloutTrackerFailsFirstReconcile(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				if terminal {
					return engine.Result{}, fmt.Errorf("observe runtime: %w", &infra.ProxyRolloutFailureError{Service: "proxy", Action: "restart", Issued: time.Unix(1, 0), ForceUpdate: 2, Reason: "paused"})
				}
				return engine.Result{}, fmt.Errorf("observe runtime: %w", infra.ErrUnavailable)
			}
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				return domain.ClusterStatus{Phase: domain.PhaseDegraded, ObservedGeneration: 1}, nil
			}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{ObservedGeneration: 1}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			c.Spec.Version = "corrected-target"
			op := &domain.Operation{Kind: domain.OperationUpdate}
			if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), op), c.Ref(), c.Spec); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(t.Context(), c.Ref())
			got, err := st.GetOperation(t.Context(), op.ID)
			if err != nil {
				t.Fatal(err)
			}
			if terminal {
				if got.Phase != domain.PhaseFailed || got.CompletedAt == nil || got.Failures != 1 || !strings.Contains(got.Message, "manual recovery required") || !strings.Contains(got.Message, "PATCH proxy count/resources") {
					t.Fatalf("terminal receipt retried: %+v", got)
				}
			} else if got.Phase != domain.PhaseRunning || got.CompletedAt != nil {
				t.Fatalf("unrelated availability error made permanent: %+v", got)
			}
			current, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || current.Status.ObservedGeneration >= current.Generation {
				t.Fatalf("failed target acknowledged: %+v %v", current, err)
			}
		})
	}
}
