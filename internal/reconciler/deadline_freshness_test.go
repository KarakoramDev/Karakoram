// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestTrackingWaitSupersedesResolvedPlacementWithoutInventingFreshness(t *testing.T) {
	p := lifecycleProvider()
	m, st := newTestManager(t, p)
	ctx := t.Context()
	op := &domain.Operation{Kind: domain.OperationUpdate}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	m.observeOperationBlocker(op, "old placement cpu exhausted", old)
	if err := st.UpdateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseUpdating, Message: "waiting for member health", ObservedAt: old.Add(time.Minute)}
	if err := m.trackingOperations(ctx, p, c, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockingMessage != "waiting for member health" || got.BlockingObservedAt == nil || !got.BlockingObservedAt.Equal(c.Status.ObservedAt) {
		t.Fatal(got)
	}
	got.Elapsed = 3 * time.Hour
	if err := st.UpdateOperation(ctx, got); err != nil {
		t.Fatal(err)
	}
	// An unrelated imperative operation can prevent an actual convergence pass.
	c.Status.Message = "unrelated operation progress"
	c.Status.ObservedAt = old.Add(time.Hour)
	if err := m.trackingOperations(ctx, p, c, nil, false, ""); err != nil {
		t.Fatal(err)
	}
	got, err = st.GetOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != domain.PhaseFailed || !strings.Contains(got.Message, "last observed at 2026-10-22T00:01:00Z: waiting for member health") || strings.Contains(got.Message, "unrelated") || strings.Contains(got.Message, "cpu") {
		t.Fatal(got)
	}
}

func TestCreateDeadlineReservesRestoreOutcomeWithLongBlocker(t *testing.T) {
	p := lifecycleProvider()
	m, st := newTestManager(t, p)
	ctx := t.Context()
	op := &domain.Operation{Kind: domain.OperationCreate}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", DataSource: &domain.DataSource{}}}
	if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
		t.Fatal(err)
	}
	op.Elapsed = 3 * time.Hour
	if err := st.UpdateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	c.Status = domain.ClusterStatus{Phase: domain.PhaseCreating, Message: strings.Repeat("界", 1000)}
	if err := m.trackingOperations(ctx, p, c, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Message) > 1024 || !strings.HasSuffix(got.Message, "; initial restore: pending") {
		t.Fatal(got.Message)
	}
}

func TestReconcilePreflightDeadlineIncludesInitialRestoreOutcome(t *testing.T) {
	for _, outcome := range []domain.Phase{domain.PhaseRunning, domain.PhaseSucceeded} {
		t.Run(string(outcome), func(t *testing.T) {
			m, st, p, c, create, native, _ := outcomeFixture(t, "postgres")
			p.finish = true
			stamp := time.Date(2026, 10, 22, 1, 1, 49, 0, time.UTC)
			restore, err := st.GetRestore(t.Context(), c.Ref(), "initial-restore")
			if err != nil {
				t.Fatal(err)
			}
			restore.Phase = outcome
			if outcome == domain.PhaseSucceeded {
				restore.CompletedAt = &stamp
				native.Phase, native.StartedAt, native.CompletedAt = domain.PhaseSucceeded, &stamp, &stamp
				if err := st.UpdateOperation(t.Context(), native); err != nil {
					t.Fatal(err)
				}
				// Match the provider's completion proof, not merely inventory.
				c.Internal = []byte(`{"applied":true}`)
				if err := st.UpdateClusterState(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.UpdateRestore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			create.Elapsed = DefaultOperationTimeout + time.Second
			create.LastError = "old original host volume blocker"
			m.observeOperationBlocker(create, "proxy: cpu exhausted on 3 nodes", stamp.Add(time.Minute))
			if err := st.UpdateOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			failed, err := st.GetOperation(t.Context(), create.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantOutcome := "; initial restore: " + string(outcome)
			if outcome == domain.PhaseSucceeded {
				wantOutcome += " at " + stamp.Format(time.RFC3339Nano)
			}
			if failed.Phase != domain.PhaseFailed || failed.Step != domain.InitialRestoreTrackerDeadline ||
				!strings.HasSuffix(failed.Message, wantOutcome) || !strings.Contains(failed.Message, "last observed at 2026-10-22T01:02:49Z: proxy: cpu exhausted on 3 nodes") ||
				strings.Contains(failed.Message, "old original") || strings.Count(failed.Message, "; initial restore:") != 1 {
				t.Fatalf("preflight lost blocker/outcome or classification: %+v", failed)
			}
		})
	}
}
