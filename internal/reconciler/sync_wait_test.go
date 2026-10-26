// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestGenerationWaitPublishesProviderBarrier(t *testing.T) {
	for _, reason := range []string{"waiting for post-exposure runtime demopx to be Running/Active", ""} {
		t.Run(reason, func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(rc *engine.Context) (engine.Result, error) {
				rc.SyncWait = reason
				return engine.Result{RequeueAfter: time.Second}, nil
			}
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
			}
			m, st := newTestManager(t, p)
			op := &domain.Operation{Kind: domain.OperationCreate}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				got, err := st.GetOperation(t.Context(), op.ID)
				want := reason
				if want == "" {
					want = "provider requested another convergence pass"
				}
				if err != nil || got.Phase != domain.PhaseRunning || !strings.Contains(got.Message, "waiting for generation 1: ") || !strings.Contains(got.Message, want) {
					t.Fatalf("operation=%+v err=%v", got, err)
				}
			}
		})
	}
}
