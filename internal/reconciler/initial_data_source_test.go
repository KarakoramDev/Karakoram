// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type initialDataSourceTestProvider struct{ *restoreTestProvider }

func (*initialDataSourceTestProvider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	var marker struct {
		Applied bool `json:"applied"`
	}
	if len(c.Internal) == 0 {
		return false, nil
	}
	err := json.Unmarshal(c.Internal, &marker)
	return marker.Applied, err
}

func TestAbortedInitialDataSourceRemainsFailedAcrossReconcileAndRestart(t *testing.T) {
	for _, name := range []string{"postgres", "supabase", "psmdb", "elasticsearch", "rabbitmq"} {
		for _, phase := range []domain.Phase{domain.PhaseCancelled, domain.PhaseFailed} {
			t.Run(name+"/"+string(phase), func(t *testing.T) {
				p := &initialDataSourceTestProvider{&restoreTestProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}, name: name}}
				p.syncFn = func(*engine.Context) (engine.Result, error) {
					t.Fatal("ordinary convergence retried an aborted initial data source")
					return engine.Result{}, nil
				}
				m, s := newTestManager(t, p)
				if err := s.CreateBackup(t.Context(), &domain.Backup{Cluster: "source", Name: "base", Phase: domain.PhaseSucceeded, Provider: name}); err != nil {
					t.Fatal(err)
				}
				c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
				op := &domain.Operation{ID: "create", Kind: domain.OperationCreate}
				if err := s.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
					t.Fatal(err)
				}
				op.Phase, op.Message = phase, "initial recovery stopped"
				if err := s.UpdateOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					_, _ = m.reconcile(t.Context(), c.Ref())
					stored, err := s.GetCluster(t.Context(), c.Ref())
					if err != nil || stored.Status.Phase != domain.PhaseFailed {
						t.Fatalf("aborted clone not Failed: %+v %v", stored, err)
					}
					ready := false
					for _, condition := range stored.Status.Conditions {
						ready = ready || condition.Type == "Ready" && condition.Status == "True"
					}
					if ready {
						t.Fatalf("aborted clone advertises Ready: %+v", stored.Status)
					}
					// Reconstruct the manager, retaining only serialized cluster/operation
					// state. The missing native completion receipt remains authoritative.
					replacement, err := New(m.opts)
					if err != nil {
						t.Fatal(err)
					}
					m = replacement
				}
			})
		}
	}
}
