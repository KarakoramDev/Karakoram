// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type restoreTestProvider struct {
	*operationProvider
	name string
}

func (p *restoreTestProvider) Name() string { return p.name }

func TestQueuedRestoreProviderCompatibility(t *testing.T) {
	for _, target := range []string{"psmdb", "test-first", "test-second"} {
		for _, source := range []string{"", "psmdb", "test-first", "test-second"} {
			t.Run(target+"/"+source, func(t *testing.T) {
				called := false
				p := &restoreTestProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}, name: target}
				p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
					called = true
					return engine.OpResult{Done: true}, nil
				}
				m, s := newTestManager(t, p)
				ctx := t.Context()
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: target}}
				if err := s.CreateCluster(ctx, c); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateBackup(ctx, &domain.Backup{Name: "saved", Cluster: c.Name, Provider: source, Phase: domain.PhaseSucceeded}); err != nil {
					t.Fatal(err)
				}
				r := &domain.Restore{Name: "restore", Cluster: c.Name, Source: domain.RestoreSource{Backup: "saved"}, Phase: domain.PhasePending}
				op := &domain.Operation{ID: "restore", Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"restore"}`)}
				if err := s.EnqueueRestore(ctx, r, op); err != nil {
					t.Fatal(err)
				}
				if _, err := m.reconcile(ctx, c.Ref()); err != nil {
					t.Fatal("terminal rejection must not retry", err)
				}
				got, err := s.GetOperation(ctx, op.ID)
				if err != nil {
					t.Fatal(err)
				}
				matches := source == target || source == "" && target == "psmdb"
				if called != matches {
					t.Fatalf("provider called=%t want=%t", called, matches)
				}
				if matches {
					if got.Phase != domain.PhaseSucceeded {
						t.Fatal(got)
					}
					return
				}
				r, err = s.GetRestore(ctx, c.Ref(), r.Name)
				if err != nil || r.Phase != domain.PhaseFailed || r.CompletedAt == nil || r.Error == "" {
					t.Fatalf("restore=%+v error=%v", r, err)
				}
				if got.Phase != domain.PhaseFailed || got.CompletedAt == nil || got.Attempts != 1 {
					t.Fatalf("operation was not terminally failed: %+v", got)
				}
				pending, err := s.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
				if err != nil || len(pending) != 0 {
					t.Fatalf("failed restore still blocks queue: %+v error=%v", pending, err)
				}
			})
		}
	}
}
