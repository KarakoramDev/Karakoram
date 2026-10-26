// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMissingCloneBackupFailsCreateTrackingAcrossProviders(t *testing.T) {
	for _, provider := range []string{"psmdb", "postgres", "rabbitmq", "elasticsearch", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p := &restoreTestProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}, name: provider}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				t.Fatal("provider entered with missing clone source")
				return engine.Result{}, nil
			}
			m, s := newTestManager(t, p)
			ctx := t.Context()
			if err := s.CreateBackup(ctx, &domain.Backup{Name: "base", Cluster: "source", Phase: domain.PhaseSucceeded, Provider: provider}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: provider, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
			op := &domain.Operation{ID: "create", Kind: domain.OperationCreate}
			if err := s.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
				t.Fatal(err)
			}
			if err := s.DeleteBackup(ctx, domain.ClusterRef{Name: "source"}, "base"); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(ctx, c.Ref())
			actual, err := s.GetOperation(ctx, op.ID)
			if err != nil || actual.Phase != domain.PhaseFailed || actual.CompletedAt == nil || !strings.Contains(actual.Message, "select another successful backup") {
				t.Fatalf("clone operation=%+v err=%v", actual, err)
			}
		})
	}
}

func TestMissingRestoreBackupFailsAcrossProviders(t *testing.T) {
	for _, provider := range []string{"psmdb", "postgres", "rabbitmq", "elasticsearch", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p := &restoreTestProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}, name: provider}
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				t.Fatal("provider entered with an authoritatively missing source")
				return engine.OpResult{}, nil
			}
			m, s := newTestManager(t, p)
			ctx := t.Context()
			c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: provider}}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			if err := s.CreateBackup(ctx, &domain.Backup{Name: "base", Cluster: "source", Phase: domain.PhaseSucceeded, Provider: provider}); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Name: "restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}}
			op := &domain.Operation{ID: "restore", Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"restore"}`)}
			if err := s.EnqueueRestore(ctx, r, op); err != nil {
				t.Fatal(err)
			}
			// Simulate externally removed/legacy inventory, bypassing API admission.
			if err := s.DeleteBackup(ctx, domain.ClusterRef{Name: "source"}, "base"); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			actual, err := s.GetOperation(ctx, op.ID)
			if err != nil || actual.Phase != domain.PhaseFailed || actual.CompletedAt == nil || !strings.Contains(actual.Message, "select another successful backup") {
				t.Fatalf("operation=%+v err=%v", actual, err)
			}
			r, err = s.GetRestore(ctx, c.Ref(), r.Name)
			if err != nil || r.Phase != domain.PhaseFailed || r.CompletedAt == nil {
				t.Fatalf("restore=%+v err=%v", r, err)
			}
			c, err = s.GetCluster(ctx, c.Ref())
			if err != nil || c.Status.Phase == domain.PhaseRestoring {
				t.Fatalf("restore lifecycle fence not released: %+v %v", c, err)
			}
		})
	}
}

type failingBackupReadStore struct {
	engine.ClusterStore
	err error
}

func (s failingBackupReadStore) GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error) {
	return nil, s.err
}

func TestRestoreSourceReadFailureClassification(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, s := newTestManager(t, p)
	c := &domain.Cluster{Name: "target"}
	if err := s.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRestore(t.Context(), &domain.Restore{Name: "restore", Cluster: c.Name, Source: domain.RestoreSource{Backup: "base"}}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	transient := errors.New("inventory temporarily unavailable")
	for _, failure := range []error{store.ErrNotFound, engine.ErrNotFound, transient} {
		rc := &engine.Context{Cluster: c, Store: failingBackupReadStore{ClusterStore: s, err: failure}}
		_, err := m.validateRestoreProvider(t.Context(), rc, op)
		var field *engine.ValidationError
		if errors.As(err, &field) != (failure != transient) {
			t.Fatalf("incorrect absence classification for %v: %v", failure, err)
		}
		if failure == transient && !errors.Is(err, transient) {
			t.Fatal("transient error was hidden", err)
		}
	}
}
