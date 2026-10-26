// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestRestoreSnapshotInventoryPreflight(t *testing.T) {
	for _, provider := range []string{"postgres", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "ha")
			rc.Cluster.Spec.Provider = provider
			b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "source", Storage: "archive", Provider: provider, Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "inventory", Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending}
			if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "inventory", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"inventory"}`)}
			present := false
			var listingErr error
			p.restoreSetPresent = func(_ context.Context, _ *domain.BackupStorage, prefix string) (bool, error) {
				if prefix != b.Postgres.Repository+"/backup/kk/"+b.Postgres.Label+"/" {
					t.Fatalf("wrong native set inventory: %s", prefix)
				}
				return present, listingErr
			}
			assertUntouched := func() {
				t.Helper()
				bs, err := loadBackupState(rc.Cluster)
				if err != nil || bs.ManagedRolesHeld || bs.RestoreOperation != "" {
					t.Fatalf("preflight acquired restore ownership: %+v %v", bs, err)
				}
				stored, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
				if err != nil || stored.Phase != domain.PhasePending {
					t.Fatalf("preflight started restore: %+v %v", stored, err)
				}
			}
			if _, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{}); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "select an existing backup") {
				t.Fatalf("absent snapshot not actionable/permanent: %v", err)
			}
			assertUntouched()
			listingErr = errors.New("temporary credentials or storage listing failure")
			if _, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{}); !errors.Is(err, listingErr) || errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("transient listing error became permanent: %v", err)
			}
			assertUntouched()
			present, listingErr = true, nil
			if result, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{}); err != nil || result.Step != "hold-access" {
				t.Fatalf("restore did not recover after inventory recovered: %+v %v", result, err)
			}
		})
	}
}

func TestCloneSnapshotRemovedWhileConfiguringBackup(t *testing.T) {
	for _, provider := range []string{"postgres", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, s := backupFixture(t, "ha")
			rc.Cluster.Spec.Provider = provider
			b := &domain.Backup{Namespace: "default", Cluster: "source", Name: "source", Storage: "archive", Provider: provider, Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source-incarnation"}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
			present := true
			var listingErr error
			p.restoreSetPresent = func(context.Context, *domain.BackupStorage, string) (bool, error) { return present, listingErr }
			configure := func() error {
				sp := pgspec.Spec{Postgres: &pgspec.Postgres{}}
				return p.configureBackup(t.Context(), rc, s, &sp)
			}
			if err := configure(); err != nil {
				t.Fatal(err)
			}
			present = false
			if err := configure(); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("clone kept waiting after snapshot removal: %v", err)
			}
			if _, err := p.syncBackups(t.Context(), rc, s); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "pgBackRest set") {
				t.Fatalf("ordinary clone reconciliation did not surface missing snapshot: %v", err)
			}
			listingErr = errors.New("temporary listing failure")
			if err := configure(); !errors.Is(err, listingErr) || errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("clone listing error became terminal: %v", err)
			}
			if _, err := p.syncBackups(t.Context(), rc, s); !errors.Is(err, listingErr) || errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("ordinary clone reconciliation made listing failure terminal: %v", err)
			}
			present, listingErr = true, nil
			if err := configure(); err != nil {
				t.Fatalf("clone configuration did not recover: %v", err)
			}
		})
	}
}
