// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestorePreflightFailureDoesNotAcquireRecovery(t *testing.T) {
	for _, fault := range []string{"deleted-source", "major-mismatch", "invalid-pitr"} {
		t.Run(fault, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "single")
			b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "source", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
			if fault == "major-mismatch" {
				b.SourceVersion = "16.1"
			}
			if fault != "deleted-source" {
				if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
					t.Fatal(err)
				}
			}
			r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "preflight", Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending}
			if fault == "invalid-pitr" {
				target := time.Now().Add(time.Hour)
				r.Source.PITRTime = &target
			}
			if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "rejected-restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"preflight"}`)}
			result, err := p.runBackupOperation(t.Context(), rc, op)
			if err == nil {
				t.Fatal("invalid restore passed source/PITR preflight")
			}
			op.Checkpoint = result.Checkpoint
			if done, err := p.CancelOperation(t.Context(), rc, op); err != nil || !done.Done {
				t.Fatalf("preflight cleanup failed: %+v %v", done, err)
			}
			s, err := load(rc.Cluster)
			if err != nil || s.RestoreRecovery != nil {
				t.Fatalf("untouched preflight owns recovery: %+v %v", s.RestoreRecovery, err)
			}
			bs, err := loadBackupState(rc.Cluster)
			if err != nil || bs.ManagedRolesHeld || bs.RestoreOperation != "" {
				t.Fatalf("preflight installed a durable hold: %+v %v", bs, err)
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
				t.Fatalf("subsequent valid restore/restart remains fenced: %t %v", allowed, err)
			}
		})
	}
}

func TestRestoreLostFirstCheckpointRetainsFrozenRecovery(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "source", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "recovery", Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhasePending}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "lost-checkpoint", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recovery"}`)}
	result, err := p.runBackupOperation(t.Context(), rc, op)
	if err != nil || result.Step != "hold-access" {
		t.Fatalf("first hold not installed: %+v %v", result, err)
	}
	// Queue checkpoint acknowledgement is lost, but the cluster-state hold and
	// selected backup/generation were saved atomically before that response.
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil || s.RestoreRecovery == nil || string(s.RestoreRecovery.Checkpoint) != string(result.Checkpoint) {
		t.Fatalf("lost response discarded frozen recovery identity: %+v %v", s.RestoreRecovery, err)
	}
}
