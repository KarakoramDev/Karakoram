// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestResolvedRestorePointDependencyOrdering(t *testing.T) {
	for _, resource := range []string{"backup", "source-cluster"} {
		for _, deleteFirst := range []bool{false, true} {
			order := "restore-first"
			if deleteFirst {
				order = "delete-first"
			}
			t.Run(resource+"/"+order, func(t *testing.T) {
				s, _ := testStore(t)
				ctx := t.Context()
				for _, name := range []string{"source", "target"} {
					if err := s.CreateCluster(ctx, &domain.Cluster{Name: name}); err != nil {
						t.Fatal(err)
					}
				}
				backup := &domain.Backup{Name: "coordinated", Cluster: "source", Provider: "postgres", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "coordinator-base", RestorePoint: "checkpoint"}}
				if err := s.CreateBackup(ctx, backup); err != nil {
					t.Fatal(err)
				}
				// HTTP validation has resolved the selector, but destructive intent
				// can still commit before the restore's writer transaction starts.
				restore := &domain.Restore{Name: "restore-point", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: backup.Name, RestorePoint: "checkpoint"}}
				op := &domain.Operation{ID: "point-dependent", Cluster: "target", Kind: domain.OperationRestore}
				remove := func() error {
					if resource == "source-cluster" {
						return s.MarkClusterDeleting(ctx, domain.ClusterRef{Name: "source"}, domain.DeleteOptions{})
					}
					return s.EnqueueBackupDeletion(ctx, backup, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup})
				}
				if deleteFirst {
					if err := remove(); err != nil {
						t.Fatal(err)
					}
					if err := s.CheckRestore(ctx, restore); !errors.Is(err, ErrConflict) {
						t.Fatalf("preview accepted stale resolved restore point: %v", err)
					}
					if err := s.EnqueueRestore(ctx, restore, op); !errors.Is(err, ErrConflict) {
						t.Fatalf("writer admitted stale resolved restore point: %v", err)
					}
					if _, err := s.GetRestore(ctx, domain.ClusterRef{Name: "target"}, restore.Name); !errors.Is(err, ErrNotFound) {
						t.Fatalf("rejected restore was persisted: %v", err)
					}
					return
				}
				if err := s.CheckRestore(ctx, restore); err != nil {
					t.Fatalf("valid restore-point preview rejected: %v", err)
				}
				if err := s.EnqueueRestore(ctx, restore, op); err != nil {
					t.Fatal(err)
				}
				if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), op.ID) {
					t.Fatalf("destructive intent ignored pending restore-point dependency: %v", err)
				}
			})
		}
	}
}
