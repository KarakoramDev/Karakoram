// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPITRRestoreDeletionOrdering(t *testing.T) {
	for _, clone := range []bool{false, true} {
		for _, deleteFirst := range []bool{false, true} {
			for _, resource := range []string{"backup", "cluster", "retained-cluster"} {
				t.Run(fmtDependencyCase(clone, deleteFirst, resource != "backup")+"/"+resource, func(t *testing.T) {
					s, _ := testStore(t)
					ctx := t.Context()
					source := &domain.Cluster{Name: "source"}
					if err := s.CreateCluster(ctx, source); err != nil {
						t.Fatal(err)
					}
					b := &domain.Backup{Name: "base", Cluster: source.Name, Phase: domain.PhaseSucceeded, PBMName: "native"}
					if err := s.CreateBackup(ctx, b); err != nil {
						t.Fatal(err)
					}
					target := &domain.Cluster{Name: "target"}
					if !clone {
						if err := s.CreateCluster(ctx, target); err != nil {
							t.Fatal(err)
						}
					}
					at := time.Now().UTC().Add(-time.Minute)
					admit := func() error {
						if clone {
							target.Spec.DataSource = &domain.DataSource{Cluster: source.Name, PITRTime: &at}
							return s.CreateCluster(WithOperation(ctx, &domain.Operation{ID: "pitr-dependent", Kind: domain.OperationCreate}), target)
						}
						return s.EnqueueRestore(ctx, &domain.Restore{Name: "recover", Cluster: target.Name, Source: domain.RestoreSource{SourceCluster: source.Name, PITRTime: &at}}, &domain.Operation{ID: "pitr-dependent", Cluster: target.Name, Kind: domain.OperationRestore})
					}
					remove := func() error {
						if resource == "backup" {
							return s.EnqueueBackupDeletion(ctx, b, &domain.Operation{Cluster: source.Name, Kind: domain.OperationDeleteBackup})
						}
						return s.MarkClusterDeleting(ctx, source.Ref(), domain.DeleteOptions{KeepBackups: resource == "retained-cluster"})
					}
					if deleteFirst {
						if err := remove(); err != nil {
							t.Fatal(err)
						}
						if err := admit(); !errors.Is(err, ErrConflict) {
							t.Fatalf("PITR admitted after destructive source intent: %v", err)
						}
						return
					}
					if err := admit(); err != nil {
						t.Fatal(err)
					}
					if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "pitr-dependent") || !strings.Contains(err.Error(), "PITR history") {
						t.Fatalf("PITR source/base/history not fenced: %v", err)
					}
					stored, err := s.GetBackup(ctx, source.Ref(), b.Name)
					if err != nil || stored.Phase != domain.PhaseSucceeded {
						t.Fatalf("refused deletion mutated base: %+v %v", stored, err)
					}
					op, err := s.GetOperation(ctx, "pitr-dependent")
					if err != nil {
						t.Fatal(err)
					}
					op.Phase = domain.PhaseFailed
					if err := s.UpdateOperation(ctx, op); err != nil {
						t.Fatal(err)
					}
					if clone {
						if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), op.ID) {
							t.Fatalf("failed initial PITR lost source fence: %v", err)
						}
						if err := s.MarkClusterDeleting(ctx, target.Ref(), domain.DeleteOptions{}); err != nil {
							t.Fatal("target deletion refused", err)
						}
					}
					if err := remove(); err != nil {
						t.Fatal("terminal PITR still pins source", err)
					}
				})
			}
		}
	}
}

func TestPostgresAncestorDeletionAndChildAdmissionOrdering(t *testing.T) {
	for _, clone := range []bool{false, true} {
		for _, deleteFirst := range []bool{false, true} {
			t.Run(fmtDependencyCase(clone, deleteFirst, false), func(t *testing.T) {
				s, _ := testStore(t)
				ctx := t.Context()
				for _, name := range []string{"source", "target"} {
					if clone && name == "target" {
						continue
					}
					if err := s.CreateCluster(ctx, &domain.Cluster{Name: name}); err != nil {
						t.Fatal(err)
					}
				}
				full := &domain.Backup{Name: "full", Cluster: "source", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "full-label"}}
				mid := &domain.Backup{Name: "mid", Cluster: "source", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "mid-label", References: []string{"full-label"}}}
				child := &domain.Backup{Name: "child", Cluster: "source", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "child-label", Prior: "mid-label"}}
				for _, b := range []*domain.Backup{child, mid, full} {
					if err := s.CreateBackup(ctx, b); err != nil {
						t.Fatal(err)
					}
				}
				admit := func() error {
					if clone {
						return s.CreateCluster(WithOperation(ctx, &domain.Operation{ID: "child-dependent", Kind: domain.OperationCreate}), &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{DataSource: &domain.DataSource{Cluster: "source", Backup: "child"}}})
					}
					return s.EnqueueRestore(ctx, &domain.Restore{Name: "recover", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: "child"}}, &domain.Operation{ID: "child-dependent", Cluster: "target", Kind: domain.OperationRestore})
				}
				remove := func() error {
					return s.EnqueueBackupDeletion(ctx, full, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup})
				}
				if deleteFirst {
					if err := remove(); err != nil {
						t.Fatal(err)
					}
					if err := admit(); !errors.Is(err, ErrConflict) {
						t.Fatalf("child admitted during ancestor expiration: %v", err)
					}
					stored, err := s.GetBackup(ctx, domain.ClusterRef{Name: "source"}, child.Name)
					if err != nil || stored.Phase != domain.PhaseSucceeded {
						t.Fatalf("fixture must leave child Succeeded while ancestor deletes: %+v %v", stored, err)
					}
					return
				}
				if err := admit(); err != nil {
					t.Fatal(err)
				}
				if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "child-dependent") {
					t.Fatalf("ancestor deletion ignored child: %v", err)
				}
			})
		}
	}
}
