// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestoreBackupDeletionOrdering(t *testing.T) {
	for _, clone := range []bool{false, true} {
		for _, deleteFirst := range []bool{false, true} {
			for _, clusterDelete := range []bool{false, true} {
				t.Run(fmtDependencyCase(clone, deleteFirst, clusterDelete), func(t *testing.T) {
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
					admit := func() error {
						if clone {
							target.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: b.Name}
							return s.CreateCluster(WithOperation(ctx, &domain.Operation{ID: "dependent", Kind: domain.OperationCreate}), target)
						}
						r := &domain.Restore{Name: "restore", Cluster: target.Name, Source: domain.RestoreSource{SourceCluster: source.Name, Backup: b.Name}}
						op := &domain.Operation{ID: "dependent", Cluster: target.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
						return s.EnqueueRestore(ctx, r, op)
					}
					remove := func() error {
						if clusterDelete {
							return s.MarkClusterDeleting(ctx, source.Ref(), domain.DeleteOptions{})
						}
						return s.EnqueueBackupDeletion(ctx, b, &domain.Operation{Cluster: source.Name, Kind: domain.OperationDeleteBackup})
					}
					if deleteFirst {
						if err := remove(); err != nil {
							t.Fatal(err)
						}
						if err := admit(); !errors.Is(err, ErrConflict) {
							t.Fatalf("restore admitted after destructive intent: %v", err)
						}
						if clone {
							if _, err := s.GetCluster(ctx, target.Ref()); !errors.Is(err, ErrNotFound) {
								t.Fatalf("rejected clone persisted: %v", err)
							}
						}
						return
					}
					if err := admit(); err != nil {
						t.Fatal(err)
					}
					if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "dependent") || !strings.Contains(err.Error(), "keepBackups=true") {
						t.Fatalf("destruction did not identify dependency/remediation: %v", err)
					}
					if err := s.MarkClusterDeleting(ctx, source.Ref(), domain.DeleteOptions{KeepBackups: true}); err != nil {
						t.Fatal("retained deletion refused", err)
					}
					if _, err := s.GetBackup(ctx, source.Ref(), b.Name); err != nil {
						t.Fatal("dependency lost", err)
					}
					op, err := s.GetOperation(ctx, "dependent")
					if err != nil {
						t.Fatal(err)
					}
					op.Phase, op.Message = domain.PhaseFailed, "restore failed safely"
					if err = s.UpdateOperation(ctx, op); err != nil {
						t.Fatal(err)
					}
					if clone {
						if err = remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), op.ID) {
							t.Fatalf("failed initial restore lost source fence: %v", err)
						}
						if err = s.MarkClusterDeleting(ctx, target.Ref(), domain.DeleteOptions{}); err != nil {
							t.Fatal("target deletion refused", err)
						}
					}
					if err = remove(); err != nil {
						t.Fatal("terminal dependent still pins backup", err)
					}
				})
			}
		}
	}
}

func fmtDependencyCase(clone, deleteFirst, clusterDelete bool) string {
	kind, order, resource := "restore", "restore-first", "backup"
	if clone {
		kind = "clone"
	}
	if deleteFirst {
		order = "delete-first"
	}
	if clusterDelete {
		resource = "cluster"
	}
	return kind + "/" + order + "/" + resource
}

func TestBackupDeletionProtectsReferencedPostgresDescendant(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	for _, name := range []string{"source", "target"} {
		if err := s.CreateCluster(ctx, &domain.Cluster{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	full := &domain.Backup{Name: "full", Cluster: "source", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "full-label"}}
	child := &domain.Backup{Name: "child", Cluster: "source", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "child-label", Prior: "full-label"}}
	for _, b := range []*domain.Backup{full, child} {
		if err := s.CreateBackup(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	r := &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: "child"}}
	if err := s.EnqueueRestore(ctx, r, &domain.Operation{ID: "child-restore", Cluster: "target", Kind: domain.OperationRestore}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueBackupDeletion(ctx, full, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup}); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "child-restore") {
		t.Fatalf("ancestor expiration destroys pinned child: %v", err)
	}
}
