// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRetainedBackupDeletionAndRestoreAdmissionAreFenced(t *testing.T) {
	for _, deleteFirst := range []bool{false, true} {
		s, _ := testStore(t)
		ctx := t.Context()
		if err := s.CreateCluster(ctx, &domain.Cluster{Name: "target"}); err != nil {
			t.Fatal(err)
		}
		b := &domain.Backup{Name: "base", Cluster: "deleted-source", Phase: domain.PhaseSucceeded, PBMName: "native"}
		if err := s.CreateBackup(ctx, b); err != nil {
			t.Fatal(err)
		}
		admit := func() error {
			return s.EnqueueRestore(ctx, &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{SourceCluster: b.Cluster, Backup: b.Name}}, &domain.Operation{ID: "dependent", Cluster: "target", Kind: domain.OperationRestore})
		}
		if deleteFirst {
			if _, err := s.MarkRetainedBackupDeleting(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := admit(); !errors.Is(err, ErrConflict) {
				t.Fatalf("restore admitted after retained deletion: %v", err)
			}
		} else {
			if err := admit(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.MarkRetainedBackupDeleting(ctx, b); !errors.Is(err, ErrConflict) {
				t.Fatalf("retained deletion ignored dependency: %v", err)
			}
		}
	}
}

func TestRetainedDeletionRollbackRestoresAdmission(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: "target"}); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "base", Cluster: "deleted-source", Phase: domain.PhaseSucceeded, PBMName: "native"}
	if err := s.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	version, err := s.MarkRetainedBackupDeleting(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RollbackRetainedBackupDeletion(ctx, b, version); err != nil {
		t.Fatal(err)
	}
	if b.Phase != domain.PhaseSucceeded {
		t.Fatal("rollback did not restore success", b.Phase)
	}
	if err := s.EnqueueRestore(ctx, &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{SourceCluster: b.Cluster, Backup: b.Name}}, &domain.Operation{ID: "dependent", Cluster: "target", Kind: domain.OperationRestore}); err != nil {
		t.Fatal("restored inventory remained fenced", err)
	}
	if _, err := s.MarkRetainedBackupDeleting(ctx, b); !errors.Is(err, ErrConflict) {
		t.Fatal("rollback weakened dependency protection", err)
	}
}

func TestRetainedDeletionFencesStaleMarksAndRollbacks(t *testing.T) {
	for _, concurrent := range []string{"retry", "record-update", "cluster-recreated", "record-deleted"} {
		t.Run(concurrent, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			b := &domain.Backup{Name: "base", Cluster: "sourcegone", Phase: domain.PhaseSucceeded, PBMName: "native"}
			if err := s.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			stale := *b
			version, err := s.MarkRetainedBackupDeleting(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.MarkRetainedBackupDeleting(ctx, &stale); !errors.Is(err, ErrConflict) {
				t.Fatal("stale successful snapshot acquired deletion", err)
			}
			switch concurrent {
			case "retry":
				retry := *b
				if _, err := s.MarkRetainedBackupDeleting(ctx, &retry); err != nil {
					t.Fatal(err)
				}
			case "record-update":
				changed := *b
				changed.Error = "cleanup partially completed"
				if err := s.UpdateBackup(ctx, &changed); err != nil {
					t.Fatal(err)
				}
			case "cluster-recreated":
				if err := s.CreateCluster(ctx, &domain.Cluster{Name: b.Cluster}); err != nil {
					t.Fatal(err)
				}
			case "record-deleted":
				if err := s.DeleteBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.RollbackRetainedBackupDeletion(ctx, b, version); !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
				t.Fatal("rollback over concurrent mutation succeeded", err)
			}
			if got, err := s.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); err == nil && got.Phase != domain.PhaseDeleting {
				t.Fatal("concurrent intent was made restorable", got)
			}
		})
	}
}

func TestRetainedDeletionAcceptsReleasedDefaultNamespaceRecord(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	// Exact fields emitted by v0.4.9 CreateBackup/json.Marshal for a default-
	// namespace PBM backup. Namespace and provider were legitimately omitted.
	raw := []byte(`{"name":"saved","cluster":"gone","storage":"archive","type":"logical","trigger":"manual","phase":"Succeeded","pbmName":"native","sourceTopology":{"type":""},"size":0,"createdAt":"2026-11-01T00:00:00Z"}`)
	if _, err := s.writer.ExecContext(ctx, "INSERT INTO records(kind,scope,key,data,secret,hash,phase,created,updated) VALUES('backup','gone','saved',?,'','','Succeeded',1,1)", raw); err != nil {
		t.Fatal(err)
	}
	b, err := s.GetBackup(ctx, domain.ClusterRef{Name: "gone"}, "saved")
	if err != nil || b.Namespace != domain.DefaultNamespace {
		t.Fatal("released record did not load with default namespace", b, err)
	}
	version, err := s.MarkRetainedBackupDeleting(ctx, b)
	if err != nil {
		t.Fatal("released record was rejected by deletion CAS", err)
	}
	if err := s.RollbackRetainedBackupDeletion(ctx, b, version); err != nil {
		t.Fatal("safe refusal could not restore released record", err)
	}
	got, err := s.GetBackup(ctx, domain.ClusterRef{Name: "gone"}, "saved")
	if err != nil || got.Phase != domain.PhaseSucceeded {
		t.Fatal("released record remained stranded", got, err)
	}
}
