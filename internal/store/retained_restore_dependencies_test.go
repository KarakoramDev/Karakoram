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
			if err := s.MarkRetainedBackupDeleting(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := admit(); !errors.Is(err, ErrConflict) {
				t.Fatalf("restore admitted after retained deletion: %v", err)
			}
		} else {
			if err := admit(); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkRetainedBackupDeleting(ctx, b); !errors.Is(err, ErrConflict) {
				t.Fatalf("retained deletion ignored dependency: %v", err)
			}
		}
	}
}
