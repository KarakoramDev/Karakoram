// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestNamedResourceErrorsPreserveSentinels(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	ref := domain.ClusterRef{Name: "demo"}
	backup := &domain.Backup{Name: "baseline", Cluster: ref.Name}
	restore := &domain.Restore{Name: "recover", Cluster: ref.Name}
	storage := &domain.BackupStorage{Name: "archive", Type: "s3"}
	for _, tc := range []struct {
		resource, name string
		create         func() error
		getMissing     func() error
		updateMissing  func() error
		deleteMissing  func() error
	}{
		{"backup", backup.Name, func() error { return s.CreateBackup(ctx, backup) }, func() error { _, err := s.GetBackup(ctx, ref, "absent"); return err }, func() error { return s.UpdateBackup(ctx, &domain.Backup{Name: "absent", Cluster: ref.Name}) }, func() error { return s.DeleteBackup(ctx, ref, "absent") }},
		{"restore", restore.Name, func() error { return s.CreateRestore(ctx, restore) }, func() error { _, err := s.GetRestore(ctx, ref, "absent"); return err }, func() error { return s.UpdateRestore(ctx, &domain.Restore{Name: "absent", Cluster: ref.Name}) }, func() error { return s.DeleteRestore(ctx, ref, "absent") }},
		{"backupStorage", storage.Name, func() error { return s.CreateBackupStorage(ctx, storage) }, func() error { _, err := s.GetBackupStorage(ctx, "default", "absent"); return err }, func() error { return s.UpdateBackupStorage(ctx, &domain.BackupStorage{Name: "absent"}) }, func() error { return s.DeleteBackupStorage(ctx, "default", "absent") }},
	} {
		t.Run(tc.resource, func(t *testing.T) {
			if err := tc.create(); err != nil {
				t.Fatal(err)
			}
			err := tc.create()
			var resource *domain.ResourceError
			if !errors.Is(err, ErrConflict) || !errors.As(err, &resource) || resource.Name != tc.name || resource.Resource != tc.resource || resource.Reason != domain.ReasonAlreadyExists {
				t.Fatal(err)
			}
			for _, missing := range []func() error{tc.getMissing, tc.updateMissing, tc.deleteMissing} {
				err = missing()
				if !errors.Is(err, ErrNotFound) || !errors.As(err, &resource) || resource.Name != "absent" || resource.Resource != tc.resource || resource.Reason != domain.ReasonNotFound {
					t.Fatal(err)
				}
			}
		})
	}
	_, err := s.GetNamespace(ctx, "absent")
	var resource *domain.ResourceError
	if !errors.Is(err, ErrNotFound) || !errors.As(err, &resource) || resource.Resource != "namespace" || resource.Name != "absent" {
		t.Fatal(err)
	}
}

func TestEnqueueDuplicateNamesAndDeletionState(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	b := &domain.Backup{Name: "baseline", Cluster: "demo", Phase: domain.PhasePending}
	if err := s.EnqueueBackup(ctx, b, &domain.Operation{Cluster: "demo", Kind: "backup"}); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Name: "recover", Cluster: "demo"}
	if err := s.EnqueueRestore(ctx, r, &domain.Operation{Cluster: "demo", Kind: "restore"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		kind, name string
		err        error
	}{
		{"backup", b.Name, s.EnqueueBackup(ctx, b, &domain.Operation{Cluster: "demo", Kind: "backup"})},
		{"restore", r.Name, s.EnqueueRestore(ctx, r, &domain.Operation{Cluster: "demo", Kind: "restore"})},
	} {
		var resource *domain.ResourceError
		if !errors.Is(tc.err, ErrConflict) || !errors.As(tc.err, &resource) || resource.Resource != tc.kind || resource.Name != tc.name || resource.Reason != domain.ReasonAlreadyExists {
			t.Fatal(tc.err)
		}
	}
	err := s.EnqueueBackupDeletion(ctx, b, &domain.Operation{Cluster: "demo", Kind: "delete-backup"})
	var resource *domain.ResourceError
	if !errors.Is(err, ErrConflict) || !errors.As(err, &resource) || resource.Resource != "backup" || resource.Name != b.Name || resource.State != "Pending" || resource.Reason != domain.ReasonInvalidState {
		t.Fatal(err)
	}
}

func TestNamespaceDuplicateNamesAreStructured(t *testing.T) {
	s, _ := testStore(t)
	n := &domain.Namespace{Name: "testing", Phase: "Ready"}
	if err := s.CreateNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	err := s.CreateNamespace(t.Context(), n)
	var conflict *domain.ResourceError
	if !errors.Is(err, ErrConflict) || !errors.As(err, &conflict) || conflict.Resource != "namespace" || conflict.Name != n.Name || conflict.Reason != domain.ReasonAlreadyExists {
		t.Fatal(err)
	}
}

func TestStorageNameCharacterLimit(t *testing.T) {
	if err := ValidateBackupStorageName(strings.Repeat("é", 128)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{strings.Repeat("é", 129), "   ", "bad/name", "bad:name"} {
		err := ValidateBackupStorageName(name)
		var field *domain.FieldError
		if !errors.Is(err, ErrInvalid) || !errors.As(err, &field) || field.Field != "name" {
			t.Fatal(name, err)
		}
	}
}
