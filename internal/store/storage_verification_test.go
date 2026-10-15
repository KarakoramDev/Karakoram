// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStorageConnectionChangesInvalidateVerification(t *testing.T) {
	changes := map[string]func(*domain.BackupStorage){
		"type":       func(b *domain.BackupStorage) { b.Type = "other" },
		"endpoint":   func(b *domain.BackupStorage) { b.EndpointURL = "https://other.example" },
		"region":     func(b *domain.BackupStorage) { b.Region = "other" },
		"bucket":     func(b *domain.BackupStorage) { b.Bucket = "other" },
		"prefix":     func(b *domain.BackupStorage) { b.Prefix = "other/" },
		"path style": func(b *domain.BackupStorage) { b.ForcePathStyle = false },
		"TLS":        func(b *domain.BackupStorage) { b.VerifyTLS = false },
		"access key": func(b *domain.BackupStorage) { b.Credentials.AccessKeyID = "other" },
		"secret key": func(b *domain.BackupStorage) { b.Credentials.SecretAccessKey = "other" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, _ := testStore(t)
			b := &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example", Region: "region", Bucket: "bucket", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			if err := s.CreateBackupStorage(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			b.Verification = &domain.StorageVerification{Verified: true, CheckedAt: time.Now().UTC(), Message: "Access verified"}
			if err := s.SaveBackupStorageVerification(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			stale := *b
			change(b)
			if err := s.UpdateBackupStorage(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if b.Verification != nil {
				t.Fatal("update response retained old check")
			}
			got, err := s.GetBackupStorage(t.Context(), "default", b.Name)
			if err != nil || got.Verification != nil {
				t.Fatal(got, err)
			}
			if err := s.SaveBackupStorageVerification(t.Context(), &stale); !errors.Is(err, ErrConflict) {
				t.Fatal("stale probe accepted", err)
			}
		})
	}
}

func TestStorageDescriptionEditPreservesVerification(t *testing.T) {
	s, _ := testStore(t)
	b := &domain.BackupStorage{Name: "archive", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := s.CreateBackupStorage(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	b.Verification = &domain.StorageVerification{Verified: false, CheckedAt: time.Now().UTC(), Message: "Access denied", FailureReason: "access_denied"}
	if err := s.SaveBackupStorageVerification(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	b.Description = "Changed description"
	if err := s.UpdateBackupStorage(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackupStorage(t.Context(), "default", b.Name)
	if err != nil || got.Verification == nil || got.Verification.FailureReason != "access_denied" {
		t.Fatal(got, err)
	}
}
