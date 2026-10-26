// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStorageEndpointPersistenceAdmission(t *testing.T) {
	s, _ := testStore(t)
	b := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test/s3"}
	if err := s.CreateBackupStorage(t.Context(), &b); err == nil || !strings.Contains(err.Error(), "path prefixes") {
		t.Fatalf("create prefix: %v", err)
	}
	b.EndpointURL = "https://s3.example.test"
	if err := s.CreateBackupStorage(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	b.EndpointURL += "/s3"
	if err := s.UpdateBackupStorage(t.Context(), &b); err == nil || !strings.Contains(err.Error(), "path prefixes") {
		t.Fatalf("update prefix: %v", err)
	}
}

func TestStorageEmptyEndpointPersistenceCompatibility(t *testing.T) {
	s, _ := testStore(t)
	b := domain.BackupStorage{Name: "archive", Type: "s3"}
	if err := s.CreateBackupStorage(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	b.Description = "Legacy registration remains editable"
	if err := s.UpdateBackupStorage(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackupStorage(t.Context(), "default", b.Name)
	if err != nil || got.EndpointURL != "" || got.Description != b.Description {
		t.Fatalf("legacy read: %+v %v", got, err)
	}
	items, err := s.ListBackupStorages(t.Context(), "default")
	if err != nil || len(items) != 1 || items[0].EndpointURL != "" {
		t.Fatalf("legacy list: %+v %v", items, err)
	}
}
