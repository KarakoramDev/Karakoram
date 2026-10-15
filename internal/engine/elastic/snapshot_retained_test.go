// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRetainedCatalogRequiresMatchingLastSnapshot(t *testing.T) {
	for _, tc := range []struct {
		catalog string
		allowed bool
	}{
		{`{"snapshots":[{"name":"snapshot-one","uuid":"unique"}]}`, true},
		{`{"snapshots":[{"name":"snapshot-one"},{"name":"snapshot-two"}]}`, false},
		{`{"snapshots":[{"name":"snapshot-two"}]}`, false},
		{`{"snapshots":[]}`, false},
		{`not-json`, false},
	} {
		if err := validateRetainedCatalog([]byte(tc.catalog), "snapshot-one"); (err == nil) != tc.allowed {
			t.Fatalf("catalog %s: %v", tc.catalog, err)
		}
	}
}

func TestRetainedDeletionRejectsBroadNamespaceBeforeS3(t *testing.T) {
	storage := domain.BackupStorage{Namespace: domain.DefaultNamespace, Name: "archive", Bucket: "backups", Prefix: "saved"}
	b := domain.Backup{Namespace: domain.DefaultNamespace, Cluster: "test", Storage: "archive", Provider: "elasticsearch", Location: snapshotLocation(&storage, "saved/default", "kk-archive", "snapshot-one")}
	err := New(ProviderOptions{}).DeleteRetainedBackup(context.Background(), storage, b)
	if !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("broad prefix was not rejected: %v", err)
	}
}
