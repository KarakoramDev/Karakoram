// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestNewClientWithAllowlistStillDeniesLoopback(t *testing.T) {
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", "127.0.0.1:1")
	c, cleanup, err := NewClientWithAllowlist(domain.BackupStorage{Type: "s3", EndpointURL: "http://127.0.0.1:1", Bucket: "backups", Region: "us-east-1", ForcePathStyle: true}, domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = c.BucketExists(ctx, "backups"); err == nil {
		t.Fatal("loopback endpoint accepted")
	}
}
