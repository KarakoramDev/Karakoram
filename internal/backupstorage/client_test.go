// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
)

func TestNewClientPreservesEndpointPolicy(t *testing.T) {
	contacted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contacted = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	// Even explicitly approved origins cannot turn loopback into a storage destination.
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", strings.TrimPrefix(server.URL, "http://"))
	storage := domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true, VerifyTLS: true}
	client, closeClient, err := NewClient(storage, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = client.StatObject(ctx, storage.Bucket, "snapshot", minio.StatObjectOptions{}); err == nil {
		t.Fatal("loopback destination accepted")
	}
	if contacted {
		t.Fatal("guarded client contacted loopback")
	}
}
