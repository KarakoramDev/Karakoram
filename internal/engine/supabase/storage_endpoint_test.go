// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestStorageEndpointProjection(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
	st := rc.Store.(*store.Store)
	storage := domain.BackupStorage{Name: "objects", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "objects", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := st.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		endpoint, bucket string
		force, path      bool
	}{
		{"https://s3.example.test", "objects", false, false},
		{"http://s3.example.test:9000", "objects", true, true},
		{"http://192.0.2.1:9000", "objects", false, true},
		{"https://[2001:db8::1]:9443", "objects", false, true},
		{"https://s3.example.test", "application.objects", false, true},
		{"http://s3.example.test", "application.objects", false, false},
	} {
		t.Run(tc.endpoint+"/"+tc.bucket, func(t *testing.T) {
			storage.EndpointURL, storage.Bucket, storage.ForcePathStyle = tc.endpoint, tc.bucket, tc.force
			if err := st.UpdateBackupStorage(t.Context(), &storage); err != nil {
				t.Fatal(err)
			}
			spec, err := p.desiredSpec(t.Context(), rc, &componentMember{Component: "storage"})
			if err != nil {
				t.Fatal(err)
			}
			if spec.Storage.Endpoint != tc.endpoint || spec.Storage.PathStyle != tc.path || spec.Storage.TLS != (tc.endpoint[:5] == "https") {
				t.Fatal("incorrect endpoint/style/TLS projection")
			}
		})
	}
}
