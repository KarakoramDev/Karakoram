// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRetryEpochPreservesNoChangeSpecUpdates(t *testing.T) {
	for _, secrets := range []bool{false, true} {
		name := "ordinary"
		if secrets {
			name = "with-secrets"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb", Version: "8.0"}}
			values := map[string][]byte{"tls/ca.key": []byte("test-key")}
			if err := s.CreateClusterWithSecrets(WithActor(ctx, "creator"), &c, values); err != nil {
				t.Fatal(err)
			}
			for epoch := int64(1); epoch <= 2; epoch++ {
				if err := s.IncrementClusterRetryEpoch(WithActor(ctx, "operator"), c.Ref()); err != nil {
					t.Fatal(err)
				}
				armed, err := s.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if armed.Spec.RetryEpoch != epoch || armed.Generation != epoch+1 || armed.RequestedBy != "operator" {
					t.Fatalf("unexpected re-armed cluster: %+v", armed)
				}
				// Both PUT and PATCH reach this same ordinary store update path.
				// Use the pre-action spec, as the server-managed epoch is preserved.
				if secrets {
					err = s.UpdateClusterSpecWithSecrets(WithActor(ctx, "noop-editor"), c.Ref(), c.Spec, values)
				} else {
					err = s.UpdateClusterSpec(WithActor(ctx, "noop-editor"), c.Ref(), c.Spec)
				}
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if got.Generation != armed.Generation || got.RequestedBy != armed.RequestedBy || got.Spec.RetryEpoch != epoch {
					t.Fatalf("unchanged update altered re-armed state: generation=%d actor=%q epoch=%d; want %d %q %d", got.Generation, got.RequestedBy, got.Spec.RetryEpoch, armed.Generation, armed.RequestedBy, epoch)
				}
				canonical, err := json.Marshal(armed.Spec)
				if err != nil {
					t.Fatal(err)
				}
				var stored []byte
				var storageType string
				if err := s.reader.QueryRowContext(ctx, "SELECT spec,typeof(spec) FROM clusters WHERE namespace=? AND name=?", armed.Namespace, armed.Name).Scan(&stored, &storageType); err != nil {
					t.Fatal(err)
				}
				if storageType != "blob" || !bytes.Equal(stored, canonical) {
					t.Fatalf("spec is not canonical bytes: type=%s spec=%s want=%s", storageType, stored, canonical)
				}
			}
		})
	}
}
