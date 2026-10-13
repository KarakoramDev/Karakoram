// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestCreateClusterSecretsDuplicateCannotOverwrite(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := s.CreateClusterWithSecrets(ctx, &c, map[string][]byte{"tls/ca.crt": []byte("first-certificate"), "tls/ca.key": []byte("first-key")}); err != nil {
		t.Fatal(err)
	}
	duplicate := domain.Cluster{Name: c.Name, Spec: c.Spec}
	if err := s.CreateClusterWithSecrets(ctx, &duplicate, map[string][]byte{"tls/ca.crt": []byte("second-certificate"), "tls/ca.key": []byte("second-key")}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"tls/ca.crt": "first-certificate", "tls/ca.key": "first-key"} {
		v, err := s.GetClusterSecret(ctx, c.Name, key)
		if err != nil || string(v) != want {
			t.Fatal(key, string(v), err)
		}
	}
}
func TestClusterSecretChangesAdvanceGenerationIdempotently(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	values := map[string][]byte{"tls/ca.crt": []byte("first-certificate"), "tls/ca.key": []byte("first-key")}
	if err := s.CreateClusterWithSecrets(ctx, &c, values); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateClusterSpecWithSecrets(ctx, c.Name, c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCluster(ctx, c.Name)
	if err != nil || got.Generation != 1 {
		t.Fatal(got, err)
	}
	values["tls/ca.crt"] = []byte("second-certificate")
	values["tls/ca.key"] = []byte("second-key")
	if err = s.UpdateClusterSpecWithSecrets(ctx, c.Name, c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetCluster(ctx, c.Name)
	if err != nil || got.Generation != 2 {
		t.Fatal(got, err)
	}
	if err = s.UpdateClusterSpecWithSecrets(ctx, c.Name, c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetCluster(ctx, c.Name)
	if err != nil || got.Generation != 2 {
		t.Fatal(got, err)
	}
	if err = s.UpdateClusterSpecWithSecrets(ctx, "missing", c.Spec, values); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = s.GetClusterSecret(ctx, "missing", "tls/ca.crt"); !errors.Is(err, ErrNotFound) {
		t.Fatal("orphaned secrets", err)
	}
}
func TestClusterAndSecretFailureRollsBackTogether(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.writer.ExecContext(ctx, `CREATE TRIGGER reject_tls_key BEFORE INSERT ON records WHEN NEW.kind='secret' AND NEW.key='tls/ca.key' BEGIN SELECT RAISE(ABORT,'test secret failure'); END`); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	values := map[string][]byte{"tls/ca.crt": []byte("certificate"), "tls/ca.key": []byte("key")}
	if err := s.CreateClusterWithSecrets(ctx, &c, values); err == nil {
		t.Fatal("failed secret insertion accepted")
	}
	if _, err := s.GetCluster(ctx, c.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("cluster survived rollback", err)
	}
	if _, err := s.GetClusterSecret(ctx, c.Name, "tls/ca.crt"); !errors.Is(err, ErrNotFound) {
		t.Fatal("certificate survived rollback", err)
	}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	changed := c.Spec
	changed.Version = "8.0"
	if err := s.UpdateClusterSpecWithSecrets(ctx, c.Name, changed, values); err == nil {
		t.Fatal("failed secret update accepted")
	}
	got, err := s.GetCluster(ctx, c.Name)
	if err != nil || got.Generation != 1 || got.Spec.Version != "" {
		t.Fatal("spec survived rollback", got, err)
	}
}
