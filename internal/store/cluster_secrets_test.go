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
		v, err := s.GetClusterSecret(ctx, domain.ParseClusterKey(c.Name), key)
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
	if err := s.UpdateClusterSpecWithSecrets(ctx, domain.ParseClusterKey(c.Name), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || got.Generation != 1 {
		t.Fatal(got, err)
	}
	values["tls/ca.crt"] = []byte("second-certificate")
	values["tls/ca.key"] = []byte("second-key")
	if err = s.UpdateClusterSpecWithSecrets(ctx, domain.ParseClusterKey(c.Name), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || got.Generation != 2 {
		t.Fatal(got, err)
	}
	if err = s.UpdateClusterSpecWithSecrets(ctx, domain.ParseClusterKey(c.Name), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || got.Generation != 2 {
		t.Fatal(got, err)
	}
	if err = s.UpdateClusterSpecWithSecrets(ctx, domain.ParseClusterKey("missing"), c.Spec, values); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = s.GetClusterSecret(ctx, domain.ParseClusterKey("missing"), "tls/ca.crt"); !errors.Is(err, ErrNotFound) {
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
	if _, err := s.GetCluster(ctx, domain.ParseClusterKey(c.Name)); !errors.Is(err, ErrNotFound) {
		t.Fatal("cluster survived rollback", err)
	}
	if _, err := s.GetClusterSecret(ctx, domain.ParseClusterKey(c.Name), "tls/ca.crt"); !errors.Is(err, ErrNotFound) {
		t.Fatal("certificate survived rollback", err)
	}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	changed := c.Spec
	changed.Version = "8.0"
	if err := s.UpdateClusterSpecWithSecrets(ctx, domain.ParseClusterKey(c.Name), changed, values); err == nil {
		t.Fatal("failed secret update accepted")
	}
	got, err := s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || got.Generation != 1 || got.Spec.Version != "" {
		t.Fatal("spec survived rollback", got, err)
	}
}

func TestIdenticalSecretUpdatePreservesOperation(t *testing.T) {
	s, _ := testStore(t)
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	create := domain.Operation{Kind: domain.OperationCreate}
	values := map[string][]byte{"input": []byte("first")}
	if err := s.CreateClusterWithSecrets(WithOperation(t.Context(), &create), &c, values); err != nil {
		t.Fatal(err)
	}
	repeat := domain.Operation{Kind: domain.OperationUpdate}
	if err := s.UpdateClusterSpecWithSecrets(WithOperation(t.Context(), &repeat), c.Ref(), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	if repeat.ID != "" {
		t.Fatal("identical secret input created an operation", repeat)
	}
	operations, err := s.ListOperations(t.Context(), c.Ref())
	if err != nil || len(operations) != 1 || operations[0].ID != create.ID || operations[0].Terminal() {
		t.Fatal("identical input superseded accepted work", operations, err)
	}
	values["input"] = []byte("second")
	changed := domain.Operation{Kind: domain.OperationUpdate}
	if err := s.UpdateClusterSpecWithSecrets(WithOperation(t.Context(), &changed), c.Ref(), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	if changed.ID == "" || changed.TargetGeneration != 2 {
		t.Fatal("changed secret did not create a new generation operation", changed)
	}
}

func TestUnchangedSpecMaintenanceOverridePreservesOperation(t *testing.T) {
	for _, secrets := range []bool{false, true} {
		t.Run(map[bool]string{false: "spec", true: "identical-secrets"}[secrets], func(t *testing.T) {
			s, _ := testStore(t)
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
			accepted := domain.Operation{Kind: domain.OperationUpdate}
			values := map[string][]byte{"input": []byte("unchanged")}
			if err := s.CreateClusterWithSecrets(WithOperation(t.Context(), &accepted), &c, values); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				override := domain.Operation{Kind: domain.OperationUpdate, IgnoreMaintenanceWindow: true}
				ctx := WithOperation(t.Context(), &override)
				var err error
				if secrets {
					err = s.UpdateClusterSpecWithSecrets(ctx, c.Ref(), c.Spec, values)
				} else {
					err = s.UpdateClusterSpec(ctx, c.Ref(), c.Spec)
				}
				if err != nil || override.ID != "" {
					t.Fatal("override replaced accepted receipt", override, err)
				}
				got, err := s.GetCluster(t.Context(), c.Ref())
				if err != nil || got.Generation != 1 || got.MaintenanceOverrideGeneration != 1 {
					t.Fatal("unchanged override was not applied to accepted generation", got, err)
				}
				operations, err := s.ListOperations(t.Context(), c.Ref())
				if err != nil || len(operations) != 1 || operations[0].ID != accepted.ID || operations[0].Terminal() {
					t.Fatal("override superseded accepted work", operations, err)
				}
			}
		})
	}
}
