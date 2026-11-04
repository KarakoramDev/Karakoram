// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPostgresBindingDeletionSerialization(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for i := range 20 {
		c := domain.Cluster{Name: fmt.Sprintf("source%d", i), Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
		if err := s.CreateCluster(ctx, &c); err != nil {
			t.Fatal(err)
		}
		b := domain.PostgresBinding{ID: fmt.Sprintf("binding%d", i), SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, Extensions: []string{}, RoleName: fmt.Sprintf("kb_%d", i), MinMajor: 16, MaxMajor: 18, State: "pending"}
		var bindErr, deleteErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, bindErr = s.EnsurePostgresBinding(ctx, &b) }()
		go func() {
			defer wg.Done()
			<-start
			deleteErr = s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{})
		}()
		close(start)
		wg.Wait()
		if (bindErr == nil) == (deleteErr == nil) {
			t.Fatalf("exactly one operation must win: binding=%v deletion=%v", bindErr, deleteErr)
		}
		if bindErr == nil {
			if !errors.Is(deleteErr, ErrConflict) || !strings.Contains(deleteErr.Error(), "identity") {
				t.Fatalf("missing consumer conflict: %v", deleteErr)
			}
			if err := s.StartClusterCleanup(ctx, &c); !errors.Is(err, ErrConflict) {
				t.Fatalf("cleanup bypass: %v", err)
			}
			got, err := s.ListPostgresBindings(ctx, c.Ref())
			if err != nil || len(got) != 1 {
				t.Fatalf("list=%+v err=%v", got, err)
			}
			replay, err := s.EnsurePostgresBinding(ctx, &b)
			if err != nil || replay.ID != b.ID {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
		}
	}
}

func TestPostgresBindingMajorAdmissionAndAdditiveEnsure(t *testing.T) {
	s, _ := testStore(t)
	c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	if err := s.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	b := domain.PostgresBinding{ID: "binding", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, MinMajor: 16, MaxMajor: 17, RoleName: "kb_test", State: "pending"}
	first, err := s.EnsurePostgresBinding(t.Context(), &b)
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.ID
	b.ID = "ignored"
	b.Databases = []string{"visibility"}
	b.Extensions = []string{"pgcrypto"}
	second, err := s.EnsurePostgresBinding(t.Context(), &b)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != second.ID || len(second.Databases) != 2 || len(second.Extensions) != 1 {
		t.Fatalf("non-additive ensure: %+v", second)
	}
	c.Spec.Version = "18.3.1"
	if err := s.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("major upgrade bypass: %v", err)
	}
	if err := s.UpdateClusterSpecWithSecrets(t.Context(), c.Ref(), c.Spec, map[string][]byte{"test": []byte("sealed")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("secret/spec major upgrade bypass: %v", err)
	}
	c.Spec.Version = "16.15.1"
	if err := s.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	c.Spec.Version = "15.17.1"
	if err := s.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("minimum major bypass: %v", err)
	}
}

func TestPostgresBindingIncarnationPersistsAndChangesOnRecreation(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	b := domain.PostgresBinding{ID: "binding", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, State: "pending"}
	first, err := s.EnsurePostgresBinding(ctx, &b)
	if err != nil {
		t.Fatal(err)
	}
	old := *first
	persisted, err := s.GetPostgresBinding(ctx, old.ID)
	if err != nil || old.Incarnation == "" || persisted.Incarnation != old.Incarnation || persisted.RoleName != old.RoleName {
		t.Fatalf("incarnation not persisted: %+v %v", persisted, err)
	}
	b.Databases = append(b.Databases, "visibility")
	replay, err := s.EnsurePostgresBinding(ctx, &b)
	if err != nil || replay.Incarnation != old.Incarnation || replay.RoleName != old.RoleName {
		t.Fatalf("additive ensure replaced incarnation: %+v %v", replay, err)
	}
	if err := s.DeletePostgresBinding(ctx, old.ID); err != nil {
		t.Fatal(err)
	}
	// Reusing a caller value must not reuse its nonce or native role name.
	recreated, err := s.EnsurePostgresBinding(ctx, &b)
	if err != nil || recreated.ID != old.ID || recreated.Incarnation == old.Incarnation || recreated.RoleName == old.RoleName {
		t.Fatalf("recreated ownership reused: %+v %v", recreated, err)
	}
}
