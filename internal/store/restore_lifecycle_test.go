// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClusterDeletionReleasesRestoreNamesAndPreservesBackups(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "copy"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Cluster: c.Name, Name: "initial-restore", Phase: domain.PhaseSucceeded}
	if err := s.CreateRestore(ctx, r); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Cluster: c.Name, Name: "retained", Phase: domain.PhaseSucceeded}
	if err := s.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	other := &domain.Restore{Namespace: "production", Cluster: c.Name, Name: r.Name, Phase: domain.PhaseSucceeded}
	if err := s.CreateRestore(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCluster(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRestore(ctx, c.Ref(), r.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := s.GetBackup(ctx, c.Ref(), b.Name); err != nil {
		t.Fatal("retained backup removed", err)
	}
	if _, err := s.GetRestore(ctx, domain.ClusterRef{Namespace: other.Namespace, Name: c.Name}, r.Name); err != nil {
		t.Fatal("another namespace was changed", err)
	}
	fresh := &domain.Cluster{Name: c.Name, Spec: domain.ClusterSpec{DataSource: &domain.DataSource{Cluster: "source", Backup: "newbackup"}}}
	if err := s.CreateCluster(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	r = &domain.Restore{Cluster: c.Name, Name: r.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "newbackup"}}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
	if err := s.EnqueueRestore(ctx, r, op); err != nil {
		t.Fatal("new cluster collided with old restore", err)
	}
}
