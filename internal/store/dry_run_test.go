// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func operationCount(t *testing.T, s *Store, ref domain.ClusterRef) int {
	t.Helper()
	ops, err := s.ListOperations(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return len(ops)
}

// Spec previews apply the same initial-restore amendment admission as writes.
func TestCheckClusterSpecMatchesInitialRestoreAmendment(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, create := initialAdmissionFixture(t, s)
	update := func() context.Context { return WithOperation(ctx, &domain.Operation{Kind: domain.OperationUpdate}) }

	amended := c.Spec
	amended.Components.Engine.Replicas = 3
	if err := s.CheckClusterSpec(update(), c.Ref(), amended, false); err != nil {
		t.Fatalf("resource amendment before the native claim must preview: %v", err)
	}
	upgraded := c.Spec
	upgraded.Version = "18"
	for name, check := range map[string]func() error{
		"version":     func() error { return s.CheckClusterSpec(update(), c.Ref(), upgraded, false) },
		"credentials": func() error { return s.CheckClusterSpec(update(), c.Ref(), amended, true) },
		"kind": func() error {
			return s.CheckClusterSpec(WithOperation(ctx, &domain.Operation{Kind: domain.OperationTLS}), c.Ref(), amended, false)
		},
	} {
		if err := check(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), create.ID) {
			t.Fatalf("%s: preview error=%v", name, err)
		}
	}

	restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}}
	if err := s.EnqueueRestore(ctx, restore, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimOperation(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	before := operationCount(t, s, c.Ref())
	preview := s.CheckClusterSpec(update(), c.Ref(), amended, false)
	write := s.UpdateClusterSpec(update(), c.Ref(), amended)
	if !errors.Is(preview, ErrConflict) || !errors.Is(write, ErrConflict) || preview.Error() != write.Error() {
		t.Fatalf("claimed amendment: preview=%v write=%v", preview, write)
	}
	if after := operationCount(t, s, c.Ref()); after != before {
		t.Fatalf("preview changed operations: %d -> %d", before, after)
	}
}

// Restore and clone previews honour the source backup dependency fence.
func TestCheckRestoreAndCreateMatchSourceDependencyFence(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: "source"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBackup(ctx, &domain.Backup{Name: "daily", Cluster: "source", Phase: domain.PhaseSucceeded, PBMName: "native"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: "target"}); err != nil {
		t.Fatal(err)
	}
	restore := func() *domain.Restore {
		return &domain.Restore{Name: "from-source", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}, Phase: domain.PhasePending}
	}
	clone := func() *domain.Cluster {
		return &domain.Cluster{Name: "clone", Spec: domain.ClusterSpec{Provider: "postgres", DataSource: &domain.DataSource{Cluster: "source", Backup: "daily"}}}
	}
	if err := s.CheckRestore(ctx, restore()); err != nil {
		t.Fatalf("available source must preview: %v", err)
	}
	if err := s.CheckCreateCluster(ctx, clone()); err != nil {
		t.Fatalf("available clone source must preview: %v", err)
	}
	if err := s.MarkClusterDeleting(ctx, domain.ClusterRef{Name: "source"}, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	preview := s.CheckRestore(ctx, restore())
	write := s.EnqueueRestore(ctx, restore(), &domain.Operation{Cluster: "target", Kind: domain.OperationRestore})
	if !errors.Is(preview, ErrConflict) || write == nil || preview.Error() != write.Error() {
		t.Fatalf("restore: preview=%v write=%v", preview, write)
	}
	preview = s.CheckCreateCluster(ctx, clone())
	write = s.CreateClusterWithSecrets(ctx, clone(), map[string][]byte{"tls/tls.crt": []byte("certificate")})
	if !errors.Is(preview, ErrConflict) || write == nil || preview.Error() != write.Error() {
		t.Fatalf("clone: preview=%v write=%v", preview, write)
	}
	if _, err := s.GetCluster(ctx, domain.ClusterRef{Name: "clone"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("refused clone was created: %v", err)
	}
	if n := operationCount(t, s, domain.ClusterRef{Name: "target"}); n != 0 {
		t.Fatalf("restore previews or refusals queued %d operations", n)
	}
}
