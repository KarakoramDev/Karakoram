// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestAdmissionConflictGuidanceMatchesClusterState(t *testing.T) {
	for _, state := range []string{"deleting", "suspended"} {
		t.Run(state, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
			if err := s.CreateCluster(ctx, &c); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: state == "suspended"}); err != nil {
				t.Fatal(err)
			}
			if state == "suspended" {
				got, err := s.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if err = s.SuspendCluster(ctx, got); err != nil {
					t.Fatal(err)
				}
			}
			err := s.EnqueueOperation(ctx, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart})
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("restart admitted in %s state: %v", state, err)
			}
			resume := strings.Contains(err.Error(), "resume it")
			if resume != (state == "suspended") {
				t.Fatalf("%s guidance mismatch: %v", state, err)
			}
		})
	}
}
