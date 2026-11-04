// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupRestoreOwnershipAtomicAndReclaim(t *testing.T) {
	for _, retirement := range []string{"delete-target", "finished", "cancelled", "release"} {
		t.Run(retirement, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			source := domain.ClusterRef{Name: "source"}
			refs := []domain.ClusterRef{{Name: "first"}, {Name: "second"}}
			for _, ref := range refs {
				if err := s.CreateCluster(ctx, &domain.Cluster{Name: ref.Name}); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateRestore(ctx, &domain.Restore{Cluster: ref.Name, Name: "recover", Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
			}
			var wg sync.WaitGroup
			acquired := make([]bool, 2)
			errs := make([]error, 2)
			start := make(chan struct{})
			for i := range refs {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					acquired[i], errs[i] = s.AcquireBackupRestore(ctx, source, "snapshot", refs[i], "recover", refs[i].Name+"-operation")
				}(i)
			}
			close(start)
			wg.Wait()
			if errs[0] != nil || errs[1] != nil || acquired[0] == acquired[1] {
				t.Fatalf("ownership was not exclusive: %v %v", acquired, errs)
			}
			winner, loser := 0, 1
			if acquired[1] {
				winner, loser = 1, 0
			}
			owner := refs[winner]
			switch retirement {
			case "delete-target":
				if err := s.DeleteCluster(ctx, owner); err != nil {
					t.Fatal(err)
				}
			case "finished", "cancelled":
				r, err := s.GetRestore(ctx, owner, "recover")
				if err != nil {
					t.Fatal(err)
				}
				r.Phase = domain.PhaseSucceeded
				if retirement == "cancelled" {
					r.Phase = domain.PhaseCancelled
				}
				if err = s.UpdateRestore(ctx, r); err != nil {
					t.Fatal(err)
				}
			case "release":
				if err := s.ReleaseBackupRestore(ctx, source, "snapshot", owner, owner.Name+"-operation"); err != nil {
					t.Fatal(err)
				}
			}
			ok, err := s.AcquireBackupRestore(ctx, source, "snapshot", refs[loser], "recover", refs[loser].Name+"-operation")
			if err != nil || !ok {
				t.Fatalf("stale ownership not reclaimed: %v %v", ok, err)
			}
			// A delayed release by the old worker must not erase its successor.
			if err = s.ReleaseBackupRestore(ctx, source, "snapshot", owner, owner.Name+"-operation"); err != nil {
				t.Fatal(err)
			}
			ok, err = s.AcquireBackupRestore(ctx, source, "snapshot", owner, "recover", "new-owner-operation")
			if err != nil || ok {
				t.Fatalf("old worker erased successor ownership: %v %v", ok, err)
			}
		})
	}
}
