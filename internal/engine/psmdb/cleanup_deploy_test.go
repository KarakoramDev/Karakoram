// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type supersedingCleanupBackend struct {
	*fakeInfra
	deployPolls int
}

func (*supersedingCleanupBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *supersedingCleanupBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "deploy" {
		b.deployPolls++
		return infra.OpState{Status: "starting", Message: "placement blocked"}, nil
	}
	return b.fakeInfra.OpStatus(ctx, op)
}

func TestCleanupSupersedesNeverPlacedDeploy(t *testing.T) {
	p, rc, f, _ := setup(t)
	s := syncToPending(t, p, rc, "deploy")
	s.Pending.Op.Kind = "deploy"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &supersedingCleanupBackend{fakeInfra: f}
	rc.Backend = b
	rc.Cluster.Deleting = true
	rc.Cluster.Status.Message = "old deploy attempt"
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if b.deployPolls != 0 {
			t.Fatal("cleanup waited behind obsolete deployment")
		}
		if strings.Contains(rc.Cluster.Status.Message, "old deploy") {
			t.Fatal("cleanup retained stale create progress")
		}
		if done {
			if len(f.services) != 0 {
				t.Fatal("cleanup left backend resources")
			}
			return
		}
	}
	t.Fatal("cleanup never completed")
}
