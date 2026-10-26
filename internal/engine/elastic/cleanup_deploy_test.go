// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type supersedingCleanupBackend struct {
	*coreInfra
	deployPolls int
}

func (*supersedingCleanupBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *supersedingCleanupBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "deploy" {
		b.deployPolls++
		return infra.OpState{Status: "starting"}, nil
	}
	return b.coreInfra.OpStatus(ctx, op)
}
func TestCleanupSupersedesNeverPlacedDeploy(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	s.Pending = &pending{Action: "deploy", Member: s.Members[0].Name, Op: infra.Op{ID: "blocked-deployment", Kind: "deploy"}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &supersedingCleanupBackend{coreInfra: f}
	rc.Backend = b
	rc.Cluster.Deleting = true
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if b.deployPolls != 0 {
			t.Fatal("cleanup waited for obsolete deployment")
		}
		if done {
			if len(f.services) != 0 {
				t.Fatal("backend resources remain")
			}
			return
		}
	}
	t.Fatal("cleanup never completed")
}
