// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type supersedingCleanupBackend struct {
	*fakeInfra
	deployPolls int
}

func (*supersedingCleanupBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *supersedingCleanupBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "deploy" {
		b.deployPolls++
		return infra.OpState{Status: "starting"}, nil
	}
	return b.fakeInfra.OpStatus(ctx, op)
}
func TestCleanupSupersedesNeverPlacedDeploy(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Pending = &pending{Action: "deploy", Member: s.Members[0].Name, Op: infra.Op{ID: "blocked-deployment", Kind: "deploy"}}
	for i := 1; i < 3; i++ {
		m := s.Members[i]
		m.Resume = true
		m.ResumeDeploy = &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "blocked-" + m.Name, Kind: "deploy"}}
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	b := &supersedingCleanupBackend{fakeInfra: f}
	rc.Backend = b
	rc.Cluster.Deleting = true
	cleanupCluster(t, p, rc, domain.DeleteOptions{KeepBackups: true})
	if b.deployPolls != 0 || len(f.services) != 0 {
		t.Fatal("obsolete deployment blocked resource deletion")
	}
}
