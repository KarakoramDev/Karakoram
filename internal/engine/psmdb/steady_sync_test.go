// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestSyncSteady(t *testing.T) {
	p, rc, _, _ := renewalCluster(t, 3)
	var steady bool
	for range 20 {
		r, err := p.sync(t.Context(), rc, nil, healthyTestReplication)
		if err != nil {
			t.Fatal(err)
		}
		if r.Steady {
			if r.RequeueAfter != 0 {
				t.Fatal(r)
			}
			steady = true
			break
		}
	}
	if !steady {
		t.Fatal("replica set did not become steady")
	}
	fresh := freshBaseState{Pending: true, Name: "fresh", Root: "fresh", RequiredAt: now(rc)}
	if err := fresh.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	ref := rc.Cluster.Ref()
	if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Namespace: ref.Namespace, Cluster: ref.Name, Name: "fresh", Phase: domain.PhaseRunning, CreatedAt: now(rc)}); err != nil {
		t.Fatal(err)
	}
	r, err := p.sync(t.Context(), rc, nil, healthyTestReplication)
	if err != nil || r.Steady || r.RequeueAfter == 0 {
		t.Fatalf("fresh PBM base pending: %+v %v", r, err)
	}
}
