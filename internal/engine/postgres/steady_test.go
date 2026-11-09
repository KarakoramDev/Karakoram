// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSyncSteady(t *testing.T) {
	for _, topology := range []string{"single", "ha", "distributed"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			if topology == "distributed" {
				p, rc, _, _ = setupDistributed(t, 1)
			} else {
				p, rc, _ = setup(t, topology)
			}
			result, err := p.Sync(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if result.Steady {
				t.Fatal("provisioning Sync is steady")
			}
			converge(t, p, rc)
			for range 20 {
				result, err = p.Sync(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if result.Steady {
					break
				}
			}
			if !result.Steady || result.RequeueAfter != 0 {
				t.Fatalf("idle Sync = %+v, wait=%s", result, rc.SyncWait)
			}
			s, _ := load(rc.Cluster)
			s.Pending = &pending{Member: s.Members[0].Name, Action: "resize", Op: infra.Op{ID: "pending-steady-test"}}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			result, _ = p.Sync(t.Context(), rc)
			if result.Steady {
				t.Fatal("pending receipt Sync is steady")
			}
		})
	}
}

func TestSteadyCitusPointDue(t *testing.T) {
	p, rc, _, _ := setupDistributed(t, 1)
	converge(t, p, rc)
	for range 20 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if result.Steady {
			break
		}
	}
	s, _ := load(rc.Cluster)
	at := time.Date(2026, 1, 1, 12, 14, 30, 0, time.UTC)
	rc.Clock = importReplayClock{at: at}
	rc.Cluster.Spec.Backup.Storage = "archive"
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	result, err := steadyResult(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Steady || result.RequeueAfter != 30*time.Second {
		t.Fatalf("quarter-hour wake = %+v", result)
	}
	s.Members[0].TLSReloadPending = true
	result, err = steadyResult(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	if result.Steady {
		t.Fatal("TLS acknowledgement wait is steady")
	}
}
