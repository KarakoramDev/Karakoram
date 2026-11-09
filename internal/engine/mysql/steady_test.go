// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSyncSteady(t *testing.T) {
	for _, topology := range []string{"single", "group", "pxc"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if topology == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, topology)
			}
			first, err := p.Sync(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if first.Steady {
				t.Fatal("provisioning Sync is steady")
			}
			converge(t, p, rc, b)
			var result engine.Result
			for range 20 {
				result, err = p.Sync(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				if result.Steady {
					break
				}
			}
			if !result.Steady || result.RequeueAfter != 0 {
				t.Fatalf("idle Sync = %+v", result)
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

func TestSteadyReanchorDue(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	at := time.Now().UTC()
	rc.Clock = trafficRenewalClock{at: at}
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	for _, tc := range []struct {
		name   string
		state  reanchorState
		want   time.Duration
		steady bool
	}{
		{"stable-writer", reanchorState{FirstSeen: at.Add(-time.Minute), LastReason: "PITRReanchorSkippedWriterNotStable"}, 4 * time.Minute, true},
		{"cooldown", reanchorState{FirstSeen: at.Add(-time.Hour), LastTriggered: at.Add(-50 * time.Minute), LastReason: "PITRReanchorSkippedCooldown"}, 10 * time.Minute, true},
		{"verification", reanchorState{VerifyAfter: at.Add(3 * time.Minute), LastReason: "PITRReanchorSkippedBaselineUnavailable"}, 3 * time.Minute, true},
		{"pending-backup", reanchorState{LastReason: "PITRReanchorSkippedBackupInProgress"}, 2 * time.Second, false},
		{"enqueued-backup", reanchorState{LastReason: "PITRReanchorTriggered"}, 2 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.PITRReanchor = tc.state
			result := steadyResult(rc, s)
			if result.Steady != tc.steady || result.RequeueAfter != tc.want {
				t.Fatalf("got %+v, want steady=%t delay=%s", result, tc.steady, tc.want)
			}
		})
	}
}
