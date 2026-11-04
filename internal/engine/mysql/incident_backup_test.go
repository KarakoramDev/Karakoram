// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPXCIncidentFullWaitsForDesiredRollout(t *testing.T) {
	for _, change := range []string{"resources", "proxy-count"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b, _, _ := latchedServingPXC(t)
			pxcHistoryStorage(t, rc, b)
			if change == "proxy-count" {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				proxyScaleFixture(t, rc, s)
			}
			rc.Cluster.Generation++
			if change == "resources" {
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			} else {
				proxy := rc.Cluster.Spec.Components.Extra["proxy"]
				if proxy.Replicas == 1 {
					proxy.Replicas = 2
				} else {
					proxy.Replicas = 1
				}
				rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			}
			update := &domain.Operation{ID: "pending-rollout", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpdate, Phase: domain.PhaseRunning, TargetGeneration: rc.Cluster.Generation}
			if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), update); err != nil {
				t.Fatal(err)
			}
			if p.IncidentBackupReady(t.Context(), rc.Cluster, rc.Secrets, "full") {
				t.Fatal("unstarted desired rollout admitted incident full")
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil || s.Rolling == nil {
				t.Fatal("real desired delta did not initialize rollout", err)
			}
			if p.IncidentBackupReady(t.Context(), rc.Cluster, rc.Secrets, "full") {
				t.Fatal("active rollout admitted incident full")
			}
			settled := false
			for range 120 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				if p.IncidentBackupReady(t.Context(), rc.Cluster, rc.Secrets, "full") {
					settled = true
					break
				}
			}
			if !settled {
				t.Fatal("settled rollout never admitted recovery full")
			}
			if rc.Cluster.Status.Phase != domain.PhaseDegraded || rc.Cluster.Status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatal("rollout bypassed history latch", rc.Cluster.Status)
			}
			queuedPXCHistoryFull(t, rc, b, "after-real-rollout")
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if rc.Cluster.Status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("recovery did not finish pending update", rc.Cluster.Status)
			}
		})
	}
}

func TestPXCQueuedIncidentFullPrecedesNewDesiredRollout(t *testing.T) {
	for _, change := range []string{"resources", "proxy-count"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b, _, _ := latchedServingPXC(t)
			pxcHistoryStorage(t, rc, b)
			if change == "proxy-count" {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				proxyScaleFixture(t, rc, s)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if !p.IncidentBackupReady(t.Context(), rc.Cluster, rc.Secrets, "full") {
				t.Fatal("settled history wait did not admit full")
			}
			queuedPXCHistoryFull(t, rc, b, "spec-raced-full", func() {
				// The full has been enqueued, but not claimed. A new spec edit
				// cannot strand it behind the rollout's backup exclusion gate.
				rc.Cluster.Generation++
				if change == "resources" {
					rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
					rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
				} else {
					proxy := rc.Cluster.Spec.Components.Extra["proxy"]
					if proxy.Replicas == 1 {
						proxy.Replicas = 2
					} else {
						proxy.Replicas = 1
					}
					rc.Cluster.Spec.Components.Extra["proxy"] = proxy
				}
				for range 3 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal("queued full caused failed Sync before claim", err)
					}
					s, err := load(rc.Cluster)
					if err != nil || s.Rolling != nil {
						t.Fatal("new rollout overtook queued full", err)
					}
					if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
						t.Fatal("queued full lost native claim admission", allowed, err)
					}
				}
			})
			s, err := load(rc.Cluster)
			if err != nil || activeGTIDIncident(s) != nil {
				t.Fatal("queued full did not acknowledge history", err)
			}
			converge(t, p, rc, b)
			if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("new desired rollout did not resume after full", rc.Cluster.Status)
			}
		})
	}
}
