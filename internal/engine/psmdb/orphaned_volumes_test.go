// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type orphanEvents struct{ items []domain.Event }

func (e *orphanEvents) Record(_ context.Context, event domain.Event) error {
	e.items = append(e.items, event)
	return nil
}

func orphanFixture(t *testing.T, action string) (*Provider, *engine.Context, *retryInfra, *retryClock, *internalState, *member, *orphanEvents) {
	t.Helper()
	p, rc, f, clock := retrySetup(t)
	s := retryState(t, rc)
	s.UID, s.CleanupBackups = "orphan-fixture", true
	plan(rc.Cluster, s)
	m := s.Members[0]
	m.NeedsAllocation = false
	m.Stage, m.Removing, m.VolumeID = "delete-volume", true, "verified-volume"
	f.services[m.Volume] = &infra.Service{ID: m.VolumeID, Hostname: m.Volume}
	if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	s.Retry = &pending{Action: action, Member: m.Name}
	s.PlatformRetries = map[string]*platformRetry{platformRetryKey(s.Retry): {Failures: 5, StillMountedFailures: 5, Message: "This Local Storage volume is STILL MOUNTED by other services", FirstError: "still mounted", FirstFailedAt: clock.at.Add(-30 * time.Minute), LastAttemptAt: clock.at.Add(-time.Minute), LastProcessID: "failed-delete-process", RetryAfter: clock.at.Add(time.Minute)}}
	events := &orphanEvents{}
	rc.Events = events
	return p, rc, f, clock, s, m, events
}

func TestVolumeOrphanRuleRequiresAllConditions(t *testing.T) {
	for _, variant := range []string{"below-count", "below-age", "other-error", "runtime-present", "unowned"} {
		t.Run(variant, func(t *testing.T) {
			p, rc, f, clock, s, m, events := orphanFixture(t, "delete-volume")
			r := s.PlatformRetries[platformRetryKey(s.Retry)]
			switch variant {
			case "below-count":
				r.Failures, r.StillMountedFailures = 4, 4
			case "below-age":
				r.FirstFailedAt = clock.at.Add(-29 * time.Minute)
			case "other-error":
				r.Message = "platform unavailable"
			case "runtime-present":
				m.ServiceID = "runtime"
				f.services[m.Name] = &infra.Service{ID: m.ServiceID, Hostname: m.Name}
			case "unowned":
				m.VolumeID = "different-service"
			}
			before := f.mutations
			busy, err := p.retryPending(context.Background(), rc, s)
			var wait *engine.PlatformRetryWait
			if !busy || (variant != "unowned" && !errors.As(err, &wait)) || err == nil {
				t.Fatal(busy, err)
			}
			items, listErr := rc.Store.ListOrphanedVolumes(context.Background(), rc.Cluster.Ref().Namespace)
			if listErr != nil || len(items) != 0 || len(events.items) != 0 || m.VolumeOrphaned || f.mutations != before {
				t.Fatal("orphan rule bypassed", items, listErr)
			}
		})
	}
}

func TestStillMountedFailureTrackingResetsForOtherErrors(t *testing.T) {
	_, rc, _, clock, s, m, _ := orphanFixture(t, "delete-volume")
	s.PlatformRetries = nil
	for range 5 {
		s.Pending = &pending{Action: "delete-volume", Member: m.Name, Op: infra.Op{ID: "last-process"}}
		_ = s.failPending(rc, "STILL MOUNTED")
		clock.at = clock.at.Add(time.Minute)
	}
	r := s.PlatformRetries[m.Name+"/delete-volume"]
	if r.StillMountedFailures != 5 || r.FirstError != "STILL MOUNTED" || r.LastProcessID != "last-process" || r.FirstFailedAt.IsZero() {
		t.Fatal(r)
	}
	s.Pending = s.Retry
	_ = s.failPending(rc, "permission denied")
	if r.StillMountedFailures != 0 || !r.FirstFailedAt.IsZero() || r.FirstError != "" {
		t.Fatal("non-mounted error did not break series", r)
	}
	for range 20 {
		s.Pending = s.Retry
		_ = s.failPending(rc, "permission denied")
	}
	if r.StillMountedFailures != 0 {
		t.Fatal(r)
	}
}

func TestLegacyVolumeRetryStartsThirtyMinuteClockOnObservation(t *testing.T) {
	p, rc, _, clock, s, m, _ := orphanFixture(t, "delete-volume")
	r := s.PlatformRetries[platformRetryKey(s.Retry)]
	*r = platformRetry{Failures: 130, Message: "still mounted", RetryAfter: clock.at.Add(time.Hour)}
	observed := clock.at
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var waiting *engine.PlatformRetryWait
	if _, err := p.Sync(context.Background(), rc); !errors.As(err, &waiting) {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	r = s.PlatformRetries[m.Name+"/delete-volume"]
	if !r.FirstFailedAt.Equal(observed) || r.StillMountedFailures != 130 {
		t.Fatal(r)
	}
	if !strings.Contains(waiting.Message, "recorded as orphaned") {
		t.Fatal(waiting.Message)
	}
	clock.at = observed.Add(30*time.Minute - time.Nanosecond)
	if _, err := p.Sync(context.Background(), rc); !errors.As(err, &waiting) {
		t.Fatal(err)
	}
	clock.at = observed.Add(30 * time.Minute)
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	items, err := rc.Store.ListOrphanedVolumes(context.Background(), rc.Cluster.Ref().Namespace)
	if err != nil || len(items) != 1 || !items[0].FirstFailedAt.Equal(observed) || items[0].Attempts != 130 {
		t.Fatal(items, err)
	}
}

func TestOrphanedRemovalConvergesAndNeverReusesIndex(t *testing.T) {
	p, rc, f, _, s, removed, events := orphanFixture(t, "delete-volume")
	// Keep an active member so this is scale-down, not an empty cluster.
	active := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
	active.Stage, active.Version, active.NeedsAllocation = "active", rc.Cluster.Spec.Version, false
	active.Resources = runtimeResources(component(rc.Cluster.Spec, active).Resources)
	active.Disk, active.Containers, active.PublicHash = component(rc.Cluster.Spec, active).Resources, 1, hash(rc.Cluster.Spec.Exposure)
	s.Sets["rs0"].Stage = "ready"
	if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileConfig(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	s.ConfigVersion = 1
	rc.Cluster.Generation, rc.Cluster.Status.ObservedGeneration = 3, 2
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s = retryState(t, rc)
	if s.find(removed.Name) != nil || s.Retry != nil || s.Pending != nil || len(s.PlatformRetries) != 0 || rc.Cluster.Status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != 3 {
		t.Fatalf("scale-down did not converge: %+v %+v", s, rc.Cluster.Status)
	}
	if len(events.items) != 1 || events.items[0].Reason != "VolumeOrphaned" || events.items[0].Type != "Warning" {
		t.Fatal(events.items)
	}
	if len(f.deletes) != 0 {
		t.Fatal("orphan handoff reissued deletion")
	}
	if _, err := rc.Secrets.GetAgentToken(context.Background(), rc.Cluster.Ref(), removed.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("member token retained", err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 2
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	for _, m := range s.Members {
		if m.Name == removed.Name || m.Volume == removed.Volume || (!m.Removing && m.Name != active.Name && m.Index <= active.Index) {
			t.Fatal("reused orphaned member index", m)
		}
	}
}

func TestCleanupOrphanContinuesLaterMembersAndCompletes(t *testing.T) {
	for _, retryForm := range []string{"cleanup", "mid-scale", "lingering-entry"} {
		t.Run(retryForm, func(t *testing.T) {
			p, rc, f, _, s, m, events := orphanFixture(t, "cleanup-volume")
			if retryForm != "cleanup" {
				r := s.PlatformRetries[platformRetryKey(s.Retry)]
				delete(s.PlatformRetries, platformRetryKey(s.Retry))
				s.Retry.Action = "delete-volume"
				s.PlatformRetries[platformRetryKey(s.Retry)] = r
				if retryForm == "lingering-entry" {
					s.Retry = nil
				}
			}
			later := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
			later.ServiceID, later.VolumeID = "later-runtime", "later-volume"
			f.services[later.Name] = &infra.Service{ID: later.ServiceID, Hostname: later.Name}
			f.services[later.Volume] = &infra.Service{ID: later.VolumeID, Hostname: later.Volume}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			done := false
			for range 12 {
				var err error
				done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
				current := retryState(t, rc)
				if current.Pending != nil {
					name := current.Pending.Member
					if current.Pending.Action == "cleanup-volume" {
						name = current.find(name).Volume
					}
					delete(f.services, name)
				}
			}
			if !done || len(f.deletes) != 2 || f.deletes[0] != later.Name || f.deletes[1] != later.Volume || f.services[m.Volume] == nil {
				t.Fatal("orphan blocked later cleanup", done, f.deletes)
			}
			current := retryState(t, rc)
			if !current.find(m.Name).VolumeOrphaned || !current.CleanupSecrets || current.Retry != nil || len(current.PlatformRetries) != 0 {
				t.Fatal(current)
			}
			if len(events.items) != 1 || events.items[0].Reason != "VolumeOrphaned" {
				t.Fatal(events.items)
			}
			if err := rc.Store.(*store.Store).DeleteCluster(context.Background(), rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
			items, err := rc.Store.ListOrphanedVolumes(context.Background(), rc.Cluster.Ref().Namespace)
			if err != nil || len(items) != 1 {
				t.Fatal(items, err)
			}
		})
	}
}

func TestSameNamedClusterCreationSkipsOrphansAndExistingServices(t *testing.T) {
	for _, collision := range []string{"orphan-record", "volume", "runtime"} {
		t.Run(collision, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			ctx := context.Background()
			if err := rc.Store.(*store.Store).DeleteCluster(ctx, rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
			if collision == "orphan-record" {
				if err := rc.Store.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Hostname: "demom0v", Member: "demom0", Cluster: "demo", ServiceID: "old-volume"}); err != nil {
					t.Fatal(err)
				}
			} else {
				name := "demom0"
				if collision == "volume" {
					name += "v"
				}
				f.services[name] = &infra.Service{ID: "foreign", Hostname: name}
			}
			rc.Cluster.Internal = nil
			if err := rc.Store.(*store.Store).CreateCluster(ctx, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			s := retryState(t, rc)
			if len(s.Members) != 1 || s.Members[0].Index != 1 || s.Members[0].Name != "demom1" || s.NextIndex["rs0"] != 2 {
				t.Fatal(s)
			}
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			if len(f.imports) != 1 || f.imports[0][0].Hostname != "demom1v" || f.imports[0][1].Hostname != "demom1" {
				t.Fatal(f.imports)
			}
		})
	}
}

func TestKeepVolumesDoesNotOrphanPendingScaleDownVolume(t *testing.T) {
	p, rc, _, _, s, m, events := orphanFixture(t, "delete-volume")
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true, KeepVolumes: true}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := rc.Store.ListOrphanedVolumes(context.Background(), rc.Cluster.Ref().Namespace)
	retained, retainedErr := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
	if err != nil || retainedErr != nil || len(items) != 0 || len(retained) != 1 || retained[0].Hostname != m.Volume || len(events.items) != 0 {
		t.Fatal(items, retained, err, retainedErr)
	}
}

func TestKeepVolumesWaitsForSubmittedVolumeDeletion(t *testing.T) {
	for _, action := range []string{"delete-volume", "cleanup-volume"} {
		for _, outcome := range []string{"succeeded", "failed"} {
			t.Run(action+"/"+outcome, func(t *testing.T) {
				p, rc, f, _, s, m, events := orphanFixture(t, action)
				ctx := context.Background()
				opts := domain.DeleteOptions{KeepBackups: true, KeepVolumes: true}
				s.Pending = &pending{Action: action, Member: m.Name, Op: infra.Op{ID: "submitted-volume-delete"}}
				s.Retry = nil
				f.done, f.failed = false, false
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					done, err := p.Cleanup(ctx, rc, opts)
					if err != nil || done {
						t.Fatal("retention completed while volume deletion was in flight", done, err)
					}
					current := retryState(t, rc)
					retained, err := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
					if err != nil || len(retained) != 0 || current.Pending == nil || current.Pending.Op.ID != "submitted-volume-delete" || current.Retry != nil || rc.Cluster.Status.Phase == domain.PhaseSuspended {
						t.Fatal("submitted deletion was discarded or retained prematurely", current, retained, err)
					}
				}
				if f.polls != 2 || len(f.deletes) != 0 {
					t.Fatal("in-flight deletion was not polled exclusively", f.polls, f.deletes)
				}
				if outcome == "succeeded" {
					f.done = true
					delete(f.services, m.Volume)
				} else {
					f.failed = true
				}
				if done, err := p.Cleanup(ctx, rc, opts); err != nil || done {
					t.Fatal("terminal observation must precede retention", done, err)
				}
				current := retryState(t, rc)
				if current.Pending != nil || current.Retry != nil || len(current.PlatformRetries) != 0 {
					t.Fatal("terminal volume deletion was queued for retry", current)
				}
				done := false
				for range 3 {
					var err error
					done, err = p.Cleanup(ctx, rc, opts)
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				retained, err := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
				want := 0
				if outcome == "failed" {
					want = 1
				}
				if err != nil || !done || rc.Cluster.Status.Phase != domain.PhaseSuspended || len(retained) != want || len(f.deletes) != 0 || len(events.items) != 0 {
					t.Fatal("retention did not follow observed deletion outcome", done, retained, f.deletes, events.items, err)
				}
			})
		}
	}
}
