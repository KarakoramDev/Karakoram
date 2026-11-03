// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPITRAutoReanchorStableWriter(t *testing.T) {
	for _, topology := range []string{"single", "group", "pxc"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, backend := backupFixture(t, topology)
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			previous := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/pitr-coverage" {
					json.NewEncoder(w).Encode(map[string]any{"sourceUUID": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "windows": []map[string]time.Time{{"start": time.Unix(10, 0), "end": time.Unix(20, 0)}}, "latestForeignEventTime": time.Unix(50, 0)})
					return true
				}
				return previous != nil && previous(w, r, name)
			}
			start := time.Now().UTC()
			for _, elapsed := range []time.Duration{0, 5*time.Minute - time.Second, 5 * time.Minute, 6 * time.Minute} {
				rc.Clock = trafficRenewalClock{at: start.Add(elapsed)}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				backups, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for _, b := range backups {
					if b.Trigger == "pitr-reanchor" {
						count++
						if b.Phase != domain.PhasePending || b.Type != "full" || b.Provider != rc.Cluster.Spec.Provider {
							t.Fatalf("wrong backup: %+v", b)
						}
					}
				}
				want := 0
				if elapsed >= 5*time.Minute {
					want = 1
				}
				if count != want {
					t.Fatalf("after %s: got %d reanchor backups, want %d", elapsed, count, want)
				}
			}
		})
	}
}

func TestPITRAutoReanchorDeferrals(t *testing.T) {
	for _, scenario := range []string{"knob-off", "no-storage", "pending-backup", "cooldown", "failback", "maintenance", "rolling", "writer-unavailable", "no-baseline"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, backend := backupFixture(t, "group")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			start := time.Now().UTC()
			const original = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			current := "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
			if scenario != "no-baseline" {
				inventoryBackup(t, rc, "base", "full", "", 10, start.Add(-time.Hour))
			}
			events := &gtidEvents{}
			rc.Events = events
			previous := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				switch r.URL.Path {
				case "/v1/mysql/status":
					native := backend.native[name]
					native.ServerUUID = current
					json.NewEncoder(w).Encode(native)
					return true
				case "/v1/mysql/pitr-coverage":
					json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{SourceUUID: original})
					return true
				}
				return previous != nil && previous(w, r, name)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			var operation *domain.Operation
			expectedReason := ""
			switch scenario {
			case "knob-off":
				off := false
				rc.Cluster.Spec.Backup.PITR.AutoReanchor = &off
			case "no-storage":
				rc.Cluster.Spec.Backup.Storage = ""
				expectedReason = "PITRReanchorSkippedNoStorage"
			case "pending-backup":
				if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "manual", Type: "full", Phase: domain.PhasePending}); err != nil {
					t.Fatal(err)
				}
				expectedReason = "PITRReanchorSkippedBackupInProgress"
			case "cooldown":
				s.PITRReanchor.LastTriggered = start.Add(-30 * time.Minute)
				expectedReason = "PITRReanchorSkippedCooldown"
			case "failback":
				expectedReason = "PITRReanchorSkippedWriterNotStable"
			case "maintenance":
				operation = &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhasePending}
				if err := rc.Store.EnqueueOperation(t.Context(), operation); err != nil {
					t.Fatal(err)
				}
				expectedReason = "PITRReanchorSkippedOperationInProgress"
			case "rolling":
				s.Rolling = &rollingState{}
				operation = &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestart, Phase: domain.PhasePending}
				if err := rc.Store.EnqueueOperation(t.Context(), operation); err != nil {
					t.Fatal(err)
				}
				expectedReason = "PITRReanchorSkippedOperationInProgress"
			case "writer-unavailable":
				current = ""
				expectedReason = "PITRReanchorSkippedWriterUnavailable"
			case "no-baseline":
				expectedReason = "PITRReanchorTriggered"
			}
			pass := func(elapsed time.Duration) {
				t.Helper()
				rc.Clock = trafficRenewalClock{at: start.Add(elapsed)}
				if err := p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
				// Round trip durable state to model a control-plane restart.
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "failback" {
				current = original
				pass(-time.Minute) // A has a verified baseline.
				current = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
			pass(0)
			if scenario == "failback" {
				current = original // A -> B -> A before the stability deadline.
			}
			pass(10 * time.Minute)
			pass(11 * time.Minute)
			backups, _ := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
			count := 0
			for _, b := range backups {
				if b.Trigger == "pitr-reanchor" {
					count++
				}
			}
			want := 0
			if scenario == "no-baseline" {
				want = 1
			}
			if count != want {
				t.Fatalf("got %d reanchors, want %d", count, want)
			}
			if expectedReason != "" {
				count := 0
				for _, event := range events.events {
					if event.Reason == expectedReason {
						count++
					}
				}
				if count != 1 {
					t.Fatalf("reason %s emitted %d times: %+v", expectedReason, count, events.events)
				}
			} else if len(events.events) != 0 {
				t.Fatal(events.events)
			}
			if operation != nil {
				finished := start.Add(12 * time.Minute)
				operation.Phase = domain.PhaseSucceeded
				operation.CompletedAt = &finished
				if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), operation); err != nil {
					t.Fatal(err)
				}
				s.Rolling = nil
				pass(12 * time.Minute)
				pass(17*time.Minute - time.Second)
				backups, _ = rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
				if len(backups) != 1 {
					t.Fatal("maintenance grace period bypassed", backups)
				}
				pass(17 * time.Minute)
				backups, _ = rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
				if len(backups) != 2 {
					t.Fatal("maintenance completion did not reanchor", backups)
				}
			}
		})
	}
}

func TestPITRAutoReanchorMemberMaintenance(t *testing.T) {
	for _, scenario := range []string{"confirmed-volume-lost", "runtime-volume-lost", "proxy-draining", "proxy-drain-operation"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, _ := backupFixture(t, "group")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			at := time.Now().UTC()
			rc.Clock = trafficRenewalClock{at: at}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			pass := func() {
				t.Helper()
				if err := p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
			}
			pass()
			s.PITRReanchor.FirstSeen = at.Add(-time.Hour)
			m := s.data()[0]
			switch scenario {
			case "confirmed-volume-lost":
				m.ConfirmedVolumeLost = true
			case "runtime-volume-lost":
				m.RuntimeRecovery.VolumeLost = true
			case "proxy-draining":
				m.ProxyDraining = true
			case "proxy-drain-operation":
				m.ProxyDrainOperation = "orphaned-rollout"
			}
			pass()
			if s.PITRReanchor.LastReason != "PITRReanchorSkippedOperationInProgress" || !s.PITRReanchor.FirstSeen.Equal(at) {
				t.Fatalf("maintenance did not reset stability: %+v", s.PITRReanchor)
			}
			backups, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
			if err != nil || len(backups) != 0 {
				t.Fatalf("maintenance queued backup: %+v, %v", backups, err)
			}
		})
	}
}

func TestPITRAutoReanchorLifecycleOperations(t *testing.T) {
	for _, kind := range []string{domain.OperationUpgradeAgents, domain.OperationReplaceMember} {
		for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning, domain.PhaseSucceeded, domain.PhaseFailed, domain.PhaseCancelled} {
			t.Run(kind+"/"+string(phase), func(t *testing.T) {
				p, rc, _ := backupFixture(t, "group")
				rc.Cluster.Spec.Backup.PITR.Enabled = true
				at := time.Now().UTC()
				rc.Clock = trafficRenewalClock{at: at}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
				s.PITRReanchor.FirstSeen = at.Add(-time.Hour)
				op := &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: kind, Phase: phase}
				active := phase == domain.PhasePending || phase == domain.PhaseRunning
				if !active {
					op.CompletedAt = &at
				}
				if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				for _, elapsed := range []time.Duration{0, domain.PITRReanchorStabilityPeriod - time.Second} {
					rc.Clock = trafficRenewalClock{at: at.Add(elapsed)}
					if err := p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
						t.Fatal(err)
					}
					reason := "PITRReanchorSkippedWriterNotStable"
					if active {
						reason = "PITRReanchorSkippedOperationInProgress"
					}
					if s.PITRReanchor.LastReason != reason {
						t.Fatalf("got %s, want %s", s.PITRReanchor.LastReason, reason)
					}
					backups, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
					if err != nil || len(backups) != 0 {
						t.Fatalf("operation grace period queued backup: %+v, %v", backups, err)
					}
				}
			})
		}
	}
}

func TestPITRReanchorOrphanDrainRestartsStability(t *testing.T) {
	p, rc, _ := backupFixture(t, "group")
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	at := time.Now().UTC()
	rc.Clock = trafficRenewalClock{at: at}
	op := &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhasePending}
	if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.PITRReanchor.FirstSeen = at.Add(-time.Hour)
	s.data()[0].ProxyDrainOperation = op.ID
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PITRReanchor.FirstSeen.IsZero() {
		t.Fatalf("busy orphan drain retained stale stability: %+v", s.PITRReanchor)
	}
}
