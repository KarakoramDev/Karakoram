// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type interleavedReanchorStore struct {
	*store.Store
	beforeEnqueue func()
}

func (s *interleavedReanchorStore) EnqueueBackup(ctx context.Context, b *domain.Backup, op *domain.Operation) error {
	if b.Trigger == domain.BackupTriggerPITRReanchor && s.beforeEnqueue != nil {
		before := s.beforeEnqueue
		s.beforeEnqueue = nil
		before()
	}
	return s.Store.EnqueueBackup(ctx, b, op)
}

func TestPITRReanchorTransactionalAdmission(t *testing.T) {
	for _, scenario := range []string{"manual-backup", "maintenance", "cancelled-maintenance"} {
		t.Run(scenario, func(t *testing.T) {
			maintenance := scenario != "manual-backup"
			p, rc, _ := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			at := time.Now().UTC()
			rc.Clock = trafficRenewalClock{at: at}
			db := rc.Store.(*store.Store)
			events := &gtidEvents{}
			rc.Events = events
			wrapper := &interleavedReanchorStore{Store: db}
			rc.Store = wrapper
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
			rc.Clock = trafficRenewalClock{at: at.Add(5 * time.Minute)}
			wrapper.beforeEnqueue = func() {
				op := &domain.Operation{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name}
				if maintenance {
					op.Kind = domain.OperationRestart
					if err := db.EnqueueOperation(t.Context(), op); err != nil {
						t.Fatal(err)
					}
					if scenario == "cancelled-maintenance" {
						finished := rc.Clock.Now()
						op.Phase = domain.PhaseCancelled
						op.CompletedAt = &finished
						if err := db.UpdateOperation(t.Context(), op); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					op.Kind = domain.OperationBackup
					b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "manual", Type: "full", Trigger: "manual", Phase: domain.PhasePending}
					if err := db.EnqueueBackup(t.Context(), b, op); err != nil {
						t.Fatal(err)
					}
				}
			}
			pass()
			items, err := db.ListBackups(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range items {
				if b.Trigger == domain.BackupTriggerPITRReanchor {
					t.Fatal("automatic full raced active work", b)
				}
			}
			reason := "PITRReanchorSkippedBackupInProgress"
			if maintenance {
				reason = "PITRReanchorSkippedOperationInProgress"
			}
			if s.PITRReanchor.LastReason != reason {
				t.Fatalf("got reason %s, want %s", s.PITRReanchor.LastReason, reason)
			}
			if len(events.events) != 2 || events.events[1].Reason != reason {
				t.Fatal("missing admission skip event", events.events)
			}
		})
	}
}
