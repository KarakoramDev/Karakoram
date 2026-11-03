// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestAdmissionConflictGuidanceMatchesClusterState(t *testing.T) {
	for _, state := range []string{"deleting", "suspended"} {
		t.Run(state, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
			if err := s.CreateCluster(ctx, &c); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: state == "suspended"}); err != nil {
				t.Fatal(err)
			}
			if state == "suspended" {
				got, err := s.GetCluster(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if err = s.SuspendCluster(ctx, got); err != nil {
					t.Fatal(err)
				}
			}
			err := s.EnqueueOperation(ctx, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart})
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("restart admitted in %s state: %v", state, err)
			}
			resume := strings.Contains(err.Error(), "resume it")
			if resume != (state == "suspended") {
				t.Fatalf("%s guidance mismatch: %v", state, err)
			}
		})
	}
}

func TestPITRReanchorLifecycleAdmission(t *testing.T) {
	for _, kind := range []string{domain.OperationUpgradeAgents, domain.OperationReplaceMember} {
		for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning, domain.PhaseSucceeded} {
			t.Run(kind+"/"+string(phase), func(t *testing.T) {
				s, _ := testStore(t)
				ctx := t.Context()
				c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "mysql"}}
				if err := s.CreateCluster(ctx, &c); err != nil {
					t.Fatal(err)
				}
				at := time.Now().UTC()
				completed := at.Add(-domain.PITRReanchorStabilityPeriod + time.Second)
				maintenance := &domain.Operation{Cluster: c.Name, Kind: kind, Phase: phase}
				if phase == domain.PhaseSucceeded {
					maintenance.CompletedAt = &completed
				}
				if err := s.EnqueueOperation(ctx, maintenance); err != nil {
					t.Fatal(err)
				}
				b := &domain.Backup{Cluster: c.Name, Name: "reanchor", Trigger: domain.BackupTriggerPITRReanchor, CreatedAt: at}
				err := s.EnqueueBackup(ctx, b, &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup})
				var busy *domain.BackupAdmissionBusyError
				if !errors.As(err, &busy) || !busy.Operation {
					t.Fatalf("reanchor admitted during lifecycle maintenance: %v", err)
				}
				backups, err := s.ListBackups(ctx, c.Ref())
				if err != nil || len(backups) != 0 {
					t.Fatalf("rejected backup persisted: %+v, %v", backups, err)
				}
			})
		}
	}
}
