// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClusterMutationsAtomicallyTrackAndSupersede(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb", Version: "7.0"}}
	first := &domain.Operation{Kind: domain.OperationCreate}
	if err := s.CreateCluster(WithOperation(ctx, first), c); err != nil {
		t.Fatal(err)
	}
	if first.TargetGeneration != 1 || first.Target.Name != c.Name {
		t.Fatal(first)
	}
	second := &domain.Operation{Kind: domain.OperationUpgrade}
	c.Spec.Version = "8.0"
	if err := s.UpdateClusterSpecWithSecrets(WithOperation(ctx, second), c.Ref(), c.Spec, nil); err != nil {
		t.Fatal(err)
	}
	retired, err := s.GetOperation(ctx, first.ID)
	if err != nil || retired.Phase != domain.PhaseCancelled || !strings.Contains(retired.Message, second.ID) || retired.CompletedAt == nil {
		t.Fatal(retired, err)
	}
	if second.TargetGeneration != 2 {
		t.Fatal(second)
	}
	// Tracking never occupies the provider operation queue.
	if _, err = s.ClaimOperation(ctx, c.Ref()); err != ErrNotFound {
		t.Fatal(err)
	}
	deletion := &domain.Operation{Kind: domain.OperationDelete}
	if err = s.MarkClusterDeleting(WithOperation(ctx, deletion), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCluster(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	done, err := s.GetOperation(ctx, deletion.ID)
	if err != nil || done.Phase != domain.PhaseSucceeded || done.Message == "" || done.CompletedAt == nil {
		t.Fatal(done, err)
	}
}

func TestCancellationFinalizesInventoryAndPreservesRequest(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning} {
		t.Run(string(phase), func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c := &domain.Cluster{Name: "demo"}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "daily", Cluster: c.Name, Type: "logical"}
			op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup, Phase: phase, Payload: json.RawMessage(`{"backup":"daily"}`)}
			if err := s.EnqueueBackup(ctx, b, op); err != nil {
				t.Fatal(err)
			}
			cancelled, err := s.RequestOperationCancellation(ctx, op.ID, op)
			if err != nil || !cancelled.CancelRequested {
				t.Fatal(cancelled, err)
			}
			if phase == domain.PhasePending {
				got, err := s.GetBackup(ctx, c.Ref(), b.Name)
				if err != nil || got.Phase != domain.PhaseFailed || got.Error != cancelled.Message {
					t.Fatal(got, err)
				}
				remove := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
				if err = s.EnqueueBackupDeletion(ctx, got, remove); err != nil {
					t.Fatal(err)
				}
				if remove.Phase != domain.PhaseSucceeded {
					t.Fatal(remove)
				}
				if err = s.CreateBackup(ctx, &domain.Backup{Cluster: c.Name, Name: b.Name, Type: "logical"}); err != nil {
					t.Fatal("name not reusable", err)
				}
			} else {
				op.Checkpoint = json.RawMessage(`{"step":"poll"}`)
				if err = s.UpdateOperationAndClusterState(ctx, op, c); err != nil {
					t.Fatal(err)
				}
				current, err := s.GetOperation(ctx, op.ID)
				if err != nil || !current.CancelRequested || current.Phase != domain.PhaseRunning {
					t.Fatal(current, err)
				}
			}
		})
	}
}

func TestRetirementFinalizesBackupsAndSuspensionIsHonest(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Status: domain.ClusterStatus{Members: []domain.MemberStatus{{Name: "demom0", State: "PRIMARY", Health: true, Agent: domain.AgentStatus{Healthy: true}}}}}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"daily"}`)}
	b := &domain.Backup{Name: "daily", Cluster: c.Name, Type: "logical"}
	if err := s.EnqueueBackup(ctx, b, op); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackup(ctx, c.Ref(), b.Name)
	if err != nil || got.Phase != domain.PhaseFailed || got.Error != "Cluster deletion requested" || got.CompletedAt == nil {
		t.Fatal(got, err)
	}
	c, err = s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SuspendCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	member := c.Status.Members[0]
	if c.Status.ObservedGeneration != c.Generation || member.State != "SUSPENDED" || member.Health || member.Agent.Healthy {
		t.Fatal(c.Status)
	}
}

func TestFailedOperationFinalizesRestore(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	r := &domain.Restore{Name: "recover", Cluster: "demo"}
	op := &domain.Operation{Cluster: r.Cluster, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
	if err := s.EnqueueRestore(ctx, r, op); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	op.Phase, op.Message, op.CompletedAt = domain.PhaseFailed, "Source backup is unavailable", &now
	if err := s.UpdateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRestore(ctx, domain.ClusterRef{Name: r.Cluster}, r.Name)
	if err != nil || got.Phase != domain.PhaseFailed || got.Error != op.Message || got.OperationID != op.ID {
		t.Fatal(got, err)
	}
}

func TestOperationMigrationFinalizesHistoricalPendingBackups(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	backup := &domain.Backup{Name: "daily", Cluster: "demo", Type: "logical"}
	op := &domain.Operation{Cluster: "demo", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"daily"}`)}
	if err := s.EnqueueBackup(ctx, backup, op); err != nil {
		t.Fatal(err)
	}
	// Reproduce the old schema's retired operation with stale Pending inventory.
	if _, err := s.writer.ExecContext(ctx, "UPDATE records SET phase='Failed',data=json_remove(json_set(data,'$.phase','Failed','$.message','Cluster deletion requested','$.completedAt',json_extract(data,'$.updatedAt')),'$.target') WHERE kind='operation' AND key=?", op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE records SET data=json_remove(data,'$.operationId') WHERE kind='backup'"); err != nil {
		t.Fatal(err)
	}
	script, err := migrations.ReadFile("migrations/006_operations.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.writer.ExecContext(ctx, string(script)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackup(ctx, domain.ClusterRef{Name: "demo"}, backup.Name)
	if err != nil || got.Phase != domain.PhaseFailed || got.OperationID != op.ID || got.Error != "Cluster deletion requested" || got.CompletedAt == nil {
		t.Fatal(got, err)
	}
	retired, err := s.GetOperation(ctx, op.ID)
	if err != nil || retired.Target.Kind != "backup" || retired.Target.Name != backup.Name {
		t.Fatal(retired, err)
	}
}
