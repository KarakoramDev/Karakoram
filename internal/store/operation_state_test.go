// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func operationStateFixture(t *testing.T, s *Store) (*domain.Cluster, *domain.Operation) {
	t.Helper()
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}, Internal: json.RawMessage(`{"stage":"old"}`)}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "replace", Cluster: c.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Checkpoint: json.RawMessage(`{"stage":"prepare"}`)}
	if err := s.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	return c, op
}

func TestUpdateOperationAndClusterStateSurvivesReopen(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	c, op := operationStateFixture(t, s)
	newSpec := c.Spec
	newSpec.Version = "8.0"
	if err := s.UpdateClusterSpec(ctx, domain.ParseClusterKey(c.Name), newSpec); err != nil {
		t.Fatal(err)
	}
	c.Internal = json.RawMessage(`{"newMember":"demom3","replaceActive":true}`)
	c.Status.Phase = domain.PhaseScaling
	op.Checkpoint = json.RawMessage(`{"stage":"join","newMember":"demom3"}`)
	if err := s.UpdateOperationAndClusterState(ctx, op, c); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || string(got.Internal) != string(c.Internal) || got.Status.Phase != domain.PhaseScaling || got.Spec.Version != "8.0" || got.Generation != 2 {
		t.Fatal(got, err)
	}
	resumed, err := reopened.ClaimRunningOperation(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || string(resumed.Checkpoint) != string(op.Checkpoint) {
		t.Fatal(resumed, err)
	}
}

func TestUpdateOperationAndClusterStateRollsBack(t *testing.T) {
	for _, failure := range []string{"cluster-write", "invalid-state", "missing-operation"} {
		t.Run(failure, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c, op := operationStateFixture(t, s)
			c.Internal = json.RawMessage(`{"newMember":"demom3"}`)
			c.Status.Phase = domain.PhaseScaling
			op.Checkpoint = json.RawMessage(`{"stage":"join"}`)
			switch failure {
			case "cluster-write":
				if _, err := s.writer.ExecContext(ctx, "CREATE TRIGGER reject_state BEFORE UPDATE ON clusters BEGIN SELECT RAISE(ABORT, 'state write failed'); END"); err != nil {
					t.Fatal(err)
				}
			case "invalid-state":
				c.Internal = json.RawMessage(`{`)
			case "missing-operation":
				op.ID = "missing"
			}
			if err := s.UpdateOperationAndClusterState(ctx, op, c); err == nil {
				t.Fatal("expected persistence failure")
			}
			got, err := s.GetOperation(ctx, "replace")
			if err != nil || string(got.Checkpoint) != `{"stage":"prepare"}` {
				t.Fatal("checkpoint advanced without provider state", got, err)
			}
			cluster, err := s.GetCluster(ctx, domain.ParseClusterKey("demo"))
			if err != nil || string(cluster.Internal) != `{"stage":"old"}` || cluster.Status.Phase != domain.PhaseCreating {
				t.Fatal("provider state advanced without checkpoint", cluster, err)
			}
		})
	}
}

func TestClaimRunningOperationDoesNotStartPending(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	pending := domain.Operation{ID: "pending", Cluster: "demo"}
	if err := s.EnqueueOperation(ctx, &pending); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimRunningOperation(ctx, domain.ParseClusterKey("demo")); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, pending.ID)
	if err != nil || got.Phase != domain.PhasePending || got.Attempts != 0 || got.StartedAt != nil {
		t.Fatal(got, err)
	}
	running := domain.Operation{ID: "running", Cluster: "demo", Phase: domain.PhaseRunning}
	if err = s.EnqueueOperation(ctx, &running); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimOperation(ctx, domain.ParseClusterKey("demo"))
	if err != nil || claimed.ID != running.ID || claimed.Attempts != 1 {
		t.Fatal("older pending work displaced active operation", claimed, err)
	}
}

func TestDeleteClusterRetiresUnfinishedOperations(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, running := operationStateFixture(t, s)
	completed := time.Date(2024, 1, 5, 9, 0, 0, 0, time.UTC)
	for _, op := range []domain.Operation{
		{ID: "pending", Cluster: "demo"},
		{ID: "succeeded", Cluster: "demo", Phase: domain.PhaseSucceeded, Message: "completed", CompletedAt: &completed},
		{ID: "failed", Cluster: "demo", Phase: domain.PhaseFailed, Message: "original failure", CompletedAt: &completed},
		{ID: "other", Cluster: "other", Phase: domain.PhaseRunning},
	} {
		if err := s.EnqueueOperation(ctx, &op); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteCluster(ctx, domain.ParseClusterKey(c.Name)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{running.ID, "pending"} {
		op, err := s.GetOperation(ctx, id)
		if err != nil || op.Phase != domain.PhaseFailed || op.CompletedAt == nil || op.Message != "Cluster deleted" {
			t.Fatal(op, err)
		}
	}
	for _, id := range []string{"succeeded", "failed"} {
		op, err := s.GetOperation(ctx, id)
		if err != nil || op.CompletedAt == nil || !op.CompletedAt.Equal(completed) || op.Message == "Cluster deleted" {
			t.Fatal("completed history changed", op, err)
		}
	}
	other, err := s.GetOperation(ctx, "other")
	if err != nil || other.Phase != domain.PhaseRunning {
		t.Fatal("another cluster's operation changed", other, err)
	}
	if err = s.CreateCluster(ctx, &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimOperation(ctx, domain.ParseClusterKey("demo")); !errors.Is(err, ErrNotFound) {
		t.Fatal("old operation resumed on new cluster", err)
	}
	fresh := domain.Operation{ID: "fresh", Cluster: "demo"}
	if err = s.EnqueueOperation(ctx, &fresh); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimOperation(ctx, domain.ParseClusterKey("demo"))
	if err != nil || claimed.ID != fresh.ID {
		t.Fatal(claimed, err)
	}
}

func TestDeleteClusterRetirementRollsBackWithDeletion(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, op := operationStateFixture(t, s)
	if _, err := s.writer.ExecContext(ctx, "CREATE TRIGGER reject_delete BEFORE DELETE ON clusters BEGIN SELECT RAISE(ABORT, 'delete failed'); END"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCluster(ctx, domain.ParseClusterKey(c.Name)); err == nil {
		t.Fatal("expected deletion failure")
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseRunning || got.CompletedAt != nil {
		t.Fatal("operation retired although deletion rolled back", got, err)
	}
	if _, err = s.GetCluster(ctx, domain.ParseClusterKey(c.Name)); err != nil {
		t.Fatal(err)
	}
}

func TestDeletionIntentPreventsOperationCheckpointRevival(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, op := operationStateFixture(t, s)
	if err := s.MarkClusterDeleting(ctx, domain.ParseClusterKey(c.Name), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = json.RawMessage(`{"stage":"join"}`)
	c.Internal = json.RawMessage(`{"newMember":"demom3"}`)
	if err := s.UpdateOperationAndClusterState(ctx, op, c); !errors.Is(err, ErrNotFound) {
		t.Fatal("late checkpoint revived retired operation", err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseFailed || got.CompletedAt == nil || got.Message != "Cluster deletion requested" {
		t.Fatal(got, err)
	}
	cluster, err := s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if err != nil || !cluster.Deleting || !cluster.DeleteOptions.KeepVolumes || string(cluster.Internal) != `{"stage":"old"}` {
		t.Fatal(cluster, err)
	}
}
