// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceClusterPersistence(t *testing.T) {
	for _, withSecrets := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "secrets"}[withSecrets], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c := &domain.Cluster{Name: "demo", ManagedBy: &domain.ManagedBy{Kind: "backup-drill", Cluster: "source", ID: "drill-1"}, Spec: domain.ClusterSpec{Provider: "fake", Maintenance: &domain.MaintenancePolicy{TimeZone: "Europe/Prague", Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * 0", Duration: "2h"}}}}}
			values := map[string][]byte(nil)
			if withSecrets {
				values = map[string][]byte{"test-key": []byte("first")}
			}
			if err := s.CreateClusterWithSecrets(ctx, c, values); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Kind: domain.OperationUpdate, IgnoreMaintenanceWindow: true}
			spec := c.Spec
			spec.Version = "2"
			if withSecrets {
				values["test-key"] = []byte("second")
			}
			if err := s.UpdateClusterSpecWithSecrets(WithOperation(ctx, op), c.Ref(), spec, values); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if got.MaintenanceOverrideGeneration != got.Generation || got.MaintenanceOverrideGeneration != 2 || got.ManagedBy == nil || got.ManagedBy.ID != "drill-1" || got.Spec.Maintenance.TimeZone != "Europe/Prague" {
				t.Fatalf("round trip: %+v", got)
			}
			got.ManagedBy = nil
			got.MaintenanceOverrideGeneration = 999
			if err := s.UpdateClusterState(ctx, got); err != nil {
				t.Fatal(err)
			}
			spec.Version = "3"
			if err := s.UpdateClusterSpecWithSecrets(ctx, c.Ref(), spec, values); err != nil {
				t.Fatal(err)
			}
			list, err := s.ListClusters(ctx, "")
			if err != nil || len(list) != 1 {
				t.Fatal(list, err)
			}
			if list[0].ManagedBy == nil || list[0].ManagedBy.ID != "drill-1" || list[0].MaintenanceOverrideGeneration != 2 || list[0].Generation != 3 {
				t.Fatalf("update overwrote server fields: %+v", list[0])
			}
		})
	}
}
func TestMaintenanceClaimSkipsBlockedActions(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	now := time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	ops := []domain.Operation{{ID: "restart", Cluster: "demo", Kind: domain.OperationRestart, Actor: "operator"}, {ID: "backup", Cluster: "demo", Kind: domain.OperationBackup}, {ID: "healing", Cluster: "demo", Kind: domain.OperationReplaceMember, Actor: "system"}, {ID: "override", Cluster: "demo", Kind: domain.OperationRotateTLS, IgnoreMaintenanceWindow: true}}
	for i := range ops {
		ops[i].CreatedAt = now.Add(time.Duration(i) * time.Second)
		if err := s.EnqueueOperation(ctx, &ops[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"backup", "healing", "override"} {
		op, err := s.ClaimOperation(WithMaintenanceTime(ctx, now), c.Ref())
		if err != nil || op.ID != id {
			t.Fatalf("claim=%+v err=%v want=%s", op, err, id)
		}
		op.Phase = domain.PhaseSucceeded
		if err = s.UpdateOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		_, err := s.ClaimOperation(WithMaintenanceTime(ctx, now), c.Ref())
		var wait *MaintenanceWindowWait
		if !errors.As(err, &wait) || !wait.NextStart.Equal(now.Add(time.Minute)) {
			t.Fatalf("wait=%+v err=%v", wait, err)
		}
	}
	blocked, err := s.GetOperation(ctx, "restart")
	if err != nil || blocked.Phase != domain.PhasePending || blocked.Attempts != 0 || blocked.StartedAt != nil || blocked.Elapsed != 0 {
		t.Fatalf("blocked=%+v err=%v", blocked, err)
	}
	events, err := s.ListEvents(ctx, EventFilter{Cluster: "demo", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Reason == "MaintenanceWindowWait" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("wait events=%d", count)
	}
	op, err := s.ClaimOperation(WithMaintenanceTime(ctx, now.Add(time.Minute)), c.Ref())
	if err != nil || op.ID != "restart" || op.Attempts != 1 {
		t.Fatalf("open claim=%+v err=%v", op, err)
	}
	op, err = s.ClaimRunningOperation(WithMaintenanceTime(ctx, now.Add(24*time.Hour)), c.Ref())
	if err != nil || op.ID != "restart" {
		t.Fatalf("running interrupted: %+v %v", op, err)
	}
}
