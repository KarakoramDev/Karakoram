// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestSyncReplacesLostFreshBase(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseFailed, domain.PhaseCancelled, "missing"} {
		t.Run(string(phase), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ctx := context.Background()
			// Reach bootstrap completion using the real Sync path, then resume
			// after the database-user setup boundary with an active replica set.
			for range 20 {
				if _, err := p.Sync(ctx, rc); err != nil {
					t.Fatal(err)
				}
				s, _ := load(rc.Cluster)
				if set := s.Sets["rs0"]; set != nil && set.Stage == "users" {
					break
				}
			}
			rc.Cluster.Spec.Backup.Storage = "primary"
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			if err := rc.Store.(*store.Store).CreateBackupStorage(ctx, &domain.BackupStorage{Name: "primary", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "backups", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			s.Sets["rs0"].Stage = "ready"
			s.Phase = "ready"
			for _, m := range s.Members {
				m.Stage, m.Version = "active", rc.Cluster.Spec.Version
				activateConfig(rc.Cluster, s, m)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: rc.Cluster.Spec.Version, ConfigVersion: 999, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
					return
				}
				_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{"snapshots":[],"pitr":{"ranges":[]}}`)})
			})
			stamp := time.Unix(100, 0).UTC()
			fresh := freshBaseState{Pending: true, Name: "restore-base-test", Root: "restore-base-test", RequiredAt: stamp}
			if err := fresh.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if phase != "missing" {
				if err := rc.Store.CreateBackup(ctx, &domain.Backup{Name: fresh.Name, Cluster: rc.Cluster.Name, Storage: "primary", Type: "logical", Trigger: "restore", Phase: phase, CreatedAt: stamp}); err != nil {
					t.Fatal(err)
				}
			}
			events := &orphanEvents{}
			rc.Events = events
			for range 10 {
				if _, err := p.Sync(ctx, rc); err != nil {
					t.Fatal("lost base blocked Sync", err)
				}
				if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil {
					t.Fatal(err)
				}
				if fresh.Retries == 1 {
					break
				}
			}
			if fresh.Retries != 1 || !fresh.Pending {
				t.Fatalf("base was not replaced: %+v", fresh)
			}
			backup, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), fresh.Name)
			if err != nil || backup.Phase != domain.PhasePending || backup.Storage != "primary" || backup.Trigger != "restore" {
				t.Fatalf("backup=%+v error=%v", backup, err)
			}
			op, err := rc.Store.GetOperation(ctx, backup.OperationID)
			if err != nil || !strings.Contains(op.Message, "retry 1/3") {
				t.Fatalf("operation=%+v error=%v", op, err)
			}
			if len(events.items) != 1 || events.items[0].Reason != "FreshBaseBackupRetry" {
				t.Fatal(events.items)
			}
			if err := refreshFreshBase(ctx, rc); err != nil {
				t.Fatal(err)
			}
			if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil || fresh.Retries != 1 {
				t.Fatal("pending attempt duplicated", fresh, err)
			}
		})
	}
}

func TestFreshBaseRetryBudgetAndLaterPrimarySnapshot(t *testing.T) {
	_, rc, _, _ := setup(t)
	ctx := context.Background()
	rc.Cluster.Spec.Backup.Storage = "primary"
	rc.Clock = rotationClock{time.Unix(200, 0).UTC()}
	fresh := freshBaseState{Pending: true, Name: "restore-base-budget", Root: "restore-base-budget", RequiredAt: time.Unix(100, 0).UTC()}
	if err := fresh.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	events := &orphanEvents{}
	rc.Events = events
	for range freshBaseRetryLimit {
		if err := refreshFreshBase(ctx, rc); err != nil {
			t.Fatal(err)
		}
		if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil {
			t.Fatal(err)
		}
		b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), fresh.Name)
		if err != nil {
			t.Fatal(err)
		}
		b.Phase = domain.PhaseFailed
		if err := rc.Store.UpdateBackup(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := refreshFreshBase(ctx, rc); err != nil {
			t.Fatal("exhaustion blocked convergence", err)
		}
	}
	if len(events.items) != freshBaseRetryLimit+1 || events.items[len(events.items)-1].Reason != "FreshBaseBackupRequired" {
		t.Fatal(events.items)
	}
	for _, b := range []domain.Backup{
		{Name: "before-restore", Storage: "primary", CreatedAt: time.Unix(99, 0)},
		{Name: "alternate", Storage: "alternate", CreatedAt: time.Unix(201, 0)},
	} {
		b.Cluster, b.Phase = rc.Cluster.Name, domain.PhaseSucceeded
		if err := rc.Store.CreateBackup(ctx, &b); err != nil {
			t.Fatal(err)
		}
	}
	if err := refreshFreshBase(ctx, rc); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil || !fresh.Pending {
		t.Fatal("unsafe snapshot cleared PITR gate", fresh, err)
	}
	if err := rc.Store.CreateBackup(ctx, &domain.Backup{Name: "manual-new-base", Cluster: rc.Cluster.Name, Storage: "primary", Phase: domain.PhaseSucceeded, CreatedAt: time.Unix(202, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := refreshFreshBase(ctx, rc); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil || fresh.Pending {
		t.Fatal("new primary base did not clear gate", fresh, err)
	}
}

func TestRestoreBaseCancellationRefused(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	for _, marked := range []bool{false, true} {
		payload := json.RawMessage(`{"backup":"restore-base-cancel"}`)
		if marked {
			payload = json.RawMessage(`{"backup":"restore-base-cancel","trigger":"restore"}`)
		} else {
			if err := rc.Store.CreateBackup(ctx, &domain.Backup{Name: "restore-base-cancel", Cluster: rc.Cluster.Name, Trigger: "restore", Phase: domain.PhasePending}); err != nil {
				t.Fatal(err)
			}
		}
		for _, checkpoint := range []string{`{}`, `{"step":"poll"}`, `{"cancelling":true}`} {
			err := p.CanCancelOperation(&domain.Operation{ID: "base", Cluster: rc.Cluster.Name, Kind: domain.OperationBackup, Payload: payload, Checkpoint: json.RawMessage(checkpoint)})
			if err == nil || !strings.Contains(err.Error(), "protects PITR continuity") {
				t.Fatal("post-restore base cancellation allowed", err)
			}
		}
	}
}
