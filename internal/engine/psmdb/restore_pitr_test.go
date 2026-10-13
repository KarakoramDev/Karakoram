// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestAlternateStorageRestoreEnqueuesFreshPITRBase(t *testing.T) {
	for _, step := range []string{"complete", "done"} {
		t.Run(step, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ctx := context.Background()
			rc.Cluster.Spec.Backup.Storage = "primary"
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			for _, name := range []string{"primary", "alternate"} {
				if err := rc.Store.(*store.Store).CreateBackupStorage(ctx, &domain.BackupStorage{Name: name, Type: "s3", EndpointURL: "https://s3.example.test", Bucket: name, VerifyTLS: true}); err != nil {
					t.Fatal(err)
				}
			}
			if err := rc.Store.CreateBackup(ctx, &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Name: "old-base", Cluster: rc.Cluster.Name, Storage: "alternate", Type: "logical", PBMName: "old-snapshot", Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateRestore(ctx, &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, Name: "restore", Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: "old-base"}}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, ID: "alternate-restore", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
			op.Checkpoint, _ = json.Marshal(map[string]string{"step": step})
			result, err := p.RunOperation(ctx, rc, op)
			if err != nil || !result.Done {
				t.Fatalf("restore result=%+v error=%v", result, err)
			}
			var state struct {
				Pending bool   `json:"freshBaseBackupPending"`
				Name    string `json:"freshBaseBackupName"`
			}
			if step == "complete" {
				var cp struct {
					Required bool `json:"freshBaseBackupRequired"`
				}
				if err := json.Unmarshal(result.Checkpoint, &cp); err != nil || !cp.Required {
					t.Fatalf("temporary alternate configuration lost desired PITR: checkpoint=%s error=%v", result.Checkpoint, err)
				}
			}
			if err := json.Unmarshal(rc.Cluster.Internal, &state); err != nil {
				t.Fatal(err)
			}
			name := "restore-base-" + secrets.Hash(op.ID)[:12]
			if !state.Pending || state.Name != name {
				t.Fatalf("PITR restarted without fresh base gate: %+v", state)
			}
			base, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
			if err != nil {
				t.Fatal(err)
			}
			if base.Storage != "primary" || base.Type != "logical" || base.Trigger != "restore" || base.Phase != domain.PhasePending {
				t.Fatalf("wrong post-restore base: %+v", base)
			}
			queued, err := rc.Store.GetOperation(ctx, op.ID+"-base")
			if err != nil || queued.Kind != domain.OperationBackup || queued.Phase != domain.PhasePending {
				t.Fatalf("base operation=%+v error=%v", queued, err)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			env, err := p.pbmEnv(ctx, rc, s)
			if err != nil || env.PITR.Enabled {
				t.Fatalf("PITR enabled before new snapshot: enabled=%v error=%v", env.PITR.Enabled, err)
			}
			// Replaying completion must reuse the same queued base snapshot.
			op.Checkpoint = result.Checkpoint
			if _, err := p.RunOperation(ctx, rc, op); err != nil {
				t.Fatal(err)
			}
		})
	}
}
