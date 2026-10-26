// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type interruptedRestoreStore struct {
	engine.ClusterStore
	stage string
}

func (s interruptedRestoreStore) GetRestore(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Restore, error) {
	if s.stage == "restore-load" {
		return nil, errors.New("restore inventory unavailable")
	}
	return s.ClusterStore.GetRestore(ctx, ref, name)
}
func (s interruptedRestoreStore) GetBackup(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	if s.stage == "backup-load" {
		return nil, errors.New("backup inventory unavailable")
	}
	return s.ClusterStore.GetBackup(ctx, ref, name)
}
func (s interruptedRestoreStore) GetBackupStorage(ctx context.Context, namespace, name string) (*domain.BackupStorage, error) {
	if s.stage == "environment-load" {
		return nil, errors.New("storage inventory unavailable")
	}
	return s.ClusterStore.GetBackupStorage(ctx, namespace, name)
}

type provedRestoreAbsence struct{}

func (provedRestoreAbsence) ResetSystemUsers(context.Context, string) error { return nil }
func (provedRestoreAbsence) BalancerEnabled(context.Context) (bool, error)  { return false, nil }
func (provedRestoreAbsence) SetBalancer(context.Context, bool) error        { return nil }
func (provedRestoreAbsence) ObserveRestoreAbsence(context.Context, string, pbm.Env) (int64, bool, error) {
	return 1792568288, true, nil
}

func TestRestoreAbsenceWindowResetsAtProviderBoundary(t *testing.T) {
	for _, stage := range []string{"restore-load", "backup-load", "environment-load", "runtime-recovery-wait", "state-load"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, backend, _ := backupCleanupSetup(t)
			clock := &retryClock{at: time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)}
			rc.Clock = clock
			rc.Cluster.Spec.Backup.Storage = "archive"
			if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: "base"}}); err != nil {
				t.Fatal(err)
			}
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{PBMRestoreFence: true})
					return
				}
				_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{ExitCode: 1, Stderr: "Error: get restore meta: not found"})
			})
			state, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			env, err := p.pbmEnvForStorage(t.Context(), rc, state, rc.Cluster.Ref().Namespace, "archive")
			if err != nil {
				t.Fatal(err)
			}
			env.Hooks = provedRestoreAbsence{}
			deadline := clock.at.Add(time.Minute)
			cp, _ := json.Marshal(map[string]any{"step": "poll", "member": env.Nodes[0].Member, "pbmName": "2026-10-21T07:38:08.180591688Z", "snapshot": "base", "restoreMetadataDeadline": deadline})
			op := &domain.Operation{ID: "restore-op", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: cp}
			proof, err := pbm.RunRestore(t.Context(), env, op)
			if err != nil || proof.Step != "poll" {
				t.Fatal(proof, err)
			}
			op.Checkpoint = proof.Checkpoint
			var before struct {
				Since *time.Time `json:"restoreAbsentSince"`
			}
			_ = json.Unmarshal(op.Checkpoint, &before)
			if before.Since == nil {
				t.Fatal("fixture did not establish the first complete proof")
			}
			clock.at = clock.at.Add(2 * time.Minute)
			originalStore := rc.Store
			originalInternal := append(json.RawMessage(nil), rc.Cluster.Internal...)
			if stage == "runtime-recovery-wait" {
				state.Members[0].RuntimeRecovery.Op = &infra.Op{ID: "accepted-start"}
				state.Members[0].RuntimeRecovery.Action = "start"
				backend.done = false
				if err := save(rc.Cluster, state); err != nil {
					t.Fatal(err)
				}
			} else if stage == "state-load" {
				rc.Cluster.Internal = json.RawMessage(`{`)
			} else {
				rc.Store = interruptedRestoreStore{ClusterStore: rc.Store, stage: stage}
			}
			interrupted, runErr := p.RunOperation(t.Context(), rc, op)
			if stage != "runtime-recovery-wait" && runErr == nil {
				t.Fatal("fixture did not interrupt before PBM polling")
			}
			if interrupted.Checkpoint != nil {
				op.Checkpoint = interrupted.Checkpoint
			}
			var held struct {
				Since *time.Time `json:"restoreAbsentSince"`
			}
			_ = json.Unmarshal(op.Checkpoint, &held)
			if held.Since != nil {
				t.Error("provider kept pre-error absence proof", held.Since)
			}
			rc.Store = originalStore
			rc.Cluster.Internal = originalInternal
			backend.done = true
			resumed, err := pbm.RunRestore(t.Context(), env, op)
			if err != nil || resumed.Step != "poll" {
				t.Fatal("recovery reused an expired pre-error proof", resumed, err)
			}
			var fresh struct {
				Since *time.Time `json:"restoreAbsentSince"`
			}
			_ = json.Unmarshal(resumed.Checkpoint, &fresh)
			if fresh.Since == nil || !fresh.Since.Equal(clock.at) {
				t.Fatal("recovery did not establish a fresh observation window", fresh.Since)
			}
		})
	}
}
