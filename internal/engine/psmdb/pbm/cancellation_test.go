// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupCancellationUsesDurableReceiptAndFinalizesRecord(t *testing.T) {
	env, st, agent, _ := setup(t)
	agent.activeBackup = true
	b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical", Phase: domain.PhaseRunning, PBMName: "2024-01-05T11:10:54Z"}
	if err := st.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "backup-task", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"poll","member":"m0","pbmName":"2024-01-05T11:10:54Z"}`)}
	immediate, err := PrepareCancellation(op)
	if err != nil || immediate {
		t.Fatal(immediate, err)
	}
	drive(t, env, op, RunBackup, true)
	got, err := st.GetBackup(context.Background(), env.RC.Cluster.Ref(), b.Name)
	if err != nil || got.Phase != domain.PhaseFailed || got.Error == "" || got.CompletedAt == nil || agent.backupCancelCalls != 1 {
		t.Fatal(got, err, agent.backupCancelCalls)
	}
	found := false
	for _, request := range agent.requests {
		if request.Command == "cancel-backup" && request.RequestID == op.ID+"/cancel" {
			found = true
		}
	}
	if !found {
		t.Fatal("cancellation had no durable receipt key")
	}
}

func TestRestoreCancellationBeforeDataMutationRestoresTargetConfiguration(t *testing.T) {
	env, st, agent, hooks := setup(t)
	r := &domain.Restore{Name: "recover", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "target"}, Phase: domain.PhaseRunning}
	if err := st.CreateRestore(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "restore-task", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: json.RawMessage(`{"step":"source-inventory","member":"m0"}`)}
	immediate, err := PrepareCancellation(op)
	if err != nil || immediate {
		t.Fatal(immediate, err)
	}
	drive(t, env, op, RunRestore, true)
	got, err := st.GetRestore(context.Background(), env.RC.Cluster.Ref(), r.Name)
	if err != nil || got.Phase != domain.PhaseFailed || got.Error != "Restore cancelled before data mutation" || agent.starts != 0 || len(hooks.reset) != 0 {
		t.Fatal(got, err, agent.starts, hooks.reset)
	}
}
