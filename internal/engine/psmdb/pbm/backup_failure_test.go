// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupFailureCleanupRequiresReceiptIdentity(t *testing.T) {
	for _, mode := range []string{"receipt-succeeded", "receipt-running", "receipt-native-failed", "receipt-selective", "manual-in-window", "manual-selective", "ambiguous", "before-start", "outside-window"} {
		t.Run(mode, func(t *testing.T) {
			env, st, agent, _ := setup(t)
			started, _ := time.Parse(time.RFC3339, "2024-01-05T11:10:50Z")
			cleanup := started.Add(time.Minute)
			b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical", PBMPrefix: env.Prefix, Phase: domain.PhasePending}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "failed-start", Kind: domain.OperationBackup, StartedAt: &started, FailureCleanupStartedAt: &cleanup, LastError: "backup exhausted 20 retries: pbm backup failed (exit -1)", Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"start","member":"m0"}`)}
			receipt := agentapi.PBMResponse{ExitCode: -1}
			if strings.HasPrefix(mode, "receipt-") {
				receipt.ExitCode = 0
				receipt.Output = json.RawMessage(`{"name":"2024-01-05T11:10:54Z"}`)
			}
			agent.mu.Lock()
			agent.receipts[op.ID] = receipt
			agent.mu.Unlock()
			if strings.HasPrefix(mode, "receipt-") {
				// Model actual structured acceptance followed by polling exhaustion.
				submitted, err := RunBackup(t.Context(), env, op)
				if err != nil {
					t.Fatal("incorrect structured initiation status", err)
				}
				op.Checkpoint = submitted.Checkpoint
			}
			// Unnamed cases resume a persisted pre-upgrade start checkpoint:
			// new code terminalizes failed initiation on its first provider pass.
			op.Step = "failure-cleanup"
			if strings.HasPrefix(mode, "receipt-") {
				op.LastError = "backup exhausted polling retries: describe-backup unavailable"
			}
			agent.mu.Lock()
			agent.requests = nil
			agent.mu.Unlock()
			switch mode {
			case "receipt-running":
				agent.activeBackup = true
			case "receipt-native-failed":
				agent.backupDescribeOutput = `{"name":"2024-01-05T11:10:54Z","type":"logical","status":"error","error":"native backup failed"}`
			case "receipt-selective", "manual-selective":
				agent.backupDescribeOutput = `{"name":"2024-01-05T11:10:54Z","type":"logical","status":"done","namespaces":["only_one.collection"]}`
			case "ambiguous":
				agent.backupListOutput = `{"snapshots":[{"name":"2024-01-05T11:10:54Z","type":"logical"},{"name":"2024-01-05T11:10:55Z","type":"logical"}]}`
			case "before-start":
				op.Checkpoint = json.RawMessage(`{"step":"configure"}`)
			case "outside-window":
				started = started.Add(time.Hour)
			}
			res, err := RunBackup(t.Context(), env, op)
			stored, e := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "receipt-succeeded":
				if err != nil || !res.Done || !res.Recovered || stored.Phase != domain.PhaseSucceeded || stored.PBMName != "2024-01-05T11:10:54Z" || stored.Size != 43178 {
					t.Fatal(res, stored, err)
				}
				op.Checkpoint = res.Checkpoint
				op.FailureCleanupElapsed = 24 * time.Hour
				replay, e := RunBackup(t.Context(), env, op)
				if e != nil || !replay.Done || !replay.Recovered {
					t.Fatal("completed recovery not durable", replay, e)
				}
			case "receipt-running":
				if err != nil || res.Done || stored.Phase != domain.PhaseRunning {
					t.Fatal(res, stored, err)
				}
				op.Checkpoint = res.Checkpoint
				op.FailureCleanupElapsed = BackupFailureCleanupTimeout
				expired, e := RunBackup(t.Context(), env, op)
				stored, _ = st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
				if e == nil || !expired.Done || expired.Recovered || stored.Phase != domain.PhaseFailed || !strings.Contains(stored.Error, "Operator action required") {
					t.Fatal(expired, stored, e)
				}
			case "receipt-native-failed":
				if err != nil || !res.Done || res.Recovered || stored.Phase != domain.PhaseFailed || !strings.Contains(res.Message, "native backup failed") {
					t.Fatal(res, stored, err)
				}
			default:
				if err == nil || !res.Done || res.Recovered || stored.Phase != domain.PhaseFailed || !strings.Contains(stored.Error, "candidate unmanaged artifacts") {
					t.Fatal(res, stored, err)
				}
				if !strings.HasPrefix(mode, "receipt-") && stored.PBMName != "" {
					t.Fatal("unproven snapshot claimed by original record", stored)
				}
			}
			requests := agent.recordedRequests()
			for _, req := range requests {
				if req.Command != "list" && req.Command != "status" && req.Command != "describe-backup" {
					t.Fatalf("mutating native command during recovery: %+v", req)
				}
			}
			starts := agent.startCount()
			agent.mu.Lock()
			cancelCalls, deleted := agent.backupCancelCalls, agent.deleted
			agent.mu.Unlock()
			if starts != 0 || cancelCalls != 0 || deleted {
				t.Fatal("native work mutated during cleanup")
			}
			if mode == "manual-in-window" {
				if !strings.Contains(stored.Error, "2024-01-05T11:10:54Z") {
					t.Fatal("missing candidate diagnostic", stored)
				}
				// Releasing the failed queue owner permits ordinary inventory convergence
				// to account for this separate native artifact, without duplicate ownership.
				if _, _, e := SyncInventory(t.Context(), env); e != nil {
					t.Fatal(e)
				}
				records, e := st.ListBackups(t.Context(), env.RC.Cluster.Ref())
				if e != nil || len(records) != 2 {
					t.Fatal(records, e)
				}
				original, _ := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
				adopted, e := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), "2024-01-05T11:10:54Z")
				if e != nil || original.Phase != domain.PhaseFailed || original.PBMName != "" || adopted.PBMName != "2024-01-05T11:10:54Z" || adopted.Phase != domain.PhaseSucceeded {
					t.Fatal(original, adopted, e)
				}
			}
		})
	}
}

func TestBackupFailureCleanupBudgetBeforeDependencies(t *testing.T) {
	env, st, agent, _ := setup(t)
	b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "missing", Type: "logical", Phase: domain.PhasePending}
	if err := st.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationBackup, Step: "failure-cleanup", LastError: "original failure", Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"start"}`), FailureCleanupElapsed: 20 * time.Minute}
	first, err := PrepareBackupFailureCleanup(t.Context(), env.RC, op)
	if err != nil || first.Done {
		t.Fatal(first, err)
	}
	op.Checkpoint = first.Checkpoint
	op.FailureCleanupElapsed += BackupFailureCleanupTimeout
	expired, err := PrepareBackupFailureCleanup(t.Context(), env.RC, op)
	stored, _ := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
	requests := agent.recordedRequests()
	if err == nil || !expired.Done || stored.Phase != domain.PhaseFailed || len(requests) != 0 {
		t.Fatal(expired, stored, err)
	}
}
