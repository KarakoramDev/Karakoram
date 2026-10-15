// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

const missingRestoreMetadata = "Error: get restore meta: not found"

func restoreStartupSetup(t *testing.T, physical bool) (Env, *fakeAgent, *routerRetryClock, *domain.Operation) {
	t.Helper()
	env, st, agent, _ := setup(t)
	clock := &routerRetryClock{at: time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)}
	env.RC.Clock = clock
	restore := &domain.Restore{Name: "clone", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}}
	if err := st.CreateRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	// Source resolution and maintenance have already committed these immutable
	// inputs before the restore is submitted into the new target cluster.
	cp := checkpoint{Step: "start", Member: "m0", Snapshot: "base-snapshot", Physical: physical, Source: "source", Storage: "storage", Prefix: "karakoram/source-incarnation"}
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "clone-op", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"clone"}`), Checkpoint: raw}
	started, err := RunRestore(t.Context(), env, op)
	if err != nil || started.Step != "poll" || agent.starts != 1 {
		t.Fatal("restore was not started", started, err, agent.starts)
	}
	op.Checkpoint = started.Checkpoint
	cp, err = loadCheckpoint(op)
	if err != nil || cp.RestoreMetadataDeadline == nil || !cp.RestoreMetadataDeadline.Equal(clock.at.Add(restoreMetadataGrace)) {
		t.Fatal("start did not checkpoint a bounded metadata deadline", cp, err)
	}
	return env, agent, clock, op
}

func TestRestoreMetadataDelayIsStartingProgress(t *testing.T) {
	for _, physical := range []bool{false, true} {
		name := "logical"
		if physical {
			name = "physical"
		}
		t.Run(name, func(t *testing.T) {
			env, agent, clock, op := restoreStartupSetup(t, physical)
			initial, _ := loadCheckpoint(op)
			agent.mu.Lock()
			agent.restoreDescribeError = missingRestoreMetadata
			agent.mu.Unlock()
			for _, elapsed := range []time.Duration{0, restoreMetadataGrace / 2, restoreMetadataGrace - time.Nanosecond} {
				clock.at = initial.RestoreMetadataDeadline.Add(-restoreMetadataGrace).Add(elapsed)
				res, err := RunRestore(t.Context(), env, op)
				if err != nil || res.Done || res.Step != "poll" || !strings.Contains(res.Message, "restore starting") || res.RequeueAfter <= 0 {
					t.Fatal("expected startup delay became an operation error", res, err)
				}
				// Deserialize each checkpoint as after a reconcile or controller restart.
				resumed := *op
				resumed.Checkpoint = append(json.RawMessage(nil), res.Checkpoint...)
				op = &resumed
				cp, err := loadCheckpoint(op)
				if err != nil || cp.RestoreMetadataDeadline == nil || !cp.RestoreMetadataDeadline.Equal(*initial.RestoreMetadataDeadline) {
					t.Fatal("polling extended the startup grace", cp, err)
				}
			}
			agent.mu.Lock()
			agent.restoreDescribeError = ""
			agent.mu.Unlock()
			res, err := RunRestore(t.Context(), env, op)
			if err != nil || res.Step != "mongod-start" {
				t.Fatal("published restore metadata did not advance recovery", res, err)
			}
			op.Checkpoint = res.Checkpoint
			drive(t, env, op, RunRestore, false)
			restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
			if err != nil || restore.Phase != domain.PhaseSucceeded || agent.starts != 1 {
				t.Fatal("restore did not finish without resubmission", restore, err, agent.starts)
			}
		})
	}
}

func TestRestoreMetadataStartupGraceDoesNotHideFailures(t *testing.T) {
	for _, scenario := range []string{"deadline", "already-reported", "legacy-checkpoint", "permission-error", "other-missing-metadata", "wrapped-real-error"} {
		t.Run(scenario, func(t *testing.T) {
			env, agent, clock, op := restoreStartupSetup(t, false)
			cp, _ := loadCheckpoint(op)
			if scenario == "already-reported" {
				agent.mu.Lock()
				agent.restoreRunning = true
				agent.mu.Unlock()
				res, err := RunRestore(t.Context(), env, op)
				if err != nil || res.Step != "poll" {
					t.Fatal(res, err)
				}
				op.Checkpoint = res.Checkpoint
				observed, err := loadCheckpoint(op)
				if err != nil || observed.RestoreMetadataDeadline != nil {
					t.Fatal("reported restore retained startup grace", observed, err)
				}
			}
			if scenario == "legacy-checkpoint" {
				cp.RestoreMetadataDeadline = nil
				op.Checkpoint, _ = json.Marshal(cp)
			}
			if scenario == "deadline" {
				clock.at = *cp.RestoreMetadataDeadline
			}
			message := missingRestoreMetadata
			if scenario == "permission-error" {
				message = "Error: access denied"
			}
			if scenario == "other-missing-metadata" {
				message = "Error: get backup meta: not found"
			}
			if scenario == "wrapped-real-error" {
				message = "Error: access denied (get restore meta: not found)"
			}
			agent.mu.Lock()
			agent.restoreDescribeError = message
			agent.mu.Unlock()
			res, err := RunRestore(t.Context(), env, op)
			if err == nil || !strings.Contains(err.Error(), message) || res.Step != "poll" || strings.Contains(res.Message, "restore starting") {
				t.Fatal("genuine restore observation failure was suppressed", res, err)
			}
			if agent.starts != 1 {
				t.Fatal("failed metadata observation resubmitted the restore", agent.starts)
			}
		})
	}
}

func TestRestoreFailureDuringStartupStillFailsAfterRecovery(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	agent.mu.Lock()
	agent.failRestore = true
	agent.mu.Unlock()
	res, err := RunRestore(t.Context(), env, op)
	if err != nil || res.Step != "mongod-start" {
		t.Fatal("restore failure did not enter compensating recovery", res, err)
	}
	op.Checkpoint = res.Checkpoint
	cp, _ := loadCheckpoint(op)
	if cp.Failure != "restore failed" || cp.RestoreMetadataDeadline != nil {
		t.Fatal("terminal restore failure was hidden by startup grace", cp)
	}
	drive(t, env, op, RunRestore, true)
	restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
	if err != nil || restore.Phase != domain.PhaseFailed || restore.Error != "restore failed" {
		t.Fatal("terminal restore error was not preserved", restore, err)
	}
}
