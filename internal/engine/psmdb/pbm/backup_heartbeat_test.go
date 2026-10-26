// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupPollUsesHeartbeatAwareNativeFailure(t *testing.T) {
	for _, matching := range []bool{true, false} {
		t.Run(map[bool]string{true: "same-backup", false: "other-backup"}[matching], func(t *testing.T) {
			env, st, agent, _ := setup(t)
			agent.activeBackup = true
			name := "2024-01-05T11:10:54Z"
			failedName := name
			if !matching {
				failedName = "another-backup"
			}
			agent.backupStatusOutput = `{"backups":{"snapshot":[{"name":"` + failedName + `","status":"error","error":"Backup stuck at running stage, last beat ts: 1792412604"}]}}`
			b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical", PBMName: name, Phase: domain.PhaseRunning}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(checkpoint{Step: "poll", Member: "m0", PBMName: name})
			op := &domain.Operation{ID: "backup-op", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: raw}
			result, err := RunBackup(t.Context(), env, op)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
			if err != nil {
				t.Fatal(err)
			}
			if matching {
				if result.Step != "target-configure" || stored.Phase != domain.PhaseFailed || stored.Error == "" {
					t.Fatalf("orphaned backup did not enter terminal finalization: %+v %+v", result, stored)
				}
				op.Checkpoint = result.Checkpoint
				// Resume the durable checkpoint as after another controller pass.
				result, err = RunBackup(t.Context(), env, op)
				if !result.Done || err == nil || agent.startCount() != 0 {
					t.Fatalf("native failure did not finish without reinitiation: %+v %v", result, err)
				}
			} else if result.Step != "poll" || stored.Phase != domain.PhaseRunning {
				t.Fatalf("unrelated native failure completed backup: %+v %+v", result, stored)
			}
			if agent.startCount() != 0 {
				t.Fatal("poll reinitiated backup")
			}
		})
	}
}

func TestBackupPollTransientStatusFailureDoesNotTerminalize(t *testing.T) {
	for _, failure := range []string{"command", "parse", "absent"} {
		t.Run(failure, func(t *testing.T) {
			env, st, agent, _ := setup(t)
			agent.activeBackup = true
			agent.backupStatusUnavailable = failure == "command"
			agent.backupStatusOutput = `{}`
			if failure == "parse" {
				agent.backupStatusOutput = `"not JSON metadata"`
			}
			b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical", PBMName: "native-backup", Phase: domain.PhaseRunning}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "backup-op", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"poll","member":"m0","pbmName":"native-backup"}`)}
			result, err := RunBackup(t.Context(), env, op)
			if (failure != "absent") != (err != nil) {
				t.Fatal("unexpected status failure handling", result, err)
			}
			got, err := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
			if err != nil || result.Done || result.Step != "poll" || got.Phase != domain.PhaseRunning || got.CompletedAt != nil || agent.startCount() != 0 {
				t.Fatal("uncertain observation terminalized or reinitiated backup", result, got, err)
			}
		})
	}
}
