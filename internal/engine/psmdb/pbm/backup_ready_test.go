// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupWaitsForNativeAgentReadiness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{"missing", `{}`},
		{"unhealthy", `{"cluster":[{"rs":"rs0","nodes":[{"host":"m0:27017","role":"P","ok":false}]}]}`},
		{"wrong-set", `{"cluster":[{"rs":"other","nodes":[{"host":"m0:27017","role":"P","ok":true}]}]}`},
		{"foreign-member", `{"cluster":[{"rs":"rs0","nodes":[{"host":"foreign:27017","role":"P","ok":true}]}]}`},
		{"arbiter", `{"cluster":[{"rs":"rs0","nodes":[{"host":"m0:27017","role":"A","ok":true}]}]}`},
		{"invalid", `not-json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			if err := st.CreateBackup(context.Background(), &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical"}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "ready", Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"configure","member":"m0"}`)}
			res, err := RunBackup(context.Background(), env, op)
			if err != nil || res.Step != "ready" {
				t.Fatalf("configure: %+v %v", res, err)
			}
			op.Checkpoint = res.Checkpoint
			fake.backupStatusOutput = tc.status
			res, err = RunBackup(context.Background(), env, op)
			starts := fake.startCount()
			if err != nil || res.Done || res.Step != "ready" || !strings.Contains(res.Message, "waiting for PBM") || starts != 0 {
				t.Fatalf("unready: %+v %v starts=%d", res, err, starts)
			}
			// Persist the observed-execution baseline across a CP restart.
			op.Checkpoint = append(json.RawMessage(nil), res.Checkpoint...)
			op.Elapsed = 2 * time.Minute
			res, err = RunBackup(context.Background(), env, op)
			starts = fake.startCount()
			if err == nil || !res.Done || !strings.Contains(res.Message, "readiness budget expired") || starts != 0 {
				t.Fatalf("bound: %+v %v starts=%d", res, err, starts)
			}
			b, _ := st.GetBackup(context.Background(), env.RC.Cluster.Ref(), "manual")
			if b.Phase != domain.PhaseFailed || b.CompletedAt == nil {
				t.Fatalf("backup %+v", b)
			}
		})
	}
}

func TestBackupReadinessRequiresEveryReplicaSet(t *testing.T) {
	env, _, fake, _ := setup(t)
	env.Nodes = append(env.Nodes, Node{Member: "cfg0", ReplSet: "cfg"})
	op := &domain.Operation{}
	res, err := waitBackupAgents(context.Background(), env, op, checkpoint{Step: "ready", Member: "m0"})
	if err != nil || res.Step != "ready" || !strings.Contains(res.Message, "cfg") {
		t.Fatalf("missing config server: %+v %v", res, err)
	}
	fake.backupStatusOutput = `{"cluster":[{"rs":"rs0","nodes":[{"host":"m1:27017","role":"S","ok":true}]},{"rs":"cfg","nodes":[{"host":"cfg0:27017","role":"P","ok":true}]}]}`
	res, err = waitBackupAgents(context.Background(), env, op, checkpoint{Step: "ready", Member: "m0"})
	if err != nil || res.Step != "start" || fake.startCount() != 0 {
		t.Fatalf("eligible: %+v %v", res, err)
	}
}

func TestBackupFailedStartTerminatesWithoutReceiptReplay(t *testing.T) {
	env, st, fake, _ := setup(t)
	if err := st.CreateBackup(context.Background(), &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical", PBMPrefix: env.Prefix}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.receipts["failed-start"] = agentapi.PBMResponse{ExitCode: -1}
	fake.mu.Unlock()
	op := &domain.Operation{ID: "failed-start", Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"start","member":"m0"}`)}
	res, err := RunBackup(context.Background(), env, op)
	if err == nil || !res.Done || !strings.Contains(res.Message, "initiation outcome unconfirmed") || !strings.Contains(res.Message, "no automatic re-initiation") {
		t.Fatalf("failed receipt: %+v %v", res, err)
	}
	op.Checkpoint = res.Checkpoint
	requests := len(fake.recordedRequests())
	res, err = RunBackup(context.Background(), env, op)
	recordedRequests := fake.recordedRequests()
	starts := fake.startCount()
	if err == nil || !res.Done || len(recordedRequests) != requests || starts != 0 {
		t.Fatalf("replay: %+v %v starts=%d requests=%d", res, err, starts, len(recordedRequests))
	}
	// The unconfirmed native command can later succeed. Inventory must account
	// for its artifact separately, never retroactively claim request ownership.
	fake.backupListOutput = sampleList
	fake.backupStatusOutput = sampleStatus
	fake.backupDescribeOutput = sampleDescribe
	if _, _, err := SyncInventory(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	records, err := st.ListBackups(context.Background(), env.RC.Cluster.Ref())
	if err != nil || len(records) != 2 {
		t.Fatal(records, err)
	}
	original, err := st.GetBackup(context.Background(), env.RC.Cluster.Ref(), "manual")
	if err != nil || original.Phase != domain.PhaseFailed || original.PBMName != "" || !strings.Contains(original.Error, "initiation outcome unconfirmed") {
		t.Fatal(original, err)
	}
	adopted, err := st.GetBackup(context.Background(), env.RC.Cluster.Ref(), "2024-01-05T11:10:54Z")
	if err != nil || adopted.PBMName != "2024-01-05T11:10:54Z" || adopted.Phase != domain.PhaseSucceeded || adopted.Size != 43178 {
		t.Fatal(adopted, err)
	}
	for _, req := range recordedRequests[requests:] {
		if req.Command != "list" && req.Command != "status" && req.Command != "describe-backup" {
			t.Fatalf("mutating native command during adoption: %+v", req)
		}
	}
}
