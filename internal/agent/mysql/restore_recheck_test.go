// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func waitRecheck(t *testing.T, name string) myspec.RestoreStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := observeRestoreReceipt(name)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Status.State != "running" {
			return receipt.Status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("recheck did not finish")
	return myspec.RestoreStatus{}
}

func TestRestoreRecheckAsyncAndTokenIdempotence(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	receipt := restoreReceipt{SafePreflight: true, Status: myspec.RestoreStatus{State: "prepared", Stage: "ready", RecheckID: "old", VerifiedRecheckID: "old"}}
	blocked, entered := make(chan struct{}), make(chan struct{})
	calls := 0
	verify := func(restoreReceipt, string, bool) error { calls++; close(entered); <-blocked; return nil }
	restoreSubmitMu.Lock()
	status, err := startRestoreRecheck(name, receipt, "fresh", filepath.Dir(name), verify)
	restoreSubmitMu.Unlock()
	if err != nil || status.State != "running" || status.VerifiedRecheckID != "" || status.RecheckID != "fresh" {
		t.Fatalf("start=%+v err=%v", status, err)
	}
	<-entered
	loaded, err := observeRestoreReceipt(name)
	if err != nil {
		t.Fatal(err)
	}
	restoreSubmitMu.Lock()
	repeated, err := startRestoreRecheck(name, loaded, "fresh", filepath.Dir(name), verify)
	restoreSubmitMu.Unlock()
	if err != nil || repeated.State != "running" {
		t.Fatalf("repeat=%+v err=%v", repeated, err)
	}
	close(blocked)
	status = waitRecheck(t, name)
	if status.State != "prepared" || status.VerifiedRecheckID != "fresh" || calls != 1 {
		t.Fatalf("completed=%+v calls=%d", status, calls)
	}
	loaded, _ = observeRestoreReceipt(name)
	restoreSubmitMu.Lock()
	repeated, err = startRestoreRecheck(name, loaded, "fresh", filepath.Dir(name), verify)
	restoreSubmitMu.Unlock()
	if err != nil || repeated.VerifiedRecheckID != "fresh" || calls != 1 {
		t.Fatalf("verified repeat=%+v calls=%d err=%v", repeated, calls, err)
	}
}

func TestRestoreRecheckFailureAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "tamper", true: "shutdown"}[shutdown], func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "receipt.json")
			blocked := make(chan struct{})
			receipt := restoreReceipt{SafePreflight: true, Status: myspec.RestoreStatus{State: "prepared", Stage: "ready"}}
			restoreSubmitMu.Lock()
			_, err := startRestoreRecheck(name, receipt, "fresh", filepath.Dir(name), func(restoreReceipt, string, bool) error {
				<-blocked
				if shutdown {
					return nil
				}
				return errors.New("checksum changed")
			})
			restoreSubmitMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if shutdown {
				stopRestores(paths{root: filepath.Dir(name)})
			}
			close(blocked)
			status := waitRecheck(t, name)
			want := "replay_plan_changed"
			if shutdown {
				want = "interrupted"
			}
			if status.State != "failed" || status.Error != want || status.VerifiedRecheckID != "" {
				t.Fatalf("status=%+v", status)
			}
		})
	}
}

func TestRestoreRecheckOrphanIsNeverVerified(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	if err := durableJSON(name, restoreReceipt{Active: true, SafePreflight: true, Status: myspec.RestoreStatus{State: "running", Stage: "rechecking", RecheckID: "fresh"}}); err != nil {
		t.Fatal(err)
	}
	status := waitRecheck(t, name)
	if status.State != "failed" || status.Error != "interrupted" || status.VerifiedRecheckID != "" {
		t.Fatalf("status=%+v", status)
	}
}

func TestRestoreRecheckTokenDoesNotChangeIntentOrDigestPin(t *testing.T) {
	req := myspec.RestoreRequest{Receipt: "restore", PlanDigest: physicalManifestTestDigest}
	hash := restoreHash(req)
	req.RecheckID = "fresh"
	if restoreHash(req) != hash {
		t.Fatal("recheck token changed restore intent")
	}
	if err := requireFrozenPlanDigest(req, restoreReceipt{Status: myspec.RestoreStatus{PlanDigest: req.PlanDigest}}, true); err != nil {
		t.Fatal(err)
	}
	req.PlanDigest = "wrong"
	if requireFrozenPlanDigest(req, restoreReceipt{Status: myspec.RestoreStatus{PlanDigest: physicalManifestTestDigest}}, true) == nil {
		t.Fatal("token bypassed frozen digest pin")
	}
}

func TestRestoreRecheckPhysicalTamperFailsClosed(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	full := filepath.Join(name+".staging", "0")
	if err := privateDir(full); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(full, "mysql.ibd")
	if err := os.WriteFile(file, []byte("prepared bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	physical, err := preparedPhysicalFiles(full)
	if err != nil {
		t.Fatal(err)
	}
	receipt := restoreReceipt{SafePreflight: true, Status: myspec.RestoreStatus{State: "prepared", Stage: "ready"}, Replay: &restoreReplayPlan{Physical: physical}}
	receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("tampered bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	restoreSubmitMu.Lock()
	_, err = startRestoreRecheck(name, receipt, "fresh", filepath.Dir(name), verifyFrozenRestorePlan)
	restoreSubmitMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	status := waitRecheck(t, name)
	if status.State != "failed" || status.Error != "replay_plan_changed" || status.VerifiedRecheckID != "" {
		t.Fatalf("status=%+v", status)
	}
}
