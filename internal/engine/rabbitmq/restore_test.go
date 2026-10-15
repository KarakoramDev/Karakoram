// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func prepareDefinitionsRestore(t *testing.T, f *definitionsFixtureEnv) *domain.Operation {
	t.Helper()
	f.store.restore = &domain.Restore{Namespace: "default", Name: "recover", Cluster: "target", Source: domain.RestoreSource{Backup: "daily"}, Phase: domain.PhasePending}
	return definitionsOperation("restore", "recover")
}
func TestDefinitionsRestoreSuccess(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	finishDefinitions(t, f, op, f.p.restoreOp)
	r := f.store.restore
	if r.Phase != domain.PhaseSucceeded || r.StartedAt == nil || r.CompletedAt == nil || f.imports != 1 {
		t.Fatalf("restore=%+v imports=%d", r, f.imports)
	}
	var got struct {
		Users []struct {
			Name string `json:"name"`
		}
		Permissions []struct {
			User string `json:"user"`
		}
		TopicPermissions []struct {
			User string `json:"user"`
		} `json:"topic_permissions"`
		Queues []struct {
			Name string `json:"name"`
		}
	}
	if err := json.Unmarshal(f.imported, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Users) != 1 || got.Users[0].Name != "customer" || len(got.Permissions) != 1 || got.Permissions[0].User != "customer" || len(got.TopicPermissions) != 1 || got.TopicPermissions[0].User != "customer" || len(got.Queues) != 1 || got.Queues[0].Name != "orders" {
		t.Fatalf("imported definitions: %s", f.imported)
	}
	if strings.Contains(string(f.imported), "source-secret") || strings.Contains(string(f.imported), "admin-secret") {
		t.Fatal("reserved hashes imported")
	}
	if f.events.events[1].Reason != "RestoreCompleted" {
		t.Fatal(f.events.events)
	}
	finishDefinitions(t, f, op, f.p.restoreOp)
	if f.imports != 1 {
		t.Fatal("completed restore repeated import")
	}
}
func TestDefinitionsRestoreInvalidPayload(t *testing.T) {
	for _, item := range []struct {
		name, data string
		gzip       bool
	}{{"gzip", "not gzip", false}, {"json", "not JSON source-secret", true}, {"array", "[]", true}, {"null", "null", true}, {"users", `{"users":"source-secret"}`, true}, {"user identity", `{"users":[{}]}`, true}} {
		t.Run(item.name, func(t *testing.T) {
			f := newDefinitionsFixture(t)
			data := []byte(item.data)
			if item.gzip {
				data = definitionsGzip(t, item.data)
			}
			seedDefinitions(t, f, data)
			op := prepareDefinitionsRestore(t, f)
			advanceDefinitions(t, f, op, f.p.restoreOp)
			advanceDefinitions(t, f, op, f.p.restoreOp)
			out, err := f.p.restoreOp(context.Background(), f.rc, op)
			if err == nil || !out.Done || f.store.restore.Phase != domain.PhaseFailed || f.store.restore.CompletedAt == nil || f.imports != 0 {
				t.Fatalf("out=%+v err=%v restore=%+v imports=%d", out, err, f.store.restore, f.imports)
			}
			if strings.Contains(err.Error(), "source-secret") || strings.Contains(string(out.Checkpoint), "source-secret") {
				t.Fatal("invalid input leaked")
			}
			if f.events.events[len(f.events.events)-1].Reason != "RestoreFailed" {
				t.Fatal(f.events.events)
			}
		})
	}
}
func TestDefinitionsRestoreTruncatedGzip(t *testing.T) {
	f := newDefinitionsFixture(t)
	data := definitionsGzip(t, definitionsFixture)
	seedDefinitions(t, f, data[:len(data)-5])
	op := prepareDefinitionsRestore(t, f)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || !out.Done || f.imports != 0 {
		t.Fatalf("out=%+v err=%v imports=%d", out, err, f.imports)
	}
}
func TestDefinitionsRestoreImportRetry(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	f.importStatus = 503
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || out.Done || out.Step != "import" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	op.Checkpoint = out.Checkpoint
	f.importStatus = 0
	finishDefinitions(t, f, op, f.p.restoreOp)
	if f.imports != 2 || f.store.restore.Phase != domain.PhaseSucceeded {
		t.Fatalf("imports=%d restore=%+v", f.imports, f.store.restore)
	}
}
func TestDefinitionsCloneOnceCrossNamespace(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.store.backup.Namespace = "source-ns"
	f.store.backup.Cluster = "source"
	f.store.storage.Namespace = "source-ns"
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Namespace: "source-ns", Cluster: "source", Backup: "daily"}
	if done, err := f.p.cloneStep(context.Background(), f.rc); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	// Round-trip persisted state rather than retaining an in-memory flag.
	raw := append(json.RawMessage(nil), f.rc.Cluster.Internal...)
	f.rc.Cluster.Internal = raw
	// A completed import must no longer resolve even the backup inventory.
	f.store.backup = nil
	if done, err := f.p.cloneStep(context.Background(), f.rc); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if f.imports != 1 {
		t.Fatalf("clone imported %d times", f.imports)
	}
	state, err := loadState(f.rc.Cluster)
	if err != nil || !state.CloneImported {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if len(f.store.backupLookups) != 1 || f.store.backupLookups[0] != (domain.ClusterRef{Namespace: "source-ns", Name: "source"}) {
		t.Fatal(f.store.backupLookups)
	}
	if len(f.store.storageLookups) != 1 || f.store.storageLookups[0] != "source-ns/store" {
		t.Fatal(f.store.storageLookups)
	}
}
func TestDefinitionsCloneFailedImportNotMarkedDone(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "target", Backup: "daily"}
	f.importStatus = 503
	if done, err := f.p.cloneStep(context.Background(), f.rc); err == nil || done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	state, err := loadState(f.rc.Cluster)
	if err != nil || state.CloneImported {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	f.importStatus = 0
	if done, err := f.p.cloneStep(context.Background(), f.rc); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
}
func TestDefinitionsRestoreCrossNamespaceEvents(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	f.store.backup.Namespace = "source-ns"
	f.store.backup.Cluster = "source"
	f.store.storage.Namespace = "source-ns"
	f.store.restore.SourceNamespace = "source-ns"
	f.store.restore.Source.SourceCluster = "source"
	finishDefinitions(t, f, op, f.p.restoreOp)
	if len(f.events.events) != 4 {
		t.Fatal(f.events.events)
	}
	if f.events.events[1].Namespace != "source-ns" || f.events.events[1].Cluster != "source" || f.events.events[3].Reason != "RestoreCompleted" {
		t.Fatal(f.events.events)
	}
}
func TestDefinitionsSourceRejectsOtherProviderAndPITR(t *testing.T) {
	for _, test := range []string{"provider", "type", "PITR", "missing backup", "unfinished backup", "missing location", "import checkpoint"} {
		t.Run(test, func(t *testing.T) {
			f := newDefinitionsFixture(t)
			seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
			op := prepareDefinitionsRestore(t, f)
			switch test {
			case "provider":
				f.store.backup.Provider = "psmdb"
			case "type":
				f.store.backup.Type = "physical"
			case "PITR":
				stamp := f.store.backup.CreatedAt
				f.store.restore.Source.PITRTime = &stamp
			case "missing backup":
				f.store.restore.Source.Backup = ""
			case "unfinished backup":
				f.store.backup.Phase = domain.PhaseRunning
			case "missing location":
				f.store.backup.Location = ""
			case "import checkpoint":
				advanceDefinitions(t, f, op, f.p.restoreOp)
				f.store.backup.Type = "physical"
			}
			out, err := f.p.restoreOp(context.Background(), f.rc, op)
			r := f.store.restore
			if err == nil || !out.Done || out.Step != "done" || r.Phase != domain.PhaseFailed || r.CompletedAt == nil || r.Error == "" || f.imports != 0 {
				t.Fatalf("out=%+v err=%v restore=%+v imports=%d", out, err, r, f.imports)
			}
			if f.events.events[len(f.events.events)-1].Reason != "RestoreFailed" {
				t.Fatal(f.events.events)
			}
			op.Checkpoint = out.Checkpoint
			out, err = f.p.restoreOp(context.Background(), f.rc, op)
			if !out.Done || err == nil || f.imports != 0 {
				t.Fatal("terminal invalid restore was retried", out, err)
			}
		})
	}
}

func TestDefinitionsRestoreCorruptGzipChecksum(t *testing.T) {
	f := newDefinitionsFixture(t)
	data := definitionsGzip(t, definitionsFixture)
	data[len(data)-8] ^= 0xff
	seedDefinitions(t, f, data)
	op := prepareDefinitionsRestore(t, f)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || !out.Done || f.imports != 0 {
		t.Fatalf("out=%+v err=%v imports=%d", out, err, f.imports)
	}
}

func TestDefinitionsRestoreCompletedImportCheckpoint(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	f.store.failRestoreUpdate = true
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || out.Done || out.Step != "complete" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	op.Checkpoint = out.Checkpoint
	finishDefinitions(t, f, op, f.p.restoreOp)
	if f.imports != 1 {
		t.Fatal("metadata retry repeated a completed import")
	}
}

func TestDefinitionsRestoreRejectedImport(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	f.importStatus = 400
	advanceDefinitions(t, f, op, f.p.restoreOp)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || !out.Done || f.store.restore.Phase != domain.PhaseFailed {
		t.Fatalf("out=%+v err=%v restore=%+v", out, err, f.store.restore)
	}
}

func TestDefinitionsRestoreSourceLookupFailureRemainsRetryable(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	op := prepareDefinitionsRestore(t, f)
	backup := f.store.backup
	f.store.backup = nil
	out, err := f.p.restoreOp(context.Background(), f.rc, op)
	if err == nil || out.Done || f.store.restore.Phase != domain.PhasePending {
		t.Fatalf("out=%+v err=%v restore=%+v", out, err, f.store.restore)
	}
	f.store.backup = backup
	finishDefinitions(t, f, op, f.p.restoreOp)
}
