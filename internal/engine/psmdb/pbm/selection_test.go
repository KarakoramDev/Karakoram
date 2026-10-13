// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPITRChoosesNewestEligibleSnapshot(t *testing.T) {
	env, st, _, _ := setup(t)
	for _, item := range []struct {
		name  string
		epoch int64
		phase domain.Phase
	}{{"older", 100, domain.PhaseSucceeded}, {"newest", 200, domain.PhaseSucceeded}, {"future", 400, domain.PhaseSucceeded}, {"failed", 290, domain.PhaseFailed}} {
		stamp := time.Unix(item.epoch, 0).UTC()
		b := &domain.Backup{Name: item.name, Cluster: "target", Storage: env.Storage.Name, PBMName: item.name, Phase: item.phase, Type: "logical", LastWriteTime: &stamp}
		if err := st.CreateBackup(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	target := time.Unix(300, 0).UTC()
	restore := &domain.Restore{Name: "pitr", Cluster: "target", Source: domain.RestoreSource{PITRTime: &target}}
	if err := st.CreateRestore(context.Background(), restore); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "pitr", Payload: json.RawMessage(`{"restore":"pitr"}`)}
	result, err := RunRestore(context.Background(), env, op)
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Snapshot != "newest" {
		t.Fatalf("selected %q instead of newest eligible snapshot", cp.Snapshot)
	}
}

func TestPhysicalBackupRequiresPSMDB(t *testing.T) {
	env, st, fake, _ := setup(t)
	env.RC.Cluster.Spec.Provider = "mongodb"
	b := &domain.Backup{Name: "physical", Cluster: "target", Type: "physical"}
	if err := st.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	_, err := RunBackup(context.Background(), env, &domain.Operation{ID: "backup", Payload: json.RawMessage(`{"backup":"physical"}`)})
	if err == nil || fake.starts != 0 {
		t.Fatalf("error=%v starts=%d", err, fake.starts)
	}
}

func TestInventoryShapeAndEpochRanges(t *testing.T) {
	var inv inventoryData
	if err := decodeLast([]byte(`{"snapshots":null,"pitr":{"ranges":[{"range":{"start":1704453060,"end":1704453147}},{"start":1704453200,"end":1704453300},{"range":{"start":1704453400,"end":1704453500},"noBaseSnapshot":true}]}}`), &inv); err != nil {
		t.Fatal(err)
	}
	ranges := windows(inv.PITR.Ranges)
	if len(ranges) != 2 || ranges[0].Start.Unix() != 1704453060 || ranges[1].End.Unix() != 1704453300 {
		t.Fatal(ranges)
	}
	if !hasJSONKey([]byte(`{"snapshots":null}`), "snapshots") || hasJSONKey([]byte(`{"msg":"Storage resync is running"}`), "snapshots") {
		t.Fatal("inventory availability classification")
	}
}

func TestCleanupResetsForDifferentDestination(t *testing.T) {
	env, _, fake, _ := setup(t)
	first, err := DeleteAll(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(first.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	original := cp.Destination
	cp.Step = "done"
	raw, _ := json.Marshal(cp)
	state := map[string]json.RawMessage{}
	if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
		t.Fatal(err)
	}
	state["pbmCleanup"] = raw
	env.RC.Cluster.Internal, _ = json.Marshal(state)
	env.Storage.Name = "other-storage"
	env.Prefix = "other-prefix"
	second, err := DeleteAll(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if second.Done || cp.Step != "resync" || cp.Destination == original {
		t.Fatalf("destination did not reset cleanup: %s", second.Checkpoint)
	}
	var cfg string
	if err := json.Unmarshal(fake.requests[len(fake.requests)-1].Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg, "prefix: other-prefix") {
		t.Fatal(cfg)
	}
}

func TestSuppressionCommitsBeforeAgentAction(t *testing.T) {
	for _, step := range []string{"routers-off", "routers-on", "maintenance-on", "maintenance-off"} {
		t.Run(step, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			env.Routers = []string{"router"}
			if step == "routers-on" {
				env.RC.Cluster.Internal = json.RawMessage(`{"uid":"target-id","pbmStopped":{"router":["mongos","other"]}}`)
			}
			if step == "maintenance-off" {
				env.RC.Cluster.Internal = json.RawMessage(`{"uid":"target-id","pbmMaintenance":{"m0":true}}`)
			}
			restore := &domain.Restore{Name: "restore", Cluster: "target", Phase: domain.PhaseRunning}
			if err := st.CreateRestore(context.Background(), restore); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(checkpoint{Step: step, Physical: true, Member: "m0"})
			op := &domain.Operation{ID: "restore", Payload: json.RawMessage(`{"restore":"restore"}`), Checkpoint: raw}
			first, err := RunRestore(context.Background(), env, op)
			if err != nil {
				t.Fatal(err)
			}
			if len(fake.actions) != 0 {
				t.Fatal("agent action happened before desired state was committed")
			}
			var internal struct {
				UID         string              `json:"uid"`
				Maintenance map[string]bool     `json:"pbmMaintenance"`
				Stopped     map[string][]string `json:"pbmStopped"`
			}
			if err := json.Unmarshal(env.RC.Cluster.Internal, &internal); err != nil {
				t.Fatal(err)
			}
			if internal.UID != "target-id" {
				t.Fatal("provider state was overwritten")
			}
			if step == "maintenance-on" && !internal.Maintenance["m0"] {
				t.Fatal("maintenance desired state missing")
			}
			if step == "maintenance-off" && internal.Maintenance["m0"] {
				t.Fatal("maintenance desired state not cleared")
			}
			if step == "routers-off" && !slices.Contains(internal.Stopped["router"], "mongos") {
				t.Fatal("router suppression missing")
			}
			if step == "routers-on" && (slices.Contains(internal.Stopped["router"], "mongos") || !slices.Contains(internal.Stopped["router"], "other")) {
				t.Fatal("router suppression update lost other desired process")
			}
			op.Checkpoint = first.Checkpoint
			if _, err := RunRestore(context.Background(), env, op); err != nil {
				t.Fatal(err)
			}
			if len(fake.actions) != 1 {
				t.Fatalf("agent actions after durable desire=%v", fake.actions)
			}
		})
	}
}

func TestCloneUsesExplicitBackupStorage(t *testing.T) {
	env, st, _, _ := setup(t)
	cluster := &domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "primary"}}, Internal: json.RawMessage(`{"uid":"source-id","pbmPrefix":"cluster-prefix"}`)}
	if err := st.CreateCluster(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	alternate := env.Storage
	alternate.Name, alternate.Prefix, alternate.Credentials = "alternate", "storage-base", env.Credentials
	if err := st.CreateBackupStorage(context.Background(), &alternate); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Name: "base", Cluster: "source", Storage: "alternate", PBMName: "snapshot", Type: "logical", Phase: domain.PhaseSucceeded}
	if err := st.CreateBackup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	restore := &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{Backup: "base", SourceCluster: "source"}}
	if err := st.CreateRestore(context.Background(), restore); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "clone", Payload: json.RawMessage(`{"restore":"restore"}`)}
	first, err := RunRestore(context.Background(), env, op)
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(first.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Storage != "alternate" {
		t.Fatal(cp.Storage)
	}
	source, err := sourceEnv(context.Background(), env, domain.ClusterRef{Namespace: cp.SourceNamespace, Name: cp.Source}, cp.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if source.Storage.Name != "alternate" || source.Prefix != "storage-base/cluster-prefix" {
		t.Fatal(source.Storage.Name, source.Prefix)
	}
}
