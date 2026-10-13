// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestFirstPITRBaseCheckFollowsDisabledConfiguration(t *testing.T) {
	env, _, fake, _ := setup(t)
	env.PITR.Enabled = true
	fake.requireConfig = true
	fake.deleted = true
	if _, err := EnsureConfig(context.Background(), env, ""); err != nil {
		t.Fatal(err)
	}
	if len(fake.requests) < 2 || fake.requests[0].Command != "config" {
		t.Fatal("list ran before initial configuration")
	}
	var config string
	_ = json.Unmarshal(fake.requests[0].Config, &config)
	if !strings.Contains(config, "enabled: false") {
		t.Fatal("PITR started without a base")
	}
}

func TestAlternateBackupReestablishesPrimaryBeforeCompletion(t *testing.T) {
	env, st, fake, _ := setup(t)
	ctx := context.Background()
	env.RC.Cluster.Spec.Backup.Storage = "storage"
	alternate := env.Storage
	alternate.Name = "alternate"
	alternate.Prefix = "alternate-base"
	alternate.Credentials = env.Credentials
	if err := st.CreateBackupStorage(ctx, &alternate); err != nil {
		t.Fatal(err)
	}
	env.Storage = alternate
	env.Prefix = "alternate-base/karakoram/target-target-id"
	if err := st.CreateBackup(ctx, &domain.Backup{Name: "alt", Cluster: "target", Storage: "alternate", Type: "logical"}); err != nil {
		t.Fatal(err)
	}
	drive(t, env, &domain.Operation{ID: "alternate-backup", Payload: json.RawMessage(`{"backup":"alt"}`)}, RunBackup, false)
	configured := []string{}
	resyncs := 0
	for _, req := range fake.requests {
		if req.Command == "config" && len(req.Config) > 0 {
			var text string
			_ = json.Unmarshal(req.Config, &text)
			configured = append(configured, text)
		}
		if req.Command == "config" && slices.Contains(req.Args, "--force-resync") {
			resyncs++
		}
	}
	if len(configured) < 2 || !strings.Contains(configured[0], "prefix: alternate-base/") || !strings.Contains(configured[len(configured)-1], "prefix: karakoram/target-target-id") || resyncs == 0 {
		t.Fatalf("primary not recovered: configs=%d resyncs=%d", len(configured), resyncs)
	}
}

func TestDeleteResynchronizesRecordedDestinationBeforeDeletion(t *testing.T) {
	env, st, fake, _ := setup(t)
	ctx := context.Background()
	env.RC.Cluster.Spec.Backup.Storage = "storage"
	alternate := env.Storage
	alternate.Name = "alternate"
	alternate.Credentials = env.Credentials
	if err := st.CreateBackupStorage(ctx, &alternate); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "alt", Cluster: "target", Storage: "alternate", PBMPrefix: "immutable/old-prefix", PBMName: "2024-01-05T11:10:54Z", Type: "logical"}
	if err := st.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	drive(t, env, &domain.Operation{ID: "delete-alt", Payload: json.RawMessage(`{"backup":"alt"}`)}, RunDeleteBackup, false)
	resynced := false
	configured := false
	deleted := false
	for _, req := range fake.requests {
		if req.Command == "config" && len(req.Config) > 0 {
			var text string
			_ = json.Unmarshal(req.Config, &text)
			if strings.Contains(text, "prefix: immutable/old-prefix") {
				configured = true
			}
		}
		if req.Command == "config" && slices.Contains(req.Args, "--force-resync") && configured {
			resynced = true
		}
		if req.Command == "delete-backup" {
			if !configured || !resynced {
				t.Fatal("delete checked wrong destination")
			}
			deleted = true
		}
	}
	if !deleted {
		t.Fatal("snapshot not deleted")
	}
}

func TestRestoreUsesProvenanceAfterSourceDeletionAndResetsAllModes(t *testing.T) {
	for _, kind := range []string{"logical", "physical"} {
		t.Run(kind, func(t *testing.T) {
			env, st, fake, hooks := setup(t)
			ctx := context.Background()
			source := &domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "psmdb"}}
			if err := st.CreateCluster(ctx, source); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "retained", Cluster: "source", Storage: "storage", Type: kind, PBMName: "2024-01-05T11:10:54Z", Phase: domain.PhaseSucceeded, PBMPrefix: "old-storage-prefix/source-incarnation", RecoveryIdentity: "backup-snapshot-identity"}
			if err := st.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := st.SetClusterSecret(ctx, b.RecoveryIdentity, "users/userAdmin", []byte("old-snapshot-password")); err != nil {
				t.Fatal(err)
			}
			if err := st.DeleteCluster(ctx, "source"); err != nil {
				t.Fatal(err)
			}
			if err := st.SetClusterSecret(ctx, "target", "users/userAdmin", []byte("current-target-password")); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRestore(ctx, &domain.Restore{Name: "retained", Cluster: "target", Source: domain.RestoreSource{SourceCluster: "source", Backup: b.Name}}); err != nil {
				t.Fatal(err)
			}
			drive(t, env, &domain.Operation{ID: "retained-restore", Payload: json.RawMessage(`{"restore":"retained"}`)}, RunRestore, false)
			if len(hooks.reset) == 0 || hooks.reset[0] != b.RecoveryIdentity {
				t.Fatal("snapshot credential recovery missing")
			}
			var first string
			for _, req := range fake.requests {
				if len(req.Config) > 0 {
					_ = json.Unmarshal(req.Config, &first)
					break
				}
			}
			if !strings.Contains(first, "prefix: old-storage-prefix/source-incarnation") {
				t.Fatal("source deletion lost prefix")
			}
			password, err := st.GetClusterSecret(ctx, b.RecoveryIdentity, "users/userAdmin")
			if err != nil || string(password) != "old-snapshot-password" {
				t.Fatal("source deletion lost protected recovery identity")
			}
		})
	}
	for _, source := range []string{"", "target"} {
		env, st, _, hooks := setup(t)
		ctx := context.Background()
		if err := st.CreateRestore(ctx, &domain.Restore{Name: "inplace", Cluster: "target", Source: domain.RestoreSource{SourceCluster: source}}); err != nil {
			t.Fatal(err)
		}
		op := &domain.Operation{Payload: json.RawMessage(`{"restore":"inplace"}`), Checkpoint: json.RawMessage(`{"step":"reset-users"}`)}
		if _, err := RunRestore(ctx, env, op); err != nil {
			t.Fatal(err)
		}
		if len(hooks.reset) != 1 {
			t.Fatal("in-place restore skipped system credential recovery")
		}
	}
}

func TestBackupPersistsIndependentProtectedProvenance(t *testing.T) {
	env, st, _, _ := setup(t)
	ctx := context.Background()
	env.RC.Cluster.Spec.Topology = domain.Topology{Type: "replicaSet"}
	env.RC.Cluster.Spec.Version = "8.0.32-14"
	if err := st.CreateBackup(ctx, &domain.Backup{Name: "protected", Cluster: "target", Storage: "storage", Type: "physical"}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "protected-backup", Payload: json.RawMessage(`{"backup":"protected"}`)}
	if _, err := RunBackup(ctx, env, op); err != nil {
		t.Fatal(err)
	}
	b, err := st.GetBackup(ctx, "target", "protected")
	if err != nil {
		t.Fatal(err)
	}
	if b.PBMPrefix != env.Prefix || b.SourceVersion != "8.0.32-14" || b.SourceTopology.Type != "replicaSet" || b.RecoveryIdentity == "" {
		t.Fatal("incomplete provenance")
	}
	password, err := st.GetClusterSecret(ctx, b.RecoveryIdentity, "users/userAdmin")
	if err != nil || string(password) != "snapshot-password" {
		t.Fatal("missing encrypted snapshot identity")
	}
	raw, _ := json.Marshal(b)
	if strings.Contains(string(raw), "snapshot-password") {
		t.Fatal("snapshot password escaped record")
	}
}

func TestRoutersWaitForWholeServiceStopAndStart(t *testing.T) {
	env, st, fake, _ := setup(t)
	ctx := context.Background()
	env.Routers = []string{"routers"}
	backend := env.RC.Backend.(*routerBackend)
	backend.done = false
	if err := st.CreateRestore(ctx, &domain.Restore{Name: "restore", Cluster: "target"}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Payload: json.RawMessage(`{"restore":"restore"}`), Checkpoint: json.RawMessage(`{"step":"routers-off"}`)}
	for range 4 {
		res, err := RunRestore(ctx, env, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = res.Checkpoint
	}
	cp, err := loadCheckpoint(op)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Step != "routers-off" || cp.Index != 0 || cp.RouterOp == nil || !slices.Equal(fake.actions, []string{"/service/routers/stop"}) {
		t.Fatalf("did not wait for entire service: %s %v", op.Checkpoint, fake.actions)
	}
	backend.done = true
	res, err := RunRestore(ctx, env, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = res.Checkpoint
	cp, _ = loadCheckpoint(op)
	if cp.Index != 1 {
		t.Fatal("completed service stop not observed")
	}
	op.Checkpoint = json.RawMessage(`{"step":"routers-on"}`)
	for range 3 {
		res, err := RunRestore(ctx, env, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = res.Checkpoint
	}
	if !slices.Equal(fake.actions, []string{"/service/routers/stop", "/service/routers/start"}) {
		t.Fatal("did not restart entire service", fake.actions)
	}
}
