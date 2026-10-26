// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestBackupLocationAtStart(t *testing.T) {
	for _, kind := range []string{"logical", "physical"} {
		for _, trigger := range []string{"manual", "schedule", "restore"} {
			t.Run(kind+"/"+trigger, func(t *testing.T) {
				env, st, _, _ := setup(t)
				b := &domain.Backup{Name: "requested", Cluster: "target", Storage: "storage", Type: kind, Trigger: trigger, PBMPrefix: env.Prefix}
				if err := st.CreateBackup(t.Context(), b); err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(checkpoint{Step: "start", Member: "m0"})
				if err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "backup-location", Payload: json.RawMessage(`{"backup":"requested"}`), Checkpoint: raw}
				if _, err := RunBackup(t.Context(), env, op); err != nil {
					t.Fatal(err)
				}
				got, err := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := env.Prefix + "/2024-01-05T11:10:54Z/"
				if got.Location != want || got.Phase != domain.PhaseRunning {
					t.Fatalf("location=%q phase=%s, want %q while Running", got.Location, got.Phase, want)
				}
			})
		}
	}
}

func TestInventoryBackupLocation(t *testing.T) {
	const name = "2024-01-05T11:10:54Z"
	for _, kind := range []string{"logical", "physical", "incremental"} {
		for _, mode := range []string{"discovered", "legacy", "legacy-prefix", "existing-location"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				env, st, agent, _ := setup(t)
				agent.backupListOutput = fmt.Sprintf(`{"snapshots":[{"name":%q,"type":%q,"status":"done"}],"pitr":{"ranges":[{"start":1704453060,"end":1704453137}]}}`, name, kind)
				prefix := env.Prefix
				if mode == "legacy-prefix" {
					prefix = "/historical/source/"
				}
				want := domain.BackupStoragePrefix(prefix) + "/" + name + "/"
				if mode != "discovered" {
					b := &domain.Backup{Name: name, Cluster: "target", Storage: "storage", PBMName: name, PBMPrefix: prefix, Type: kind}
					if mode == "legacy" {
						b.PBMPrefix = ""
					}
					if mode == "existing-location" {
						b.Location = "recorded/snapshot/reference"
						want = b.Location
					}
					if err := st.CreateBackup(t.Context(), b); err != nil {
						t.Fatal(err)
					}
				}
				_, ranges, err := SyncInventory(t.Context(), env)
				if err != nil {
					t.Fatal(err)
				}
				got, err := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), name)
				if err != nil {
					t.Fatal(err)
				}
				if got.Location != want || got.Type != kind || len(ranges) != 1 {
					t.Fatalf("location=%q type=%s ranges=%v, want %q type=%s and PITR window", got.Location, got.Type, ranges, want, kind)
				}
			})
		}
	}
}

func TestSnapshotLocationRequiresRecordedIdentity(t *testing.T) {
	for _, b := range []domain.Backup{{}, {PBMPrefix: "prefix"}, {PBMName: "snapshot"}} {
		applySnapshot(&b, snapshot{Name: b.PBMName, Status: "done"}, time.Time{})
		if b.Location != "" {
			t.Fatalf("invented location without complete identity: %+v", b)
		}
	}
}

func TestSnapshotDeletionIgnoresPublicLocation(t *testing.T) {
	s := domain.BackupStorage{Name: "storage", Prefix: "archive"}
	b := domain.Backup{Storage: s.Name, PBMPrefix: "archive/original", PBMName: "native-snapshot", Location: "unrelated/public/reference/"}
	directory, metadata, err := backupstorage.SnapshotKeys(s, b)
	if err != nil || directory != "archive/original/native-snapshot/" || metadata != "archive/original/native-snapshot.pbm.json" {
		t.Fatalf("directory=%q metadata=%q err=%v", directory, metadata, err)
	}
	b.PBMName = ""
	if _, _, err := backupstorage.SnapshotKeys(s, b); err == nil {
		t.Fatal("public location must not replace missing native identity")
	}
}
