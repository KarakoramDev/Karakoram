// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestIncrementalPinsPhysicalSource(t *testing.T) {
	for _, change := range []string{"same", "role-switch", "unavailable", "replacement", "legacy"} {
		t.Run(change, func(t *testing.T) {
			p, rc, backend := backupFixture(t, "group")
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			source := s.data()[0].Name
			parent := inventoryBackup(t, rc, "parent", "full", "", 10, time.Now().UTC())
			parent.MySQL.PhysicalIdentity = backend.native[source].PhysicalIdentity
			if change == "legacy" {
				parent.MySQL.PhysicalIdentity = ""
			}
			if err = rc.Store.UpdateBackup(t.Context(), &parent); err != nil {
				t.Fatal(err)
			}
			status := backend.native[source]
			switch change {
			case "role-switch":
				status.MemberRole = "PRIMARY"
				status.ReadOnly, status.SuperReadOnly = false, false
			case "unavailable":
				status.MemberState = "OFFLINE"
			case "replacement":
				status.PhysicalIdentity += "-new-datadir"
			}
			backend.native[source] = status
			item := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "incremental", Storage: "archive", Type: "incremental", Phase: domain.PhasePending}
			if err = rc.Store.CreateBackup(t.Context(), item); err != nil {
				t.Fatal(err)
			}
			result, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: change, Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"incremental"}`)})
			if change == "unavailable" || change == "replacement" || change == "legacy" {
				if err == nil {
					t.Fatal("incremental accepted without original physical source")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var cp backupCheckpoint
			if err = json.Unmarshal(result.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Member != source {
				t.Fatalf("source switched from %s to %s", source, cp.Member)
			}
			saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), item.Name)
			if err != nil {
				t.Fatal(err)
			}
			if saved.MySQL.FromLSN != 10 || saved.MySQL.PhysicalIdentity != parent.MySQL.PhysicalIdentity {
				t.Fatalf("lost provenance: %+v", saved.MySQL)
			}
		})
	}
}

func TestRestoreChainRejectsCrossPhysicalSources(t *testing.T) {
	_, rc, _ := backupFixture(t, "single")
	base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
	leaf := inventoryBackup(t, rc, "leaf", "incremental", "base", 20, time.Unix(20, 0))
	for _, identity := range []string{"different-source", ""} {
		leaf.MySQL.PhysicalIdentity = identity
		if _, err := restoreChain([]domain.Backup{base, leaf}, leaf); err == nil {
			t.Fatal("cross-source or legacy incremental accepted")
		}
	}
	base.MySQL.PhysicalIdentity = ""
	if _, err := restoreChain([]domain.Backup{base}, base); err != nil {
		t.Fatalf("legacy full rejected: %v", err)
	}
}

func TestDesiredMajorUpgradeBackupUsesRunningVersion(t *testing.T) {
	p, rc, backend := backupFixture(t, "single")
	const running = "8.0.45-36"
	for name, status := range backend.native {
		status.Version = running
		backend.native[name] = status
	}
	rc.Cluster.Spec.Version = "8.4.11-11"
	requested := time.Now().UTC().Add(-time.Minute)
	var request myspec.BackupRequest
	backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		switch r.URL.Path {
		case "/v1/mysql/backup":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(myspec.BackupResponse{State: "running"})
			return true
		case "/v1/mysql/backup-info":
			json.NewEncoder(w).Encode(myspec.BackupResponse{State: "done", Result: &myspec.BackupResult{ManifestDigest: fixtureManifestDigest, ManifestSize: 1, BackupType: request.Type, ServerVersion: running, ToolVersion: "8.0.35-36", ToLSN: 10, EndTime: time.Now().UTC(), ObjectPrefix: request.ObjectPrefix, PhysicalIdentity: request.PhysicalIdentity}})
			return true
		}
		return false
	}
	item := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "upgrade", Storage: "archive", Type: "full", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "upgrade-backup", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"upgrade"}`)}
	for range 4 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			break
		}
	}
	saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), item.Name)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Phase != domain.PhaseSucceeded || saved.SourceVersion != running || saved.MySQL.ServerVersion != running {
		t.Fatalf("desired version poisoned source backup: %+v", saved)
	}
	if err = requireUpgradeBackup(t.Context(), rc, &rollingState{From: running, RequestedAt: requested}); err != nil {
		t.Fatalf("fresh running-version backup did not satisfy upgrade: %v", err)
	}
}

func TestLegacyIncrementalCheckpointFailsClosed(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	item := inventoryBackup(t, rc, "legacy-running", "incremental", "base", 20, time.Unix(20, 0))
	item.Phase = domain.PhaseRunning
	item.MySQL.PhysicalIdentity = ""
	if err := rc.Store.UpdateBackup(t.Context(), &item); err != nil {
		t.Fatal(err)
	}
	checkpoint, _ := json.Marshal(backupCheckpoint{Step: "poll", Member: "demom0"})
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "legacy-running", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"legacy-running"}`), Checkpoint: checkpoint}); err == nil {
		t.Fatal("legacy in-flight incremental accepted without provenance")
	}
}
