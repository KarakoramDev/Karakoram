// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRunningClusterDeletionFinalizesPBMNamespacesAfterRuntimes(t *testing.T) {
	for _, interruption := range []string{"none", "progress"} {
		t.Run(interruption, func(t *testing.T) {
			p, rc, backend, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			s.UsedStorages = []string{"archive", "alternate"}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			calls := map[string]int{}
			p.deleteClusterObjects = func(_ context.Context, storage domain.BackupStorage, cluster, uid string) error {
				if backend.services[s.Members[0].Name] != nil {
					t.Fatal("namespace deletion ran before runtime fencing")
				}
				directory, err := backupstorage.ClusterDirectory(storage, cluster, uid)
				if err != nil || directory != storage.Prefix+"/"+s.PBMPrefix+"/" {
					t.Fatal("wrong namespace", directory, err)
				}
				calls[storage.Name]++
				if storage.Name == "archive" && calls[storage.Name] == 1 && interruption != "none" {
					return backupstorage.ErrDeletionInProgress
				}
				return nil
			}
			finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
			expected := 1
			if interruption != "none" {
				expected = 2
			}
			if calls["archive"] != expected || calls["alternate"] != 1 {
				t.Fatal("per-storage checkpoints failed", calls)
			}
			finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
			if calls["archive"] != expected || calls["alternate"] != 1 {
				t.Fatal("completed cleanup replayed", calls)
			}
		})
	}
}

func TestKeepingBackupsSkipsPendingNamespaceFinalization(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	s.CleanupBackups = true
	s.CleanedStorages = []string{"archive"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
		t.Fatal("--keep-backups performed namespace deletion")
		return nil
	}
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if len(retryState(t, rc).FinalizedStorages) != 0 {
				t.Fatal("retained namespace was marked deleted")
			}
			return
		}
	}
	t.Fatal("cleanup did not finish")
}

func TestFinalNamespaceCleanupRequiresRecordedIncarnation(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	s.CleanupBackups = true // Legacy PBM cleanup completed, final sweep pending.
	s.CleanedStorages = []string{"archive"}
	s.PBMPrefix = "karakoram/another-incarnation"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
		t.Fatal("invalid incarnation reached direct deletion")
		return nil
	}
	finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
	if len(rc.Events.(*orphanEvents).items) != 1 {
		t.Fatal("unsafe namespace was not reported retained")
	}
}

func TestResumeClearsNamespaceFinalizationCheckpoint(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	suspendForBackupCleanup(t, p, rc)
	s := retryState(t, rc)
	s.FinalizedStorages = []string{"archive"}
	s.CleanedStorages = []string{"archive"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(retryState(t, rc).FinalizedStorages) != 0 || strings.Contains(string(rc.Cluster.Internal), "finalizedStorages") {
		t.Fatal("resume retained a namespace checkpoint that PBM can invalidate")
	}
	if len(retryState(t, rc).CleanedStorages) != 0 || strings.Contains(string(rc.Cluster.Internal), "cleanedStorages") {
		t.Fatal("resume retained a completed storage cleanup checkpoint")
	}
}

func TestResumeReappliesPBMConfigAfterDeletingBackupNamespace(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	rc.Cluster.Spec.Backup.Storage = "archive"
	var mu sync.Mutex
	configCalls := 0
	marker := false
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		encoder := json.NewEncoder(w)
		if r.URL.Path == "/v1/status" {
			_ = encoder.Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
			return
		}
		if r.URL.Path != "/v1/pbm" {
			http.NotFound(w, r)
			return
		}
		var request agentapi.PBMRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		output := json.RawMessage(`{}`)
		switch request.Command {
		case "config":
			if len(request.Config) > 0 {
				configCalls++
				marker = true
			}
		case "status":
			output = json.RawMessage(`{"running":{}}`)
		case "list":
			output = json.RawMessage(`{"snapshots":[],"pitr":{"ranges":[]}}`)
		}
		_ = encoder.Encode(agentapi.PBMResponse{Output: output})
	})
	s := retryState(t, rc)
	env, err := p.pbmEnv(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := ensurePBM(t.Context(), env, "")
	if err != nil {
		t.Fatal(err)
	}
	s.PBMHash = applied
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
		mu.Lock()
		marker = false
		mu.Unlock()
		return nil
	}
	suspended := false
	for range 40 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			suspended = true
			break
		}
	}
	if !suspended || !retryState(t, rc).Suspended {
		t.Fatal("backup-deleting suspension did not finish")
	}
	mu.Lock()
	if marker {
		t.Error("suspension retained PBM's initialization marker")
	}
	before := configCalls
	mu.Unlock()
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	resumed := retryState(t, rc)
	env, err = p.pbmEnv(t.Context(), rc, resumed)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the ordinary resumed-member configuration boundary with the
	// persisted hash, not an independently reset hash supplied by the test.
	hash, err := ensurePBM(t.Context(), env, resumed.PBMHash)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if hash != applied || configCalls != before+1 || !marker {
		t.Fatal("resume skipped reinitializing an unchanged PBM configuration", configCalls, before, marker)
	}
}
