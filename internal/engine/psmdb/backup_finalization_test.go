// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestRunningClusterDeletionFinalizesPBMNamespacesAfterRuntimes(t *testing.T) {
	for _, interruption := range []string{"none", "storage", "progress", "checkpoint-loss"} {
		t.Run(interruption, func(t *testing.T) {
			p, rc, backend, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			s.UsedStorages = []string{"archive", "alternate"}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			retained := &domain.Backup{Name: "retained", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseSucceeded, PBMName: "retained-snapshot", PBMPrefix: "archive/karakoram/" + rc.Cluster.Name + "-prior", RecoveryIdentity: "retained-recovery"}
			if err := rc.Store.CreateBackup(t.Context(), retained); err != nil {
				t.Fatal(err)
			}
			identity := domain.ClusterRef{Name: retained.RecoveryIdentity}
			if err := rc.Secrets.SetClusterSecret(t.Context(), identity, "users/userAdmin", []byte("recovery")); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			objects := map[string]bool{retained.PBMPrefix + "/retained-snapshot/data": true}
			pbmCalls := 0
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
				pbmCalls++
				output := json.RawMessage(`{}`)
				switch request.Command {
				case "config":
					if len(request.Config) != 0 {
						var config struct {
							Storage struct {
								S3 struct {
									Prefix string `yaml:"prefix"`
								} `yaml:"s3"`
							} `yaml:"storage"`
						}
						var text string
						if err := json.Unmarshal(request.Config, &text); err != nil {
							t.Error(err)
						}
						if err := yaml.Unmarshal([]byte(text), &config); err != nil {
							t.Error(err)
						}
						if config.Storage.S3.Prefix == "" {
							t.Error("PBM configuration lacks an incarnation prefix")
						}
						objects[config.Storage.S3.Prefix+"/.pbm.init"] = true
					}
				case "status":
					output = json.RawMessage(`{"running":{}}`)
				case "list":
					output = json.RawMessage(`{"snapshots":[],"pitr":{"ranges":[]}}`)
				}
				_ = encoder.Encode(agentapi.PBMResponse{Output: output})
			})
			calls := map[string]int{}
			interrupted := false
			p.deleteClusterObjects = func(_ context.Context, storage domain.BackupStorage, cluster, uid string) error {
				for _, member := range s.Members {
					if backend.services[member.Name] != nil {
						t.Fatal("namespace deletion ran while a runtime could recreate the marker")
					}
				}
				if _, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "users/userAdmin"); err != nil {
					t.Fatal("cleanup discarded credentials before namespace finalization", err)
				}
				directory, err := backupstorage.ClusterDirectory(storage, cluster, uid)
				if err != nil || directory != storage.Prefix+"/"+s.PBMPrefix+"/" {
					t.Fatal("invalid finalization scope", directory, err)
				}
				calls[storage.Name]++
				if storage.Name == "archive" && calls[storage.Name] == 1 && interruption == "storage" {
					return errors.New("storage temporarily unavailable")
				}
				mu.Lock()
				defer mu.Unlock()
				if pbmCalls == 0 {
					t.Fatal("test did not exercise live PBM cleanup")
				}
				for key := range objects {
					if strings.HasPrefix(key, directory) {
						delete(objects, key)
					}
				}
				if storage.Name == "archive" && calls[storage.Name] == 1 {
					if interruption == "progress" {
						return backupstorage.ErrDeletionInProgress
					}
					if interruption == "checkpoint-loss" {
						return errors.New("interrupted before recording completed namespace deletion")
					}
				}
				return nil
			}
			done := false
			for range 80 {
				var err error
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					if interrupted || interruption != "storage" && interruption != "checkpoint-loss" {
						t.Fatal(err)
					}
					interrupted = true
					if retryState(t, rc).CleanupSecrets || len(retryState(t, rc).FinalizedStorages) != 0 {
						t.Fatal("failure advanced finalization checkpoints")
					}
					if !strings.Contains(err.Error(), "repair storage access") {
						t.Fatal("storage failure is not actionable", err)
					}
				}
				// Each reconciliation resumes from a newly allocated durable checkpoint.
				clone := *rc.Cluster
				clone.Internal = append([]byte(nil), rc.Cluster.Internal...)
				rc.Cluster = &clone
				if done {
					break
				}
			}
			if !done {
				t.Fatal("running cluster deletion did not finish")
			}
			expected := 1
			if interruption != "none" {
				expected = 2
			}
			if calls["archive"] != expected || calls["alternate"] != 1 || len(retryState(t, rc).FinalizedStorages) != 2 {
				t.Fatal("namespace finalization did not resume per destination", calls, retryState(t, rc).FinalizedStorages)
			}
			mu.Lock()
			if len(objects) != 1 || !objects[retained.PBMPrefix+"/retained-snapshot/data"] {
				t.Error("current marker remained or prior incarnation was deleted", objects)
			}
			mu.Unlock()
			if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), retained.Name); err != nil {
				t.Fatal("prior-incarnation backup record was removed", err)
			}
			if _, err := rc.Secrets.GetClusterSecret(t.Context(), identity, "users/userAdmin"); err != nil {
				t.Fatal("retained recovery identity was removed", err)
			}
			for range 2 {
				if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{}); !done || err != nil {
					t.Fatal("completed cleanup is not idempotent", done, err)
				}
			}
			if calls["archive"] != expected || calls["alternate"] != 1 {
				t.Fatal("completed namespace checkpoints were replayed", calls)
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
	s.CleanupBackups = true
	s.CleanedStorages = []string{"archive"}
	s.PBMPrefix = "karakoram/another-incarnation"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
		t.Fatal("invalid incarnation reached direct deletion")
		return nil
	}
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if done {
			t.Fatal("unsafe namespace was accepted")
		}
		if err != nil {
			if !strings.Contains(err.Error(), "verified PBM namespace") || retryState(t, rc).CleanupSecrets {
				t.Fatal("invalid identity did not preserve finalization state", err)
			}
			return
		}
	}
	t.Fatal("cleanup did not reach namespace validation")
}

func TestResumeClearsNamespaceFinalizationCheckpoint(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	suspendForBackupCleanup(t, p, rc)
	s := retryState(t, rc)
	s.FinalizedStorages = []string{"archive"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(retryState(t, rc).FinalizedStorages) != 0 || strings.Contains(string(rc.Cluster.Internal), "finalizedStorages") {
		t.Fatal("resume retained a namespace checkpoint that PBM can invalidate")
	}
}
