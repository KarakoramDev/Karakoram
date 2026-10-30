// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCCompletedFullNeverRetriesBaselineObservation(t *testing.T) {
	for _, scenario := range []string{"no-incident", "acknowledged", "old-agent", "timeout", "journal", "audit"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			detected := time.Unix(10, 0)
			if scenario != "no-incident" {
				s.GTIDIncidents = []gtidIncident{{DetectedAt: detected, Audited: true}}
			}
			if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			start := detected.Add(time.Second)
			full := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "completed-full", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &start, MySQL: &domain.MySQLBackup{EndTime: start.Add(time.Second)}}
			if err := rc.Store.CreateBackup(t.Context(), full); err != nil {
				t.Fatal(err)
			}
			prior := b.action
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			var observations atomic.Int32
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/galera-status" {
					observations.Add(1)
					if scenario == "timeout" {
						select {
						case <-r.Context().Done():
						case <-release:
						}
						return true
					}
					if scenario == "old-agent" {
						_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: "11111111-2222-3333-4444-555555555555"})
						return true
					}
				}
				return prior != nil && prior(w, r, name)
			}
			if scenario == "journal" {
				rc.Secrets = &accessFaultSecrets{SecretStore: rc.Secrets, key: "mysql/gtid/history"}
			}
			if scenario == "audit" {
				rc.Events = gtidEventFunc(func(context.Context, domain.Event) error { return context.Canceled })
			}
			op := &domain.Operation{ID: "completed-full-op", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"completed-full"}`)}
			began := time.Now()
			result, err := p.runBackup(t.Context(), rc, op, backupCheckpoint{Step: "poll"})
			if err != nil || !result.Done {
				t.Fatalf("completed native backup became a retryable failure: %+v %v", result, err)
			}
			if time.Since(began) >= 15*time.Second {
				t.Fatal("post-backup GTID observation exceeded availability lease")
			}
			stored, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), full.Name)
			if err != nil || stored.Phase != domain.PhaseSucceeded {
				t.Fatalf("backup and operation terminal outcomes disagree: %+v %v", stored, err)
			}
			s, _ = load(rc.Cluster)
			if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if scenario == "acknowledged" || scenario == "no-incident" {
				if result.Step != "done" || activeGTIDIncident(s) != nil {
					t.Fatal("complete healthy sample did not acknowledge baseline")
				}
				if scenario == "no-incident" && observations.Load() != 0 {
					t.Fatal("ordinary full completion unnecessarily sampled GTIDs")
				}
			} else {
				if result.Step != "done-baseline-pending" || !strings.Contains(result.Message, "Backup succeeded") {
					t.Fatalf("baseline failure lacks explicit non-retry status: %+v", result)
				}
				if scenario == "audit" {
					if activeGTIDIncident(s) != nil || !s.GTIDIncidents[0].BaselineAuditPending {
						t.Fatal("audit failure lost its durable pending baseline event")
					}
				} else if activeGTIDIncident(s) == nil {
					t.Fatal("unproven baseline silently cleared real divergence")
				}
			}
		})
	}
}

func TestPXCFullBackupPollDoesNotWaitForGTIDObservation(t *testing.T) {
	p, rc, b := backupFixture(t, "single")
	rc.Cluster.Spec.Provider = "pxc"
	p = New(Options{Provider: "pxc"})
	full := inventoryBackup(t, rc, "running-full", "full", "", 10, time.Now())
	full.Phase = domain.PhaseRunning
	if err := rc.Store.UpdateBackup(t.Context(), &full); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	var observations atomic.Int32
	b.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
		switch r.URL.Path {
		case "/v1/mysql/galera-status":
			observations.Add(1)
			http.Error(w, "GTID transport unavailable", http.StatusServiceUnavailable)
			return true
		case "/v1/mysql/backup-info":
			_ = json.NewEncoder(w).Encode(myspec.BackupResponse{State: "running"})
			return true
		}
		return false
	}
	op := &domain.Operation{ID: "running-full-op", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"running-full"}`)}
	result, err := p.runBackup(t.Context(), rc, op, backupCheckpoint{Step: "poll", Member: s.data()[0].Name})
	if err != nil || result.Done || observations.Load() != 0 {
		t.Fatalf("native full poll was gated by unrelated GTID reads: %+v %v calls=%d", result, err, observations.Load())
	}
}
