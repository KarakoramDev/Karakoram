// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestReanchorRetention(t *testing.T) {
	for _, tc := range []struct {
		name      string
		provider  string
		schedules []domain.BackupSchedule
		triggers  []string
		want      []string
	}{
		{name: "fallback", provider: "mysql", triggers: []string{"pitr-reanchor", "pitr-reanchor", "pitr-reanchor", "pitr-reanchor"}, want: []string{"b0", "b1"}},
		{name: "shared-schedule", provider: "mysql", schedules: []domain.BackupSchedule{{Name: "fulls", Type: "full", RetainCount: 2}}, triggers: []string{"pitr-reanchor", "schedule", "pitr-reanchor", "schedule"}, want: []string{"b0", "b1"}},
		{name: "largest-schedule", provider: "mysql", schedules: []domain.BackupSchedule{{Name: "small", Type: "full", RetainCount: 1}, {Name: "fulls", Type: "full", RetainCount: 3}}, triggers: []string{"pitr-reanchor", "schedule", "pitr-reanchor", "schedule"}, want: []string{"b0"}},
		{name: "schedule-tie", provider: "mysql", schedules: []domain.BackupSchedule{{Name: "z", Type: "full", RetainCount: 2}, {Name: "fulls", Type: "full", RetainCount: 2}}, triggers: []string{"pitr-reanchor", "schedule", "pitr-reanchor", "schedule"}, want: []string{"b0", "b1"}},
		{name: "other-triggers", provider: "mysql", triggers: []string{"restore", "manual", "pitr-reanchor", "pitr-reanchor", "pitr-reanchor"}, want: []string{"b2"}},
		{name: "incremental-schedule-not-full", provider: "mysql", schedules: []domain.BackupSchedule{{Name: "fulls", Type: "incremental", RetainCount: 1}}, triggers: []string{"pitr-reanchor", "pitr-reanchor", "pitr-reanchor"}, want: []string{"b0"}},
		{name: "non-mysql", provider: "psmdb", triggers: []string{"pitr-reanchor", "pitr-reanchor", "pitr-reanchor"}},
		{name: "pxc", provider: "pxc", triggers: []string{"pitr-reanchor", "pitr-reanchor", "pitr-reanchor"}, want: []string{"b0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, s, c := reanchorFixture(t, tc.provider, tc.schedules)
			for i, trigger := range tc.triggers {
				addReanchorBackup(t, db, c, i, trigger)
			}
			if err := s.Reload(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertReanchorDeletions(t, db, c, tc.want)
			if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			assertReanchorDeletions(t, db, c, tc.want)
		})
	}
}

func TestReanchorRetentionProtectsNewestFull(t *testing.T) {
	db, s, c := reanchorFixture(t, "mysql", []domain.BackupSchedule{{Name: "fulls", Type: "full", RetainCount: 1}})
	// Completion ordering wins over creation ordering, and a failed newer full
	// must not make the last successful full eligible for expiration.
	b := addReanchorBackup(t, db, c, 0, "pitr-reanchor")
	at := b.CreatedAt.Add(24 * time.Hour)
	b.CompletedAt = &at
	if err := db.UpdateBackup(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	addReanchorBackup(t, db, c, 1, "schedule")
	failed := addReanchorBackup(t, db, c, 2, "pitr-reanchor")
	failed.Phase = domain.PhaseFailed
	if err := db.UpdateBackup(t.Context(), &failed); err != nil {
		t.Fatal(err)
	}
	if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertReanchorDeletions(t, db, c, []string{"b1"})
}

func TestReanchorRetentionProtectsNewestFullAfterScheduleTypeChange(t *testing.T) {
	db, s, c := reanchorFixture(t, "mysql", []domain.BackupSchedule{{Name: "fulls", Type: "full", RetainCount: 1}})
	addReanchorBackup(t, db, c, 0, "pitr-reanchor")
	// Historical scheduled incrementals still belong to this schedule's count.
	// They cannot evict the cluster's only successful full after a policy edit.
	for i := 1; i < 3; i++ {
		b := addReanchorBackup(t, db, c, i, "schedule")
		b.Type = "incremental"
		if err := db.UpdateBackup(t.Context(), &b); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertReanchorDeletions(t, db, c, []string{"b1"})
}

func TestReanchorRetentionSkipsInflightSources(t *testing.T) {
	for _, clone := range []bool{false, true} {
		for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning} {
			t.Run(string(phase)+map[bool]string{false: "restore", true: "clone"}[clone], func(t *testing.T) {
				db, s, c := reanchorFixture(t, "mysql", nil)
				for i := range 5 {
					addReanchorBackup(t, db, c, i, "pitr-reanchor")
				}
				target := &domain.Cluster{Name: "target"}
				op := &domain.Operation{ID: "dependent", Cluster: target.Name, Phase: phase}
				if clone {
					op.Kind = domain.OperationCreate
					target.Spec.DataSource = &domain.DataSource{Cluster: c.Name, Backup: "b0"}
					if err := db.CreateCluster(store.WithOperation(t.Context(), op), target); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := db.CreateCluster(t.Context(), target); err != nil {
						t.Fatal(err)
					}
					op.Kind = domain.OperationRestore
					r := &domain.Restore{Name: "restore", Cluster: target.Name, Phase: phase, Source: domain.RestoreSource{SourceCluster: c.Name, Backup: "b0"}}
					if err := db.EnqueueRestore(t.Context(), r, op); err != nil {
						t.Fatal(err)
					}
					r.Phase = phase
					if err := db.UpdateRestore(t.Context(), r); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				assertReanchorDeletions(t, db, c, []string{"b1", "b2"})
				if clone {
					if err := db.MarkClusterDeleting(t.Context(), target.Ref(), domain.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				} else {
					op.Phase = domain.PhaseSucceeded
					if err := db.UpdateOperation(t.Context(), op); err != nil {
						t.Fatal(err)
					}
					r, err := db.GetRestore(t.Context(), target.Ref(), "restore")
					if err != nil {
						t.Fatal(err)
					}
					r.Phase = domain.PhaseSucceeded
					if err := db.UpdateRestore(t.Context(), r); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
				assertReanchorDeletions(t, db, c, []string{"b0", "b1", "b2"})
			})
		}
	}
}

func TestReanchorRetentionPreservesIncrementalChain(t *testing.T) {
	db, s, c := reanchorFixture(t, "mysql", nil)
	registry, err := engine.NewRegistry(mysql.New(mysql.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	s.opts.Registry = registry
	for i := range 3 {
		addReanchorBackup(t, db, c, i, "pitr-reanchor")
	}
	child := addReanchorBackup(t, db, c, 3, "manual")
	child.Type = "incremental"
	child.MySQL.ParentLabel = "b0"
	if err := db.UpdateBackup(t.Context(), &child); err != nil {
		t.Fatal(err)
	}
	if err := s.EnforceRetention(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertReanchorDeletions(t, db, c, nil)
}

func reanchorFixture(t *testing.T, provider string, schedules []domain.BackupSchedule) (*store.Store, *Scheduler, *domain.Cluster) {
	t.Helper()
	key, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(t.TempDir(), "retention.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: provider, Backup: domain.BackupConfig{Schedules: schedules}}}
	if err := db.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{Store: db})
	if err != nil {
		t.Fatal(err)
	}
	return db, s, c
}
func addReanchorBackup(t *testing.T, db *store.Store, c *domain.Cluster, i int, trigger string) domain.Backup {
	t.Helper()
	at := time.Date(2026, 10, 1, 0, i, 0, 0, time.UTC)
	name := string(rune('0' + i))
	name = "b" + name
	b := domain.Backup{Name: name, Cluster: c.Name, Provider: c.Spec.Provider, Type: "full", Trigger: trigger, Schedule: "fulls", Phase: domain.PhaseSucceeded, CreatedAt: at, CompletedAt: &at, Storage: "archive", MySQL: &domain.MySQLBackup{Label: name, Repository: "repo", ClusterUID: "uid"}}
	if err := db.CreateBackup(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	return b
}
func assertReanchorDeletions(t *testing.T, db *store.Store, c *domain.Cluster, want []string) {
	t.Helper()
	ops, err := db.ListOperations(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, op := range ops {
		if op.Kind != domain.OperationDeleteBackup {
			continue
		}
		var payload map[string]string
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, payload["backup"])
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deleted=%v want=%v", got, want)
	}
}
