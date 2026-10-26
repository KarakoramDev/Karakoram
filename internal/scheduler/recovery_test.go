// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestTransientFireFailureRetriesOccurrenceAfterRestart(t *testing.T) {
	key, _ := secrets.New(strings.Repeat("k", 32))
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	at := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Schedules: []domain.BackupSchedule{{Name: "daily", Cron: "@daily", Enabled: true}}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{Store: st, Clock: fixedClock{at}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TRIGGER reject_backup BEFORE INSERT ON records WHEN NEW.kind='backup' BEGIN SELECT RAISE(FAIL, 'temporary write failure'); END"); err != nil {
		t.Fatal(err)
	}
	at = at.Add(24 * time.Hour)
	s.opts.Clock = fixedClock{at}
	if err := s.Reload(t.Context()); err == nil {
		t.Error("lost scheduled occurrence without reporting persistence failure")
	}
	if _, err := db.Exec("DROP TRIGGER reject_backup"); err != nil {
		t.Fatal(err)
	}
	s, err = New(Options{Store: st, Clock: fixedClock{at}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Reload(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	backups, err := st.ListBackups(t.Context(), c.Ref())
	if err != nil || len(backups) != 1 {
		t.Fatalf("missed or duplicate durable occurrence: %d %v", len(backups), err)
	}
	ops, err := st.ListOperations(t.Context(), c.Ref())
	if err != nil || len(ops) != 1 {
		t.Fatalf("missed or duplicate operation: %d %v", len(ops), err)
	}
	s.opts.Clock = fixedClock{at.Add(-12 * time.Hour)}
	if err := s.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	backups, err = st.ListBackups(t.Context(), c.Ref())
	if err != nil || len(backups) != 1 {
		t.Fatal("backward clock duplicated occurrence", err)
	}
	// An unrelated malformed row must not remove the healthy schedule.
	if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: "broken"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE clusters SET spec=? WHERE name='broken'", []byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := s.Reload(t.Context()); err != nil {
		t.Fatal("malformed row stopped all schedules", err)
	}
	if len(s.entries) != 1 {
		t.Fatal("healthy schedule missing")
	}
}
