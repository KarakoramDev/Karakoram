// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeTrigger struct{ names []string }

func (t *fakeTrigger) Trigger(name string) { t.names = append(t.names, name) }
func TestScheduleReloadFireAndRetention(t *testing.T) {
	ctx := store.WithActor(context.Background(), "interactive-user")
	key, _ := secrets.New(strings.Repeat("k", 32))
	db, err := store.Open(filepath.Join(t.TempDir(), "schedule.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2024, 1, 5, 12, 0, 0, 0, time.UTC)
	trigger := &fakeTrigger{}
	s, err := New(Options{Store: db, Trigger: trigger, Clock: fixedClock{now}})
	if err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "archive", Schedules: []domain.BackupSchedule{{Name: "daily", Cron: "@daily", Type: "physical", RetainCount: 2, Enabled: true}}}}}
	if err = db.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if err = s.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.entries) != 1 {
		t.Fatal(s.entries)
	}
	if err = s.Fire(ctx, "demo", "daily"); err != nil {
		t.Fatal(err)
	}
	backups, _ := db.ListBackups(ctx, "demo")
	ops, _ := db.ListOperations(ctx, "demo")
	if len(backups) != 1 || len(ops) != 1 || backups[0].Trigger != "schedule" || backups[0].Storage != "archive" || backups[0].Type != "physical" || !backups[0].CreatedAt.Equal(now) || len(trigger.names) != 1 {
		t.Fatal(backups, ops, trigger)
	}
	if ops[0].Actor != "scheduler" {
		t.Fatalf("scheduled backup actor=%q", ops[0].Actor)
	}
	var payload struct {
		Backup string `json:"backup"`
	}
	if err = json.Unmarshal(ops[0].Payload, &payload); err != nil || payload.Backup != backups[0].Name {
		t.Fatal(payload, err)
	}
	for i, name := range []string{"oldest", "middle", "newer", "newest"} {
		at := now.Add(time.Duration(i) * time.Hour)
		b := domain.Backup{Name: name, Cluster: "demo", Schedule: "daily", Trigger: "schedule", Phase: domain.PhaseSucceeded, CompletedAt: &at, CreatedAt: at}
		if err = db.CreateBackup(ctx, &b); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.EnforceRetention(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	ops, _ = db.ListOperations(ctx, "demo")
	deleted := map[string]bool{}
	for _, op := range ops {
		if op.Actor != "scheduler" {
			t.Fatalf("scheduled operation actor=%q", op.Actor)
		}
		if op.Kind == domain.OperationDeleteBackup {
			json.Unmarshal(op.Payload, &payload)
			deleted[payload.Backup] = true
		}
	}
	if len(deleted) != 2 || !deleted["oldest"] || !deleted["middle"] {
		t.Fatal(deleted)
	}
	if err = s.EnforceRetention(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	again, _ := db.ListOperations(ctx, "demo")
	if len(again) != len(ops) {
		t.Fatal("duplicate retention ops")
	}
	c.Spec.Backup.Schedules[0].Cron = "0 */6 * * *"
	if err = db.UpdateClusterSpec(ctx, "demo", c.Spec); err != nil {
		t.Fatal(err)
	}
	old := s.entries["demo/daily"].id
	if err = s.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if s.entries["demo/daily"].id == old {
		t.Fatal("cron not replaced")
	}
	c.Spec.Backup.Schedules[0].Enabled = false
	db.UpdateClusterSpec(ctx, "demo", c.Spec)
	if err = s.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.entries) != 0 {
		t.Fatal("disabled entry retained")
	}
	if err = s.Fire(ctx, "demo", "daily"); err != nil {
		t.Fatal(err)
	}
	after, _ := db.ListBackups(ctx, "demo")
	if len(after) != 5 {
		t.Fatal("disabled schedule fired")
	}
}
func TestCronGrammar(t *testing.T) {
	for _, spec := range []string{"@daily", "@every 1h", "0 0 * * *", "*/5 * * * *"} {
		if err := ValidateCron(spec); err != nil {
			t.Fatal(spec, err)
		}
	}
	for _, spec := range []string{"0 0 0 * * *", "bad", "99 * * * *"} {
		if err := ValidateCron(spec); err == nil {
			t.Fatal("invalid cron accepted", spec)
		}
	}
}
