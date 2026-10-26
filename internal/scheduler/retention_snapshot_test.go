// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type changingRetentionPolicy struct {
	*psmdb.Provider
	change func()
}

func (p changingRetentionPolicy) FilterRetention(_ []domain.Backup, candidates []domain.Backup) []domain.Backup {
	p.change()
	return candidates
}

func TestRetentionAbandonsStalePolicyAndIncarnation(t *testing.T) {
	for _, change := range []string{"increase", "remove", "recreate"} {
		t.Run(change, func(t *testing.T) {
			ctx := t.Context()
			key, err := secrets.New(strings.Repeat("k", 32))
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb", Backup: domain.BackupConfig{Schedules: []domain.BackupSchedule{{Name: "daily", RetainCount: 1}}}}}
			if err = db.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"one", "two", "three"} {
				if err = db.CreateBackup(ctx, &domain.Backup{Name: name, Cluster: c.Name, Schedule: "daily", Trigger: "schedule", Phase: domain.PhaseSucceeded}); err != nil {
					t.Fatal(err)
				}
			}
			policy := changingRetentionPolicy{Provider: psmdb.New(psmdb.Options{}), change: func() {
				switch change {
				case "increase":
					c.Spec.Backup.Schedules[0].RetainCount = 10
				case "remove":
					c.Spec.Backup.Schedules = nil
				case "recreate":
					if err := db.DeleteCluster(ctx, c.Ref()); err != nil {
						t.Fatal(err)
					}
					if err := db.CreateCluster(ctx, &domain.Cluster{Name: c.Name, Spec: c.Spec}); err != nil {
						t.Fatal(err)
					}
					return
				}
				if err := db.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
					t.Fatal(err)
				}
			}}
			registry, err := engine.NewRegistry(policy)
			if err != nil {
				t.Fatal(err)
			}
			s, err := New(Options{Store: db, Registry: registry})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.EnforceRetention(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			backups, err := db.ListBackups(ctx, c.Ref())
			if err != nil || len(backups) != 3 {
				t.Fatal(backups, err)
			}
			for _, b := range backups {
				if b.Phase != domain.PhaseSucceeded {
					t.Fatal("stale sweep changed backup", b)
				}
			}
			ops, err := db.ListOperations(ctx, c.Ref())
			if err != nil || len(ops) != 0 {
				t.Fatal("stale sweep admitted destructive work", ops, err)
			}
		})
	}
}
