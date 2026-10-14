//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestBackup(t *testing.T) {
	t.Run("Recovery", func(t *testing.T) {
		s := newSuite(t, true)
		storage := s.storage()
		spec := s.spec(1)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.exerciseRecovery(name, storage, nil)
	})
	t.Run("ScheduleRetention", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage()
		spec := s.privateSpec(1)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.exerciseRetention(name, storage)
	})
}

// exerciseRecovery runs every recovery form advertised by PSMDB, on either
// topology. Clone specs are read back from the source to preserve shard layout.
func (s *suite) exerciseRecovery(name, storage string, extraProof func(string)) {
	s.t.Helper()
	proof := func(target string) {
		s.proof(target, "backup-proof", "original")
		if extraProof != nil {
			extraProof(target)
		}
	}
	s.put(name, "backup-proof", "original")
	backups := []api.Backup{
		s.backup(name, storage, api.CreateBackupRequestTypeLogical),
		s.backup(name, storage, api.CreateBackupRequestTypePhysical),
	}
	inventory := s.inventory(name)
	s.snapshot(name+"-inventory", inventory)
	found := map[string]bool{}
	for _, b := range inventory {
		if b.Phase == api.BackupPhaseSucceeded {
			found[b.Name] = true
		}
	}
	for _, b := range backups {
		if !found[b.Name] {
			s.t.Fatalf("inventory omitted successful %s backup %s", b.Type, b.Name)
		}
		s.put(name, "backup-proof", "mutated")
		s.restore(name, api.RestoreSource{Backup: ptr(b.Name), SourceCluster: name})
		proof(name)
		spec := s.cluster(name).Spec
		spec.DataSource = &api.DataSource{Cluster: name, Backup: ptr(b.Name)}
		clone := s.create(spec)
		proof(clone)
		// A clone must use its own reconciled system identities, not just start.
		s.backup(clone, storage, api.CreateBackupRequestTypeLogical)
		s.assertMetrics(clone)
		if !s.keep {
			s.deleteCluster(clone, false)
		}
	}
	s.update(name, func(spec *api.ClusterSpec) { spec.Backup.Pitr = &api.PITRSpec{Enabled: true, OplogSpanMinutes: ptr(1)} })
	// A fresh logical base is required after physical in-place recovery.
	s.backup(name, storage, api.CreateBackupRequestTypeLogical)
	s.put(name, "pitr-proof", "before-target")
	target := s.mongoTime(name)
	s.waitPITR(name, target)
	s.put(name, "pitr-proof", "after-target")
	s.put(name, "after-target-only", "must-disappear")
	s.update(name, func(spec *api.ClusterSpec) { spec.Backup.Pitr.Enabled = false })
	// Clone before restoring the source: the original PITR slices must survive
	// until the clone has consumed them and the source then recovers in place.
	spec := s.cluster(name).Spec
	spec.DataSource = &api.DataSource{Cluster: name, PitrTime: &target}
	clone := s.create(spec)
	s.proof(clone, "pitr-proof", "before-target")
	s.absent(clone, "after-target-only")
	proof(clone)
	if !s.keep {
		s.deleteCluster(clone, false)
	}
	s.restore(name, api.RestoreSource{PitrTime: &target, SourceCluster: name})
	s.proof(name, "pitr-proof", "before-target")
	s.absent(name, "after-target-only")
	proof(name)
	var restores api.RestoreList
	s.expect(&restores)(s.client.ListRestores(s.ctx, name, &api.ListRestoresParams{Limit: ptr(100)}))
	if len(restores.Items) != 3 {
		s.t.Fatalf("restore inventory count=%d want=3", len(restores.Items))
	}
	for _, restore := range restores.Items {
		if restore.Phase != api.RestorePhaseSucceeded {
			s.t.Fatalf("restore inventory contains non-successful restore %s", restore.Name)
		}
	}
	s.snapshot(name+"-restore-inventory", restores)
}

func (s *suite) waitPITR(name string, target time.Time) {
	s.t.Helper()
	s.poll("PITR window covers target", 20*time.Minute, func() bool {
		var window api.PITRWindow
		s.expect(&window)(s.client.GetPITRWindow(s.ctx, name, nil))
		s.t.Logf("cluster=%s pitrAvailable=%t start=%v end=%v target=%s", name, window.Available, window.Start, window.End, target.Format(time.RFC3339))
		if window.Available && window.Start != nil && window.End != nil && !target.Before(*window.Start) && target.Before(*window.End) {
			s.snapshot(name+"-pitr", window)
			return true
		}
		return false
	})
}

func (s *suite) exerciseRetention(name, storage string) {
	s.t.Helper()
	schedule := s.name()
	s.update(name, func(spec *api.ClusterSpec) {
		spec.Backup.Schedules = ptr([]api.BackupSchedule{{Name: schedule, Cron: "*/5 * * * *", Enabled: true, RetainCount: 2, Storage: storage, Type: api.BackupScheduleTypeLogical}})
	})
	seen := map[string]bool{}
	s.poll("three scheduled runs and retainCount=2", 30*time.Minute, func() bool {
		items := s.inventory(name)
		succeeded := 0
		present := map[string]bool{}
		for _, b := range items {
			if b.Schedule != nil && *b.Schedule == schedule {
				if b.Phase == api.BackupPhaseFailed {
					s.t.Fatalf("scheduled backup %s failed", b.Name)
				}
				if b.Phase == api.BackupPhaseSucceeded {
					succeeded++
					present[b.Name] = true
					seen[b.Name] = true
				}
			}
		}
		removed := false
		for n := range seen {
			if !present[n] {
				removed = true
			}
		}
		s.t.Logf("schedule=%s observedSuccesses=%d retainedSuccesses=%d evicted=%t", schedule, len(seen), succeeded, removed)
		if len(seen) >= 3 && succeeded == 2 && removed {
			s.snapshot(name+"-retained-inventory", items)
			return true
		}
		return false
	})
	s.update(name, func(spec *api.ClusterSpec) { spec.Backup.Schedules = ptr([]api.BackupSchedule{}) })
}
