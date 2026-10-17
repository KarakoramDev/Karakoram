// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/robfig/cron/v3"
)

func normalizedDataSource(source *domain.DataSource, namespace string) *domain.DataSource {
	if source == nil {
		return nil
	}
	copy := *source
	if copy.Namespace == "" {
		copy.Namespace = domain.NormalizedNamespace(namespace)
	}
	return &copy
}
func (p *Core) validateBackups(ctx context.Context, old, n *domain.Cluster) error {
	policy := n.Spec.Backup
	if policy.PITR.Enabled && policy.Storage == "" {
		return engine.Invalid("PITR requires backup storage")
	}
	if policy.PITR.OplogSpanMinutes < 0 || policy.PITR.OplogSpanMinutes > 1440 {
		return engine.Invalid("PITR oplogSpanMinutes must be between 1 and 1440, or omitted for the default")
	}
	storages := map[string]bool{}
	if policy.Storage != "" {
		storages[policy.Storage] = true
	}
	names := map[string]bool{}
	for i, schedule := range policy.Schedules {
		if schedule.Name == "" || names[schedule.Name] {
			return engine.Invalid("backup schedule names must be nonempty and unique")
		}
		names[schedule.Name] = true
		if _, err := cron.ParseStandard(schedule.Cron); err != nil {
			return engine.Invalid("invalid backup schedule %q", schedule.Name)
		}
		if schedule.Type != "full" && schedule.Type != "diff" && schedule.Type != "incr" {
			return engine.Invalid("backup schedule type must be full, diff or incr")
		}
		if schedule.RetainCount < 1 {
			return engine.InvalidField(fmt.Sprintf("spec.backup.schedules.%d.retainCount", i), domain.ReasonOutOfRange, "backup retainCount must be at least 1")
		}
		storage := schedule.Storage
		if storage == "" {
			storage = policy.Storage
		}
		if storage == "" {
			return engine.Invalid("backup schedule requires storage")
		}
		storages[storage] = true
	}
	for storage := range storages {
		if p.options.Store == nil {
			return engine.Invalid("backup storage validation requires a configured store")
		}
		if _, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, storage); err != nil {
			return engine.Invalid("backup storage %q is unavailable", storage)
		}
	}
	if old != nil {
		before, _ := json.Marshal(normalizedDataSource(old.Spec.DataSource, old.Namespace))
		after, _ := json.Marshal(normalizedDataSource(n.Spec.DataSource, n.Namespace))
		if string(before) != string(after) {
			return engine.Invalid("dataSource is immutable")
		}
		return nil
	}
	source := normalizedDataSource(n.Spec.DataSource, n.Namespace)
	if source == nil {
		return nil
	}
	if policy.Storage == "" {
		return engine.Invalid("dataSource requires target-local backup storage for an independent repository and fresh full backup")
	}
	ref := domain.ClusterRef{Namespace: source.Namespace, Name: source.Cluster}
	if ref.Validate() != nil || ref == n.Ref() || (source.Backup == "") == (source.PITRTime == nil) {
		return engine.Invalid("dataSource requires another cluster and exactly one of backup or pitrTime")
	}
	if p.options.Store == nil {
		return engine.Invalid("dataSource validation requires a configured store")
	}
	if source.Backup != "" {
		backup, err := p.options.Store.GetBackup(ctx, ref, source.Backup)
		if err != nil || backup.Phase != domain.PhaseSucceeded || backup.Postgres == nil || backup.RecoveryIdentity == "" {
			return engine.Invalid("dataSource backup must be succeeded with pgBackRest provenance and sealed recovery credentials")
		}
		if !backup.ProviderMatches(n.Spec.Provider) {
			return engine.Invalid("dataSource requires exactly matching providers")
		}
		if major(backup.SourceVersion) != major(n.Spec.Version) {
			return engine.Invalid("dataSource PostgreSQL major must match the target")
		}
	} else {
		cluster, err := p.options.Store.GetCluster(ctx, ref)
		if err != nil {
			return engine.Invalid("dataSource cluster is unavailable")
		}
		if cluster.Spec.Provider != n.Spec.Provider || major(cluster.Spec.Version) != major(n.Spec.Version) {
			return engine.Invalid("dataSource provider and PostgreSQL major must match the target")
		}
		window := cluster.Status.Backup.PITRWindow
		if !cluster.Spec.Backup.PITR.Enabled || source.PITRTime.IsZero() || window == nil || source.PITRTime.Before(window.Start) || source.PITRTime.After(window.End) {
			return engine.Invalid("dataSource PITR time must be in the source cluster's observed conservative WAL window")
		}
	}
	return nil
}
func (p *Core) backupStatus(ctx context.Context, rc *engine.Context, status *domain.ClusterStatus) error {
	if rc.Store == nil {
		return nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, b := range items {
		if b.Phase == domain.PhaseSucceeded && b.CompletedAt != nil && (status.Backup.LastBackup == nil || b.CompletedAt.After(*status.Backup.LastBackup)) {
			stamp := *b.CompletedAt
			status.Backup.LastBackup = &stamp
		}
	}
	if rc.Cluster.Spec.Backup.PITR.Enabled {
		windows, err := p.PITRWindow(ctx, rc)
		if err != nil {
			return err
		}
		if len(windows) > 0 {
			status.Backup.PITRWindow = &windows[0]
		}
	}
	return nil
}

// FilterRetention preserves pgBackRest's newest full and any chain needed by
// backups retained by another schedule (or created manually). Skipped sets stay
// eligible on later passes, once a newer full and independent retained sets exist.
func (p *Core) FilterRetention(backups, candidates []domain.Backup) []domain.Backup {
	eligible := make(map[string]bool, len(candidates))
	for _, b := range candidates {
		eligible[b.Name] = true
	}
	for changed := true; changed; {
		changed = false
		for _, b := range candidates {
			if !eligible[b.Name] || b.Postgres == nil {
				continue
			}
			var repository []domain.Backup
			newerFull := false
			for _, other := range backups {
				if other.Postgres == nil || other.Storage != b.Storage || other.Postgres.Repository != b.Postgres.Repository {
					continue
				}
				repository = append(repository, other)
				if other.Type == "full" && other.Postgres.Label > b.Postgres.Label && other.Phase == domain.PhaseSucceeded {
					newerFull = true
				}
			}
			protect := b.Type == "full" && !newerFull
			removed := removedBackupLabels(repository, b.Postgres.Label)
			for _, other := range repository {
				if other.Name != b.Name && removed[other.Postgres.Label] && !eligible[other.Name] {
					protect = true
				}
			}
			if protect {
				delete(eligible, b.Name)
				changed = true
			}
		}
	}
	retained := candidates[:0]
	for _, b := range candidates {
		if eligible[b.Name] {
			retained = append(retained, b)
		}
	}
	return retained
}

var _ engine.RetentionPolicy = (*Core)(nil)
