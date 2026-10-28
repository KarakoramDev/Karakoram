// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
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
	if n.Spec.Topology.Type == "distributed" && source.PITRTime != nil {
		return engine.Invalid("distributed PostgreSQL clone requires a coordinated backup, not timestamp PITR")
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
		if err := p.validateBackupProfiles(backup, n); err != nil {
			return err
		}
		if n.Spec.Topology.Type == "distributed" {
			options, err := ParseOptions(n.Spec.EngineOptions)
			if err != nil {
				return err
			}
			if backup.SourceTopology.Type != "distributed" || len(backup.Postgres.Groups) != options.citusOptions().WorkerGroups+1 {
				return engine.Invalid("distributed clone requires exactly matching source and target group counts")
			}
		} else if len(backup.Postgres.Groups) > 0 {
			return engine.Invalid("distributed backups require a distributed clone target")
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
		items, err := p.options.Store.ListBackups(ctx, ref)
		if err != nil {
			return err
		}
		backup, err := p.selectPITRBackup(ctx, &engine.Context{Cluster: n, Store: p.options.Store}, cluster, *source.PITRTime, items)
		if err != nil {
			return err
		}
		if err := p.validateBackupProfiles(backup, n); err != nil {
			return err
		}
	}
	return nil
}
func (p *Core) backupStatus(ctx context.Context, rc *engine.Context, status *domain.ClusterStatus, s *state, primary *pgspec.StatusResponse) error {
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
		if s.Topology == "distributed" {
			status.Conditions = append(status.Conditions, domain.Condition{Type: "PITR", Status: "False", Reason: "NamedPointsOnly", Message: "Citus recovery uses coordinated named restore points, not a continuous timestamp recovery window. Select a completed named restore point.", LastTransitionTime: now(rc)})
			return nil
		}
		var windows []domain.TimeWindow
		var err error
		if s.Topology != "distributed" {
			if primary == nil {
				err = engine.ErrUnavailable
			} else {
				windows, err = p.pitrWindowFromPrimary(ctx, rc, s, *primary, items)
			}
		}
		if err != nil {
			status.Conditions = append(status.Conditions, domain.Condition{Type: "PITR", Status: "Unknown", Reason: "ObservationUnavailable", Message: "The conservative PITR window could not be collected; restore writer/repository access. Database and DCS health are reported independently.", LastTransitionTime: now(rc)})
			return nil
		}
		if len(windows) > 0 {
			status.Backup.PITRWindow = &windows[0]
		}
		condition := domain.Condition{Type: "PITR", Status: "True", Reason: "WindowAvailable", LastTransitionTime: now(rc)}
		if len(windows) == 0 {
			condition.Status, condition.Reason = "False", "WindowUnavailable"
			condition.Message = "No conservative WAL recovery window is available. Check WALArchive health and the repository backup chain."
		}
		status.Conditions = append(status.Conditions, condition)
	}
	return nil
}

// postgresRepositoryIdentity includes the complete recorded path: source
// incarnation, restore generation and (for Citus) group incarnation. Labels are
// meaningful only within this identity, never across storage or source scopes.
type postgresRepositoryIdentity struct {
	namespace, cluster, storage, repository string
	group                                   int
}

func postgresRepositoryInventory(backups []domain.Backup) map[postgresRepositoryIdentity][]domain.Backup {
	repositories := make(map[postgresRepositoryIdentity][]domain.Backup)
	for _, b := range backups {
		if b.Postgres == nil {
			continue
		}
		key := postgresRepositoryIdentity{
			namespace: domain.NormalizedNamespace(b.Namespace),
			cluster:   b.Cluster, storage: b.Storage,
			repository: b.Postgres.Repository, group: -1,
		}
		if len(b.Postgres.Groups) == 0 {
			repositories[key] = append(repositories[key], b)
			continue
		}
		for _, group := range b.Postgres.Groups {
			key.repository, key.group = group.Backup.Repository, group.Group
			copy := b
			meta := group.Backup
			copy.Postgres = &meta
			repositories[key] = append(repositories[key], copy)
		}
	}
	return repositories
}

// FilterRetention preserves pgBackRest's newest full and any chain needed by
// backups retained by another schedule (or created manually). Skipped sets stay
// eligible on later passes, once a newer full and independent retained sets exist.
func (p *Core) FilterRetention(backups, candidates []domain.Backup) []domain.Backup {
	eligible := make(map[string]bool, len(candidates))
	for _, b := range candidates {
		eligible[b.Name] = true
	}
	repositories := postgresRepositoryInventory(backups)
	candidateRepositories := postgresRepositoryInventory(candidates)
	for changed := true; changed; {
		changed = false
		for key, sets := range candidateRepositories {
			repository := repositories[key]
			for _, b := range sets {
				if !eligible[b.Name] {
					continue
				}
				newerFull := false
				for _, other := range repository {
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
