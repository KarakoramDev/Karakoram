// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Core) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if !b.ProviderMatches(p.Name()) {
		return engine.Invalid("backup provider mismatch")
	}
	if p.options.Store == nil {
		return engine.ErrUnavailable
	}
	return deleteMySQLBackup(ctx, p.options.Store, storage, b)
}
func deleteMySQLBackup(ctx context.Context, store engine.ClusterStore, storage domain.BackupStorage, b domain.Backup) error {
	items, err := store.ListBackups(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster})
	if err != nil {
		return err
	}
	if err = backupstorage.DeleteMySQLBackup(ctx, storage, b, items); err != nil {
		return err
	}
	for _, item := range backupstorage.MySQLDeletionClosure(items, b) {
		secretStore, _ := store.(engine.SecretStore)
		if err = deleteBackupAccess(ctx, secretStore, item); err != nil {
			return err
		}
		if err = store.DeleteBackup(ctx, domain.ClusterRef{Namespace: item.Namespace, Name: item.Cluster}, item.Name); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}
func (p *Core) runDeleteBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return backupResult(cp, false), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		return backupResult(backupCheckpoint{Step: "done"}, true), nil
	}
	if err != nil {
		return backupResult(cp, false), err
	}
	if b.MySQL == nil {
		if b.Phase != domain.PhasePending && b.Phase != domain.PhaseFailed {
			return backupResult(cp, false), engine.Invalid("backup lacks MySQL provenance")
		}
		err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name)
	} else {
		storage, e := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
		if e != nil {
			return backupResult(cp, false), e
		}
		err = deleteMySQLBackup(ctx, rc.Store, *storage, *b)
	}
	if errors.Is(err, backupstorage.ErrDeletionInProgress) {
		return backupResult(cp, false), nil
	}
	return backupResult(backupCheckpoint{Step: "done"}, err == nil), err
}
func (p *Core) cleanupBackups(ctx context.Context, rc *engine.Context, s *state, opts domain.DeleteOptions) (bool, error) {
	if opts.KeepBackups || rc.Store == nil {
		return true, nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return false, err
	}
	names := map[string]bool{}
	if rc.Cluster.Spec.Backup.Storage != "" {
		names[rc.Cluster.Spec.Backup.Storage] = true
	}
	for _, name := range bs.Storages {
		names[name] = true
	}
	for name := range bs.Repositories {
		names[name] = true
	}
	for _, schedule := range rc.Cluster.Spec.Backup.Schedules {
		if schedule.Storage != "" {
			names[schedule.Storage] = true
		}
	}
	for _, b := range items {
		if b.Storage != "" {
			names[b.Storage] = true
		}
	}
	for name := range names {
		storage, e := p.backupStorage(ctx, rc, name, rc.Cluster.Ref().Namespace)
		if e != nil {
			return false, e
		}
		prefixes := map[string]bool{}
		for generation := range bs.Generation + 1 {
			prefixes[repoPrefix(storage, rc.Cluster, s, generation)] = true
		}
		for _, prefix := range bs.Repositories[name] {
			prefixes[prefix] = true
		}
		for _, b := range items {
			if b.MySQL != nil && b.Storage == name && b.MySQL.ClusterUID == s.UID {
				prefixes[b.MySQL.Repository] = true
			}
		}
		for prefix := range prefixes {
			if e = backupstorage.DeleteMySQLRepository(ctx, *storage, p.Name(), rc.Cluster.Ref().Namespace, rc.Cluster.Name, prefix); errors.Is(e, backupstorage.ErrDeletionInProgress) {
				return false, nil
			}
			if e != nil {
				return false, e
			}
		}
		for _, b := range items {
			if b.Storage == name && (b.MySQL == nil || prefixes[b.MySQL.Repository]) {
				if e = deleteBackupAccess(ctx, rc.Secrets, b); e != nil {
					return false, e
				}
				if e = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); e != nil && !missing(e) {
					return false, e
				}
			}
		}
	}
	return true, nil
}
func (*Core) FilterRetention(backups, candidates []domain.Backup) []domain.Backup {
	eligible := map[string]bool{}
	for _, b := range candidates {
		eligible[b.Name] = true
	}
	for changed := true; changed; {
		changed = false
		for _, b := range candidates {
			if !eligible[b.Name] || b.MySQL == nil {
				continue
			}
			for _, dependent := range backupstorage.MySQLDeletionClosure(backups, b) {
				if !eligible[dependent.Name] {
					delete(eligible, b.Name)
					changed = true
					break
				}
			}
		}
	}
	result := make([]domain.Backup, 0, len(candidates))
	for _, b := range candidates {
		if eligible[b.Name] {
			result = append(result, b)
		}
	}
	return result
}

var _ engine.RetentionPolicy = (*Core)(nil)
var _ engine.RetainedBackupDeleter = (*Core)(nil)
