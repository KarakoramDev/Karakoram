// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Restore points share their base sets; deleting a point removes only its
// recovery target, not the pgBackRest sets shared by other coordinated targets.
func deleteCitusPoint(ctx context.Context, b domain.Backup, store engine.ClusterStore, secrets engine.SecretStore) error {
	if err := deleteRecoverySecrets(ctx, secrets, b); err != nil {
		return err
	}
	if store == nil {
		return nil
	}
	return store.DeleteBackup(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}, b.Name)
}

func removeCitusBackupInventory(ctx context.Context, b domain.Backup, store engine.ClusterStore, secrets engine.SecretStore) error {
	items, err := store.ListBackups(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster})
	if err != nil {
		return err
	}
	removed := map[string]bool{}
	repositories := postgresRepositoryInventory(items)
	for key, targets := range postgresRepositoryInventory([]domain.Backup{b}) {
		virtual := repositories[key]
		labels := removedBackupLabels(virtual, targets[0].Postgres.Label)
		for _, item := range virtual {
			if labels[item.Postgres.Label] {
				removed[item.Name] = true
			}
		}
	}
	for _, item := range items {
		if removed[item.Name] {
			if err = deleteCitusPoint(ctx, item, store, secrets); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Core) runDeleteCitusBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint, s *state, b *domain.Backup, storage *domain.BackupStorage) (engine.OpResult, error) {
	if b.Trigger == "pitr" {
		if err := deleteCitusPoint(ctx, *b, rc.Store, rc.Secrets); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	}
	if cp.Step == "" {
		cp.Step = "citus-delete-preflight"
	}
	if cp.Index >= len(b.Postgres.Groups) {
		if cp.Step == "citus-delete-preflight" {
			cp.Index = 0
			cp.Step = "citus-delete"
			return backupResult(cp, false), nil
		}
		if err := removeCitusBackupInventory(ctx, *b, rc.Store, rc.Secrets); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	}
	group := b.Postgres.Groups[cp.Index]
	active := false
	for _, current := range s.groups() {
		if current.id == group.Group && group.Backup.Repository == groupRepoPrefix(storage, rc.Cluster, s, group.Group) {
			active = true
			break
		}
	}
	if !active {
		if cp.Step != "citus-delete-preflight" {
			retired := *b
			meta := group.Backup
			retired.Postgres = &meta
			if err := backupstorage.DeletePostgresBackup(ctx, *storage, retired); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Index++
		return backupResult(cp, false), nil
	}
	leader, _, err := s.group(group.Group).primary(ctx, rc)
	if err != nil {
		return backupResult(cp, false), err
	}
	repo, err := repository(storage, group.Backup.Repository)
	if err != nil {
		return backupResult(cp, false), err
	}
	var inventory pgspec.BackupInfoResponse
	if err = pgAction(ctx, rc, leader.Name, "backup-info", pgspec.RepositoryRequest{Repository: &repo}, &inventory); err != nil {
		return backupResult(cp, false), err
	}
	present, newerFull := false, false
	for _, item := range inventory.Backups {
		present = present || item.Label == group.Backup.Label
		if item.Type == "full" && item.Label > group.Backup.Label {
			newerFull = true
		}
	}
	if cp.Step == "citus-delete-observe" {
		if present {
			return backupResult(cp, false), engine.Invalid("group %d retained expired set %s", group.Group, group.Backup.Label)
		}
		cp.Index++
		cp.Step = "citus-delete"
		return backupResult(cp, false), nil
	}
	if cp.Step == "citus-delete-preflight" {
		if present && b.Type == "full" && !newerFull {
			return p.failBackupDeletion(ctx, rc, b, cp, "create a newer full backup in every group before deleting this coordinated set")
		}
		cp.Index++
		return backupResult(cp, false), nil
	}
	if present {
		if err = pgAction(ctx, rc, leader.Name, "expire", pgspec.ExpireRequest{Set: group.Backup.Label, Repository: &repo}, nil); err != nil {
			return backupResult(cp, false), fmt.Errorf("expire group %d backup: %w", group.Group, err)
		}
	}
	cp.Step = "citus-delete-observe"
	return backupResult(cp, false), nil
}

func deleteRetiredCitusBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup, store engine.ClusterStore, secrets engine.SecretStore) error {
	if b.Trigger == "pitr" {
		return deleteCitusPoint(ctx, b, store, secrets)
	}
	for _, group := range b.Postgres.Groups {
		copy := b
		meta := group.Backup
		copy.Postgres = &meta
		if err := backupstorage.DeletePostgresBackup(ctx, storage, copy); err != nil {
			return err
		}
	}
	if store == nil {
		return nil
	}
	return removeCitusBackupInventory(ctx, b, store, secrets)
}
