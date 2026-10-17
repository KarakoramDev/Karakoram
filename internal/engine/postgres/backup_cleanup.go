// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"path"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func deleteRecoverySecrets(ctx context.Context, secrets engine.SecretStore, b domain.Backup) error {
	if secrets == nil || b.RecoveryIdentity == "" {
		return nil
	}
	for _, user := range systemNames {
		err := secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/"+user)
		if err != nil && !missing(err) {
			return err
		}
	}
	if err := secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "access/manifest"); err != nil && !missing(err) {
		return err
	}
	return nil
}

// DeleteRetainedBackup expires a repository set with no remaining live writer.
// Supabase promotes this method unchanged through the PostgreSQL core.
func (p *Core) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	secretStore, _ := p.options.Store.(engine.SecretStore)
	return deleteRetiredBackup(ctx, storage, b, p.options.Store, secretStore)
}

func deleteRetiredBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup, store engine.ClusterStore, secretStore engine.SecretStore) error {
	if err := backupstorage.DeletePostgresBackup(ctx, storage, b); err != nil {
		return err
	}
	if store == nil {
		return nil
	}
	items, err := store.ListBackups(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster})
	if err != nil {
		return err
	}
	removed := removedBackupLabels(items, b.Postgres.Label)
	for _, item := range items {
		if item.Postgres == nil || item.Storage != b.Storage || item.Postgres.Repository != b.Postgres.Repository || !removed[item.Postgres.Label] {
			continue
		}
		if err = deleteRecoverySecrets(ctx, secretStore, item); err != nil {
			return err
		}
		if err = store.DeleteBackup(ctx, domain.ClusterRef{Namespace: item.Namespace, Name: item.Cluster}, item.Name); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}
func (p *Core) cleanupBackups(ctx context.Context, rc *engine.Context, s *state, opts domain.DeleteOptions) (bool, error) {
	if opts.KeepBackups || rc.Store == nil {
		return true, nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	storages := map[string]bool{}
	if name := rc.Cluster.Spec.Backup.Storage; name != "" {
		storages[name] = true
	}
	for _, schedule := range rc.Cluster.Spec.Backup.Schedules {
		if schedule.Storage != "" {
			storages[schedule.Storage] = true
		}
	}
	for _, b := range items {
		if b.Storage != "" {
			storages[b.Storage] = true
		}
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return false, err
	}
	for _, name := range bs.RepositoryStorages {
		storages[name] = true
	}
	for name := range storages {
		storage, err := p.backupStorage(ctx, rc, name, rc.Cluster.Ref().Namespace)
		if err != nil {
			return false, err
		}
		prefixes := map[string]bool{repoPrefix(storage, rc.Cluster, s): true}
		identities := map[string]bool{path.Base(repoPrefix(storage, rc.Cluster, s)): true}
		for _, generation := range s.RetiredRepositoryGenerations {
			previous := *s
			previous.RepositoryGeneration = generation
			prefix := repoPrefix(storage, rc.Cluster, &previous)
			prefixes[prefix] = true
			identities[path.Base(prefix)] = true
		}
		for _, backup := range items {
			if backup.Storage == name && backup.Postgres != nil &&
				identities[path.Base(backup.Postgres.Repository)] {
				prefixes[backup.Postgres.Repository] = true
			}
		}
		for prefix := range prefixes {
			err = backupstorage.DeletePostgresRepository(ctx, *storage, rc.Cluster.Ref().Namespace, rc.Cluster.Name, prefix)
			if errors.Is(err, backupstorage.ErrDeletionInProgress) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
		}
		for _, b := range items {
			if b.Storage != name || (b.Postgres != nil && !prefixes[b.Postgres.Repository]) {
				continue
			}
			if err = deleteRecoverySecrets(ctx, rc.Secrets, b); err != nil {
				return false, err
			}
			if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); err != nil && !missing(err) {
				return false, err
			}
		}
	}
	return true, nil
}

var _ engine.RetainedBackupDeleter = (*Core)(nil)
