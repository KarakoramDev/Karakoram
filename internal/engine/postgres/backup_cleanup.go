// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

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
	if s.BackupCleanupStarted == nil {
		start := now(rc)
		s.BackupCleanupStarted = &start
		return false, nil // Persist the bound before any external object deletion.
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
	names := make([]string, 0, len(storages))
	for name := range storages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if slices.Contains(s.RetainedBackupStorages, name) {
			continue
		}
		storage, storageErr := p.backupStorage(ctx, rc, name, rc.Cluster.Ref().Namespace)
		if storageErr != nil {
			recorded := make([]string, 0)
			for _, b := range items {
				if b.Storage == name && b.Postgres != nil {
					recorded = append(recorded, b.Postgres.Repository)
				}
			}
			sort.Strings(recorded)
			scope := fmt.Sprintf("binding unavailable; recorded repositories %q; verify incarnation ownership before removal", strings.Join(recorded, ", "))
			if done, err := retainBackupStorage(ctx, rc, s, name, scope); !done || err != nil {
				return false, err
			}
			continue
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
		ordered := make([]string, 0, len(prefixes))
		for prefix := range prefixes {
			ordered = append(ordered, prefix)
		}
		sort.Strings(ordered)
		retained := false
		for _, prefix := range ordered {
			key := name + "\x00" + prefix
			if slices.Contains(s.CleanedBackupRepositories, key) {
				continue
			}
			deadline := s.BackupCleanupStarted.Add(2 * time.Minute)
			if now(rc).Before(deadline) {
				deleteRepository := p.deleteRepository
				if deleteRepository == nil {
					deleteRepository = backupstorage.DeletePostgresRepository
				}
				pass, cancel := context.WithTimeout(ctx, deadline.Sub(now(rc)))
				// Never infer ownership from a mutable storage binding alone.
				_, err = backupstorage.PostgresDirectory(*storage, rc.Cluster.Namespace, rc.Cluster.Name, prefix)
				if err == nil && s.UID != "" && s.UID != "." && s.UID != ".." && !strings.ContainsAny(s.UID, "/\\") {
					err = deleteRepository(pass, *storage, rc.Cluster.Namespace, rc.Cluster.Name, prefix)
				} else if err == nil {
					err = errors.New("missing cluster incarnation identity")
				}
				cancel()
				if err == nil {
					s.CleanedBackupRepositories = append(s.CleanedBackupRepositories, key)
					return false, nil // Commit each exact repository independently.
				}
			}
			scope := fmt.Sprintf("bucket %q repositories %q", storage.Bucket, strings.Join(ordered, ", "))
			done, err := retainBackupStorage(ctx, rc, s, name, scope)
			if !done || err != nil {
				return false, err
			}
			retained = true
			break
		}
		if retained {
			continue
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

// Errors are progress until the durable deadline, not generic operation failures:
// a transient outage must not consume the 20-failure tracking budget first.
func retainBackupStorage(ctx context.Context, rc *engine.Context, s *state, name, scope string) (bool, error) {
	if now(rc).Before(s.BackupCleanupStarted.Add(2 * time.Minute)) {
		rc.Cluster.Status.Message = "Deleting: retrying control-plane backup storage cleanup within its two-minute bound"
		return false, nil
	}
	if rc.Events == nil {
		return false, errors.New("cannot record retained backup artifacts")
	}
	message := fmt.Sprintf("PostgreSQL backup/WAL artifacts may remain in storage %q (%s; cluster incarnation %s). The two-minute cleanup bound expired; cluster deletion continues and affected backup inventory is retained. Restore the original backup-store binding, verify recorded repository ownership, then remove only those exact incarnation repository prefixes with object-store tooling and delete retained backup records. Never guess an unknown prefix.", name, scope, s.UID)
	if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "BackupArtifactsRetained", Message: message, CreatedAt: now(rc)}); err != nil {
		return false, err
	}
	s.RetainedBackupStorages = append(s.RetainedBackupStorages, name)
	return true, nil
}

var _ engine.RetainedBackupDeleter = (*Core)(nil)
