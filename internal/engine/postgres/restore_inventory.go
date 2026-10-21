// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// preflightRestoreSet reads the native pgBackRest set inventory directly: unlike
// an agent action this also works before a clone's first config is installed or
// after the source cluster has been deleted. A successful empty listing is
// authoritative; failed listings (including credentials and missing buckets)
// must remain retryable. Never infer absence from an HTTP error.
func (p *Core) preflightRestoreSet(ctx context.Context, rc *engine.Context, b *domain.Backup) error {
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return err
	}
	present := p.restoreSetPresent
	if present == nil {
		present = storedRestoreSetPresent
	}
	check := func(repository, label string) error {
		prefix := strings.Trim(repository, "/") + "/backup/kk/" + label + "/"
		found, err := present(ctx, storage, prefix)
		if err != nil {
			return fmt.Errorf("list source pgBackRest backup set: %w", err)
		}
		if !found {
			return engine.InvalidField("source.backup", domain.ReasonNotFound,
				"source backup %q pgBackRest set %q is absent from storage; select an existing backup or create a new backup before restoring", b.Name, label)
		}
		return nil
	}
	if len(b.Postgres.Groups) == 0 {
		return check(b.Postgres.Repository, b.Postgres.Label)
	}
	for _, group := range b.Postgres.Groups {
		if err := check(group.Backup.Repository, group.Backup.Label); err != nil {
			return err
		}
	}
	return nil
}

func storedRestoreSetPresent(ctx context.Context, storage *domain.BackupStorage, prefix string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, closeClient, err := backupstorage.NewClient(*storage, storage.Credentials)
	if err != nil {
		return false, err
	}
	defer closeClient()
	found := false
	for object := range client.ListObjects(ctx, storage.Bucket, minio.ListObjectsOptions{Prefix: prefix + "backup.manifest", Recursive: true}) {
		if object.Err != nil {
			return false, object.Err
		}
		if object.Key == prefix+"backup.manifest" || object.Key == prefix+"backup.manifest.copy" {
			found = true
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return found, nil
}
