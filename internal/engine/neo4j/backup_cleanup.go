// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// storedPrefix uses snapshot-time provenance, not the storage's mutable prefix.
func storedPrefix(b domain.Backup) (string, error) {
	if !b.ProviderMatches("neo4j") || b.Type != "dump" {
		return "", engine.Invalid("backup is not a Neo4j dump")
	}
	prefix := b.PBMPrefix
	parts := strings.Split(prefix, "/")
	if prefix == "" || path.Clean(prefix) != prefix || strings.HasPrefix(prefix, "/") || len(parts) < 4 {
		return "", engine.Invalid("backup has no safe object prefix")
	}
	n := len(parts)
	namespace := b.Namespace
	if namespace == "" {
		namespace = domain.DefaultNamespace
	}
	if parts[n-4] != "neo4j" || parts[n-3] != namespace || !strings.HasPrefix(parts[n-2], b.Cluster+"-") || parts[n-2] == b.Cluster+"-" || parts[n-1] != b.Name || b.Name == "." || b.Name == ".." {
		return "", engine.Invalid("backup object prefix does not match its provenance")
	}
	return prefix, nil
}
func deleteBackupObjects(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	prefix, err := storedPrefix(b)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, closeClient, err := backupstorage.NewClient(storage, storage.Credentials)
	if err != nil {
		return errors.New("cannot initialize backup storage client")
	}
	defer closeClient()
	// The slash is essential: deleting backup 'a' must never remove backup 'ab'.
	prefix += "/"
	for obj := range client.ListObjectsIter(ctx, storage.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return errors.New("cannot list backup objects")
		}
		if !strings.HasPrefix(obj.Key, prefix) {
			return engine.Invalid("storage returned object outside backup prefix")
		}
		if err = client.RemoveObject(ctx, storage.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return errors.New("cannot remove backup object")
		}
	}
	return ctx.Err()
}
func (p *Provider) runDeleteBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp operationCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return checkpoint(cp, false), err
	}
	if rc.Store == nil {
		return checkpoint(cp, false), engine.ErrUnavailable
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		cp.Step = "done"
		return checkpoint(cp, true), nil
	}
	if err != nil {
		return checkpoint(cp, false), err
	}
	// The destination is committed before the first agent request. A pending
	// or rejected backup without one has never written any objects.
	if b.PBMPrefix != "" {
		storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
		if err != nil {
			return checkpoint(cp, false), err
		}
		if err = deleteBackupObjects(ctx, *storage, *b); err != nil {
			return checkpoint(cp, false), err
		}
	} else if b.Phase != domain.PhasePending && b.Phase != domain.PhaseFailed {
		return checkpoint(cp, false), engine.Invalid("backup has no recorded object prefix")
	}
	if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return checkpoint(cp, false), err
	}
	cp.Step = "done"
	return checkpoint(cp, true), nil
}
func (p *Provider) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	// Resolve sealed credentials through the provider store when available.
	if p.options.Store != nil {
		resolved, err := p.options.Store.GetBackupStorage(ctx, b.Namespace, b.Storage)
		if err != nil {
			return err
		}
		storage = *resolved
	}
	return deleteBackupObjects(ctx, storage, b)
}
func (p *Provider) cleanupBackups(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) error {
	if opts.KeepBackups || rc.Store == nil {
		return nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, b := range items {
		if b.PBMPrefix != "" {
			storage, e := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
			if e != nil {
				return e
			}
			if e = deleteBackupObjects(ctx, *storage, b); e != nil {
				return e
			}
		}
		if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}

var _ engine.RetainedBackupDeleter = (*Provider)(nil)
