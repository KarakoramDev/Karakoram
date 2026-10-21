// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/minio/minio-go/v7"
)

// Full backup objects are self-contained: backup-s3/client.go bucketAndPath
// uses makeObjectName(overridePath, backupID, key), i.e. path/backupID/key.
// Never derive this prefix from the current storage prefix or cluster state.
func (p *Provider) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	if b.Phase == domain.PhaseRunning || b.Phase == domain.PhasePending {
		return fmt.Errorf("%w: cancel the in-progress backup before deleting its objects", engine.ErrConflict)
	}
	m, err := recordedBackup(&storage, &b)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := backupstorage.NewClient(storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	return deleteBackupObjects(ctx, client, m)
}

func deleteBackupObjects(ctx context.Context, client *minio.Client, m *domain.WeaviateBackup) error {
	prefix := m.Path + "/" + m.ID + "/"
	for object := range client.ListObjects(ctx, m.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return object.Err
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return errors.New("S3 returned an object outside the recorded backup prefix")
		}
		if err := client.RemoveObject(ctx, m.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (p *Provider) cancelBackupWriter(ctx context.Context, rc *engine.Context, s *state, b *domain.Backup, storage *domain.BackupStorage) (bool, error) {
	m, err := recordedBackup(storage, b)
	if err != nil {
		return false, err
	}
	client, err := p.native(ctx, rc, s)
	if err != nil {
		return false, err
	}
	defer client.Close()
	info, err := client.BackupStatus(ctx, m.ID, nativeLocation(m))
	if wvapi.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if terminalBackup(info.Status) {
		return true, nil
	}
	if err = client.CancelBackup(ctx, m.ID, nativeLocation(m)); err != nil && !wvapi.IsNotFound(err) {
		return false, err
	}
	return false, nil
}
func (p *Provider) runDeleteBackup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (engine.OpResult, error) {
	result := func(done bool, err error) (engine.OpResult, error) {
		r, e := operationResult(cp, done)
		return r, errors.Join(err, e)
	}
	if name == "" {
		return result(true, invalid("backup name is required"))
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		return result(true, nil)
	}
	if err != nil {
		return result(false, err)
	}
	storage, err := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
	if err != nil {
		return result(false, err)
	}
	// Even a record marked Deleting can still have an active native writer after
	// a crash, so always observe/cancel before touching its object prefix.
	stopped, err := p.cancelBackupWriter(ctx, rc, s, b, storage)
	if err != nil || !stopped {
		return result(false, err)
	}
	b.Phase = domain.PhaseDeleting
	if err = rc.Store.UpdateBackup(ctx, b); err != nil {
		return result(false, err)
	}
	if err = p.DeleteRetainedBackup(ctx, *storage, *b); err != nil {
		return result(false, err)
	}
	if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return result(false, err)
	}
	cp.Stage = "done"
	return result(true, nil)
}
