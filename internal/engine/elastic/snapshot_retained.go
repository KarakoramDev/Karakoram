// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// DeleteRetainedBackup removes a retired repository only when its last snapshot
// is the requested backup. Individual snapshots share blobs; without a running
// Elasticsearch writer, deleting per-snapshot objects can corrupt other backups.
// For a multi-snapshot repository, register it in another cluster and delete the
// snapshot through Elasticsearch instead. No writer may use a retired base path.
func (p *Provider) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	return p.retainedBackupDeletion(ctx, storage, b, true)
}

func (p *Provider) ValidateRetainedBackupDeletion(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	return p.retainedBackupDeletion(ctx, storage, b, false)
}

func (p *Provider) retainedBackupDeletion(ctx context.Context, storage domain.BackupStorage, b domain.Backup, remove bool) error {
	base, _, snapshot, err := recordedSnapshot(&storage, &b)
	if err != nil {
		return &engine.RetainedBackupDeletionRefusal{Err: &domain.ResourceError{Resource: "backup", Name: b.Name, Reason: domain.ReasonInvalidState, State: string(b.Phase), Err: engine.ErrConflict, Message: err.Error()}}
	}
	// The immutable recorded base ends in namespace/cluster/incarnation.
	// Today's storage prefix only controls future repositories and may differ.
	// Match the complete trailing tuple, never a tuple found inside a broader
	// path or a namespace derived by stripping the mutable storage prefix.
	parts := strings.Split(base, "/")
	if len(parts) < 3 || parts[len(parts)-3] != domain.NormalizedNamespace(b.Namespace) || parts[len(parts)-2] != b.Cluster || parts[len(parts)-1] == "" {
		return &engine.RetainedBackupDeletionRefusal{Err: &domain.ResourceError{Resource: "backup", Name: b.Name, Reason: domain.ReasonInvalidState, State: string(b.Phase), Err: engine.ErrConflict, Message: "retained snapshot lacks a complete cluster repository namespace; preserve its record and repair provenance before deletion"}}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := backupstorage.NewClient(storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	prefix := base + "/"
	var keys []string
	latest := ""
	generation := int64(-1)
	for object := range client.ListObjectsIter(ctx, storage.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return fmt.Errorf("list retained repository: %w", object.Err)
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return errors.New("S3 returned an object outside the repository namespace")
		}
		keys = append(keys, object.Key)
		relative := strings.TrimPrefix(object.Key, prefix)
		if strings.HasPrefix(relative, "index-") && !strings.Contains(relative, "/") {
			n, e := strconv.ParseInt(strings.TrimPrefix(relative, "index-"), 10, 64)
			if e == nil && n >= 0 && n > generation {
				generation = n
				latest = object.Key
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if latest == "" {
		return errors.New("retained repository has no readable snapshot catalog; register the repository in another Elasticsearch cluster before deletion")
	}
	object, err := client.GetObject(ctx, storage.Bucket, latest, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	// Repository index-N is JSON. Bound metadata to avoid loading arbitrary blobs.
	raw, readErr := io.ReadAll(io.LimitReader(object, 16*1024*1024+1))
	closeErr := object.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = validateRetainedCatalog(raw, snapshot); err != nil {
		// This attempt has not changed storage. The catalog does not prove
		// that an earlier attempt could not have partially removed objects.
		return &engine.RetainedBackupDeletionRefusal{Err: &domain.ResourceError{Resource: "backup", Name: b.Name, Reason: domain.ReasonInvalidState, State: string(b.Phase), Err: engine.ErrConflict, Message: err.Error()}}
	}
	if !remove {
		return nil
	}
	// Delete the selected catalog last. A partial deletion remains retryable while
	// the authoritative last-snapshot check is still available. Shared blobs are
	// now safe to delete because the entire retired repository is being removed.
	for _, key := range keys {
		if key == latest {
			continue
		}
		if err = client.RemoveObject(ctx, storage.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("delete retained repository object: %w", err)
		}
	}
	if err = client.RemoveObject(ctx, storage.Bucket, latest, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete retained repository catalog: %w", err)
	}
	return nil
}

func validateRetainedCatalog(raw []byte, snapshot string) error {
	var catalog struct {
		Snapshots []struct {
			Name string `json:"name"`
		} `json:"snapshots"`
	}
	if len(raw) > 16*1024*1024 || json.Unmarshal(raw, &catalog) != nil {
		return errors.New("retained repository catalog is unreadable; register the repository in another Elasticsearch cluster before deletion")
	}
	if len(catalog.Snapshots) != 1 || catalog.Snapshots[0].Name != snapshot {
		return errors.New("individual snapshots of a deleted cluster cannot be deleted safely while other snapshots remain; restore or register the repository in another Elasticsearch cluster and delete the snapshot there")
	}
	return nil
}
