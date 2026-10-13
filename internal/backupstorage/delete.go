// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
)

// SnapshotKeys resolves the immutable PBM snapshot namespace. Never derive it
// from a recreated cluster, or use a broad prefix that includes other snapshots.
func SnapshotKeys(s domain.BackupStorage, b domain.Backup) (directory, metadata string, err error) {
	prefix := strings.TrimRight(b.PBMPrefix, "/")
	storagePrefix := strings.TrimRight(s.Prefix, "/")
	if b.Storage != s.Name || prefix == "" || b.PBMName == "" || strings.ContainsAny(b.PBMName, "/\\") || b.PBMName == "." || b.PBMName == ".." {
		return "", "", errors.New("backup lacks a safe recorded PBM snapshot identity")
	}
	if storagePrefix != "" && !strings.HasPrefix(prefix, storagePrefix+"/") {
		return "", "", errors.New("recorded PBM prefix is outside the storage prefix")
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return "", "", errors.New("recorded PBM prefix is not a safe object namespace")
		}
	}
	base := prefix + "/" + b.PBMName
	return base + "/", base + ".pbm.json", nil
}

// DeleteSnapshot removes only the snapshot directory and its sibling PBM
// metadata file. Missing objects are success. Delete metadata last, and retain
// the control-plane record until this call succeeds so interruptions are retryable.
// PITR slices, other snapshots, and PBM namespace markers are not touched.
func DeleteSnapshot(ctx context.Context, s domain.BackupStorage, b domain.Backup) error {
	directory, metadata, err := SnapshotKeys(s, b)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := newClient(s, s.Credentials, nil)
	if err != nil {
		return err
	}
	defer closeClient()
	for object := range client.ListObjects(ctx, s.Bucket, minio.ListObjectsOptions{Prefix: directory, Recursive: true}) {
		if object.Err != nil {
			return probeError("list snapshot", object.Err)
		}
		if !strings.HasPrefix(object.Key, directory) {
			return errors.New("S3 returned an object outside the snapshot namespace")
		}
		if err := client.RemoveObject(ctx, s.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return probeError("delete snapshot object", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := client.RemoveObject(ctx, s.Bucket, metadata, minio.RemoveObjectOptions{}); err != nil {
		return probeError("delete snapshot metadata", err)
	}
	return nil
}
