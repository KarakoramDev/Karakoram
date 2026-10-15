// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
)

// ClusterDirectory resolves only the cluster incarnation's private PBM namespace.
// A trailing slash separates it from similarly named clusters and incarnations.
func ClusterDirectory(s domain.BackupStorage, cluster, uid string) (string, error) {
	for _, part := range []string{cluster, uid} {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "/\\") {
			return "", errors.New("cluster lacks a safe recorded PBM namespace identity")
		}
	}
	prefix := strings.TrimRight(s.Prefix, "/")
	if prefix != "" {
		for _, part := range strings.Split(prefix, "/") {
			if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
				return "", errors.New("storage prefix is not a safe object namespace")
			}
		}
		prefix += "/"
	}
	return prefix + "karakoram/" + cluster + "-" + uid + "/", nil
}

// DeleteCluster removes snapshots, metadata, PITR chunks and namespace markers
// for one cluster incarnation. Callers must first establish that all its PBM
// runtimes are absent, so no process can write new objects during deletion.
// Missing objects are success; any failure leaves inventory intact for retry.
func DeleteCluster(ctx context.Context, s domain.BackupStorage, cluster, uid string) error {
	return deleteCluster(ctx, s, cluster, uid, nil)
}

func deleteCluster(ctx context.Context, s domain.BackupStorage, cluster, uid string, transport http.RoundTripper) error {
	directory, err := ClusterDirectory(s, cluster, uid)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := newClient(s, s.Credentials, transport)
	if err != nil {
		return err
	}
	defer closeClient()
	for object := range client.ListObjects(ctx, s.Bucket, minio.ListObjectsOptions{Prefix: directory, Recursive: true}) {
		if object.Err != nil {
			return probeError("list cluster backups", object.Err)
		}
		if !strings.HasPrefix(object.Key, directory) {
			return errors.New("S3 returned an object outside the cluster namespace")
		}
		if err := client.RemoveObject(ctx, s.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return probeError("delete cluster backup object", err)
		}
	}
	return ctx.Err()
}
