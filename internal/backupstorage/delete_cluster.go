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

// ErrDeletionInProgress means a bounded cleanup pass deleted objects but needs
// another pass. It is normal reconciliation progress, not a storage failure.
var ErrDeletionInProgress = errors.New("backup object deletion is in progress")

// DeleteCluster removes snapshots, metadata, PITR chunks and namespace markers
// for one cluster incarnation. Callers must first establish that all its PBM
// runtimes are absent, so no process can write new objects during deletion.
// Missing objects are success. A pass that exhausts its budget after confirmed
// progress returns ErrDeletionInProgress; inventory must remain until completion.
func DeleteCluster(ctx context.Context, s domain.BackupStorage, cluster, uid string) error {
	return deleteCluster(ctx, s, cluster, uid, nil)
}

func deleteCluster(ctx context.Context, s domain.BackupStorage, cluster, uid string, transport http.RoundTripper) error {
	return deleteClusterPass(ctx, s, cluster, uid, transport, 2*time.Minute)
}

func deleteClusterPass(ctx context.Context, s domain.BackupStorage, cluster, uid string, transport http.RoundTripper, budget time.Duration) error {
	directory, err := ClusterDirectory(s, cluster, uid)
	if err != nil {
		return err
	}
	pass, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	client, closeClient, err := newClient(s, s.Credentials, transport)
	if err != nil {
		return err
	}
	defer closeClient()
	progress, err := deleteStoredObjects(pass, client, s.Bucket, directory, false)
	if err != nil {
		if progress && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return ErrDeletionInProgress
		}
		return probeError("delete cluster backup objects", err)
	}
	return nil
}
