// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"errors"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ElasticsearchDirectory verifies the recorded writable incarnation namespace,
// retaining historical writer normalization for already-persisted bindings.
func ElasticsearchDirectory(storage domain.BackupStorage, namespace, cluster, uid, base string) (string, error) {
	namespace = domain.NormalizedNamespace(namespace)
	if domain.NormalizedNamespace(storage.Namespace) != namespace || cluster == "" || uid == "" {
		return "", errors.New("repository lacks an owned cluster incarnation")
	}
	for _, segment := range []string{namespace, cluster, uid} {
		if segment == "." || segment == ".." || strings.ContainsAny(segment, "/\\") {
			return "", errors.New("unsafe repository incarnation")
		}
	}
	expected := domain.ElasticsearchObjectPrefix(storage.Prefix, namespace+"/"+cluster+"/"+uid)
	if base != expected {
		return "", errors.New("recorded repository differs from the current storage binding")
	}
	for _, segment := range strings.Split(base, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.Contains(segment, "\\") {
			return "", errors.New("unsafe repository prefix")
		}
	}
	return base + "/", nil
}

// DeleteElasticsearchRepository removes a complete repository only after its
// writers have retired. Shared snapshot blobs must never be deleted individually.
func DeleteElasticsearchRepository(ctx context.Context, storage domain.BackupStorage, namespace, cluster, uid, base string) error {
	directory, err := ElasticsearchDirectory(storage, namespace, cluster, uid, base)
	if err != nil {
		return err
	}
	client, closeClient, err := NewClient(storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	_, err = deleteStoredObjects(ctx, client, storage.Bucket, directory, false)
	return err
}
