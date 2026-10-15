// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"errors"
	"strings"

	"github.com/minio/minio-go/v7"
)

// deleteStoredObjects streams validated versions (including delete markers) to
// the SDK's batched deletion API. The result stream provides positive evidence
// of progress rather than treating submitted objects as successfully deleted.
func deleteStoredObjects(ctx context.Context, client *minio.Client, bucket, prefix string, exact bool) (bool, error) {
	objects := make(chan minio.ObjectInfo)
	listing := make(chan error, 1)
	go func() {
		defer close(objects)
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: true}) {
			if object.Err != nil {
				listing <- object.Err
				return
			}
			if !strings.HasPrefix(object.Key, prefix) {
				listing <- errors.New("S3 returned an object outside the selected namespace")
				return
			}
			if exact && object.Key != prefix {
				continue
			}
			select {
			case objects <- object:
			case <-ctx.Done():
				listing <- ctx.Err()
				return
			}
		}
		listing <- ctx.Err()
	}()
	progress := false
	var failure error
	for result := range client.RemoveObjectsWithResult(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
		if result.Err != nil {
			if failure == nil {
				failure = result.Err
			}
		} else {
			progress = true
		}
	}
	listingErr := <-listing
	if failure != nil && !errors.Is(failure, context.DeadlineExceeded) && !errors.Is(failure, context.Canceled) {
		return progress, failure
	}
	if listingErr != nil {
		return progress, listingErr
	}
	return progress, failure
}
