// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"errors"
	"strings"

	"github.com/minio/minio-go/v7"
)

type objectListingFailure struct{ err error }

func (e *objectListingFailure) Error() string { return e.err.Error() }
func (e *objectListingFailure) Unwrap() error { return e.err }

// deleteStoredObjects prefers removing versions, but retains ordinary list/delete
// compatibility when the endpoint or credentials cannot list object versions.
func deleteStoredObjects(ctx context.Context, client *minio.Client, bucket, prefix string, exact bool) (bool, error) {
	progress, err := deleteStoredObjectsMode(ctx, client, bucket, prefix, exact, true)
	var listing *objectListingFailure
	if errors.As(err, &listing) && versionListingUnavailable(listing.err) {
		more, fallbackErr := deleteStoredObjectsMode(ctx, client, bucket, prefix, exact, false)
		return progress || more, fallbackErr
	}
	return progress, err
}

func versionListingUnavailable(err error) bool {
	response := minio.ToErrorResponse(err)
	switch response.Code {
	case "AccessDenied", "NotImplemented", "MethodNotAllowed", "InvalidRequest", "InvalidArgument", "UnsupportedOperation", "XNotImplemented":
		return true
	}
	return response.StatusCode == 403 || response.StatusCode == 405 || response.StatusCode == 501
}

// The synchronous listing iterator stops immediately on invalid namespace data;
// unlike a channel listing, it cannot leave an unread SDK producer behind.
func deleteStoredObjectsMode(ctx context.Context, client *minio.Client, bucket, prefix string, exact, versions bool) (bool, error) {
	objects := make(chan minio.ObjectInfo)
	listing := make(chan error, 1)
	go func() {
		defer close(objects)
		for object := range client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: versions}) {
			if object.Err != nil {
				listing <- &objectListingFailure{object.Err}
				return
			}
			if !strings.HasPrefix(object.Key, prefix) {
				listing <- errors.New("S3 returned an object outside the selected namespace")
				return
			}
			if exact && object.Key != prefix {
				continue
			}
			if !versions {
				object.VersionID = ""
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
	if failure != nil {
		return progress, failure
	}
	// The SDK suppresses some per-object errors. Successful completion therefore
	// requires observing absence, not merely an error-free result stream.
	for object := range client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: versions, MaxKeys: 1}) {
		if object.Err != nil {
			return progress, &objectListingFailure{object.Err}
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return progress, errors.New("S3 returned an object outside the selected namespace")
		}
		if !exact || object.Key == prefix {
			return progress, errors.New("S3 object deletion left selected objects present; retry deletion and check object/version delete permissions")
		}
	}
	return progress, ctx.Err()
}
