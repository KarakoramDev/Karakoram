// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

// walkReplayObjects hashes a streaming inventory rather than retaining its keys.
// Use the synchronous iterator: the SDK channel adapter can block forever while
// sending its cancellation error after a consumer has already returned.
func walkReplayObjects(ctx context.Context, c *minio.Client, source myspec.Repository, digest hash.Hash, visit func(minio.ObjectInfo) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var encoder *json.Encoder
	if digest != nil {
		encoder = json.NewEncoder(digest)
	}
	for obj := range c.ListObjectsIter(ctx, source.S3.Bucket, minio.ListObjectsOptions{Prefix: path.Join(source.Path, "binlogs") + "/", Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		if encoder != nil {
			// Include every remotely observable identity field. A replacement,
			// deletion, append or repository/baseline change must invalidate.
			if err := encodeReplayObject(encoder, obj); err != nil {
				return err
			}
		}
		if visit != nil {
			if err := visit(obj); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

func encodeReplayObject(encoder *json.Encoder, obj minio.ObjectInfo) error {
	return encoder.Encode(struct {
		Key, ETag, VersionID string
		Size                 int64
		Modified             time.Time
	}{obj.Key, obj.ETag, obj.VersionID, obj.Size, obj.LastModified})
}

func readReplaySidecar(ctx context.Context, c *minio.Client, source myspec.Repository, objectKey string) (archivedBinlog, error) {
	o, err := c.GetObject(ctx, source.S3.Bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return archivedBinlog{}, err
	}
	var metadata binlogSidecar
	err = json.NewDecoder(io.LimitReader(o, 32<<20)).Decode(&metadata)
	o.Close()
	if err != nil {
		return archivedBinlog{}, err
	}
	key := strings.TrimSuffix(objectKey, ".json")
	if key != path.Join(source.Path, "binlogs", metadata.ServerUUID, metadata.File) {
		return archivedBinlog{}, errors.New("invalid_sidecar")
	}
	return archivedBinlog{key, metadata}, nil
}

func readReplaySidecars(ctx context.Context, c *minio.Client, source myspec.Repository) ([]archivedBinlog, error) {
	var logs []archivedBinlog
	err := walkReplayObjects(ctx, c, source, nil, func(obj minio.ObjectInfo) error {
		log, err := readReplaySidecar(ctx, c, source, obj.Key)
		if err != nil {
			return err
		}
		logs = append(logs, log)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return logs, nil
}

type replayCoverageCache struct {
	key, inventory, lastKey string
	state                   *replayCoverageState
	lastSequence            uint64
	lastOrderedKey          string
}

func loadPITRBaseline(ctx context.Context, s myspec.Spec, p paths, request myspec.PITRCoverageRequest) (myspec.BackupResult, error) {
	var empty myspec.BackupResult
	intent := myspec.Restore{Source: request.Source, BackupID: "coverage", Label: "coverage", ObjectPaths: request.ObjectPaths, ObjectManifests: request.ObjectManifests}
	if err := intent.Validate(); err != nil {
		return empty, err
	}
	dir, err := os.MkdirTemp(p.root, "pitr-coverage-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(dir)
	metadata := filepath.Join(dir, "metadata")
	if err = privateDir(metadata); err != nil {
		return empty, err
	}
	// The source UUID must come from the same stream as the binlog coordinates.
	last := request.ObjectManifests[len(request.ObjectManifests)-1]
	if err = extractVerifiedBackup(ctx, s, request.Source, last, metadata, []string{"xtrabackup_checkpoints", "xtrabackup_binlog_info", "xtrabackup_info", "backup-my.cnf"}); err != nil {
		return empty, err
	}
	baseline, err := ParseBackupMetadata(metadata)
	if err != nil {
		return empty, err
	}
	baseline.SourceUUID, err = backupSourceUUID(metadata)
	baseline.ManifestDigest = last.Digest
	baseline.ManifestSize = last.Size
	return baseline, err
}

func (e *Runner) pitrCoverage(ctx context.Context, request myspec.PITRCoverageRequest) (myspec.PITRCoverageResponse, error) {
	intent := myspec.Restore{Source: request.Source, BackupID: "coverage", Label: "coverage", ObjectPaths: request.ObjectPaths, ObjectManifests: request.ObjectManifests}
	if err := intent.Validate(); err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	service, err := e.coverageService()
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	return service.poll(ctx, request)
}
