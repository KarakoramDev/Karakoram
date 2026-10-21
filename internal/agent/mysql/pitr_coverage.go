// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

func readReplaySidecars(ctx context.Context, c *minio.Client, source myspec.Repository) ([]archivedBinlog, error) {
	var logs []archivedBinlog
	for obj := range c.ListObjects(ctx, source.S3.Bucket, minio.ListObjectsOptions{Prefix: path.Join(source.Path, "binlogs") + "/", Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		o, err := c.GetObject(ctx, source.S3.Bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		var metadata binlogSidecar
		err = json.NewDecoder(io.LimitReader(o, 32<<20)).Decode(&metadata)
		o.Close()
		if err != nil {
			return nil, err
		}
		key := strings.TrimSuffix(obj.Key, ".json")
		if key != path.Join(source.Path, "binlogs", metadata.ServerUUID, metadata.File) {
			return nil, errors.New("invalid_sidecar")
		}
		logs = append(logs, archivedBinlog{key, metadata})
	}
	return logs, nil
}

func replayCoverageWindows(logs []archivedBinlog, backup myspec.BackupResult) ([]myspec.PITRWindow, error) {
	trusted, last, err := trustedReplayHistory(logs, backup)
	if err != nil {
		return nil, err
	}
	var timestamps []int64
	for _, l := range trusted {
		for _, tx := range l.meta.Transactions {
			timestamps = append(timestamps, tx.OriginalCommitTimestamp)
		}
	}
	// A time cut is valid iff every included predecessor is <= the cut and
	// every omitted successor is > it. Original commit times may regress.
	suffixMin := make([]int64, len(timestamps)+1)
	suffixMin[len(timestamps)] = math.MaxInt64
	for i := len(timestamps) - 1; i >= 0; i-- {
		suffixMin[i] = min(timestamps[i], suffixMin[i+1])
	}
	lower := backup.EndTime.UnixMicro()
	var windows []myspec.PITRWindow
	for i := 0; i <= len(timestamps); i++ {
		upper := last
		if suffixMin[i] != math.MaxInt64 {
			upper = min(upper, suffixMin[i]-1)
		}
		if lower <= upper {
			w := myspec.PITRWindow{Start: time.UnixMicro(lower).UTC(), End: time.UnixMicro(upper).UTC()}
			if len(windows) > 0 && !w.Start.After(windows[len(windows)-1].End.Add(time.Microsecond)) {
				windows[len(windows)-1].End = w.End
			} else {
				windows = append(windows, w)
			}
		}
		if i < len(timestamps) {
			lower = max(lower, timestamps[i])
		}
	}
	return windows, nil
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
	baseline, err := parseBackupMetadata(metadata)
	if err != nil {
		return empty, err
	}
	baseline.SourceUUID, err = backupSourceUUID(metadata)
	baseline.ManifestDigest = last.Digest
	baseline.ManifestSize = last.Size
	return baseline, err
}

func (e *Runner) pitrCoverage(ctx context.Context, request myspec.PITRCoverageRequest) (myspec.PITRCoverageResponse, error) {
	s, p := e.state()
	baseline, err := loadPITRBaseline(ctx, s, p, request)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	client, err := s3Client(request.Source)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	logs, err := readReplaySidecars(ctx, client, request.Source)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	windows, err := replayCoverageWindows(logs, baseline)
	return myspec.PITRCoverageResponse{Windows: windows}, err
}
