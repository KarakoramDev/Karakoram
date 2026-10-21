// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

type binlogSidecar struct {
	ServerUUID           string              `json:"server_uuid"`
	File                 string              `json:"file"`
	Size                 int64               `json:"size"`
	SHA256               string              `json:"sha256"`
	PreviousGTIDSet      string              `json:"previous_gtid_set"`
	GTIDSet              string              `json:"gtid_set"`
	FirstEventTime       time.Time           `json:"first_event_time"`
	LastEventTime        time.Time           `json:"last_event_time"`
	FirstCommitTimestamp int64               `json:"first_commit_timestamp"`
	LastCommitTimestamp  int64               `json:"last_commit_timestamp"`
	Transactions         []binlogTransaction `json:"transactions"`
}
type archiveIndex struct {
	Files   map[string]binlogSidecar   `json:"files"`
	Pending map[string]bool            `json:"pending"`
	Status  myspec.BinlogArchiveStatus `json:"status"`
}

var archiveMu sync.Mutex
var binlogName = regexp.MustCompile(`^[a-zA-Z0-9_-]+\.[0-9]+$`)

func archiveObject(repo myspec.Repository, uuid, file string) string {
	return path.Join(repo.Path, "binlogs", uuid, file)
}
func archiveIndexPath(p paths, r myspec.Repository) string {
	h := requestDigest([]any{r.S3.Endpoint, r.S3.Port, r.S3.Bucket, r.Path})
	return filepath.Join(p.root, "archive-index-"+h+".json")
}
func readArchiveIndex(name string) (archiveIndex, error) {
	out := archiveIndex{Files: map[string]binlogSidecar{}, Pending: map[string]bool{}}
	b, err := os.ReadFile(name)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	if out.Files == nil {
		out.Files = map[string]binlogSidecar{}
	}
	if out.Pending == nil {
		out.Pending = map[string]bool{}
	}
	return out, err
}
func (e *Runner) archiveStatus() (myspec.BinlogArchiveStatus, error) {
	s, p := e.state()
	if s.Backup == nil {
		return myspec.BinlogArchiveStatus{}, nil
	}
	archiveMu.Lock()
	defer archiveMu.Unlock()
	idx, err := readArchiveIndex(archiveIndexPath(p, s.Backup.Repository))
	if err != nil {
		return idx.Status, errors.New("archive_index_invalid")
	}
	if !idx.Status.LastEventTime.IsZero() {
		idx.Status.LagSeconds = time.Since(idx.Status.LastEventTime).Seconds()
		if idx.Status.LagSeconds < 0 {
			idx.Status.LagSeconds = 0
		}
	}
	return idx.Status, nil
}

// resetArchiveLoop cancels the previous configuration's loop. Each iteration
// rechecks the live role and the intentional-stop fence before issuing SQL.
func (e *Runner) resetArchiveLoop(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.archiveCancel != nil {
		e.archiveCancel()
		e.archiveCancel = nil
	}
	if !enabled {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.archiveCancel = cancel
	go func() {
		for {
			s, _ := e.state()
			if s.Backup == nil || !s.Backup.Binlog.Enabled {
				return
			}
			interval := time.Duration(s.Backup.Binlog.RotateIntervalSeconds) * time.Second
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
			run, cancel := context.WithTimeout(ctx, 5*time.Minute)
			_ = e.archiveBinlogs(run)
			cancel()
		}
	}()
}
func hashFile(name string) (string, int64, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
func decodeBinlog(ctx context.Context, s myspec.Spec, name string) (binlogSidecar, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqlbinlog"), "--base64-output=DECODE-ROWS", "--verbose", name)
	cmd.Env = append(nativeEnv(), "TZ=UTC")
	b, err := cmd.Output()
	if err != nil {
		return binlogSidecar{}, errors.New("binlog_decode_failed")
	}
	return parseBinlogOutput(b)
}
func archiveGap(idx *archiveIndex, reason string) {
	if !idx.Status.Gap {
		idx.Status.ContiguousEndTime = idx.Status.LastEventTime
		idx.Status.ContiguousGTIDSet = idx.Status.GTIDSet
	}
	idx.Status.Gap = true
	idx.Status.GapReason = reason
}
func (e *Runner) archiveBinlogs(ctx context.Context) error {
	archiveMu.Lock()
	defer archiveMu.Unlock()
	s, p := e.state()
	if s.Backup == nil || !s.Backup.Binlog.Enabled || s.Role != myspec.RoleData {
		return nil
	}
	st, err := e.status(ctx)
	if err != nil || st.MemberState != "ONLINE" || st.MemberRole != "PRIMARY" {
		return nil
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return errors.New("member_unavailable")
	}
	var uuid, base string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid,@@log_bin_basename").Scan(&uuid, &base); err != nil {
		return errors.New("binlog_query_failed")
	}
	repo := s.Backup.Repository
	indexPath := archiveIndexPath(p, repo)
	idx, err := readArchiveIndex(indexPath)
	if err != nil {
		return errors.New("archive_index_invalid")
	}
	type logFile struct {
		name string
		size int64
	}
	list := func() ([]logFile, error) {
		rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		var out []logFile
		for rows.Next() {
			var item logFile
			var encrypted string
			args := []any{&item.name, &item.size}
			if len(columns) == 3 {
				args = append(args, &encrypted)
			}
			if err = rows.Scan(args...); err != nil {
				return nil, err
			}
			if !binlogName.MatchString(item.name) {
				return nil, errors.New("binlog_name_invalid")
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	logs, err := list()
	if err != nil || len(logs) == 0 {
		return errors.New("binlog_query_failed")
	}
	// Only a file containing a committed transaction needs bounded rotation.
	current := logs[len(logs)-1]
	decoded, err := decodeBinlog(ctx, s, filepath.Join(filepath.Dir(base), current.name))
	if err != nil {
		return err
	}
	if len(decoded.Transactions) > 0 {
		latest, err := e.status(ctx)
		if err != nil || latest.MemberRole != "PRIMARY" || latest.MemberState != "ONLINE" {
			return nil
		}
		if _, err = db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
			return errors.New("binlog_rotate_failed")
		}
		logs, err = list()
		if err != nil || len(logs) == 0 {
			return errors.New("binlog_query_failed")
		}
	}
	present := map[string]bool{}
	for _, f := range logs {
		key := uuid + "/" + f.name
		present[key] = true
		if _, ok := idx.Files[key]; !ok {
			idx.Pending[key] = true
		}
	}
	for key := range idx.Pending {
		if strings.HasPrefix(key, uuid+"/") && !present[key] {
			archiveGap(&idx, "unarchived_binlog_purged")
		}
	}
	if err = durableJSON(indexPath, idx); err != nil {
		return errors.New("archive_index_write")
	}
	client, err := s3Client(repo)
	if err != nil {
		return errors.New("repository_invalid")
	}
	for _, f := range logs[:len(logs)-1] {
		key := uuid + "/" + f.name
		local := filepath.Join(filepath.Dir(base), f.name)
		if _, ok := idx.Files[key]; ok {
			continue
		}
		sidecar, err := decodeBinlog(ctx, s, local)
		if err != nil {
			return err
		}
		sidecar.ServerUUID = uuid
		sidecar.File = f.name
		sidecar.SHA256, sidecar.Size, err = hashFile(local)
		if err != nil {
			return errors.New("binlog_read_failed")
		}
		if sidecar.Size != f.size {
			return errors.New("binlog_size_changed")
		}
		object := archiveObject(repo, uuid, f.name)
		if err = uploadArchive(ctx, client, repo, object, local, sidecar); err != nil {
			return err
		}
		idx.Files[key] = sidecar
		delete(idx.Pending, key)
		idx.Status.LastArchivedFile = f.name
		if sidecar.LastEventTime.After(idx.Status.LastEventTime) {
			idx.Status.LastEventTime = sidecar.LastEventTime
		}
		if !idx.Status.Gap {
			idx.Status.ContiguousEndTime = idx.Status.LastEventTime
		}
		archived, parseErr := parseRecoveryGTIDs(idx.Status.GTIDSet)
		if parseErr != nil {
			return errors.New("archive_index_invalid")
		}
		added, parseErr := parseRecoveryGTIDs(sidecar.GTIDSet)
		if parseErr != nil {
			return errors.New("archive_metadata_invalid")
		}
		archived.add(added)
		idx.Status.GTIDSet = archived.String()
		if !idx.Status.Gap {
			idx.Status.ContiguousGTIDSet = idx.Status.GTIDSet
		}
		if err = durableJSON(indexPath, idx); err != nil {
			return errors.New("archive_index_write")
		}
	}
	return nil
}
func uploadArchive(ctx context.Context, c *minio.Client, r myspec.Repository, object, local string, sidecar binlogSidecar) error {
	// A completed object has a durable SHA256 user metadata value. A retry can
	// reuse it, but never overwrite a conflicting immutable archive identity.
	existing, err := c.StatObject(ctx, r.S3.Bucket, object, minio.StatObjectOptions{})
	if err == nil {
		if existing.UserMetadata["Sha256"] != sidecar.SHA256 && existing.UserMetadata["sha256"] != sidecar.SHA256 {
			return errors.New("archive_checksum_conflict")
		}
	} else {
		code := minio.ToErrorResponse(err).Code
		if code != "NoSuchKey" && code != "NotFound" && code != "NoSuchObject" {
			return errors.New("archive_upload_failed")
		}
		if _, err = c.FPutObject(ctx, r.S3.Bucket, object, local, minio.PutObjectOptions{ContentType: "application/octet-stream", UserMetadata: map[string]string{"sha256": sidecar.SHA256}}); err != nil {
			return errors.New("archive_upload_failed")
		}
	}
	b, err := json.Marshal(sidecar)
	if err != nil {
		return errors.New("archive_metadata_invalid")
	}
	if _, err = c.PutObject(ctx, r.S3.Bucket, object+".json", strings.NewReader(string(b)), int64(len(b)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return errors.New("archive_upload_failed")
	}
	return nil
}
