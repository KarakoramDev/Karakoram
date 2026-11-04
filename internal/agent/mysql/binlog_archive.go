// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	DecodePosition       uint64              `json:"-"`
}
type archiveIndex struct {
	Files   map[string]binlogSidecar   `json:"files,omitempty"`
	Pending map[string]bool            `json:"pending,omitempty"`
	Status  myspec.BinlogArchiveStatus `json:"status"`
	Version int                        `json:"version,omitempty"`
	Active  *binlogResume              `json:"active,omitempty"`
	// The summary proves this upload completed even if receipt publication fails.
	LastCompletedKey string `json:"last_completed_key,omitempty"`
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

// Completed history lives in immutable per-file receipts. Version 2 stores the
// pending native inventory separately, so per-file summary commits do not
// rewrite a large backlog. Migration leaves the old index authoritative until
// all receipts and the pending inventory are durable.
func archiveReceiptPath(indexPath, key string) string {
	return indexPath + ".file-" + requestDigest(key)
}

func writeArchivePending(name string, pending map[string]bool) error {
	return durableJSON(name+".pending", pending)
}

func writeArchiveSummary(name string, idx archiveIndex) error {
	idx.Pending = nil
	return durableJSON(name, idx)
}

func loadArchiveState(name string) (archiveIndex, error) {
	idx, err := readArchiveIndex(name)
	if err != nil {
		return idx, err
	}
	if idx.Version == 2 {
		raw, err := os.ReadFile(name + ".pending")
		if err != nil {
			return idx, err
		}
		var pending map[string]bool
		if err = json.Unmarshal(raw, &pending); err != nil {
			return idx, err
		}
		if pending == nil {
			return idx, errors.New("archive_pending_invalid")
		}
		idx.Pending = pending
		idx.Files = nil
		return idx, nil
	}
	if idx.Version != 0 && idx.Version != 1 {
		return idx, errors.New("archive_index_version")
	}
	// A status observation before initialization has no index (or even root
	// directory) yet. It is empty, not corrupt, and must remain read-only.
	if _, err := os.Stat(name); os.IsNotExist(err) {
		idx.Version = 2
		idx.Files = nil
		return idx, nil
	} else if err != nil {
		return idx, err
	}
	for key, meta := range idx.Files {
		if err = durableJSON(archiveReceiptPath(name, key), meta); err != nil {
			return idx, err
		}
	}
	idx.Files = nil
	idx.Version = 2
	if err = writeArchivePending(name, idx.Pending); err != nil {
		return idx, err
	}
	return idx, writeArchiveSummary(name, idx)
}

func archiveRecorded(name, key string) (bool, error) {
	_, err := os.Stat(archiveReceiptPath(name, key))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func archiveCompleted(name, key, lastCompleted string) (bool, error) {
	if key == lastCompleted {
		return true, nil
	}
	recorded, err := archiveRecorded(name, key)
	if err != nil || recorded {
		return recorded, err
	}
	_, err = os.Stat(archiveCompletionPath(name, key))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func archiveCompletionPath(name, key string) string {
	return name + ".complete-" + requestDigest(key)
}

// Keep the committed identity independently before the next summary replaces
// LastCompletedKey. This is an explicit completion proof, not a fabricated
// sidecar receipt; it remains valid even if remote retention removed the object.
func preserveArchiveCompletion(name, key string) error {
	if key == "" {
		return nil
	}
	completed, err := archiveCompleted(name, key, "")
	if err != nil || completed {
		return err
	}
	return durableJSON(archiveCompletionPath(name, key), key)
}
func (e *Runner) archiveStatus() (myspec.BinlogArchiveStatus, error) {
	s, p := e.state()
	if s.Backup == nil || !s.Backup.Binlog.Enabled || s.Galera != nil && s.Galera.RestoreFence {
		return myspec.BinlogArchiveStatus{}, errors.New("binlog_archiving_disabled")
	}
	archiveMu.Lock()
	defer archiveMu.Unlock()
	idx, err := loadArchiveState(archiveIndexPath(p, s.Backup.Repository))
	if err != nil {
		return myspec.BinlogArchiveStatus{Error: "archive_index_invalid"}, nil
	}
	e.mu.RLock()
	idx.Status.Error = e.archiveError
	e.mu.RUnlock()
	if !idx.Status.LastEventTime.IsZero() {
		idx.Status.LagSeconds = time.Since(idx.Status.LastEventTime).Seconds()
		if idx.Status.LagSeconds < 0 {
			idx.Status.LagSeconds = 0
		}
	}
	return idx.Status, nil
}

func archiveConfigChanged(previous, current *myspec.Backup) bool {
	return previous == nil || current == nil ||
		previous.Repository != current.Repository ||
		previous.Binlog.RotateIntervalSeconds != current.Binlog.RotateIntervalSeconds
}

// resetArchiveLoop preserves deadlines across unchanged configuration pulls.
// Interval/repository changes cancel the old wait or attempt and start anew.
// Each iteration rechecks the live role and intentional-stop fence before SQL.
func (e *Runner) resetArchiveLoop(enabled bool) {
	if e.AccessRestoreFenced() {
		enabled = false
	}
	e.configureArchiveLoop(enabled, e.archiveBinlogs)
}

func (e *Runner) configureArchiveLoop(enabled bool, archive func(context.Context) error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if enabled && e.archiveCancel != nil && !archiveConfigChanged(&e.archiveBackup, e.current.Backup) {
		return
	}
	if e.archiveCancel != nil {
		e.archiveCancel()
		e.archiveCancel = nil
	}
	if !enabled {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.archiveCancel = cancel
	e.archiveBackup = *e.current.Backup
	go func() {
		for {
			s, _ := e.state()
			if s.Backup == nil || !s.Backup.Binlog.Enabled || s.Galera != nil && s.Galera.RestoreFence {
				return
			}
			interval := time.Duration(s.Backup.Binlog.RotateIntervalSeconds) * time.Second
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
			run, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := archive(run)
			cancel()
			if ctx.Err() != nil {
				return
			}
			e.recordArchiveResult(err)
		}
	}()
}

// Keep failures separate from durable upload receipts: an unsuccessful attempt
// never advances the archived position, and the next successful pass clears it.
func (e *Runner) recordArchiveResult(err error) {
	message := ""
	if err != nil {
		// Native diagnostics can contain SQL or local paths. Publish only the
		// stable error category; full diagnostics belong in the agent log.
		message, _, _ = strings.Cut(err.Error(), ":")
	}
	e.mu.Lock()
	e.archiveError = message
	node := e.node
	e.mu.Unlock()
	if err != nil && node != nil {
		node.Logger().Warn("MySQL binlog archive failed", "error", err)
	}
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
	return decodeBinlogFrom(ctx, s, name, 4, nil)
}

type boundedDiagnostics struct{ data []byte }

func (b *boundedDiagnostics) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := 8192 - len(b.data); remaining > 0 {
		b.data = append(b.data, p[:min(remaining, len(p))]...)
	}
	return n, nil
}

func decodeBinlogFrom(ctx context.Context, s myspec.Spec, name string, position uint64, emit func(binlogTransaction) error) (binlogSidecar, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqlbinlog"),
		"--base64-output=DECODE-ROWS", "--verbose", "--start-position="+strconv.FormatUint(position, 10), name)
	cmd.Env = append(nativeEnv(), "TZ=UTC")
	var diagnostics boundedDiagnostics
	cmd.Stderr = &diagnostics
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return binlogSidecar{}, fmt.Errorf("binlog_decode_failed: %w", err)
	}
	if err = cmd.Start(); err != nil {
		return binlogSidecar{}, fmt.Errorf("binlog_decode_failed: %w", err)
	}
	out, parseErr := parseBinlogStream(stdout, emit)
	if parseErr != nil {
		cancel()
		_ = stdout.Close()
	}
	waitErr := cmd.Wait()
	if err := errors.Join(parseErr, waitErr); err != nil {
		return binlogSidecar{}, fmt.Errorf("binlog_decode_failed: %w; stderr: %s", err, diagnostics.data)
	}
	return out, nil
}

type binlogResume struct {
	Key        string `json:"key"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	Prefix     string `json:"prefix"`
	PrefixSize int    `json:"prefix_size"`
	Position   uint64 `json:"position"`
}

func resumeBinlog(name, key string, previous *binlogResume) (binlogResume, error) {
	f, err := os.Open(name)
	if err != nil {
		return binlogResume{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return binlogResume{}, err
	}
	st := info.Sys().(*syscall.Stat_t)
	const prefixSize = 128
	n := int(min(info.Size(), prefixSize))
	var prefixBuffer [prefixSize]byte
	prefix := prefixBuffer[:n]
	if _, err = io.ReadFull(f, prefix); err != nil {
		return binlogResume{}, err
	}
	sum := sha256.Sum256(prefix)
	next := binlogResume{Key: key, Device: uint64(st.Dev), Inode: st.Ino,
		Prefix: hex.EncodeToString(sum[:]), PrefixSize: n, Position: 4}
	if previous != nil && n == prefixSize && previous.PrefixSize == n &&
		previous.Key == key && previous.Device == next.Device &&
		previous.Inode == next.Inode && previous.Prefix == next.Prefix &&
		previous.Position >= 4 && previous.Position <= uint64(info.Size()) {
		next.Position = previous.Position
	}
	return next, nil
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
	if s.Backup == nil || !s.Backup.Binlog.Enabled || s.Role != myspec.RoleData || s.Galera != nil && s.Galera.RestoreFence {
		return nil
	}
	st, err := e.status(ctx)
	if err != nil || !st.WriterEligible(s.Galera != nil) {
		return nil
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return errors.New("member_unavailable")
	}
	return archiveNativeBinlogs(ctx, s, p, db, e.status)
}

// archiveNativeBinlogs performs one serialized pass after local writer admission.
// Recheck writer eligibility immediately before rotating the native log.
func archiveNativeBinlogs(ctx context.Context, s myspec.Spec, p paths, db *sql.DB, status func(context.Context) (myspec.StatusResponse, error)) error {
	var uuid, base string
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid,@@log_bin_basename").Scan(&uuid, &base); err != nil {
		return errors.New("binlog_query_failed")
	}
	repo := s.Backup.Repository
	indexPath := archiveIndexPath(p, repo)
	idx, err := loadArchiveState(indexPath)
	if err != nil {
		return errors.New("archive_index_invalid")
	}
	if err = preserveArchiveCompletion(indexPath, idx.LastCompletedKey); err != nil {
		return errors.New("archive_index_write")
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
	present := map[string]bool{}
	for _, f := range logs {
		key := uuid + "/" + f.name
		present[key] = true
		recorded, err := archiveCompleted(indexPath, key, idx.LastCompletedKey)
		if err != nil {
			return fmt.Errorf("archive_index_invalid: %w", err)
		}
		if !recorded {
			idx.Pending[key] = true
		} else {
			delete(idx.Pending, key)
		}
	}
	for key := range idx.Pending {
		if present[key] {
			continue
		}
		recorded, err := archiveCompleted(indexPath, key, idx.LastCompletedKey)
		if err != nil {
			return fmt.Errorf("archive_index_invalid: %w", err)
		}
		if recorded {
			// A receipt or the durable last-completion identity proves the
			// upload preceded any subsequent native expiry.
			delete(idx.Pending, key)
			continue
		}
		if strings.HasPrefix(key, uuid+"/") {
			archiveGap(&idx, "unarchived_binlog_purged")
		}
	}
	if err = writeArchivePending(indexPath, idx.Pending); err != nil {
		return errors.New("archive_index_write")
	}
	// Persist the native inventory before decoding can fail. A later purge
	// must remain distinguishable from a successfully archived file.
	if err = writeArchiveSummary(indexPath, idx); err != nil {
		return errors.New("archive_index_write")
	}
	// Only a file containing a committed transaction needs bounded rotation.
	current := logs[len(logs)-1]
	localCurrent := filepath.Join(filepath.Dir(base), current.name)
	resume, err := resumeBinlog(localCurrent, uuid+"/"+current.name, idx.Active)
	if err != nil {
		return fmt.Errorf("binlog_read_failed: %w", err)
	}
	hasTransactions := false
	decoded, err := decodeBinlogFrom(ctx, s, localCurrent, resume.Position, func(binlogTransaction) error {
		hasTransactions = true
		return nil
	})
	if err != nil {
		return err
	}
	if decoded.DecodePosition >= resume.Position {
		resume.Position = decoded.DecodePosition
	}
	idx.Active = &resume
	if hasTransactions {
		latest, err := status(ctx)
		if err != nil || !latest.WriterEligible(s.Galera != nil) {
			return nil
		}
		if _, err = db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
			return errors.New("binlog_rotate_failed")
		}
		logs, err = list()
		if err != nil || len(logs) == 0 {
			return errors.New("binlog_query_failed")
		}
		idx.Pending[uuid+"/"+logs[len(logs)-1].name] = true
		if err = writeArchivePending(indexPath, idx.Pending); err != nil {
			return errors.New("archive_index_write")
		}
	}
	if err = writeArchiveSummary(indexPath, idx); err != nil {
		return errors.New("archive_index_write")
	}
	client, closeClient, err := s3Client(repo)
	if err != nil {
		return errors.New("repository_invalid")
	}
	defer closeClient()
	completed := false
	for _, f := range logs[:len(logs)-1] {
		key := uuid + "/" + f.name
		local := filepath.Join(filepath.Dir(base), f.name)
		recorded, err := archiveCompleted(indexPath, key, idx.LastCompletedKey)
		if err != nil {
			return fmt.Errorf("archive_index_invalid: %w", err)
		}
		if recorded {
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
		idx.LastCompletedKey = key
		if err = writeArchiveSummary(indexPath, idx); err != nil {
			return errors.New("archive_index_write")
		}
		// The completion identity and GTID/time summary commit together before
		// receipt publication, so a missing receipt cannot create a purge gap.
		if err = durableJSON(archiveReceiptPath(indexPath, key), sidecar); err != nil {
			return errors.New("archive_index_write")
		}
		completed = true
	}
	if completed {
		if err = writeArchivePending(indexPath, idx.Pending); err != nil {
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
	if _, err = c.PutObject(ctx, r.S3.Bucket, object+".json", bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return errors.New("archive_upload_failed")
	}
	return nil
}
