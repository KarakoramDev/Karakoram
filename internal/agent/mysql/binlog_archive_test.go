// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestArchivePaths(t *testing.T) {
	r := testBackupRepository()
	got := archiveObject(r, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "binlog.000004")
	if got != "repo/mysql/cluster/binlogs/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/binlog.000004" {
		t.Fatal(got)
	}
	p := paths{root: t.TempDir()}
	a := archiveIndexPath(p, r)
	r.S3.SecretKey = "rotated"
	if archiveIndexPath(p, r) != a {
		t.Fatal("credential rotation reset archive index")
	}
	r.Path = "other/repository"
	if archiveIndexPath(p, r) == a {
		t.Fatal("repository change reused archive index")
	}
}
func TestArchiveIndexSurvivesRestart(t *testing.T) {
	p := paths{root: t.TempDir()}
	r := testBackupRepository()
	name := archiveIndexPath(p, r)
	index, err := readArchiveIndex(name)
	if err != nil {
		t.Fatal(err)
	}
	key := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/binlog.000004"
	index.Pending[key] = true
	index.Files[key] = binlogSidecar{ServerUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", File: "binlog.000004", SHA256: "digest", Size: 42, GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-4", FirstEventTime: time.Unix(100, 0).UTC(), LastEventTime: time.Unix(101, 0).UTC(), FirstCommitTimestamp: 100000001, LastCommitTimestamp: 101000001}
	archiveGap(&index, "unarchived_binlog_purged")
	if err = durableJSON(name, index); err != nil {
		t.Fatal(err)
	}
	restored, err := readArchiveIndex(name)
	if err != nil {
		t.Fatal(err)
	}
	if !restored.Pending[key] || restored.Files[key].Size != 42 || !restored.Status.Gap || restored.Status.GapReason != "unarchived_binlog_purged" {
		t.Fatalf("lost index state: %+v", restored)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("index not private")
	}
	b, err := json.Marshal(restored.Files[key])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"server_uuid", "file", "size", "sha256", "previous_gtid_set", "gtid_set", "first_event_time", "last_event_time", "first_commit_timestamp", "last_commit_timestamp", "transactions"} {
		if _, ok := fields[field]; !ok {
			t.Fatal("missing sidecar field", field)
		}
	}
}
func TestArchiveHash(t *testing.T) {
	name := filepath.Join(t.TempDir(), "binlog.000001")
	data := []byte("immutable binary log bytes")
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, n, err := hashFile(name)
	want := sha256.Sum256(data)
	if err != nil || n != int64(len(data)) || got != hex.EncodeToString(want[:]) {
		t.Fatalf("hash %s %d %v", got, n, err)
	}
}
func TestArchiveStatusLagAndGap(t *testing.T) {
	dir := t.TempDir()
	p := dataPaths(dir)
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	r := testBackupRepository()
	index, _ := readArchiveIndex(archiveIndexPath(p, r))
	index.Status.LastEventTime = time.Now().Add(-2 * time.Minute)
	archiveGap(&index, "unarchived_binlog_purged")
	if err := durableJSON(archiveIndexPath(p, r), index); err != nil {
		t.Fatal(err)
	}
	e := New()
	e.dir = dir
	e.current.Backup = &myspec.Backup{Repository: r, Binlog: myspec.Binlog{Enabled: true}}
	status, err := e.archiveStatus()
	if err != nil || !status.Gap || status.LagSeconds < 119 {
		t.Fatalf("archive status %+v: %v", status, err)
	}
}

func TestArchiveGapFreezesContiguousEnd(t *testing.T) {
	before := time.Unix(100, 0).UTC()
	index := archiveIndex{}
	index.Status.LastEventTime = before
	index.Status.GTIDSet = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-4"
	archiveGap(&index, "unarchived_binlog_purged")
	index.Status.LastEventTime = before.Add(time.Hour)
	index.Status.GTIDSet = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-4:6-8"
	archiveGap(&index, "unarchived_binlog_purged")
	if !index.Status.ContiguousEndTime.Equal(before) {
		t.Fatal("archive gap advanced contiguous coverage")
	}
	if index.Status.ContiguousGTIDSet != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-4" {
		t.Fatal("archive gap advanced contiguous GTID coverage")
	}
}

func TestArchiveLoopSurvivesConfigurationRefresh(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	e.current.Backup = &myspec.Backup{Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	fired := make(chan struct{}, 10)
	archive := func(context.Context) error { fired <- struct{}{}; return errArchiveSkipped }
	e.configureArchiveLoop(true, archive)
	defer e.resetArchiveLoop(false)
	refresh := time.NewTicker(100 * time.Millisecond)
	defer refresh.Stop()
	deadline := time.NewTimer(1800 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case <-refresh.C:
			e.configureArchiveLoop(true, archive)
		case <-fired:
			e.resetArchiveLoop(false)
			e.configureArchiveLoop(true, archive)
			select {
			case <-fired:
				return // Re-enabling after clone/restore also starts a real timer.
			case <-time.After(1800 * time.Millisecond):
				t.Fatal("archiving did not resume")
			}
		case <-deadline.C:
			t.Fatal("configuration refresh postponed the archive deadline")
		}
	}
}

func TestArchiveLoopIntervalChangeReschedules(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	e.current.Backup = &myspec.Backup{Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 86400}}
	fired := make(chan struct{}, 10)
	archive := func(context.Context) error { fired <- struct{}{}; return errArchiveSkipped }
	e.configureArchiveLoop(true, archive)
	defer e.resetArchiveLoop(false)
	e.mu.Lock()
	next := *e.current.Backup
	next.Binlog.RotateIntervalSeconds = 1
	e.current.Backup = &next
	e.mu.Unlock()
	e.configureArchiveLoop(true, archive)
	select {
	case <-fired:
	case <-time.After(1800 * time.Millisecond):
		t.Fatal("new interval did not replace the old deadline")
	}
}

func TestArchiveLoopRepositoryChangeCancelsAttempt(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	e.current.Backup = &myspec.Backup{Repository: testBackupRepository(), Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	started := make(chan struct{})
	canceled := make(chan struct{})
	e.configureArchiveLoop(true, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	})
	defer e.resetArchiveLoop(false)
	select {
	case <-started:
	case <-time.After(1800 * time.Millisecond):
		t.Fatal("initial archive attempt did not start")
	}
	e.mu.Lock()
	next := *e.current.Backup
	next.Repository.Path = "replacement/repository"
	e.current.Backup = &next
	e.mu.Unlock()
	fired := make(chan struct{}, 10)
	e.configureArchiveLoop(true, func(context.Context) error { fired <- struct{}{}; return errArchiveSkipped })
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("repository replacement did not cancel the in-flight attempt")
	}
	select {
	case <-fired:
	case <-time.After(1800 * time.Millisecond):
		t.Fatal("replacement repository archive timer did not start")
	}
}

func TestArchiveStatusDisabledAndEmpty(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	if _, err := e.archiveStatus(); err == nil {
		t.Fatal("missing backup configuration reported a successful observation")
	}
	e.current.Backup = &myspec.Backup{Repository: testBackupRepository()}
	if _, err := e.archiveStatus(); err == nil {
		t.Fatal("disabled archiving reported a successful observation")
	}
	e.current.Backup.Binlog.Enabled = true
	status, err := e.archiveStatus()
	if err != nil || status.LastArchivedFile != "" || !status.LastEventTime.IsZero() {
		t.Fatalf("empty archive must remain distinguishable from healthy coverage: %+v %v", status, err)
	}
}

func TestArchiveLoopReportsFailureAndRecovery(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	e.current.Backup = &myspec.Backup{Repository: testBackupRepository(), Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	var attempts atomic.Int32
	e.configureArchiveLoop(true, func(context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("binlog_decode_failed: private native diagnostics")
		}
		return nil
	})
	defer e.resetArchiveLoop(false)
	wait := func(want string) {
		t.Helper()
		deadline := time.After(1800 * time.Millisecond)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			status, err := e.archiveStatus()
			if err != nil {
				t.Fatal(err)
			}
			if status.Error == want {
				return
			}
			select {
			case <-tick.C:
			case <-deadline:
				t.Fatalf("archive error = %q, want %q", status.Error, want)
			}
		}
	}
	wait("binlog_decode_failed")
	wait("")
}

func TestArchiveFailureDoesNotAdvanceCoverage(t *testing.T) {
	dir := t.TempDir()
	p := dataPaths(dir)
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	repo := testBackupRepository()
	index, _ := readArchiveIndex(archiveIndexPath(p, repo))
	index.Status.LastArchivedFile = "binlog.000001"
	index.Status.LastEventTime = time.Unix(100, 0).UTC()
	index.Status.ContiguousEndTime = index.Status.LastEventTime
	index.Status.GTIDSet = recoverySID + ":1-4"
	index.Status.ContiguousGTIDSet = index.Status.GTIDSet
	if err := durableJSON(archiveIndexPath(p, repo), index); err != nil {
		t.Fatal(err)
	}
	e := New()
	e.dir = dir
	e.current.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true}}
	e.recordArchiveResult(p, repo, errors.New("binlog_decode_failed: private native diagnostics"))
	status, err := e.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" || status.LastArchivedFile != index.Status.LastArchivedFile || !status.LastEventTime.Equal(index.Status.LastEventTime) || !status.ContiguousEndTime.Equal(index.Status.ContiguousEndTime) || status.GTIDSet != index.Status.GTIDSet || status.ContiguousGTIDSet != index.Status.ContiguousGTIDSet {
		t.Fatalf("failure changed archived coverage: %+v %v", status, err)
	}
	restarted := New()
	restarted.dir = dir
	restarted.current = e.current
	status, err = restarted.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" || status.LastArchivedFile != index.Status.LastArchivedFile || !status.LastEventTime.Equal(index.Status.LastEventTime) || !status.ContiguousEndTime.Equal(index.Status.ContiguousEndTime) || status.GTIDSet != index.Status.GTIDSet || status.ContiguousGTIDSet != index.Status.ContiguousGTIDSet {
		t.Fatalf("restart forgot failure or changed archived coverage: %+v %v", status, err)
	}
	restarted.recordArchiveResult(p, repo, nil)
	healthy := New()
	healthy.dir = dir
	healthy.current = e.current
	status, err = healthy.archiveStatus()
	if err != nil || status.Error != "" || !status.LastEventTime.Equal(index.Status.LastEventTime) || status.GTIDSet != index.Status.GTIDSet {
		t.Fatalf("successful pass did not durably clear attempt failure: %+v %v", status, err)
	}
}

func TestArchiveAttemptResultIsRepositoryScoped(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	original := testBackupRepository()
	replacement := original
	replacement.Path += "/replacement"
	e.current.Backup = &myspec.Backup{Repository: replacement, Binlog: myspec.Binlog{Enabled: true}}
	_, p := e.state()
	// An old attempt must not write its result into a replacement repository.
	e.recordArchiveResult(p, original, errors.New("binlog_decode_failed: private native diagnostics"))
	status, err := e.archiveStatus()
	if err != nil || status.Error != "" {
		t.Fatalf("replacement inherited old attempt: %+v %v", status, err)
	}
	restarted := New()
	restarted.dir = e.dir
	restarted.current.Backup = &myspec.Backup{Repository: original, Binlog: myspec.Binlog{Enabled: true}}
	status, err = restarted.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" {
		t.Fatalf("original lost its failed attempt: %+v %v", status, err)
	}
	name := archiveIndexPath(p, original) + ".attempt"
	raw, err := os.ReadFile(name)
	if err != nil || string(raw) != `"binlog_decode_failed"` {
		t.Fatalf("attempt did not persist only its safe category: %q %v", raw, err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("attempt result is not private", err)
	}
}

func TestArchiveCancelledAttemptRetainsFailure(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	repo := testBackupRepository()
	e.current.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	_, p := e.state()
	e.recordArchiveResult(p, repo, errors.New("binlog_decode_failed"))
	started, finished := make(chan struct{}), make(chan struct{})
	e.configureArchiveLoop(true, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil
	})
	defer e.resetArchiveLoop(false)
	select {
	case <-started:
	case <-time.After(1800 * time.Millisecond):
		t.Fatal("archive attempt did not start")
	}
	e.resetArchiveLoop(false)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("archive attempt did not cancel")
	}
	// Cancellation must not clear the durable failure, even when the callback
	// returns nil rather than context.Canceled.
	restarted := New()
	restarted.dir = e.dir
	restarted.current = e.current
	status, err := restarted.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" {
		t.Fatalf("cancellation cleared failure: %+v %v", status, err)
	}
}

func TestArchiveAttemptPersistenceFailureIsNotHealthy(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	repo := testBackupRepository()
	e.current.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true}}
	_, p := e.state()
	e.recordArchiveResult(p, repo, errors.New("binlog_decode_failed"))
	name := archiveIndexPath(p, repo) + ".attempt"
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	e.recordArchiveResult(p, repo, nil)
	status, err := e.archiveStatus()
	if err != nil || status.Error != "archive_attempt_write_failed" {
		t.Fatalf("failed persistence appeared healthy: %+v %v", status, err)
	}
	restarted := New()
	restarted.dir = e.dir
	restarted.current = e.current
	status, err = restarted.archiveStatus()
	if err != nil || status.Error != "archive_attempt_unavailable" {
		t.Fatalf("unreadable durable result appeared healthy: %+v %v", status, err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	e.recordArchiveResult(p, repo, nil)
	status, err = e.archiveStatus()
	if err != nil || status.Error != "" {
		t.Fatalf("repaired persistence did not clear failure: %+v %v", status, err)
	}
}

func TestArchiveRejectsUnboundStateDirectory(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, dir := range []string{"", "relative"} {
		e := New()
		e.dir = dir
		e.current.Role = myspec.RoleData
		e.current.Backup = &myspec.Backup{Repository: testBackupRepository(), Binlog: myspec.Binlog{Enabled: true}}
		_, p := e.state()
		e.recordArchiveResult(p, e.current.Backup.Repository, errors.New("binlog_decode_failed"))
		status, err := e.archiveStatus()
		if err != nil || status.Error != "archive_state_directory_invalid" {
			t.Fatalf("unbound state directory appeared healthy: %+v %v", status, err)
		}
		if err := e.archiveBinlogs(t.Context()); !errors.Is(err, errArchiveStateDir) {
			t.Fatalf("unbound state directory reached native SQL: %v", err)
		}
	}
	entries, err := os.ReadDir(cwd)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unbound state directory wrote relative artifacts: count=%d error=%v", len(entries), err)
	}
}
