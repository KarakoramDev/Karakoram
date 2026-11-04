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
	e.current.Backup = &myspec.Backup{Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	fired := make(chan struct{}, 10)
	archive := func(context.Context) error { fired <- struct{}{}; return nil }
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
	e.current.Backup = &myspec.Backup{Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 86400}}
	fired := make(chan struct{}, 10)
	archive := func(context.Context) error { fired <- struct{}{}; return nil }
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
	e.configureArchiveLoop(true, func(context.Context) error { fired <- struct{}{}; return nil })
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
	e.recordArchiveResult(errors.New("binlog_decode_failed: private native diagnostics"))
	status, err := e.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" || status.LastArchivedFile != index.Status.LastArchivedFile || !status.LastEventTime.Equal(index.Status.LastEventTime) || !status.ContiguousEndTime.Equal(index.Status.ContiguousEndTime) || status.GTIDSet != index.Status.GTIDSet || status.ContiguousGTIDSet != index.Status.ContiguousGTIDSet {
		t.Fatalf("failure changed archived coverage: %+v %v", status, err)
	}
}
