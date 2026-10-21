// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"os"
	"path/filepath"
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
	e.current.Backup = &myspec.Backup{Repository: r}
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
