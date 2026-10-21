// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func metadataFixture(t *testing.T, checkpoints string) string {
	t.Helper()
	dir := t.TempDir()
	for name, text := range map[string]string{
		"xtrabackup_checkpoints": checkpoints,
		"xtrabackup_binlog_info": "binlog.000003\t198\taaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10\n",
		"xtrabackup_info":        "tool_version = 8.4.0-7\nserver_version = 8.4.11-11\nend_time = 2026-10-21 09:07:58\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
func TestBackupMetadata(t *testing.T) {
	for _, test := range []struct {
		name, checkpoints string
		from, to          uint64
		invalid           bool
	}{
		{"full", "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 19418282\n", 0, 19418282, false},
		{"incremental", "backup_type = incremental\nfrom_lsn = 19418282\nto_lsn = 19418299\n", 19418282, 19418299, false},
		{"reversed", "backup_type = incremental\nfrom_lsn = 100\nto_lsn = 99\n", 0, 0, true},
		{"bad_lsn", "backup_type = full-backuped\nfrom_lsn = x\nto_lsn = 99\n", 0, 0, true},
		{"unknown_type", "backup_type = prepared\nfrom_lsn = 0\nto_lsn = 99\n", 0, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseBackupMetadata(metadataFixture(t, test.checkpoints))
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid checkpoint")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.FromLSN != test.from || result.ToLSN != test.to || result.ToolVersion != "8.4.0-7" || result.ServerVersion != "8.4.11-11" || result.GTIDSet != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10" || !result.EndTime.Equal(time.Date(2026, 10, 21, 9, 7, 58, 0, time.UTC)) {
				t.Fatalf("unexpected metadata: %+v", result)
			}
			if result.BinlogFile != "binlog.000003" || result.BinlogPosition != 198 {
				t.Fatalf("missing exact snapshot coordinates: %+v", result)
			}
		})
	}
}
func TestBackupReceiptReplay(t *testing.T) {
	p := paths{root: t.TempDir()}
	r := myspec.BackupRequest{Receipt: "operation/a", Type: "full", ObjectPrefix: "repo/base/full", Repository: testBackupRepository()}
	name, err := receiptPath(p, "backup", r.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	e := New()
	want := myspec.BackupResponse{Receipt: r.Receipt, State: "done", Result: &myspec.BackupResult{ToLSN: 42}}
	if err = durableJSON(name, backupReceipt{RequestHash: requestDigest(r), Response: want}); err != nil {
		t.Fatal(err)
	}
	got, err := e.backup(context.Background(), myspec.Spec{}, p, r)
	if err != nil || got.State != "done" || got.Result.ToLSN != 42 {
		t.Fatalf("replay: %+v %v", got, err)
	}
	r.BaseLSN = 42
	r.Type = "incremental"
	if _, err = e.backup(context.Background(), myspec.Spec{}, p, r); err == nil {
		t.Fatal("receipt accepted different request")
	}
	r.BaseLSN = 0
	r.Type = "full"
	want.State = "running"
	want.Result = nil
	if err = durableJSON(name, backupReceipt{RequestHash: requestDigest(r), Response: want}); err != nil {
		t.Fatal(err)
	}
	activeBackups.Store(name, true)
	got, err = e.backup(context.Background(), myspec.Spec{}, p, r)
	activeBackups.Delete(name)
	if err != nil || got.State != "running" {
		t.Fatalf("active replay: %+v %v", got, err)
	}
	got, err = e.backup(context.Background(), myspec.Spec{}, p, r)
	if err != nil || got.State != "failed" || got.Error != "backup_interrupted" {
		t.Fatalf("interrupted replay: %+v %v", got, err)
	}
}
func TestBackupReceiptPath(t *testing.T) {
	p := paths{root: t.TempDir()}
	a, err := receiptPath(p, "backup", "../../receipt")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(a) != p.root || !strings.HasPrefix(filepath.Base(a), "backup-") {
		t.Fatal(a)
	}
	b, _ := receiptPath(p, "restore", "../../receipt")
	if a == b {
		t.Fatal("receipt domains collide")
	}
	if _, err = receiptPath(p, "backup", ""); err == nil {
		t.Fatal("empty receipt accepted")
	}
}
func testBackupRepository() myspec.Repository {
	return myspec.Repository{Path: "repo/mysql/cluster", S3: myspec.S3{Endpoint: "http://127.0.0.1:25100", Bucket: "backups", Region: "us-east-1", PathStyle: true, AccessKey: "access-key", SecretKey: "secret-key"}}
}
func TestCloudProtectedDefaults(t *testing.T) {
	r := testBackupRepository()
	name, err := cloudConfig(r, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer removeCloudConfig(name)
	info, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatal("credentials not private")
	}
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[xbcloud]", "s3-secret-key=\"secret-key\"", "s3-bucket-lookup=PATH"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing option %s", want)
		}
	}
	if got := metadataObject("repo/base/label", physicalManifestName); got != "repo/base/label.meta/physical-manifest.json" {
		t.Fatal(got)
	}
}

func TestBackupShutdownCancelsOnlyOwnedJobs(t *testing.T) {
	p := paths{root: t.TempDir()}
	other := paths{root: t.TempDir()}
	ownedName, _ := receiptPath(p, "backup", "owned")
	otherName, _ := receiptPath(other, "backup", "other")
	ownedCtx, ownedCancel := context.WithCancel(context.Background())
	defer ownedCancel()
	otherCtx, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()
	activeBackups.Store(ownedName, ownedCancel)
	activeBackups.Store(otherName, otherCancel)
	defer activeBackups.Delete(ownedName)
	defer activeBackups.Delete(otherName)
	stopBackups(p)
	if ownedCtx.Err() == nil {
		t.Fatal("owned backup survived shutdown")
	}
	if otherCtx.Err() != nil {
		t.Fatal("shutdown canceled another member's backup")
	}
}

// Regression: XtraBackup never streams the datadir auto.cnf, so the source
// identity must come from the generated backup-my.cnf (upstream layout below).
func TestBackupSourceUUIDFromSnapshotBytes(t *testing.T) {
	dir := t.TempDir()
	const uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	if err := os.WriteFile(filepath.Join(dir, "auto.cnf"), []byte("[auto]\nserver-uuid="+uuid+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := backupSourceUUID(dir); err == nil {
		t.Fatal("identity accepted from a file XtraBackup does not stream")
	}
	config := "# This MySQL options file was generated by innobackupex.\n\n# The MySQL server\n[mysqld]\ninnodb_checksum_algorithm=crc32\ninnodb_page_size=16384\nserver_id=1\nserver_uuid=" + uuid + "\nmaster_key_id=0\n"
	if err := os.WriteFile(filepath.Join(dir, "backup-my.cnf"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := backupSourceUUID(dir); err != nil || got != uuid {
		t.Fatalf("source UUID=%q err=%v", got, err)
	}
	for _, bad := range []string{"[mysqld]\nserver_id=1\n", "[mysqld]\nserver_uuid=invalid\n"} {
		if err := os.WriteFile(filepath.Join(dir, "backup-my.cnf"), []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := backupSourceUUID(dir); err == nil {
			t.Fatalf("invalid source identity admitted: %q", bad)
		}
	}
}

func TestBackupMetadataRequiresRealBinlogCoordinates(t *testing.T) {
	for _, coordinate := range []string{"binlog.000003 invalid", "binlog.000003 0", "not-a-binlog 198"} {
		t.Run(coordinate, func(t *testing.T) {
			dir := metadataFixture(t, "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 99\n")
			if err := os.WriteFile(filepath.Join(dir, "xtrabackup_binlog_info"), []byte(coordinate+" aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := parseBackupMetadata(dir); err == nil {
				t.Fatal("unanchored backup metadata accepted")
			}
		})
	}
}
