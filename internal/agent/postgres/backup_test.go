// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestBackupInfoProjection(t *testing.T) {
	data := []byte(`[{"name":"kk","db":[{"id":1,"system-id":7620575555724598831,"version":"17"}],"archive":[{"min":"000000010000000000000001","max":"000000010000000000000010"}],"backup":[{"label":"20261017-021859F","type":"full","prior":null,"reference":null,"database":{"id":1},"timestamp":{"start":1792203539,"stop":1792203546},"archive":{"start":"00000001000000000000000A","stop":"00000001000000000000000A"},"info":{"size":23427935,"repository":{"size":3037143}},"annotation":{"karakoram-operation":"op"}},{"label":"20261017-021859F_20261017-021921D","type":"diff","prior":"20261017-021859F","reference":["20261017-021859F"],"database":{"id":1},"timestamp":{"start":1792203561,"stop":1792203562},"info":{"size":24000000,"repository":{"size":3070000}}}]}]`)
	info, err := projectBackupInfo(data, "kk")
	if err != nil {
		t.Fatal(err)
	}
	if info.SystemID != "7620575555724598831" || len(info.Backups) != 2 {
		t.Fatalf("incorrect numeric system id or inventory: %+v", info)
	}
	full := info.Backups[0]
	if full.SizeBytes != 23427935 || full.RepoBytes != 3037143 || full.SystemID != info.SystemID || full.Version != "17" || full.Annotation["karakoram-operation"] != "op" {
		t.Fatalf("incomplete metadata: %+v", full)
	}
	if info.Backups[1].Prior != full.Label || len(info.Backups[1].References) != 1 {
		t.Fatal("backup dependency lost")
	}
}
func TestDurableBackupReceiptReplay(t *testing.T) {
	p := dataPaths(t.TempDir())
	req := pgspec.BackupRequest{Receipt: "operation/backup", Type: "full"}
	name, err := receiptPath(p, "backup", req.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	saved := backupReceipt{RequestHash: strings.ToLower(strings.TrimSpace(hashString(req))), Response: pgspec.BackupResponse{State: "done", Label: "20261017-021859F"}}
	if err = durableJSON(name, saved); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(name)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("receipt is not owner-only", err)
	}
	// A new runner simulates agent restart. No PostgreSQL/native binary is needed
	// to replay a completed receipt, and no command may be restarted.
	out, err := New().backup(context.Background(), testSpec(), p, req)
	if err != nil || out != saved.Response {
		t.Fatal(out, err)
	}
	req.Type = "incr"
	if _, err = New().backup(context.Background(), testSpec(), p, req); err == nil {
		t.Fatal("receipt reused for another request")
	}
	if _, err = receiptPath(p, "backup", ""); err == nil {
		t.Fatal("empty receipt accepted")
	}
	if !strings.HasPrefix(name, filepath.Join(p.root, "receipts", "backup")) {
		t.Fatal("receipt escaped protected state directory")
	}
}
func hashString(v any) string {
	sum := hash(v)
	const digits = "0123456789abcdef"
	var out strings.Builder
	for _, b := range sum {
		out.WriteByte(digits[b>>4])
		out.WriteByte(digits[b&15])
	}
	return out.String()
}
func TestRestoreRenderingIsolationAndImmediate(t *testing.T) {
	s := testSpec()
	repo := pgspec.Repository{Stanza: "kk", Path: "/archive/postgres/default/source-incarnation", S3: pgspec.S3{Endpoint: "https://objects.example", Bucket: "backups", Region: "region", AccessKey: "access", SecretKey: "secret", VerifyTLS: true}}
	s.Backup = &pgspec.Backup{Repository: repo}
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{Source: repo, Set: "20261017-021859F"}}
	p := dataPaths(t.TempDir())
	restore, err := restoreArgs(s, p)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(restore, " ")
	if !strings.Contains(args, "--type=immediate") || strings.Contains(args, "--archive-mode=off") {
		t.Fatal("in-place archive policy", args)
	}
	s.Backup.Repository.Path = "/archive/postgres/default/target-incarnation"
	restore, err = restoreArgs(s, p)
	if err != nil {
		t.Fatal(err)
	}
	args = strings.Join(restore, " ")
	if !strings.Contains(args, "--archive-mode=off") || parameters(s, p)["archive_mode"] != "off" {
		t.Fatal("clone can write source archive")
	}
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range files {
		if file.name == "pgbackrest.conf" {
			found = true
			for _, want := range []string{"repo1-retention-full=9999999", "expire-auto=n", "repo1-storage-verify-tls=y"} {
				if !strings.Contains(file.content, want) {
					t.Fatal("missing retention or TLS setting", want)
				}
			}
		}
	}
	if !found {
		t.Fatal("repository not rendered")
	}
	data, _ := json.Marshal(s)
	if !json.Valid(data) {
		t.Fatal("invalid specification")
	}
}

func TestStorageOverrideArchivesToBothRepositories(t *testing.T) {
	s := testSpec()
	repo := pgspec.Repository{
		Stanza: "kk", Path: "/default/postgres/default/cluster-incarnation",
		S3: pgspec.S3{
			Endpoint: "https://objects.example", Bucket: "backups", Region: "region",
			AccessKey: "access", SecretKey: "secret", VerifyTLS: true,
		},
	}
	override := repo
	override.Path = "/override/postgres/default/cluster-incarnation"
	s.Backup = &pgspec.Backup{
		Repository: repo, AdditionalRepositories: []pgspec.Repository{override},
	}
	if err := validate(s); err != nil {
		t.Fatal(err)
	}
	files, err := render(s, dataPaths(t.TempDir()), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.name != "pgbackrest.conf" {
			continue
		}
		for _, required := range []string{
			"repo1-path=" + repo.Path, "repo2-path=" + override.Path,
			"repo2-retention-full=9999999", "repo2-storage-verify-tls=y",
		} {
			if !strings.Contains(file.content, required) {
				t.Fatal("archive repository omitted", required)
			}
		}
		return
	}
	t.Fatal("pgBackRest configuration missing")
}

func TestAppliedBootstrapGenerationIsDurable(t *testing.T) {
	p := dataPaths(t.TempDir())
	if appliedBootstrapGeneration(p) != 0 {
		t.Fatal("missing receipt must not echo desired generation")
	}
	if err := durableJSON(filepath.Join(p.root, "bootstrap-generation.json"), 3); err != nil {
		t.Fatal(err)
	}
	if appliedBootstrapGeneration(p) != 3 {
		t.Fatal("completed restore generation did not survive runner replacement")
	}
}

func TestRestoreTargetUsesPostgresTimestamp(t *testing.T) {
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{Source: pgspec.Repository{Stanza: "kk"}}}
	p := dataPaths(t.TempDir())
	for _, tc := range []struct{ input, want string }{
		{"2026-10-17T06:41:16.846757Z", "--target=2026-10-17 06:41:16.846757+00:00"},
		{"2026-10-17T08:41:16.846757+02:00", "--target=2026-10-17 06:41:16.846757+00:00"},
		{"2026-10-17T06:41:16.123456789Z", "--target=2026-10-17 06:41:16.123456+00:00"},
		{"2026-10-17T06:41:16Z", "--target=2026-10-17 06:41:16+00:00"},
	} {
		s.Bootstrap.Restore.TargetTime = tc.input
		args, err := restoreArgs(s, p)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, arg := range args {
			found = found || arg == tc.want
		}
		if !found {
			t.Fatalf("native target missing %q: %v", tc.want, args)
		}
		files, err := render(s, p, nil)
		if err != nil {
			t.Fatal(err)
		}
		found = false
		for _, file := range files {
			if file.name == "bootstrap-restore.sh" && strings.Contains(file.content, shellQuote(tc.want)) {
				found = true
			}
		}
		if !found {
			t.Fatal("Patroni bootstrap did not use the same formatted target")
		}
	}
	s.Bootstrap.Restore.TargetTime = "not-a-timestamp"
	if _, err := restoreArgs(s, p); err == nil {
		t.Fatal("invalid PITR target reached a native command")
	}
	if _, err := render(s, p, nil); err == nil {
		t.Fatal("invalid PITR target reached the bootstrap script")
	}
}
