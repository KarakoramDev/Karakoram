// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
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
	req.ObserveOnly = true
	out, err = New().backup(context.Background(), testSpec(), p, req)
	if err != nil || out != saved.Response {
		t.Fatal("observation changed receipt identity", out, err)
	}
	req.ObserveOnly = false
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

func TestObserveAbsentBackupDoesNotStart(t *testing.T) {
	p := dataPaths(t.TempDir())
	req := pgspec.BackupRequest{Receipt: "never-started", Type: "full", ObserveOnly: true}
	out, err := New().backup(context.Background(), testSpec(), p, req)
	if err != nil || out.State != "absent" {
		t.Fatal(out, err)
	}
	name, err := receiptPath(p, "backup", req.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(name); !os.IsNotExist(err) {
		t.Fatal("observation created a receipt", err)
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

func TestBackupDiagnosticIsBoundedAndSecretSafe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		output string
		want   string
	}{
		{"archive", "ERROR: [082]: WAL segment was not archived; secret=private\n", "pgBackRest error [082]: required WAL segment was not archived"},
		{"native", "P00 ERROR: [103]: https://user:private@storage/path?signature=private\n", "pgBackRest error [103]: native backup command failed"},
		{"no-newline", "ERROR: [042]: response body private", "pgBackRest error [042]"},
		{"oversized", strings.Repeat("private", 10000) + "\nERROR: [103]: private\n", "pgBackRest error [103]"},
		{"unknown", "private\n", "pgBackRest backup could not complete; no native error code was captured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			// Pipe writes can split both the error marker and its numeric code.
			for i := 0; i < len(tc.output); i += 3 {
				part := tc.output[i:min(i+3, len(tc.output))]
				if n, err := diagnostic.Write([]byte(part)); err != nil || n != len(part) {
					t.Fatal(n, err)
				}
			}
			message := diagnostic.failure(nil)
			if !strings.HasPrefix(message, tc.want) || strings.Contains(message, "private") {
				t.Fatal(message)
			}
		})
	}
}

func TestBackupDiagnosticExitStatusAndSignal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		want    string
	}{
		{"exit-status", "exit 23", "pgBackRest backup exited with status 23; no native error code was captured"},
		{"signal", "kill -TERM $$", "pgBackRest backup terminated by signal terminated; inspect member process and memory events"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := exec.Command("/bin/sh", "-c", tc.command).Run()
			if err == nil {
				t.Fatal("expected failed child process")
			}
			var diagnostic backupDiagnostic
			if message := diagnostic.failure(err); message != tc.want {
				t.Fatal(message)
			}
		})
	}
}

func TestBackupRepositoryDiagnostic(t *testing.T) {
	// pgBackRest 2.59 httpRequestError emits ProtocolError [039], followed by
	// Path/Query, Request Headers, Response Headers and Response Content blocks.
	const request = "\n*** Path/Query ***:\nPUT /private-object?uploadId=private&signature=private\n" +
		"*** Request Headers ***:\nauthorization: private\nx-amz-security-token: private\n" +
		"*** Response Headers ***:\nx-private-response: private\n*** Response Content ***:\n"
	const repositoryFull = "repository storage is full or over quota — free or extend repository storage, then run a new backup"
	const repositoryCheck = "check repository credentials, permissions, health and capacity"
	const memberFull = "pgBackRest error [064]: member backup spool/disk is full — free or extend member storage, then run a new backup"
	for _, tc := range []struct {
		name   string
		output string
		want   string
	}{
		{"quota", "P00 ERROR: [039]: HTTP request failed with 500 (Internal Server Error):" + request +
			"<Error><Code>InternalError</Code><Message>disk quota exceeded: private</Message></Error>\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 500, S3 InternalError); " + repositoryFull},
		{"access-denied", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):" + request +
			"<Error><Code>AccessDenied</Code><Message>private</Message></Error>\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 403, S3 AccessDenied); " + repositoryCheck},
		{"opaque-server-error", "P00 ERROR: [039]: HTTP request failed with 500 (Internal Server Error):" + request +
			"<Error><Code>InternalError</Code><Message>private</Message></Error>\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 500, S3 InternalError); " + repositoryCheck},
		{"http-507", "P00 ERROR: [039]: HTTP request failed with 507 (Insufficient Storage):" + request + "private",
			"pgBackRest error [039]: backup repository request failed (HTTP 507); " + repositoryFull},
		{"minio-full", "P00 ERROR: [039]: HTTP request failed with 500 (Internal Server Error):" + request +
			"<Error><Code>XMinioStorageFull</Code><Message>private</Message></Error>",
			"pgBackRest error [039]: backup repository request failed (HTTP 500, S3 XMinioStorageFull); " + repositoryFull},
		{"s3-only", "P00 ERROR: [039]: private\n<Error><Code>QuotaExceeded</Code><Message>private</Message></Error>",
			"pgBackRest error [039]: backup repository request failed (S3 QuotaExceeded); " + repositoryFull},
		{"local-enospc", "P00 ERROR: [064]: unable to write private: [28] No space left on device\n", memberFull},
		{"local-disk-full", "P00 ERROR: [064]: private DISK FULL", memberFull},
		{"local-storage-full", "P00 ERROR: [064]: private STORAGE FULL", memberFull},
		{"invalid-s3-code", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):" + request +
			"<Code>AccessDenied-private</Code>", "pgBackRest error [039]: backup repository request failed (HTTP 403); " + repositoryCheck},
		{"overlong-s3-code", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):" + request +
			"<Code>" + strings.Repeat("A", 65) + "</Code>", "pgBackRest error [039]: backup repository request failed (HTTP 403); " + repositoryCheck},
		{"max-s3-code", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):" + request +
			"<Code>" + strings.Repeat("A", 64) + "</Code>", "pgBackRest error [039]: backup repository request failed (HTTP 403, unrecognized S3 error); " + repositoryCheck},
		{"credential-in-body-code", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):" + request +
			"<Code>privateCredentialAKIA123456</Code>", "pgBackRest error [039]: backup repository request failed (HTTP 403, unrecognized S3 error); " + repositoryCheck},
		{"credential-in-header-code", "P00 ERROR: [039]: HTTP request failed with 403 (Forbidden):\n*** Response Headers ***:\nx-error: <Code>privateHeaderSignatureABC123</Code>\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 403, unrecognized S3 error); " + repositoryCheck},
		{"invalid-http-status", "P00 ERROR: [039]: HTTP request failed with 5000 private",
			"pgBackRest error [039]: native backup command failed; inspect member diagnostics"},
		{"http-without-native-code", "HTTP request failed with 403 (Forbidden):" + request + "<Code>AccessDenied</Code>",
			"pgBackRest backup: backup repository request failed (HTTP 403, S3 AccessDenied); " + repositoryCheck},
		{"capacity-case-insensitive", "P00 ERROR: [039]: HTTP request failed with 500 (Internal Server Error):" + request +
			"<Code>InternalError</Code><Message>hTtP 507 private</Message>", "pgBackRest error [039]: backup repository request failed (HTTP 507, S3 InternalError); " + repositoryFull},
		{"oversized-line-tail", "P00 ERROR: [039]: HTTP request failed with 500 (Internal Server Error):\n" +
			strings.Repeat("private", 10000) + "<Code>InternalError</Code> DISK QUOTA EXCEEDED",
			"pgBackRest error [039]: backup repository request failed (HTTP 500, S3 InternalError); " + repositoryFull},
		{"archive-mapping-preserved", "P00 ERROR: [082]: private\nHTTP request failed with 500\nquota exceeded",
			"pgBackRest error [082]: required WAL segment was not archived before the archive timeout; inspect target archiving and repository reachability"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			for i := 0; i < len(tc.output); i += 7 {
				_, _ = diagnostic.Write([]byte(tc.output[i:min(i+7, len(tc.output))]))
			}
			if got := diagnostic.failure(nil); got != tc.want || strings.Contains(got, "private") {
				t.Fatalf("unsafe or incorrect repository diagnostic: %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBackupDiagnosticScanWindowBoundaries(t *testing.T) {
	const generic = "pgBackRest error [039]: native backup command failed; inspect member diagnostics"
	const repositoryCheck = "; check repository credentials, permissions, health and capacity"
	const repositoryFull = "; repository storage is full or over quota — free or extend repository storage, then run a new backup"
	for _, tc := range []struct {
		name     string
		atWindow string
		after    string
		want     string
	}{
		{"invalid-http-5000", "HTTP request failed with 500", "0\n", generic},
		{"invalid-http-5070", "HTTP request failed with 507", "0\n", generic},
		{"invalid-capacity-5070", "HTTP 507", "0\n", generic},
		{"invalid-capacity-5070-eof", "HTTP 507", "0", generic},
		{"status-spanning-window", "HTTP request failed with 50", "3 (Service Unavailable)\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 503)" + repositoryCheck},
		{"status-delimiter-after-window", "HTTP request failed with 500", " (Internal Server Error)\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 500)" + repositoryCheck},
		{"status-eof-at-window", "HTTP request failed with 500", "",
			"pgBackRest error [039]: backup repository request failed (HTTP 500)" + repositoryCheck},
		{"status-newline-at-window", "HTTP request failed with 507", "\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 507)" + repositoryFull},
		{"capacity-spanning-window", "HTTP 50", "7\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 507)" + repositoryFull},
		{"capacity-delimiter-after-window", "HTTP 507", " \n",
			"pgBackRest error [039]: backup repository request failed (HTTP 507)" + repositoryFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			const prefix = "P00 ERROR: [039]: "
			output := prefix + strings.Repeat("x", len(diagnostic.line)-len(prefix)-len(tc.atWindow)) + tc.atWindow + tc.after
			for i := 0; i < len(output); i += 7 {
				_, _ = diagnostic.Write([]byte(output[i:min(i+7, len(output))]))
			}
			if got := diagnostic.failure(nil); got != tc.want {
				t.Fatalf("scan window was treated as end-of-line: %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBackupNativeServiceErrorScanWindowBoundaries(t *testing.T) {
	const generic = "pgBackRest error [101]: native backup command failed; inspect member diagnostics"
	const full = "pgBackRest error [101]: backup repository request failed (HTTP 507); repository storage is full or over quota — free or extend repository storage, then run a new backup"
	for _, tc := range []struct {
		name     string
		atWindow string
		after    string
		want     string
	}{
		{"invalid-5070", "P00 ERROR: [101]: [507", "0]\n", generic},
		{"invalid-5000", "P00 ERROR: [101]: [500", "0]\n", generic},
		{"invalid-suffix-after-window", "P00 ERROR: [101]: [507]", "0\n", generic},
		{"valid-status-spanning-window", "P00 ERROR: [101]: [50", "7] Insufficient Storage\n", full},
		{"valid-delimiter-after-window", "P00 ERROR: [101]: [507]", " Insufficient Storage\n", full},
		{"valid-eof-at-window", "P00 ERROR: [101]: [507]", "", full},
		{"valid-newline-at-window", "P00 ERROR: [101]: [507]", "\n", full},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			output := strings.Repeat(" ", len(diagnostic.line)-len(tc.atWindow)) + tc.atWindow + tc.after
			for i := 0; i < len(output); i += 7 {
				_, _ = diagnostic.Write([]byte(output[i:min(i+7, len(output))]))
			}
			if got := diagnostic.failure(nil); got != tc.want {
				t.Fatalf("native ServiceError scan-window boundary mismatch: %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBackupNativeServiceErrorDiagnostic(t *testing.T) {
	const full = "repository storage is full or over quota — free or extend repository storage, then run a new backup"
	const check = "check repository credentials, permissions, health and capacity"
	for _, tc := range []struct {
		name   string
		output string
		want   string
	}{
		{"native-storage-full", "P00 ERROR: [101]: [507] Insufficient Storage\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 507); " + full},
		{"wrapped-first-507-later-500", "P00 ERROR: [101]: raised from local-1 protocol: [507] Insufficient Storage\n    [ServiceError] on retry at 100ms: [500] Internal Server Error\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 500); " + full},
		{"wrapped-retry-507", "P00 ERROR: [101]: [500] Internal Server Error\n    [ServiceError] on retry at 100ms: raised from local-2 protocol: [507] Insufficient Storage\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 507); " + full},
		{"unrecognized-protocol-prefix", "P00 ERROR: [101]: raised from private protocol: [507] Insufficient Storage\n",
			"pgBackRest error [101]: native backup command failed; inspect member diagnostics"},
		{"overlong-protocol-id", "P00 ERROR: [101]: raised from local-12345678901 protocol: [507] Insufficient Storage\n",
			"pgBackRest error [101]: native backup command failed; inspect member diagnostics"},
		{"native-opaque-500", "P00 ERROR: [101]: [500] Internal Server Error\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 500); " + check},
		{"native-retry-capacity", "P00 ERROR: [101]: [500] Internal Server Error\n    [ServiceError] on 3 retries from 100-300ms: [507] Insufficient Storage\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 507); " + full},
		{"native-single-retry", "P00 ERROR: [101]: [500] Internal Server Error\n    [ServiceError] on retry at 100ms: [503] Service Unavailable\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 503); " + check},
		{"native-request-timeout", "P00 ERROR: [101]: [408] Request Timeout\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 408); " + check},
		{"native-507-eof", "P00 ERROR: [101]: [507]",
			"pgBackRest error [101]: backup repository request failed (HTTP 507); " + full},
		{"wrong-native-error-type", "P00 ERROR: [039]: [507] Insufficient Storage\n",
			"pgBackRest error [039]: native backup command failed; inspect member diagnostics"},
		{"bare-status", "[507] Insufficient Storage\n",
			"pgBackRest backup could not complete; no native error code was captured"},
		{"non-retryable-status", "P00 ERROR: [101]: [404] Not Found\n",
			"pgBackRest error [101]: native backup command failed; inspect member diagnostics"},
		{"overlong-status", "P00 ERROR: [101]: [5070] private\n",
			"pgBackRest error [101]: native backup command failed; inspect member diagnostics"},
		{"overlong-500", "P00 ERROR: [101]: [5000] private\n",
			"pgBackRest error [101]: native backup command failed; inspect member diagnostics"},
		{"malformed-retry", "P00 ERROR: [101]: [500] Internal Server Error\n    [ServiceError] on retry at privatems: [507] Insufficient Storage\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 500); " + check},
		{"private-server-text", "P00 ERROR: [101]: [507] private\n*** Request Headers ***:\nauthorization: private\n",
			"pgBackRest error [101]: backup repository request failed (HTTP 507); " + full},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			for i := 0; i < len(tc.output); i += 7 {
				_, _ = diagnostic.Write([]byte(tc.output[i:min(i+7, len(tc.output))]))
			}
			if got := diagnostic.failure(nil); got != tc.want || strings.Contains(got, "private") {
				t.Fatalf("unsafe or incorrect native retry diagnostic: %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBackupMissingRepositoryDiagnostic(t *testing.T) {
	const upload = "repository multipart upload is missing; check capacity, upload lifecycle and concurrent cleanup, then run a new backup"
	const object = "repository object is missing; check capacity, object lifecycle and concurrent cleanup, then run a new backup"
	const missingUpload = "P00 ERROR: [039]: HTTP request failed with 404 (Not Found):\n<Error><Code>NoSuchUpload</Code><Message>private</Message></Error>\n"
	const missingKey = "P00 ERROR: [039]: HTTP request failed with 404 (Not Found):\n<Error><Code>NoSuchKey</Code><Message>private</Message></Error>\n"
	for _, tc := range []struct {
		name   string
		output string
		want   string
	}{
		{"missing-upload", missingUpload,
			"pgBackRest error [039]: backup repository request failed (HTTP 404, S3 NoSuchUpload); " + upload},
		{"missing-object", missingKey,
			"pgBackRest error [039]: backup repository request failed (HTTP 404, S3 NoSuchKey); " + object},
		{"opaque-500-then-missing-upload", "P00 ERROR: [101]: [500] Internal Server Error\n" + missingUpload,
			"pgBackRest error [039]: backup repository request failed (HTTP 404, last observed HTTP 5xx 500, S3 NoSuchUpload); " + upload},
		{"last-observed-5xx", "P00 ERROR: [101]: [500] Internal Server Error\n    [ServiceError] on retry at 100ms: [503] Service Unavailable\n" + missingKey,
			"pgBackRest error [039]: backup repository request failed (HTTP 404, last observed HTTP 5xx 503, S3 NoSuchKey); " + object},
		{"opaque-500-then-bare-404", "P00 ERROR: [101]: [500] Internal Server Error\nP00 ERROR: [039]: HTTP request failed with 404 (Not Found)\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 404, last observed HTTP 5xx 500); check repository credentials, permissions, health and capacity"},
		{"explicit-capacity-then-404", "P00 ERROR: [101]: [507] Insufficient Storage\n" + missingUpload,
			"pgBackRest error [039]: backup repository request failed (HTTP 404, last observed HTTP 5xx 507, S3 NoSuchUpload); repository storage is full or over quota — free or extend repository storage, then run a new backup"},
		{"unknown-code-stays-private", "P00 ERROR: [039]: HTTP request failed with 404 (Not Found):\n<Code>privateNoSuchUploadCredential</Code>\n",
			"pgBackRest error [039]: backup repository request failed (HTTP 404, unrecognized S3 error); check repository credentials, permissions, health and capacity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic backupDiagnostic
			for i := 0; i < len(tc.output); i += 7 {
				_, _ = diagnostic.Write([]byte(tc.output[i:min(i+7, len(tc.output))]))
			}
			if got := diagnostic.failure(nil); got != tc.want || strings.Contains(got, "private") {
				t.Fatalf("unsafe or incorrect missing repository diagnostic: %q, want %q", got, tc.want)
			}
		})
	}
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
