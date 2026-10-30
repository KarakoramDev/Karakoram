// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestBackupNativeProcessDiagnostic(t *testing.T) {
	for _, tc := range []struct{ name, script, want string }{
		{"exit", "exit 23", "xbcloud exit status 23"},
		{"signal", "kill -KILL $$", "xbcloud killed by SIGKILL (possible OOM; not confirmed)"},
		{"success", "exit 0", "xbcloud exit status 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "sh", "-c", tc.script)
			var tail backupStderrTail
			cmd.Stderr = &tail
			err := cmd.Run()
			got := backupProcessDiagnostic("xbcloud", cmd, err, false, &tail)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("diagnostic=%q, want %q", got, tc.want)
			}
			if tc.name == "signal" {
				got = backupProcessDiagnostic("xbcloud", cmd, err, true, &tail)
				if !strings.Contains(got, "possible OOM or pipeline cleanup; not confirmed") {
					t.Fatal("pipeline cleanup was mislabeled as a confirmed native OOM")
				}
			}
		})
	}
}

func TestBackupNativeStderrTailRedaction(t *testing.T) {
	for _, raw := range []string{
		"Lost connection to MySQL server during query; password='obsolete-secret'",
		"Failed to upload https://user:obsolete-secret@example.invalid/object?X-Amz-Signature=private-signature&key=private-key",
		"Authorization: Bearer old-token\nmysql server has gone away\nopaque-old-credential",
		"private-key-line-without-markers\nno space left on device",
	} {
		var tail backupStderrTail
		for i := 0; i < len(raw); i += 3 {
			_, _ = tail.Write([]byte(raw[i:min(i+3, len(raw))]))
		}
		got := tail.diagnostic()
		for _, secret := range []string{"obsolete-secret", "private-signature", "private-key", "old-token", "opaque-old-credential", "https://", "example.invalid"} {
			if strings.Contains(got, secret) {
				t.Fatalf("native secret leaked in %q", got)
			}
		}
		if strings.Contains(raw, "gone away") || strings.Contains(raw, "Lost connection") {
			if !strings.Contains(got, "mysqld connection lost") {
				t.Fatal("connection-loss classification discarded")
			}
		}
		if strings.Contains(raw, "Failed to upload") && !strings.Contains(got, "object upload failed") {
			t.Fatal("upload-failure classification discarded")
		}
	}
	var tail backupStderrTail
	_, _ = tail.Write([]byte("lost connection to mysql server\n"))
	_, _ = tail.Write([]byte(strings.Repeat("unknown-secret", 1000)))
	if tail.used != len(tail.data) || strings.Contains(tail.diagnostic(), "connection lost") || strings.Contains(tail.diagnostic(), "unknown-secret") {
		t.Fatal("stderr was not bounded to a safely normalized tail")
	}
}

func TestBackupPipelinePublishesNativeFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	source := exec.CommandContext(ctx, "sh", "-c", "echo 'Lost connection to MySQL server: password=private-credential' >&2; exit 7")
	sink := exec.CommandContext(ctx, "cat")
	_, err := backupManifestPipeline(source, sink, io.Discard, "repo/base/full")
	if err == nil {
		t.Fatal("failed native source accepted")
	}
	message := err.Error()
	for _, want := range []string{"pipeline_stream_failed", "xtrabackup exit status 7", "mysqld connection lost", "xbcloud"} {
		if !strings.Contains(message, want) {
			t.Fatalf("diagnostic %q lacks %q", message, want)
		}
	}
	if strings.Contains(message, "private-credential") {
		t.Fatal("native credential reached the backup receipt error")
	}
}
