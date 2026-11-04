// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

type archiveProbeConn struct {
	segment string
	done    bool
	err     error
	sql     string
	lsn     string
}

func (c *archiveProbeConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	c.sql = sql
	c.lsn = args[0].(string)
	return receiptRow{scan: func(dest ...any) error {
		*dest[0].(*string) = c.segment
		*dest[1].(*bool) = c.done
		return c.err
	}}
}

func TestArchiveCoversRepositoryFallback(t *testing.T) {
	const segment = "000000070000000100000002"
	for _, mode := range []string{"receipt", "recycled", "missing", "failure", "timeout", "empty", "invalid-segment", "sql-error", "unconfigured"} {
		t.Run(mode, func(t *testing.T) {
			s := testSpec()
			s.Backup = &pgspec.Backup{Repository: pgspec.Repository{Stanza: "group7"}}
			s.Superuser.Password = "never-expose-this-secret"
			p := dataPaths(t.TempDir())
			if err := os.MkdirAll(p.root, 0700); err != nil {
				t.Fatal(err)
			}
			c := &archiveProbeConn{segment: segment, done: mode == "receipt"}
			if mode == "invalid-segment" {
				c.segment = "../other-timeline"
			}
			if mode == "sql-error" {
				c.err = errors.New("query failed")
			}
			if mode == "unconfigured" {
				s.Backup = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			calls := 0
			var destination string
			run := func(probeCtx context.Context, _ pgspec.Spec, _ paths, args ...string) ([]byte, error) {
				calls++
				deadline, ok := probeCtx.Deadline()
				parentDeadline, _ := ctx.Deadline()
				if !ok || deadline.After(parentDeadline) || time.Until(deadline) > 10*time.Second {
					t.Fatal("repository probe has no bounded parent deadline")
				}
				if len(args) != 8 || args[0] != "--config="+filepath.Join(p.root, "pgbackrest.conf") || args[1] != "--stanza=group7" || args[2] != "--archive-async=n" || args[3] != "--log-level-console=off" || args[4] != "--log-level-file=off" || args[5] != "archive-get" || args[6] != segment {
					t.Fatalf("incorrect repository fetch arguments: %v", args)
				}
				if strings.Contains(strings.Join(args, " "), s.Superuser.Password) {
					t.Fatal("secret in command arguments")
				}
				destination = args[7]
				info, err := os.Stat(filepath.Dir(destination))
				if err != nil || info.Mode().Perm() != 0700 || filepath.Dir(filepath.Dir(destination)) != p.root {
					t.Fatal("fetch destination is not in a private temporary directory")
				}
				switch mode {
				case "missing":
					// Match the real archive-get not-found exit status, including
					// backupCommand's error wrapping; no repository is contacted.
					return nil, fmt.Errorf("pgBackRest command failed: %w", exec.Command("sh", "-c", "exit 1").Run())
				case "failure":
					return nil, errors.New(s.Superuser.Password)
				case "timeout":
					cancel()
					return nil, probeCtx.Err()
				case "empty":
					return nil, nil
				default:
					return nil, os.WriteFile(destination, []byte("archived WAL"), 0600)
				}
			}
			out, err := archiveCovers(ctx, c, s, p, "1/2000001", run)
			wantCovers := mode == "receipt" || mode == "recycled"
			wantError := mode != "receipt" && mode != "recycled" && mode != "missing"
			if out.Covers != wantCovers || (err != nil) != wantError {
				t.Fatalf("covers=%v err=%v", out.Covers, err)
			}
			if err != nil && strings.Contains(err.Error(), s.Superuser.Password) {
				t.Fatal("command diagnostics exposed a secret")
			}
			if mode == "timeout" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			wantCalls := 1
			if mode == "receipt" || mode == "invalid-segment" || mode == "sql-error" || mode == "unconfigured" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("repository calls=%d, want %d", calls, wantCalls)
			}
			if mode != "unconfigured" && (c.lsn != "1/2000001" || !strings.Contains(c.sql, "pg_catalog.pg_walfile_name($1::pg_lsn)") || !strings.Contains(c.sql, "pg_catalog.pg_ls_archive_statusdir()")) {
				t.Fatal("coverage did not resolve the exact PostgreSQL WAL segment and receipt")
			}
			if destination != "" {
				if _, err := os.Stat(filepath.Dir(destination)); !os.IsNotExist(err) {
					t.Fatalf("temporary archive fetch survived: %v", err)
				}
			}
		})
	}
}
