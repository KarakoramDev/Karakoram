// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/minio/minio-go/v7"
)

func TestArchiveFailureSkipRestartRecovery(t *testing.T) {
	e := New()
	e.dir = t.TempDir()
	repo := testBackupRepository()
	e.current.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 1}}
	_, p := e.state()
	e.recordArchiveResult(p, repo, errors.New("binlog_decode_failed"))
	var attempts atomic.Int32
	second := make(chan struct{})
	e.configureArchiveLoop(true, func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			return archiveWriterAdmission(myspec.StatusResponse{}, nil, false)
		}
		// Reaching the next attempt proves the skipped result was processed.
		close(second)
		<-ctx.Done()
		return ctx.Err()
	})
	defer e.resetArchiveLoop(false)
	select {
	case <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("skipped pass did not defer to next interval")
	}
	e.resetArchiveLoop(false)
	restarted := New()
	restarted.dir = e.dir
	restarted.current = e.current
	status, err := restarted.archiveStatus()
	if err != nil || status.Error != "binlog_decode_failed" {
		t.Fatalf("skip/restart erased failed attempt: %+v %v", status, err)
	}
	restarted.configureArchiveLoop(true, func(context.Context) error { return nil })
	defer restarted.resetArchiveLoop(false)
	deadline := time.After(1800 * time.Millisecond)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err = restarted.archiveStatus()
		if err != nil {
			t.Fatal(err)
		}
		if status.Error == "" {
			break
		}
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("completed pass did not clear failure")
		}
	}
	fresh := New()
	fresh.dir = e.dir
	fresh.current = e.current
	status, err = fresh.archiveStatus()
	if err != nil || status.Error != "" {
		t.Fatalf("successful recovery was not durable: %+v %v", status, err)
	}
}

type attemptRotationConnector struct {
	*pendingRecoveryConnector
	failure    error
	executions int
}

func (c *attemptRotationConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *attemptRotationConnector) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.executions++
	return nil, c.failure
}

func TestArchivePreRotationAdmissionAndCause(t *testing.T) {
	privateCause := &mysqldriver.MySQLError{Number: 1227, SQLState: [5]byte{'4', '2', '0', '0', '0'}, Message: "private SQL/account payload"}
	for _, stage := range []string{"probe", "ineligible", "rotation"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "binlog.000001"), make([]byte, 512), 0600); err != nil {
				t.Fatal(err)
			}
			connector := &attemptRotationConnector{pendingRecoveryConnector: &pendingRecoveryConnector{archiveMemoryConnector: &archiveMemoryConnector{groupTestConnector: groupTestConnector{t: t}, dir: dir}, logs: [][]driver.Value{{"binlog.000001", int64(512)}}}, failure: privateCause}
			db := sql.OpenDB(connector)
			defer db.Close()
			s := fakeMysqlbinlog(t, "printf '%s' "+shellQuote(streamTransaction())+"\n")
			s.Backup = &myspec.Backup{Repository: testBackupRepository(), Binlog: myspec.Binlog{Enabled: true}}
			status := func(context.Context) (myspec.StatusResponse, error) {
				if stage == "probe" {
					return myspec.StatusResponse{}, privateCause
				}
				if stage == "ineligible" {
					return myspec.StatusResponse{}, nil
				}
				return myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"}, nil
			}
			err := archiveNativeBinlogs(t.Context(), s, paths{root: dir}, db, status)
			if stage == "ineligible" {
				if !errors.Is(err, errArchiveSkipped) || connector.executions != 0 {
					t.Fatal("ineligible pass was not skipped")
				}
				return
			}
			category := "binlog_status_failed"
			if stage == "rotation" {
				category = "binlog_rotate_failed"
			}
			if err == nil || !strings.HasPrefix(err.Error(), category+":") || !errors.Is(err, privateCause) {
				t.Fatal("underlying native failure was discarded")
			}
			if (stage == "probe" && connector.executions != 0) || (stage == "rotation" && connector.executions != 1) {
				t.Fatal("unexpected rotation admission")
			}
			diagnostic := archiveDiagnostic(err)
			if !strings.Contains(diagnostic, "1227") || !strings.Contains(diagnostic, "42000") || strings.Contains(diagnostic, privateCause.Message) {
				t.Fatalf("unsafe or missing typed native diagnostic: %q", diagnostic)
			}
			runner := New()
			runner.dir = dir
			runner.current = s
			_, p := runner.state()
			runner.recordArchiveResult(p, s.Backup.Repository, err)
			observed, statusErr := runner.archiveStatus()
			if statusErr != nil || observed.Error != category {
				t.Fatalf("public failure is not category-only: %+v %v", observed, statusErr)
			}
		})
	}
	disabled := New()
	if !errors.Is(disabled.archiveBinlogs(t.Context()), errArchiveSkipped) {
		t.Fatal("disabled archive pass was reported as successful")
	}
}

func TestArchiveUploadRetainsSanitizedCause(t *testing.T) {
	for _, stage := range []string{"stat", "binary", "metadata"} {
		t.Run(stage, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fail := (stage == "stat" && r.Method == http.MethodHead) || (stage == "binary" && r.Method == http.MethodPut) || (stage == "metadata" && strings.HasSuffix(r.URL.Path, ".json"))
				w.Header().Set("Content-Type", "application/xml")
				if fail {
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code><Message>private SQL/account payload</Message></Error>")
					return
				}
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
			}))
			defer server.Close()
			repo := testBackupRepository()
			repo.S3.Endpoint = server.URL
			client, closeClient, err := s3Client(repo)
			if err != nil {
				t.Fatal(err)
			}
			defer closeClient()
			local := filepath.Join(t.TempDir(), "binlog.000001")
			if err := os.WriteFile(local, []byte("fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			err = uploadArchive(t.Context(), client, repo, "binlog.000001", local, binlogSidecar{SHA256: "fixture-checksum"})
			var cause minio.ErrorResponse
			if err == nil || !strings.HasPrefix(err.Error(), "archive_upload_failed:") || !errors.As(err, &cause) || cause.Code != "AccessDenied" {
				t.Fatal("underlying object-store failure was discarded")
			}
			diagnostic := archiveDiagnostic(err)
			if !strings.Contains(diagnostic, "AccessDenied") || !strings.Contains(diagnostic, "403") || strings.Contains(diagnostic, "private SQL/account payload") {
				t.Fatalf("unsafe or missing typed storage diagnostic: %q", diagnostic)
			}
		})
	}
	diagnostic := archiveDiagnostic(fmt.Errorf("binlog_decode_failed: %w; stderr: private SQL/account payload", errors.New("incomplete_binlog_transaction")))
	if diagnostic != "incomplete_binlog_transaction" {
		t.Fatalf("decoder diagnostic disclosed native output: %q", diagnostic)
	}
}
