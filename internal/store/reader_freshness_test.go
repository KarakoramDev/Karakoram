// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

var readerCancelFunctionID atomic.Uint64

func TestCanceledReaderQueryReleasesSnapshot(t *testing.T) {
	ctx := t.Context()
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := make(chan struct{})
	function := fmt.Sprintf("karakoram_read_started_%d", readerCancelFunctionID.Add(1))
	if err := sqlite.RegisterScalarFunction(function, 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
		close(started)
		<-readCtx.Done()
		// Let the cancellation watcher mark the first-row acquisition before
		// the callback returns, as in the original driver regression.
		time.Sleep(100 * time.Millisecond)
		return int64(1), nil
	}); err != nil {
		t.Fatal(err)
	}
	s, _ := testStore(t)
	if _, err := s.writer.ExecContext(ctx, "CREATE TABLE read_freshness(value INTEGER); INSERT INTO read_freshness VALUES(0)"); err != nil {
		t.Fatal(err)
	}
	// Cancel while SQLite produces the first row. Exercise the public driver
	// contract rather than holding its private interruption mutex: since
	// v1.42.2 cancellation and connection reuse synchronize on that mutex.
	queryDone := make(chan error, 1)
	go func() {
		rows, err := s.reader.QueryContext(readCtx, "SELECT value, "+function+"() FROM read_freshness")
		if rows != nil {
			_ = rows.Close()
		}
		queryDone <- err
	}()
	<-started
	cancel()
	if err := <-queryDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected first-row acquisition to be canceled, got %v", err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE read_freshness SET value=1"); err != nil {
		t.Fatal(err)
	}
	assertPooledReaderValue(t, s, 1)
	var busy, log, checkpointed int
	if err := s.writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || log != 0 || checkpointed != 0 {
		t.Fatalf("canceled reader retained WAL: busy=%d log=%d checkpointed=%d", busy, log, checkpointed)
	}
}

func TestReaderCleanupPathsReleaseSnapshots(t *testing.T) {
	tests := []struct {
		name     string
		exercise func(*testing.T, *Store)
	}{
		{"undrained-rows", func(t *testing.T, s *Store) {
			rows, err := s.reader.QueryContext(t.Context(), "SELECT value FROM read_freshness UNION ALL SELECT value FROM read_freshness")
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				_ = rows.Close()
				t.Fatal("missing first row")
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"canceled-iteration", func(t *testing.T, s *Store) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rows, err := s.reader.QueryContext(ctx, "SELECT value FROM read_freshness UNION ALL SELECT value FROM read_freshness")
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				_ = rows.Close()
				t.Fatal("missing first row")
			}
			cancel()
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"multi-row-query-row", func(t *testing.T, s *Store) {
			var value int
			if err := s.reader.QueryRowContext(t.Context(), "SELECT value FROM read_freshness UNION ALL SELECT value FROM read_freshness").Scan(&value); err != nil {
				t.Fatal(err)
			}
		}},
		{"scan-error", func(t *testing.T, s *Store) {
			var value int
			if err := s.reader.QueryRowContext(t.Context(), "SELECT 'not-an-integer' FROM read_freshness").Scan(&value); err == nil {
				t.Fatal("expected scan error")
			}
		}},
		{"list-json-error", func(t *testing.T, s *Store) {
			if _, err := listQuery[map[string]any](t.Context(), s, "SELECT '{' FROM read_freshness UNION ALL SELECT '{}' FROM read_freshness"); err == nil {
				t.Fatal("expected JSON error")
			}
		}},
		{"event-replay-json-error", func(t *testing.T, s *Store) {
			if _, err := s.writer.ExecContext(t.Context(), "INSERT INTO events(namespace,cluster,type,data,created) VALUES('default','orders','test','{',0),('default','orders','test','{}',0)"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ListEvents(t.Context(), EventFilter{Limit: 1000}); err == nil {
				t.Fatal("expected JSON error")
			}
		}},
		{"metric-range-json-error", func(t *testing.T, s *Store) {
			if _, err := s.writer.ExecContext(t.Context(), "INSERT INTO metrics(namespace,cluster,member,metric,timestamp,data) VALUES('default','orders','ordersm0','test',0,'{'),('default','orders','ordersm0','test',0,'{}')"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.QueryMetricSamples(t.Context(), MetricQuery{Start: time.Unix(0, 0)}); err == nil {
				t.Fatal("expected JSON error")
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, _ := testStore(t)
			if _, err := s.writer.ExecContext(t.Context(), "CREATE TABLE read_freshness(value INTEGER); INSERT INTO read_freshness VALUES(0)"); err != nil {
				t.Fatal(err)
			}
			assertPooledReaderValue(t, s, 0)
			test.exercise(t, s)
			if _, err := s.writer.ExecContext(t.Context(), "UPDATE read_freshness SET value=1"); err != nil {
				t.Fatal(err)
			}
			assertPooledReaderValue(t, s, 1)
		})
	}
}

func assertPooledReaderValue(t *testing.T, s *Store, want int) {
	t.Helper()
	var connections []*sql.Conn
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for range 4 {
		conn, err := s.reader.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var got int
		if err := conn.QueryRowContext(t.Context(), "SELECT value FROM read_freshness").Scan(&got); err != nil || got != want {
			t.Fatalf("reader value %d, want %d: %v", got, want, err)
		}
	}
}
