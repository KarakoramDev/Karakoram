// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"modernc.org/sqlite"
)

var readerCancelFunctionID atomic.Uint64

func TestCanceledReaderQueryReleasesSnapshot(t *testing.T) {
	// The deliberately blocked driver mutex prevents virtual time advancing.
	// Keep the original cancellation scheduling delay on real time.
	ticks := time.NewTicker(100 * time.Millisecond)
	defer ticks.Stop()
	synctest.Test(t, func(t *testing.T) { canceledReaderQueryReleasesSnapshot(t, ticks.C) })
}

func canceledReaderQueryReleasesSnapshot(t *testing.T, realTicks <-chan time.Time) {
	ctx := t.Context()
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := make(chan struct{})
	function := fmt.Sprintf("karakoram_read_started_%d", readerCancelFunctionID.Add(1))
	if err := sqlite.RegisterScalarFunction(function, 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
		close(started)
		<-readCtx.Done()
		// Let the driver's cancellation watcher mark the query canceled before
		// its first row returns. Interruption itself is delayed below.
		// One tick may already be buffered; three retain at least the original
		// 100ms delay without requiring the fake clock to advance.
		for range 3 {
			<-realTicks
		}
		return int64(1), nil
	}); err != nil {
		t.Fatal(err)
	}
	s, _ := testStore(t)
	if _, err := s.writer.ExecContext(ctx, "CREATE TABLE read_freshness(value INTEGER); INSERT INTO read_freshness VALUES(0)"); err != nil {
		t.Fatal(err)
	}
	// Initialize every reader connection, leaving three borrowed so the
	// canceled query's connection is the next one reused from the pool.
	var held []*sql.Conn
	for range 4 {
		conn, err := s.reader.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
		var initial int
		if err := conn.QueryRowContext(ctx, "SELECT value FROM read_freshness").Scan(&initial); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	conn := held[3]
	// modernc serializes sqlite3_interrupt and connection closure with this
	// mutex. Holding it reproduces the cancellation watcher's preemption
	// between marking the query canceled and actually interrupting SQLite.
	var interruptLock sync.Locker
	if err := conn.Raw(func(raw any) error {
		var ok bool
		interruptLock, ok = raw.(sync.Locker)
		if !ok {
			return fmt.Errorf("SQLite connection does not expose its interruption mutex")
		}
		interruptLock.Lock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		interruptLock.Unlock()
		// The delayed cancellation watcher reads the driver's db handle
		// before taking this mutex. Join it before Store.Close cleanup, not
		// while the lock is held. All snapshot/reuse assertions run first.
		synctest.Wait()
	}()
	queryDone := make(chan error, 1)
	go func() {
		rows, err := conn.QueryContext(readCtx, "SELECT value, "+function+"() FROM read_freshness")
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
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE read_freshness SET value=1"); err != nil {
		t.Fatal(err)
	}
	var got int
	if err := s.reader.QueryRowContext(ctx, "SELECT value FROM read_freshness").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("reader retained a canceled query snapshot: got %d, committed 1", got)
	}
	for _, conn := range held[:3] {
		if err := conn.QueryRowContext(ctx, "SELECT value FROM read_freshness").Scan(&got); err != nil || got != 1 {
			t.Fatalf("reader missed committed update: got %d, error %v", got, err)
		}
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
