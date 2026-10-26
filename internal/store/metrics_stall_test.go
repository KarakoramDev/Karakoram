// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestMetricPruningBoundsWriterWork(t *testing.T) {
	s, _ := testStore(t)
	if _, err := s.writer.ExecContext(t.Context(), `WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<4097) INSERT INTO metrics(cluster,member,metric,timestamp,data) SELECT 'cluster','member','up',1,'{}' FROM n`); err != nil {
		t.Fatal(err)
	}
	removed, err := s.PruneMetricSamples(t.Context(), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 4096 {
		t.Fatalf("removed %d rows; one sweep must leave work beyond its 4096-row budget", removed)
	}
	removed, err = s.PruneMetricSamples(t.Context(), time.Unix(1, 0))
	if err != nil || removed != 1 {
		t.Fatalf("next sweep removed=%d err=%v", removed, err)
	}
	var detail string
	rows, err := s.reader.QueryContext(t.Context(), "EXPLAIN QUERY PLAN SELECT id FROM metrics WHERE timestamp<? ORDER BY timestamp,id LIMIT 256", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var text string
		if err := rows.Scan(&id, &parent, &unused, &text); err != nil {
			t.Fatal(err)
		}
		detail += text
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(detail, "metrics_retention") {
		t.Fatalf("retention lookup lacks timestamp index: %s", detail)
	}
}

func TestMetricPruningBoundsWriterWait(t *testing.T) {
	s, _ := testStore(t)
	conn, err := s.writer.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done := make(chan error, 1)
	go func() { _, err := s.PruneMetricSamples(context.Background(), time.Now()); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("writer wait err=%v", err)
		}
	case <-time.After(3 * time.Second):
		_ = conn.Close()
		<-done
		t.Fatal("retention waited indefinitely for the SQLite writer")
	}
}

func TestWriterWaitDiagnostics(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	logWriterWait(logger, "PruneMetricSamples", time.Second)
	if out.Len() != 0 {
		t.Fatal("fast writer admission emitted a warning")
	}
	logWriterWait(logger, "PruneMetricSamples", 3*time.Second)
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "WARN" || record["operation"] != "PruneMetricSamples" || record["duration"] != float64(3*time.Second) {
		t.Fatalf("missing writer diagnostic fields: %v", record)
	}
}
