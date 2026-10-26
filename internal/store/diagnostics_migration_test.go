// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestLegacyDiagnosticsReadAndDurableMigration(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	diagnostic := `AuthenticationFailed credentials: {"SCRAM-SHA-256":{"salt":"fake-legacy-salt","storedKey":"fake-legacy-stored","serverKey":"fake-legacy-server"}}`
	raw, _ := json.Marshal(map[string]any{"message": diagnostic, "error": diagnostic, "step": diagnostic, "name": "unchanged", "unknown": json.RawMessage(`9007199254740993`)})
	// Insert directly, bypassing all modern write boundaries, and exceed one batch.
	for i := range 260 {
		if _, err := s.writer.ExecContext(ctx, `INSERT INTO records(kind,scope,key,data,created,updated) VALUES('operation','demo',?,?,7,9)`, fmt.Sprintf("legacy-%03d", i), raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.writer.ExecContext(ctx, `INSERT INTO events(cluster,type,data,created) VALUES('demo','Warning',?,7)`, raw); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"backup", "restore"} {
		if _, err := s.writer.ExecContext(ctx, `INSERT INTO records(kind,scope,key,data,created,updated) VALUES(?,'demo','legacy',?,7,9)`, kind, raw); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := s.CreateCluster(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	status, _ := json.Marshal(map[string]any{"message": diagnostic, "conditions": []any{map[string]any{"type": "Ready", "message": diagnostic}}})
	if _, err := s.writer.ExecContext(ctx, `UPDATE clusters SET status=? WHERE name='demo'`, status); err != nil {
		t.Fatal(err)
	}
	ordinary := []byte(`{"message":"ordinary message","unknown":9007199254740993}`)
	if _, err := s.writer.ExecContext(ctx, `INSERT INTO records(kind,scope,key,data,created,updated) VALUES('backup','demo','ordinary',?,7,9)`, ordinary); err != nil {
		t.Fatal(err)
	}
	check := func(data []byte) {
		t.Helper()
		for _, value := range []string{"fake-legacy-salt", "fake-legacy-stored", "fake-legacy-server"} {
			if bytes.Contains(data, []byte(value)) {
				t.Fatal("legacy verifier escaped")
			}
		}
	}
	op, err := s.GetOperation(ctx, "legacy-000")
	if err != nil {
		t.Fatal(err)
	}
	served, _ := json.Marshal(op)
	check(served)
	ops, err := s.ListOperations(ctx, domain.ClusterRef{Name: "demo"})
	if err != nil || len(ops) != 260 {
		t.Fatal(err, len(ops))
	}
	served, _ = json.Marshal(ops)
	check(served)
	events, err := s.ListEvents(ctx, EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	served, _ = json.Marshal(events)
	check(served)
	c, err := s.GetCluster(ctx, cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	served, _ = json.Marshal(c.Status)
	check(served)
	for _, kind := range []string{"backup", "restore"} {
		var record map[string]any
		if _, _, err := s.get(ctx, kind, "demo", "legacy", &record); err != nil {
			t.Fatal(err)
		}
		served, _ = json.Marshal(record)
		check(served)
	}
	if _, err = s.writer.ExecContext(ctx, `DELETE FROM schema_migrations WHERE name='012_sanitize_diagnostics.sql'`); err != nil {
		t.Fatal(err)
	}
	if err = s.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT data FROM records", "SELECT data FROM events", "SELECT status FROM clusters"} {
		rows, err := s.writer.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var data []byte
			if err = rows.Scan(&data); err != nil {
				t.Fatal(err)
			}
			check(data)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	var durable []byte
	var created, updated int64
	if err = s.writer.QueryRowContext(ctx, `SELECT data,created,updated FROM records WHERE key='legacy-000'`).Scan(&durable, &created, &updated); err != nil {
		t.Fatal(err)
	}
	check(durable)
	if !strings.Contains(string(durable), "9007199254740993") || created != 7 || updated != 9 {
		t.Fatal("migration changed non-diagnostic data")
	}
	before := append([]byte(nil), durable...)
	if err = s.sanitizeLegacyDiagnostics(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.writer.QueryRowContext(ctx, `SELECT data FROM records WHERE key='legacy-000'`).Scan(&durable); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, durable) {
		t.Fatal("migration is not idempotent")
	}
	if err = s.writer.QueryRowContext(ctx, `SELECT data FROM records WHERE key='ordinary'`).Scan(&durable); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ordinary, durable) {
		t.Fatal("ordinary record changed")
	}
	var n int
	if err = s.writer.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations WHERE name='012_sanitize_diagnostics.sql'`).Scan(&n); err != nil || n != 1 {
		t.Fatal(err, n)
	}
}
