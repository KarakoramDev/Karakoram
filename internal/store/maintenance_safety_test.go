// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestLateTokenTouchPreservesEditsAndDeletion(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "edited"
		if deleted {
			name = "deleted"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := testStore(t)
			token := &domain.ApiToken{ID: "usage", Name: "original", Owner: "owner", Hash: "original-hash"}
			if err := s.CreateApiToken(t.Context(), token); err != nil {
				t.Fatal(err)
			}
			conn, err := s.writer.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			at := time.Now().UTC()
			done := make(chan error, 1)
			go func() { done <- s.TouchApiToken(t.Context(), token.ID, at) }()
			// The touch cannot obtain the sole writer until after this newer
			// lifecycle mutation commits. Its old metadata must not replace it.
			if deleted {
				_, err = conn.ExecContext(t.Context(), "DELETE FROM records WHERE kind='api-token' AND key=?", token.ID)
			} else {
				_, err = conn.ExecContext(t.Context(), "UPDATE records SET data=json_set(data,'$.name','renamed','$.expiresAt',?),hash='new-hash' WHERE kind='api-token' AND key=?", at.Add(-time.Hour).Format(time.RFC3339Nano), token.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			err = <-done
			got, readErr := s.GetApiToken(t.Context(), token.ID)
			if deleted {
				if !errors.Is(err, ErrNotFound) || !errors.Is(readErr, ErrNotFound) {
					t.Fatalf("deleted token resurrected: touch=%v read=%v", err, readErr)
				}
				return
			}
			if err != nil || readErr != nil {
				t.Fatalf("touch=%v read=%v", err, readErr)
			}
			if got.Name != "renamed" || got.Hash != "new-hash" || got.ExpiresAt == nil || !got.ExpiresAt.Equal(at.Add(-time.Hour)) || got.LastUsedAt == nil || !got.LastUsedAt.Equal(at) {
				t.Fatal("late usage touch overwrote edited token fields or failed to update metadata")
			}
		})
	}
}

func TestRetentionIndexUpgradeAndOlderMigrationRunner(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	// Reconstruct the exact pre-011 state with a populated metrics table.
	if _, err := s.writer.ExecContext(ctx, "DROP INDEX metrics_retention; DELETE FROM schema_migrations WHERE name='011_metrics_retention.sql'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, `WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100000) INSERT INTO metrics(cluster,member,metric,timestamp,data) SELECT 'cluster','member','up',i,'{}' FROM n`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := s.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("011 index build over 100000 pre-existing rows: %s (local fixture, not a platform SLA)", time.Since(started))
	var count int
	if err := s.reader.QueryRowContext(ctx, "SELECT count(*) FROM metrics").Scan(&count); err != nil || count != 100000 {
		t.Fatalf("migration changed samples: count=%d err=%v", count, err)
	}
	// The older runner enumerates only its own embedded migration files; an
	// unknown newer entry must not make it replay any earlier migration.
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "011_metrics_retention.sql" {
			continue
		}
		if err := s.reader.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE name=?", entry.Name()).Scan(&count); err != nil || count != 1 {
			t.Fatalf("older migration %s would replay: count=%d err=%v", entry.Name(), count, err)
		}
	}
	// Exercise the old insert/query/delete SQL with the additive index present.
	if err := s.InsertMetricSamples(ctx, []domain.MetricSample{{Cluster: "old-binary", Member: "m0", Metric: "up", Timestamp: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "DELETE FROM metrics WHERE timestamp<?", int64(100001)); err != nil {
		t.Fatal(err)
	}
	samples, err := s.QueryMetricSamples(ctx, MetricQuery{Cluster: "old-binary"})
	if err != nil || len(samples) != 1 {
		t.Fatalf("older SQL incompatible: samples=%d err=%v", len(samples), err)
	}
	if err := s.migrate(context.Background()); err != nil {
		t.Fatalf("upgrade migration is not idempotent: %v", err)
	}
}
