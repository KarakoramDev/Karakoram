// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// sanitizeLegacyDiagnostics runs before recording migration 012. Each bounded
// transaction commits independently; interruption simply repeats safe work on the
// next startup. Only diagnostic JSON fields change, never row metadata or secrets.
func (s *Store) sanitizeLegacyDiagnostics(ctx context.Context) error {
	for _, target := range []struct{ table, column string }{
		{"records", "data"}, {"clusters", "status"}, {"events", "data"}, {"events", "details"},
		{"namespaces", "data"},
	} {
		var cursor int64
		for {
			tx, err := s.writer.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT rowid,%s FROM %s WHERE rowid>? ORDER BY rowid LIMIT 128", target.column, target.table), cursor)
			if err != nil {
				tx.Rollback()
				return err
			}
			type change struct {
				id   int64
				data []byte
			}
			changes := make([]change, 0)
			count := 0
			for rows.Next() {
				var id int64
				var data []byte
				if err = rows.Scan(&id, &data); err != nil {
					break
				}
				cursor = id
				count++
				safe := redact.DiagnosticsJSON(data)
				if target.table == "events" && target.column == "details" {
					safe = redact.EventDetailsJSON(data)
				}
				if !bytes.Equal(data, safe) {
					changes = append(changes, change{id, safe})
				}
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
			if err != nil {
				tx.Rollback()
				return err
			}
			for _, change := range changes {
				if _, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s=? WHERE rowid=?", target.table, target.column), change.data, change.id); err != nil {
					break
				}
			}
			if err != nil {
				tx.Rollback()
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			if count < 128 {
				break
			}
		}
	}
	return nil
}
