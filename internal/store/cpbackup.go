// SPDX-License-Identifier: Apache-2.0
package store

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"time"
)

func (s *Store) EnqueueControlPlaneBackup(ctx context.Context, b *domain.ControlPlaneBackup, cursor json.RawMessage) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO control_plane_backups(id,phase,created,data) VALUES(?,?,?,?)", b.ID, b.Phase, b.CreatedAt.UnixNano(), data); err != nil {
		return typed(err)
	}
	if cursor != nil {
		if err = put(ctx, tx, "setting", "", "controlPlaneBackup/cursor", cursor, "", "", "", false); err != nil {
			return err
		}
	}
	// The backup record itself is accepted work; persist its recovery response
	// with admission, just like cluster operation journals.
	if commit, ok := ctx.Value(idempotentOperationKey{}).(idempotentOperationCommit); ok {
		if err := commit(ctx, tx, nil); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListControlPlaneBackups(ctx context.Context) ([]domain.ControlPlaneBackup, error) {
	return listQuery[domain.ControlPlaneBackup](ctx, s, "SELECT data FROM control_plane_backups ORDER BY created DESC,id DESC")
}

func (s *Store) SaveControlPlaneBackup(ctx context.Context, b *domain.ControlPlaneBackup, expected domain.Phase) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	result, err := s.writer.ExecContext(ctx, "UPDATE control_plane_backups SET phase=?,data=? WHERE id=? AND phase=?", b.Phase, data, b.ID, expected)
	if err != nil {
		return typed(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) DeleteControlPlaneBackup(ctx context.Context, id string) error {
	_, err := s.writer.ExecContext(ctx, "DELETE FROM control_plane_backups WHERE id=? AND phase='Succeeded'", id)
	return err
}

// LatestControlPlaneBackupTimes returns completion times for state metrics.
func (s *Store) LatestControlPlaneBackupTimes(ctx context.Context) (success, failure time.Time, err error) {
	runs, err := s.ListControlPlaneBackups(ctx)
	if err != nil {
		return success, failure, err
	}
	for _, b := range runs {
		if b.CompletedAt == nil {
			continue
		}
		if b.Phase == domain.PhaseSucceeded && b.CompletedAt.After(success) {
			success = *b.CompletedAt
		}
		if b.Phase == domain.PhaseFailed && b.CompletedAt.After(failure) {
			failure = *b.CompletedAt
		}
	}
	return
}
