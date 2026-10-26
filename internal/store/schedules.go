// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ScheduleCursor is the last admitted occurrence for one schedule and cluster
// incarnation. It survives both retention of backups and control-plane restarts.
type ScheduleCursor struct {
	ClusterCreatedAt time.Time             `json:"clusterCreatedAt"`
	Spec             domain.BackupSchedule `json:"spec"`
	LastDueAt        time.Time             `json:"lastDueAt"`
}

type scheduleAdvance struct {
	cursor ScheduleCursor
	due    time.Time
}

// LoadScheduleCursor initializes a newly observed or edited schedule at now.
// Existing schedules retain their cursor during downtime and storage failures.
func (s *Store) LoadScheduleCursor(ctx context.Context, c *domain.Cluster, spec domain.BackupSchedule, now time.Time) (ScheduleCursor, error) {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return ScheduleCursor{}, err
	}
	defer tx.Rollback()
	var data []byte
	err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='schedule-cursor' AND scope=? AND key=?", c.Ref().Key(), spec.Name).Scan(&data)
	var cursor ScheduleCursor
	if err == nil {
		if err = json.Unmarshal(data, &cursor); err != nil {
			return cursor, err
		}
		if cursor.ClusterCreatedAt.Equal(c.CreatedAt) && cursor.Spec == spec {
			return cursor, nil
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return cursor, err
	}
	cursor = ScheduleCursor{ClusterCreatedAt: c.CreatedAt, Spec: spec, LastDueAt: now.UTC()}
	if err = put(ctx, tx, "schedule-cursor", c.Ref().Key(), spec.Name, cursor, "", "", "", false); err != nil {
		return cursor, err
	}
	return cursor, tx.Commit()
}

// EnqueueScheduledBackup commits backup, operation and occurrence advancement
// atomically. Concurrent callbacks cannot admit the same occurrence twice.
func (s *Store) EnqueueScheduledBackup(ctx context.Context, backup *domain.Backup, op *domain.Operation, cursor ScheduleCursor, due time.Time) error {
	return s.enqueueBackup(ctx, backup, op, &scheduleAdvance{cursor: cursor, due: due})
}

func advanceSchedule(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, advance *scheduleAdvance) error {
	if advance == nil {
		return nil
	}
	var data []byte
	if err := tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='schedule-cursor' AND scope=? AND key=?", ref.Key(), advance.cursor.Spec.Name).Scan(&data); err != nil {
		return typed(err)
	}
	var current ScheduleCursor
	if err := json.Unmarshal(data, &current); err != nil {
		return err
	}
	if !current.ClusterCreatedAt.Equal(advance.cursor.ClusterCreatedAt) || current.Spec != advance.cursor.Spec || !current.LastDueAt.Equal(advance.cursor.LastDueAt) || !advance.due.After(current.LastDueAt) {
		return ErrConflict
	}
	current.LastDueAt = advance.due.UTC()
	return put(ctx, tx, "schedule-cursor", ref.Key(), current.Spec.Name, current, "", "", "", false)
}
