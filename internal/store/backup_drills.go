// SPDX-License-Identifier: Apache-2.0
package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

const terminalBackupDrillPhases = "('Succeeded','Failed')"

// CreateBackupDrill admits a durable drill. Replaying an ID for the same source
// is a no-op, never an overwrite of the controller's progress.
func (s *Store) CreateBackupDrill(ctx context.Context, drill domain.BackupDrill) error {
	ref := domain.ClusterRef{Namespace: drill.Namespace, Name: drill.Cluster}
	if err := ref.Validate(); err != nil {
		return err
	}
	if drill.ID == "" {
		return ErrInvalid
	}
	drill.Namespace = domain.NormalizedNamespace(drill.Namespace)
	data, err := json.Marshal(drill)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Serialize admission with source deletion. Existing reports outlive their
	// source for cleanup, but a missing source cannot admit new work.
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT 1 FROM clusters WHERE namespace=? AND name=?", drill.Namespace, drill.Cluster).Scan(&exists); err != nil {
		return typed(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO backup_drills(id,namespace,cluster,phase,trigger,created,updated,report) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", drill.ID, drill.Namespace, drill.Cluster, drill.Phase, drill.Trigger, drill.CreatedAt.UnixNano(), drill.CreatedAt.UnixNano(), data)
	if err != nil {
		return typed(err)
	}
	var namespace, cluster string
	if err = tx.QueryRowContext(ctx, "SELECT namespace,cluster FROM backup_drills WHERE id=?", drill.ID).Scan(&namespace, &cluster); err != nil {
		return typed(err)
	}
	if namespace != drill.Namespace || cluster != drill.Cluster {
		return ErrConflict
	}
	if commit, ok := ctx.Value(idempotentOperationKey{}).(idempotentOperationCommit); ok {
		if err = commit(ctx, tx, nil); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetBackupDrill(ctx context.Context, id string) (domain.BackupDrill, error) {
	var drill domain.BackupDrill
	var data []byte
	if err := s.reader.QueryRowContext(ctx, "SELECT report FROM backup_drills WHERE id=?", id).Scan(&data); err != nil {
		return drill, typed(err)
	}
	err := json.Unmarshal(data, &drill)
	return drill, err
}

// UpdateBackupDrill compares the persisted phase before replacing a report.
// False means the row disappeared or its phase/immutable identity changed.
// Terminal reports cannot be resumed. Retention is committed with completion.
func (s *Store) UpdateBackupDrill(ctx context.Context, drill domain.BackupDrill, expectedPhase string) (bool, error) {
	ref := domain.ClusterRef{Namespace: drill.Namespace, Name: drill.Cluster}
	if err := ref.Validate(); err != nil {
		return false, err
	}
	drill.Namespace = domain.NormalizedNamespace(drill.Namespace)
	data, err := json.Marshal(drill)
	if err != nil {
		return false, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "UPDATE backup_drills SET phase=?,updated=?,report=? WHERE id=? AND phase=? AND phase NOT IN "+terminalBackupDrillPhases+" AND namespace=? AND cluster=? AND created=? AND trigger=?", drill.Phase, time.Now().UTC().UnixNano(), data, drill.ID, expectedPhase, drill.Namespace, drill.Cluster, drill.CreatedAt.UnixNano(), drill.Trigger)
	if err != nil {
		return false, typed(err)
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if drill.Phase == "Succeeded" || drill.Phase == "Failed" {
		if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='setting' AND scope='' AND EXISTS (SELECT 1 FROM backup_drills WHERE namespace=? AND cluster=? AND phase IN "+terminalBackupDrillPhases+" AND records.key IN ('drills/cleanup/' || id,'drills/event/' || id))", drill.Namespace, drill.Cluster); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM backup_drills WHERE namespace=? AND cluster=? AND phase IN "+terminalBackupDrillPhases+" AND id NOT IN (SELECT id FROM backup_drills WHERE namespace=? AND cluster=? AND phase IN "+terminalBackupDrillPhases+" ORDER BY created DESC,id DESC LIMIT 50)", drill.Namespace, drill.Cluster, drill.Namespace, drill.Cluster)
		if err != nil {
			return false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ListBackupDrills returns newest first; API collection helpers own pagination.
func (s *Store) ListBackupDrills(ctx context.Context, ref domain.ClusterRef) ([]domain.BackupDrill, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return listQuery[domain.BackupDrill](ctx, s, "SELECT report FROM backup_drills WHERE namespace=? AND cluster=? ORDER BY created DESC,id DESC", domain.NormalizedNamespace(ref.Namespace), ref.Name)
}

// ListActiveBackupDrills includes cleanup work, even when CompletedAt is set.
// Reports remain discoverable after source deletion until cleanup is terminal.
func (s *Store) ListActiveBackupDrills(ctx context.Context) ([]domain.BackupDrill, error) {
	return listQuery[domain.BackupDrill](ctx, s, "SELECT report FROM backup_drills WHERE phase NOT IN "+terminalBackupDrillPhases+" ORDER BY created,id")
}

// BackupDrillTimes summarizes retained completed reports for a metrics scrape.
type BackupDrillTimes struct {
	Success             time.Time
	Failure             time.Time
	LastRestoreDuration time.Duration
}

// LatestBackupDrillTimes scans all retained terminal reports once, rather than
// issuing a query per cluster. Duration belongs to the latest successful report.
func (s *Store) LatestBackupDrillTimes(ctx context.Context) (map[domain.ClusterRef]BackupDrillTimes, error) {
	rows, err := s.reader.QueryContext(ctx, "SELECT report FROM backup_drills WHERE phase IN "+terminalBackupDrillPhases)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[domain.ClusterRef]BackupDrillTimes)
	for rows.Next() {
		var data []byte
		var drill domain.BackupDrill
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &drill); err != nil {
			return nil, err
		}
		if drill.CompletedAt == nil {
			continue
		}
		ref := domain.ClusterRef{Namespace: drill.Namespace, Name: drill.Cluster}
		times := out[ref]
		if drill.Phase == "Succeeded" && drill.CompletedAt.After(times.Success) {
			times.Success = *drill.CompletedAt
			times.LastRestoreDuration = time.Duration(drill.RestoreDurationSeconds) * time.Second
		}
		if drill.Phase == "Failed" && drill.CompletedAt.After(times.Failure) {
			times.Failure = *drill.CompletedAt
		}
		out[ref] = times
	}
	return out, rows.Err()
}
