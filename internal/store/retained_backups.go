// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// MarkRetainedBackupDeleting records direct cleanup intent while the cluster is
// absent and no restore depends on the deletion closure. The returned fence
// belongs only to this attempt; retries invalidate earlier rollback authority.
func (s *Store) MarkRetainedBackupDeleting(ctx context.Context, b *domain.Backup) (int64, error) {
	if err := (domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}).Validate(); err != nil {
		return 0, err
	}
	expected := *b
	expected.Namespace = domain.NormalizedNamespace(expected.Namespace)
	before, err := json.Marshal(&expected)
	if err != nil {
		return 0, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Released records can omit the default namespace and other zero fields.
	// Compare the same decoded view GetBackup exposes, not its re-encoded bytes
	// against the original representation. Keep the raw row as the write CAS.
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='backup' AND scope=? AND key=?", domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}.Key(), b.Name).Scan(&raw); err != nil {
		return 0, typed(err)
	}
	var current domain.Backup
	if err = json.Unmarshal(redact.DiagnosticsJSON(raw), &current); err != nil {
		return 0, err
	}
	canonical, err := json.Marshal(&current)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(before, canonical) {
		return 0, ErrConflict
	}
	if err = refuseBackupDependents(ctx, tx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}, b.Name, false, false); err != nil {
		return 0, err
	}
	deleting := expected
	deleting.Phase = domain.PhaseDeleting
	data, err := json.Marshal(&deleting)
	if err != nil {
		return 0, err
	}
	var version int64
	err = tx.QueryRowContext(ctx, "UPDATE records SET data=?,phase='Deleting',updated=MAX(updated+1,?) WHERE kind='backup' AND scope=? AND key=? AND data=? AND NOT EXISTS (SELECT 1 FROM clusters WHERE namespace=? AND name=?) RETURNING updated", data, time.Now().UTC().UnixNano(), domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}.Key(), b.Name, raw, domain.NormalizedNamespace(b.Namespace), b.Cluster).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrConflict
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	*b = deleting
	return version, nil
}

// RollbackRetainedBackupDeletion is only for a newly marked successful backup
// and a proven non-destructive refusal, not arbitrary remote errors. Inventory
// and attempt fences prevent rollback over changed records or concurrent cleanup.
func (s *Store) RollbackRetainedBackupDeletion(ctx context.Context, b *domain.Backup, version int64) error {
	if b.Phase != domain.PhaseDeleting {
		return ErrConflict
	}
	before, err := json.Marshal(b)
	if err != nil {
		return err
	}
	restored := *b
	restored.Phase = domain.PhaseSucceeded
	data, err := json.Marshal(&restored)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = refuseBackupDependents(ctx, tx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}, b.Name, false, false); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase='Succeeded',updated=MAX(updated+1,?) WHERE kind='backup' AND scope=? AND key=? AND phase='Deleting' AND data=? AND updated=? AND NOT EXISTS (SELECT 1 FROM clusters WHERE namespace=? AND name=?)", data, time.Now().UTC().UnixNano(), domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}.Key(), b.Name, before, version, domain.NormalizedNamespace(b.Namespace), b.Cluster)
	if err = affected(result, err); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*b = restored
	return nil
}
