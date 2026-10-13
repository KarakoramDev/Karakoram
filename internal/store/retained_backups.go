// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// MarkRetainedBackupDeleting records direct S3 cleanup intent only while the
// source cluster is absent. A retry resumes the same immutable backup record.
func (s *Store) MarkRetainedBackupDeleting(ctx context.Context, b *domain.Backup) error {
	if err := (domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}).Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b.Phase = domain.PhaseDeleting
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase='Deleting',updated=? WHERE kind='backup' AND scope=? AND key=? AND NOT EXISTS (SELECT 1 FROM clusters WHERE namespace=? AND name=?)", data, time.Now().UTC().UnixNano(), domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}.Key(), b.Name, domain.NormalizedNamespace(b.Namespace), b.Cluster)
	if err := affected(result, err); err != nil {
		return err
	}
	return tx.Commit()
}
