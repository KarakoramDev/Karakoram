// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type backupRestoreOwner struct {
	Target    domain.ClusterRef `json:"target"`
	Operation string            `json:"operation"`
	Restore   string            `json:"restore"`
}

// AcquireBackupRestore serializes native restores sharing one source backup.
// Ownership has no clock expiry: a slow but live restore must not be fenced out.
// Deleted targets and terminal restores/operations relinquish ownership atomically
// when another target requests it, including after cancellation or a worker crash.
func (s *Store) AcquireBackupRestore(ctx context.Context, source domain.ClusterRef, backup string, target domain.ClusterRef, restore, operation string) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	if err := target.Validate(); err != nil {
		return false, err
	}
	if backup == "" || restore == "" || operation == "" {
		return false, ErrInvalid
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var data []byte
	err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='backup-restore-owner' AND scope=? AND key=?", source.Key(), backup).Scan(&data)
	if err == nil {
		var owner backupRestoreOwner
		if err = json.Unmarshal(data, &owner); err != nil {
			return false, err
		}
		if owner.Target.Key() == target.Key() && owner.Operation == operation && owner.Restore == restore {
			return true, tx.Commit()
		}
		var exists int
		err = tx.QueryRowContext(ctx, "SELECT count(*) FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(owner.Target.Namespace), owner.Target.Name).Scan(&exists)
		if err != nil {
			return false, err
		}
		active := exists != 0
		for _, record := range []struct{ kind, key string }{{"restore", owner.Restore}, {"operation", owner.Operation}} {
			var phase string
			err = tx.QueryRowContext(ctx, "SELECT phase FROM records WHERE kind=? AND scope=? AND key=?", record.kind, owner.Target.Key(), record.key).Scan(&phase)
			if errors.Is(err, sql.ErrNoRows) {
				if record.kind == "restore" {
					active = false
				}
				continue
			}
			if err != nil {
				return false, err
			}
			if phase == string(domain.PhaseSucceeded) || phase == string(domain.PhaseFailed) || phase == string(domain.PhaseCancelled) {
				active = false
			}
		}
		if active {
			return false, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	owner := backupRestoreOwner{Target: target, Operation: operation, Restore: restore}
	if err = put(ctx, tx, "backup-restore-owner", source.Key(), backup, owner, "", target.Key()+"\x00"+operation, "", false); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// ReleaseBackupRestore cannot release a successor's ownership.
func (s *Store) ReleaseBackupRestore(ctx context.Context, source domain.ClusterRef, backup string, target domain.ClusterRef, operation string) error {
	_, err := s.writer.ExecContext(ctx, "DELETE FROM records WHERE kind='backup-restore-owner' AND scope=? AND key=? AND hash=?", source.Key(), backup, target.Key()+"\x00"+operation)
	return err
}
