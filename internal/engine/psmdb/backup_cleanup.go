// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Never fall back to direct object deletion merely because an agent is
// unreachable: a live PBM process may still be writing snapshots or PITR chunks.
func backupRuntimesAbsent(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range s.Members {
		if m.Role == agentapi.RoleRouter {
			continue
		}
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return false, err
		}
		if service != nil {
			return false, nil
		}
	}
	return true, nil
}

func backupCleanupError(storage string, err error) error {
	return fmt.Errorf("cannot delete backup artifacts from storage %q: %w; repair storage access and retry, or delete with --keep-backups to retain backup data", storage, err)
}

func deleteBackupRecord(ctx context.Context, rc *engine.Context, b domain.Backup) error {
	if b.RecoveryIdentity != "" {
		if err := rc.Secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin"); err != nil && !missing(err) {
			return err
		}
	}
	if err := rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); err != nil && !missing(err) {
		return err
	}
	return nil
}

// Namespace removal is safe only after no database runtime can recreate PBM's
// initialization marker. The incarnation identity must be durable and exact.
func (p *Provider) deleteBackupNamespace(ctx context.Context, rc *engine.Context, s *internalState, storage domain.BackupStorage) error {
	if s.UID == "" || s.PBMPrefix != "karakoram/"+rc.Cluster.Name+"-"+s.UID {
		return fmt.Errorf("cannot delete backup artifacts without runtimes: cluster lacks a verified PBM namespace; resume the cluster or delete with --keep-backups")
	}
	absent, err := backupRuntimesAbsent(ctx, rc, s)
	if err != nil {
		return err
	}
	if !absent {
		return fmt.Errorf("cannot finalize backup storage while database runtimes still exist; finish runtime deletion and retry, or delete with --keep-backups")
	}
	deleteObjects := p.deleteClusterObjects
	if deleteObjects == nil {
		deleteObjects = backupstorage.DeleteCluster
	}
	if err := deleteObjects(ctx, storage, rc.Cluster.Name, s.UID); err != nil {
		return backupCleanupError(storage.Name, err)
	}
	return nil
}
