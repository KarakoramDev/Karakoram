// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// cleanupBackupArtifacts uses only the control-plane storage binding. Runtime
// absence is established before this stage, so PBM cannot recreate deleted data.
// Inventory survives unsuccessful removal and makes retained artifacts discoverable.
func (p *Provider) cleanupBackupArtifacts(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.BackupCleanupStarted == nil {
		start := now(rc)
		s.BackupCleanupStarted = &start
		return false, nil // Persist the deadline before object-store side effects.
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	storages := slices.Clone(s.UsedStorages)
	if s.PBMStorage != "" && !slices.Contains(storages, s.PBMStorage) {
		storages = append(storages, s.PBMStorage)
	}
	current := backups[:0]
	for _, b := range backups {
		if b.PBMPrefix != "" && b.PBMPrefix != s.PBMPrefix && !strings.HasSuffix(b.PBMPrefix, "/"+s.PBMPrefix) {
			continue // Prior incarnations and restore sources are not ours.
		}
		current = append(current, b)
		if !slices.Contains(storages, b.Storage) {
			storages = append(storages, b.Storage)
		}
	}
	backups = current
	started := s.PBMStorage != "" || s.PBMHash != "" || len(s.UsedStorages) != 0
	for _, b := range backups {
		started = started || b.PBMName != "" || b.StartedAt != nil
	}
	if !started {
		if inventory, ok := rc.Store.(interface {
			ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
		}); ok {
			operations, err := inventory.ListOperations(ctx, rc.Cluster.Ref())
			if err != nil {
				return false, err
			}
			for _, op := range operations {
				if op.Kind != domain.OperationBackup || len(op.Checkpoint) == 0 {
					continue
				}
				var checkpoint struct {
					Step string `json:"step"`
				}
				if err := json.Unmarshal(op.Checkpoint, &checkpoint); err != nil {
					return false, err
				}
				started = started || checkpoint.Step != "" && checkpoint.Step != "configure"
			}
		} else {
			started = true // Missing inventory cannot prove no native start.
		}
	}
	if !started {
		for _, b := range backups {
			if err := deleteBackupRecord(ctx, rc, b); err != nil {
				return false, err
			}
		}
		s.CleanupBackups = true
		return true, nil
	}
	if len(storages) == 0 {
		storages = append(storages, "") // Native start without a binding must be reported retained.
	}
	for _, name := range storages {
		if slices.Contains(s.RetainedStorages, name) {
			continue
		}
		if !slices.Contains(s.FinalizedStorages, name) {
			storage, failure := rc.Store.GetBackupStorage(ctx, rc.Cluster.Namespace, name)
			scope := fmt.Sprintf("storage %q recorded PBM namespace %q", name, s.PBMPrefix)
			if failure == nil {
				directory, e := backupstorage.ClusterDirectory(*storage, rc.Cluster.Name, s.UID)
				failure = e
				scope = fmt.Sprintf("storage %q bucket %q candidate prefix %q; recorded PBM namespace %q", name, storage.Bucket, directory, s.PBMPrefix)
				for _, b := range backups {
					if b.Storage == name && b.PBMPrefix != "" && domain.BackupStoragePrefix(b.PBMPrefix)+"/" != directory {
						failure = errors.New("recorded backup prefix differs from the current storage binding; restore its original prefix before manual removal")
						scope += fmt.Sprintf("; backup %q recorded prefix %q", b.Name, b.PBMPrefix+"/")
					}
				}
			}
			deadline := s.BackupCleanupStarted.Add(2 * time.Minute)
			if failure == nil {
				if !now(rc).Before(deadline) {
					failure = errors.New("two-minute artifact cleanup deadline exhausted")
				} else {
					pass, cancel := context.WithTimeout(ctx, deadline.Sub(now(rc)))
					failure = p.deleteBackupNamespace(pass, rc, s, *storage)
					cancel()
					if errors.Is(failure, backupstorage.ErrDeletionInProgress) {
						return false, nil
					}
				}
			}
			if failure != nil {
				// Do not persist native/storage errors, which may contain credentials.
				message := fmt.Sprintf("Backup artifacts may remain in %s (cluster incarnation %s). Runtime deletion continues; backup inventory is retained. Restore the original backup-store binding, verify recorded ownership, and remove only the exact owned incarnation prefix with object-store tooling; never guess a missing/ambiguous prefix. Then remove retained backup records. Storage cleanup was unavailable, unsafe, or exceeded its two-minute bound.", scope, s.UID)
				if rc.Events == nil {
					return false, errors.New("cannot record retained backup artifact outcome")
				}
				if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "BackupArtifactsRetained", Message: message, CreatedAt: now(rc)}); err != nil {
					return false, err
				}
				s.RetainedStorages = append(s.RetainedStorages, name)
				return false, nil
			}
			s.FinalizedStorages = append(s.FinalizedStorages, name)
			if !slices.Contains(s.CleanedStorages, name) {
				s.CleanedStorages = append(s.CleanedStorages, name)
			}
		}
		for _, b := range backups {
			if b.Storage == name {
				if err := deleteBackupRecord(ctx, rc, b); err != nil {
					return false, err
				}
			}
		}
	}
	s.CleanupBackups = true
	return true, nil
}

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
		return errors.New("cluster lacks a verified PBM namespace")
	}
	absent, err := backupRuntimesAbsent(ctx, rc, s)
	if err != nil {
		return err
	}
	if !absent {
		return errors.New("database runtimes still exist")
	}
	deleteObjects := p.deleteClusterObjects
	if deleteObjects == nil {
		deleteObjects = backupstorage.DeleteCluster
	}
	return deleteObjects(ctx, storage, rc.Cluster.Name, s.UID)
}
