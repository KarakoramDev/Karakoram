// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Existing backup endpoints retain their namespace/cluster RBAC object even after
// cluster deletion. All other cluster endpoints still require a live resource.
func (s *Server) retainedBackup(ctx context.Context, name string, in input) response {
	if name == "ListBackups" {
		items, err := s.Store.ListBackups(ctx, clusterRef(ctx, in.Cluster))
		if err != nil {
			return s.failure(err)
		}
		if len(items) == 0 {
			return fail(404, "not_found", "Cluster has no retained backups")
		}
		out := make([]api.Backup, 0, len(items))
		for _, b := range items {
			if s.allowed(ctx, "backups", "read", clusterObject(b.Namespace, b.Cluster)) {
				out = append(out, backupAPI(b))
			}
		}
		return listResponse(s, ctx, out, nil)
	}
	b, err := s.Store.GetBackup(ctx, clusterRef(ctx, in.Cluster), in.Backup)
	if errors.Is(err, store.ErrNotFound) && name == "DeleteBackup" {
		return response{status: 204}
	}
	if err != nil {
		return s.failure(err)
	}
	action := "read"
	if name == "DeleteBackup" {
		action = "delete"
	}
	if !s.allowed(ctx, "backups", action, clusterObject(b.Namespace, b.Cluster)) {
		return fail(403, "forbidden", "Permission denied")
	}
	if name == "GetBackup" {
		return ok(backupAPI(*b))
	}
	if b.PBMName == "" && b.Location == "" && b.Postgres == nil && (b.Phase == domain.PhaseFailed || b.Phase == domain.PhasePending) {
		op := &domain.Operation{Namespace: b.Namespace, Cluster: b.Cluster, Kind: domain.OperationDeleteBackup, Actor: store.Actor(ctx)}
		if err = s.Store.EnqueueBackupDeletion(ctx, b, op); err != nil {
			return s.failure(err)
		}
		return response{status: 204, operationID: op.ID}
	}
	storage, err := s.Store.GetBackupStorage(ctx, b.Namespace, b.Storage)
	if err != nil {
		return s.failure(err)
	}
	var deleteBackup func(context.Context, domain.BackupStorage, domain.Backup) error
	if b.Provider == "" || b.Provider == "psmdb" {
		if _, _, err := backupstorage.SnapshotKeys(*storage, *b); err != nil {
			return s.failure(&domain.ResourceError{Resource: "backup", Name: b.Name, Reason: domain.ReasonInvalidState, State: string(b.Phase), Err: store.ErrConflict, Message: "backup " + b.Name + " lacks a safe recorded snapshot namespace; preserve its record and repair its storage/provenance before deletion"})
		}
		deleteBackup = backupstorage.DeleteSnapshot
	} else {
		p, err := s.Registry.Get(b.Provider)
		if err != nil {
			return s.failure(engine.ErrNotSupported)
		}
		deleter, ok := p.(engine.RetainedBackupDeleter)
		if !ok {
			return s.failure(engine.ErrNotSupported)
		}
		deleteBackup = deleter.DeleteRetainedBackup
	}
	if err := s.Store.MarkRetainedBackupDeleting(ctx, b); err != nil {
		return s.failure(err)
	}
	if err := deleteBackup(ctx, *storage, *b); err != nil {
		// Keep Deleting inventory and recovery credentials for a retry after
		// cancellation, partial storage failure, or a control-plane restart.
		return s.failure(err)
	}
	if (b.Provider == "" || b.Provider == "psmdb") && b.RecoveryIdentity != "" {
		if err := s.Context.Secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin"); err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, engine.ErrNotFound) {
			return s.failure(err)
		}
	}
	if err := s.Store.DeleteBackup(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}, b.Name); err != nil && !errors.Is(err, store.ErrNotFound) {
		return s.failure(err)
	}
	return response{status: 204}
}

// Reject legacy/unrecoverable retained snapshots before provisioning a clone
// or enqueueing destructive restore work. Modern records are self-contained:
// physical recovery uses a separately protected snapshot userAdmin identity.
func (s *Server) retainedRestoreSource(ctx context.Context, cluster domain.ClusterRef, backup string) *response {
	_, err := s.Store.GetCluster(ctx, cluster)
	if err == nil {
		return nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		r := s.failure(err)
		return &r
	}
	b, err := s.Store.GetBackup(ctx, cluster, backup)
	if err != nil {
		r := s.backupDependencyFailure(err, cluster, backup)
		return &r
	}
	message := ""
	if b.Phase != domain.PhaseSucceeded {
		message = "Retained backup must be succeeded and not deleting"
	} else if b.Provider != "" && b.Provider != "psmdb" {
		if b.Location == "" {
			message = "Retained backup lacks a snapshot-time artifact reference"
		}
	} else if b.PBMPrefix == "" || b.PBMName == "" || b.SourceTopology.Type == "" || b.SourceVersion == "" {
		message = "Retained backup lacks snapshot/topology/version provenance and its source cluster no longer exists"
	} else if b.Type == "physical" {
		if b.RecoveryIdentity == "" {
			message = "Retained physical backup has no protected recovery identity and its source credentials no longer exist"
		} else {
			pw, err := s.Context.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin")
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound) || (err == nil && len(pw) == 0) {
				message = "Retained physical backup's protected recovery credential is unavailable"
			} else if err != nil {
				r := s.failure(err)
				return &r
			}
		}
	}
	if message != "" {
		r := s.failure(&domain.ResourceError{Resource: "backup", Name: b.Name, Reason: domain.ReasonInvalidState, State: string(b.Phase), Err: store.ErrConflict, Message: "backup " + b.Name + ": " + message})
		return &r
	}
	return nil
}
