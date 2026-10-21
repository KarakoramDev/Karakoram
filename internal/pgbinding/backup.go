// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgbinding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var _ BackupBinder = (*Manager)(nil)

func requestName(binding, kind, key string) string {
	sum := sha256.Sum256([]byte(binding + "\x00" + kind + "\x00" + key))
	return "binding-" + hex.EncodeToString(sum[:16])
}
func phase(p domain.Phase) string {
	switch p {
	case domain.PhaseSucceeded:
		return PhaseCompleted
	case domain.PhaseFailed, domain.PhaseCancelled:
		return PhaseFailed
	case domain.PhaseRunning:
		return PhaseRunning
	default:
		return PhasePending
	}
}

func (m *Manager) Backup(ctx context.Context, id, kind, key string) (BackupRef, error) {
	if key == "" {
		return BackupRef{}, engine.Invalid("backup idempotency key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return BackupRef{}, err
	}
	rc, a, err := m.source(ctx, b)
	if err != nil {
		return BackupRef{}, err
	}
	name := requestName(id, "backup", key)
	existing, err := m.options.Store.GetBackup(ctx, b.SourceRef(), name)
	if err == nil {
		if existing.Type != kind {
			return BackupRef{}, engine.ErrConflict
		}
		return BackupRef{ID: name}, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return BackupRef{}, err
	}
	catalog, ok := a.(interface{ Describe() domain.ProviderInfo })
	if !ok || !slices.Contains(catalog.Describe().BackupTypes, kind) {
		return BackupRef{}, engine.Invalid("unsupported source backup type")
	}
	c := rc.Cluster
	if (c.Status.Phase != domain.PhaseReady && c.Status.Phase != domain.PhaseDegraded) || c.Status.ObservedGeneration < c.Generation {
		return BackupRef{}, engine.ErrConflict
	}
	storage := c.Spec.Backup.Storage
	if storage == "" {
		return BackupRef{}, engine.Invalid("source backup storage is not configured")
	}
	if _, err := m.options.Store.GetBackupStorage(ctx, c.Namespace, storage); err != nil {
		return BackupRef{}, err
	}
	backup := domain.Backup{Namespace: c.Namespace, Cluster: c.Name, Name: name, Storage: storage, Type: kind, Provider: "postgres", Trigger: "binding", Phase: domain.PhasePending}
	payload, _ := json.Marshal(map[string]string{"backup": name})
	op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: "backup", Actor: "binding/" + id, Payload: payload}
	if err := m.options.Store.EnqueueBackup(ctx, &backup, &op); err != nil {
		return BackupRef{}, err
	}
	m.trigger(b)
	return BackupRef{ID: name}, nil
}
func (m *Manager) BackupStatus(ctx context.Context, id, backupID string) (State, error) {
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return State{}, err
	}
	if _, _, err := m.source(ctx, b); err != nil {
		return State{}, err
	}
	backup, err := m.options.Store.GetBackup(ctx, b.SourceRef(), backupID)
	if err != nil {
		return State{}, err
	}
	state := State{Phase: phase(backup.Phase), Error: backup.Error}
	if backup.CompletedAt != nil {
		state.CompletedAt = *backup.CompletedAt
	}
	return state, nil
}
func (m *Manager) Restore(ctx context.Context, id, backupID, key string, ack bool) (OperationRef, error) {
	if key == "" {
		return OperationRef{}, engine.Invalid("restore idempotency key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return OperationRef{}, err
	}
	if _, _, err := m.source(ctx, b); err != nil {
		return OperationRef{}, err
	}
	// Fast feedback only; EnqueueRestore repeats this admission atomically.
	all, err := m.options.Store.ListPostgresBindings(ctx, b.SourceRef())
	if err != nil {
		return OperationRef{}, err
	}
	if len(all) > 1 && !ack {
		return OperationRef{}, ErrSharedSource
	}
	name := requestName(id, "restore", key)
	existing, err := m.options.Store.GetRestore(ctx, b.SourceRef(), name)
	if err == nil {
		if existing.Source.Backup != backupID {
			return OperationRef{}, engine.ErrConflict
		}
		return OperationRef{ID: existing.OperationID}, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return OperationRef{}, err
	}
	backup, err := m.options.Store.GetBackup(ctx, b.SourceRef(), backupID)
	if err != nil {
		return OperationRef{}, err
	}
	if backup.Phase != domain.PhaseSucceeded || backup.Provider != "postgres" {
		return OperationRef{}, engine.Invalid("restore requires a successful PostgreSQL backup")
	}
	restore := domain.Restore{Namespace: b.SourceNamespace, Cluster: b.SourceCluster, SourceNamespace: b.SourceNamespace, Name: name, Source: domain.RestoreSource{Backup: backupID, SourceCluster: b.SourceCluster}, Phase: domain.PhasePending}
	payload, _ := json.Marshal(map[string]string{"restore": name})
	op := domain.Operation{Namespace: b.SourceNamespace, Cluster: b.SourceCluster, Kind: "restore", Actor: "binding/" + id, Payload: payload}
	if ack {
		ctx = store.WithRestoreAcknowledgement(ctx)
	} else {
		ctx = store.WithRestoreBinding(ctx, id)
	}
	if err := m.options.Store.EnqueueRestore(ctx, &restore, &op); err != nil {
		var bound *store.BoundConsumersError
		if errors.As(err, &bound) {
			return OperationRef{}, errors.Join(ErrSharedSource, err)
		}
		return OperationRef{}, err
	}
	m.trigger(b)
	return OperationRef{ID: op.ID}, nil
}
func (m *Manager) OperationStatus(ctx context.Context, id, operationID string) (State, error) {
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return State{}, err
	}
	if _, _, err := m.source(ctx, b); err != nil {
		return State{}, err
	}
	op, err := m.options.Store.GetOperation(ctx, operationID)
	if err != nil {
		return State{}, err
	}
	if domain.NormalizedNamespace(op.Namespace) != b.SourceNamespace || op.Cluster != b.SourceCluster || op.Actor != "binding/"+id {
		return State{}, engine.ErrNotFound
	}
	state := State{Phase: phase(op.Phase), Error: op.LastError}
	if op.CompletedAt != nil {
		state.CompletedAt = *op.CompletedAt
	}
	return state, nil
}
