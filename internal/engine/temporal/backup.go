// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

// Backup inventory has no generic engine metadata field. These non-secret manifests
// live in Cluster.Internal["temporal"].backups keyed by the Temporal backup name.
// SourceID is the binding backup ID; source PostgreSQL owns its retention lifecycle.
type backupManifest struct {
	Version                 string            `json:"version"`
	SchemaVersion           string            `json:"schemaVersion"`
	VisibilitySchemaVersion string            `json:"visibilitySchemaVersion"`
	HistoryShards           int               `json:"historyShards"`
	ClusterName             string            `json:"clusterName"`
	Namespaces              []tspec.Namespace `json:"namespaces"`
	Databases               []string          `json:"databases"`
	Source                  domain.ClusterRef `json:"source"`
	CAFingerprintSHA256     string            `json:"caFingerprintSha256"`
	SourceID                string            `json:"sourceId,omitempty"`
	Complete                bool              `json:"complete"`
}

func compatibleManifest(c *domain.Cluster, o EngineOptions, m backupManifest) error {
	if m.ClusterName != c.Name || m.HistoryShards != o.HistoryShards {
		return engine.Invalid("restore requires the same Temporal cluster name and historyShards")
	}
	if m.Version != c.Spec.Version && !upgradeAllowed(m.Version, c.Spec.Version) {
		return engine.Invalid("target Temporal version cannot run the backup schema within supported upgrade paths")
	}
	pg := o.Persistence.Postgres
	if len(m.Databases) != 2 || m.Databases[0] != pg.Database || m.Databases[1] != pg.VisibilityDatabase ||
		m.Source.Name != pg.Cluster || m.Source.Namespace != pg.Namespace {
		return engine.Invalid("restore requires the original PostgreSQL source and database names")
	}
	if !m.Complete || m.SourceID == "" {
		return engine.Invalid("Temporal backup is not complete")
	}
	return nil
}

func (p *Provider) runBackupOperation(ctx context.Context, rc *engine.Context, s *componentState, op *domain.Operation, cp operationCheckpoint, payload operationPayload) (engine.OpResult, error) {
	if op.Kind != domain.OperationDeleteBackup {
		if _, ok := p.options.Bindings.(pgbinding.BackupBinder); !ok {
			return operationResult(cp, false), engine.Invalid("the PostgreSQL binding service does not support backups")
		}
	}
	if rc.Store == nil {
		return operationResult(cp, false), engine.ErrUnavailable
	}
	if op.Kind == domain.OperationDeleteBackup {
		if payload.Backup == "" {
			return operationResult(cp, false), engine.Invalid("backup is required")
		}
		// Delete only Temporal inventory/manifest. PostgreSQL archives remain owned by source retention.
		if e := rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), payload.Backup); e != nil && !missing(e) {
			return operationResult(cp, false), e
		}
		delete(s.Backups, payload.Backup)
		cp.Stage = "done"
		return operationResult(cp, true), nil
	}
	binder := p.options.Bindings.(pgbinding.BackupBinder)
	if s.BindingID == "" {
		return operationResult(cp, false), engine.ErrUnavailable
	}
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		return operationResult(cp, false), e
	}
	if op.Kind == domain.OperationBackup {
		b, e := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), payload.Backup)
		if e != nil {
			return operationResult(cp, false), e
		}
		if b.Type != "full" && b.Type != "diff" && b.Type != "incr" {
			return operationResult(cp, false), engine.Invalid("backup type must be full, diff or incr")
		}
		if s.Backups == nil {
			s.Backups = map[string]backupManifest{}
		}
		m, exists := s.Backups[b.Name]
		if !exists {
			ca, e := ensureCA(ctx, rc)
			if e != nil {
				return operationResult(cp, false), e
			}
			member := s.Members[frontendComponent(rc.Cluster)]
			if member == nil {
				return operationResult(cp, false), engine.ErrUnavailable
			}
			st, e := componentStatus(ctx, rc, member)
			if e != nil || !st.Ready {
				return operationResult(cp, false), engine.ErrUnavailable
			}
			pg := o.Persistence.Postgres
			m = backupManifest{
				Version: rc.Cluster.Spec.Version, ClusterName: rc.Cluster.Name,
				SchemaVersion: st.SchemaVersion, VisibilitySchemaVersion: st.VisibilitySchemaVersion,
				HistoryShards: o.HistoryShards, Namespaces: desiredNamespaces(o),
				Databases:           []string{pg.Database, pg.VisibilityDatabase},
				Source:              domain.ClusterRef{Namespace: pg.Namespace, Name: pg.Cluster},
				CAFingerprintSHA256: fingerprint(ca.Cert.Raw),
			}
			s.Backups[b.Name] = m
			cp.Stage = "request-backup"
			return operationResult(cp, false), nil
		}
		if m.SourceID == "" {
			ref, e := binder.Backup(ctx, s.BindingID, b.Type, op.ID)
			if e != nil {
				return operationResult(cp, false), engine.ErrUnavailable
			}
			if ref.ID == "" {
				return operationResult(cp, false), engine.ErrUnavailable
			}
			m.SourceID = ref.ID
			s.Backups[b.Name] = m
			cp.SourceID = ref.ID
			cp.Stage = "wait-backup"
			b.Location = ref.ID
			b.SourceVersion = m.Version
			b.Provider = tspec.Engine
			b.Phase = domain.PhaseRunning
			if e = rc.Store.UpdateBackup(ctx, b); e != nil {
				return operationResult(cp, false), e
			}
			return operationResult(cp, false), nil
		}
		state, e := binder.BackupStatus(ctx, s.BindingID, m.SourceID)
		if e != nil {
			return operationResult(cp, false), engine.ErrUnavailable
		}
		b.Location = m.SourceID
		b.SourceVersion = m.Version
		b.Provider = tspec.Engine
		switch state.Phase {
		case pgbinding.PhaseFailed:
			b.Phase = domain.PhaseFailed
			b.Error = "PostgreSQL source backup failed"
			if e = rc.Store.UpdateBackup(ctx, b); e != nil {
				return operationResult(cp, false), e
			}
			return operationResult(cp, false), engine.Invalid("PostgreSQL source backup failed")
		case pgbinding.PhaseCompleted:
			m.Complete = true
			s.Backups[b.Name] = m
			b.Phase = domain.PhaseSucceeded
			t := state.CompletedAt
			if t.IsZero() {
				t = clockNow(rc)
			}
			b.CompletedAt = &t
			if e = rc.Store.UpdateBackup(ctx, b); e != nil {
				return operationResult(cp, false), e
			}
			cp.Stage = "done"
			return operationResult(cp, true), nil
		}
		cp.Stage = "wait-backup"
		return operationResult(cp, false), nil
	}
	r, e := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), payload.Restore)
	if e != nil {
		return operationResult(cp, false), e
	}
	if cp.Stage == "failed" {
		return operationResult(cp, false), engine.Invalid("%s", cp.Failure)
	}
	if cp.Stage == "start" || cp.Stage == "recover" || cp.Stage == "ready" {
		if s.RestoreRecovery == nil && cp.Stage != "recover" {
			s.RestoreRecovery = &restoreRecoveryState{
				OperationID: op.ID, Order: cp.Order, SourceFinished: true,
			}
		}
		ca, err := ensureCA(ctx, rc)
		if err != nil {
			return operationResult(cp, false), err
		}
		done, err := p.recoverRestore(ctx, rc, s, ca)
		if err != nil || !done {
			return operationResult(cp, false), err
		}
		if s.RestoreRecovery != nil && cp.Failure == "" {
			cp.Failure = s.RestoreRecovery.Failure
		}
		if cp.Failure != "" {
			r.Phase = domain.PhaseFailed
			r.Error = cp.Failure
		} else {
			if err = p.syncNamespaces(ctx, rc, s, o); err != nil {
				return operationResult(cp, false), err
			}
			r.Phase = domain.PhaseSucceeded
		}
		now := clockNow(rc)
		r.CompletedAt = &now
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return operationResult(cp, false), err
		}
		s.RestoreRecovery = nil
		if cp.Failure != "" {
			cp.Stage = "failed"
			return operationResult(cp, false), engine.Invalid("%s", cp.Failure)
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	}
	if r.Source.PITRTime != nil {
		return operationResult(cp, false), engine.Invalid("Temporal restores require a recorded backup")
	}
	source := rc.Cluster.Ref()
	if r.Source.SourceCluster != "" {
		source.Name = r.Source.SourceCluster
	}
	if r.SourceNamespace != "" {
		source.Namespace = r.SourceNamespace
	}
	sourceCluster, e := rc.Store.GetCluster(ctx, source)
	if e != nil {
		return operationResult(cp, false), e
	}
	sourceState, e := loadState(sourceCluster)
	if e != nil {
		return operationResult(cp, false), e
	}
	m, ok := sourceState.Backups[r.Source.Backup]
	if !ok {
		return operationResult(cp, false), engine.ErrNotFound
	}
	if e = compatibleManifest(rc.Cluster, o, m); e != nil {
		return operationResult(cp, false), e
	}
	if cp.Stage == "" {
		conn, e := p.options.Bindings.Resolve(ctx, s.BindingID)
		if e != nil {
			return operationResult(cp, false), engine.ErrUnavailable
		}
		if conn.Shared && !payload.AcknowledgeSharedSource {
			return operationResult(cp, false), fmt.Errorf("%w: %w; acknowledgeSharedSource is required", engine.ErrConflict, pgbinding.ErrSharedSource)
		}
		for _, name := range tspec.RollOrder {
			if s.Members[name] != nil {
				cp.Order = append(cp.Order, name)
			}
		}
		s.RestoreRecovery = &restoreRecoveryState{
			OperationID: op.ID, SourceBackupID: m.SourceID, Order: cp.Order,
			AcknowledgeShared: payload.AcknowledgeSharedSource,
		}
		cp.Stage = "stop"
		return operationResult(cp, false), nil
	}
	// Adopt older operation checkpoints before any further stop/source action.
	if s.RestoreRecovery == nil {
		order := append([]string(nil), cp.Order...)
		if len(order) == 0 {
			for _, name := range tspec.RollOrder {
				if s.Members[name] != nil {
					order = append(order, name)
				}
			}
		}
		s.RestoreRecovery = &restoreRecoveryState{
			OperationID: op.ID, SourceBackupID: m.SourceID, SourceID: cp.SourceID,
			Order: order, AcknowledgeShared: payload.AcknowledgeSharedSource,
			SourceRequested: cp.Stage == "wait-restore",
		}
	}
	switch cp.Stage {
	case "stop":
		if cp.Pending != nil {
			st, e := rc.Backend.OpStatus(ctx, *cp.Pending)
			if e != nil {
				return operationResult(cp, false), e
			}
			if !st.Done {
				return operationResult(cp, false), nil
			}
			if st.Failed {
				return operationResult(cp, false), engine.Invalid("Temporal restore runtime transition failed")
			}
			cp.Pending = nil
			s.RestoreRecovery.Stopping = nil
			cp.Index++
			return operationResult(cp, false), nil
		}
		if cp.Index < len(cp.Order) {
			m := s.Members[cp.Order[cp.Index]]
			if m == nil {
				return operationResult(cp, false), engine.ErrNotFound
			}
			pending, pendingErr := rc.Backend.Stop(ctx, m.Name)
			if pendingErr != nil {
				return operationResult(cp, false), pendingErr
			}
			cp.Pending = &pending
			s.RestoreRecovery.Stopping = &pending
			return operationResult(cp, false), nil
		}
		cp.Index = 0
		cp.Stage = "restore-source"
	case "restore-source":
		if !s.RestoreRecovery.SourceRequested {
			s.RestoreRecovery.SourceRequested = true
			return operationResult(cp, false), nil
		}
		ref, e := binder.Restore(ctx, s.BindingID, m.SourceID, op.ID, payload.AcknowledgeSharedSource)
		if e != nil {
			if errors.Is(e, pgbinding.ErrSharedSource) {
				s.RestoreRecovery.SourceFinished = true
				s.RestoreRecovery.Failure = "PostgreSQL source restore refused: shared source"
				cp.Failure = s.RestoreRecovery.Failure
				cp.Stage = "recover"
				return operationResult(cp, false), fmt.Errorf("%w: %w", engine.ErrConflict, pgbinding.ErrSharedSource)
			}
			return operationResult(cp, false), engine.ErrUnavailable
		}
		if ref.ID == "" {
			return operationResult(cp, false), engine.ErrUnavailable
		}
		cp.SourceID = ref.ID
		s.RestoreRecovery.SourceID = ref.ID
		cp.Stage = "wait-restore"
	case "wait-restore":
		st, e := binder.OperationStatus(ctx, s.BindingID, cp.SourceID)
		if e != nil {
			return operationResult(cp, false), engine.ErrUnavailable
		}
		if st.Phase == pgbinding.PhaseFailed {
			cp.Failure = "PostgreSQL source restore failed"
			cp.Stage = "recover"
			s.RestoreRecovery.Failure = cp.Failure
			s.RestoreRecovery.SourceFinished = true
		}
		if st.Phase == pgbinding.PhaseCompleted {
			cp.Stage = "start"
			s.RestoreRecovery.SourceFinished = true
		}
	default:
		return operationResult(cp, false), engine.Invalid("unknown restore checkpoint")
	}
	return operationResult(cp, false), nil
}
