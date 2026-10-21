// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

// The journal outlives an operation checkpoint or retry deadline. Ordinary Sync
// owns recovery once an interrupted restore no longer owns the cluster worker.
type restoreRecoveryState struct {
	OperationID       string    `json:"operationId"`
	SourceBackupID    string    `json:"sourceBackupId,omitempty"`
	SourceID          string    `json:"sourceId,omitempty"`
	SourceRequested   bool      `json:"sourceRequested,omitempty"`
	SourceFinished    bool      `json:"sourceFinished,omitempty"`
	AcknowledgeShared bool      `json:"acknowledgeShared,omitempty"`
	Failure           string    `json:"failure,omitempty"`
	Order             []string  `json:"order"`
	Index             int       `json:"index"`
	Pending           *infra.Op `json:"pending,omitempty"`
	Stopping          *infra.Op `json:"stopping,omitempty"`
	Waiting           bool      `json:"waiting,omitempty"`
}

func (p *Provider) recoverRestore(ctx context.Context, rc *engine.Context, s *componentState, ca *pki.CA) (bool, error) {
	r := s.RestoreRecovery
	if r == nil {
		return true, nil
	}
	if r.Stopping != nil {
		status, err := rc.Backend.OpStatus(ctx, *r.Stopping)
		if err != nil {
			return false, err
		}
		if !status.Done {
			return false, nil
		}
		r.Stopping = nil
		return false, nil
	}
	// A lost source request response is replayed with its original idempotency
	// key before observing it. Never start roles while source restoration runs.
	if r.SourceRequested && !r.SourceFinished {
		binder, ok := p.options.Bindings.(pgbinding.BackupBinder)
		if !ok {
			return false, engine.ErrUnavailable
		}
		if r.SourceID == "" {
			ref, err := binder.Restore(ctx, s.BindingID, r.SourceBackupID, r.OperationID, r.AcknowledgeShared)
			if errors.Is(err, pgbinding.ErrSharedSource) {
				r.Failure = "PostgreSQL source restore refused: shared source"
				r.SourceFinished = true
				return false, nil
			}
			if err != nil || ref.ID == "" {
				return false, engine.ErrUnavailable
			}
			r.SourceID = ref.ID
			return false, nil
		}
		state, err := binder.OperationStatus(ctx, s.BindingID, r.SourceID)
		if err != nil {
			return false, engine.ErrUnavailable
		}
		switch state.Phase {
		case pgbinding.PhaseFailed:
			r.Failure = "PostgreSQL source restore failed"
		case pgbinding.PhaseCompleted:
		default:
			return false, nil
		}
		r.SourceFinished = true
		return false, nil
	}
	if r.Index >= len(r.Order) {
		return true, nil
	}
	m := s.Members[r.Order[r.Index]]
	if m == nil {
		return false, engine.ErrNotFound
	}
	if r.Pending != nil {
		status, err := rc.Backend.OpStatus(ctx, *r.Pending)
		if err != nil {
			return false, err
		}
		if !status.Done {
			return false, nil
		}
		if status.Failed {
			r.Pending = nil
			r.Waiting = false
			return false, engine.Invalid("Temporal restore recovery runtime start failed")
		}
		r.Pending = nil
	}
	if r.Waiting {
		observed, err := componentStatus(ctx, rc, m)
		if err != nil {
			return false, err
		}
		m.Observed = observed
		if !observed.Ready {
			reloadComponent(ctx, rc, m)
			return false, nil
		}
		m.Stage = engine.ComponentJoined
		r.Waiting = false
		r.Index++
		return false, nil
	}
	if busy, err := syncUIExposure(ctx, rc, s, m); busy || err != nil {
		return false, err
	}
	if _, err := verifyComponent(ctx, rc, m); err != nil {
		return false, err
	}
	conn, err := p.options.Bindings.Resolve(ctx, s.BindingID)
	if err != nil {
		return false, engine.ErrUnavailable
	}
	m.RestartNonce = r.OperationID + "/restore-recovery"
	spec, err := p.desiredSpec(ctx, rc, m, conn, ca)
	if err != nil {
		return false, err
	}
	if spec.RestartToken != m.ConfigHash {
		if err = p.publishConfig(ctx, rc, m, spec, ca); err != nil {
			return false, err
		}
	}
	pending, err := rc.Backend.Start(ctx, m.Name)
	if err != nil {
		return false, err
	}
	m.Stage = engine.ComponentDeployed
	r.Pending = &pending
	r.Waiting = true
	return false, nil
}

// journalStoppedComponents adopts stopped runtimes from pre-journal restores.
// Imported services belong to provisioning or an explicit exposure transition.
func journalStoppedComponents(ctx context.Context, rc *engine.Context, s *componentState) (bool, error) {
	if s.Pending != nil {
		return false, nil
	}
	var order []string
	for _, name := range tspec.RollOrder {
		m := s.Members[name]
		if m == nil || m.Stage != engine.ComponentJoined {
			continue
		}
		service, err := verifyComponent(ctx, rc, m)
		if err != nil {
			return false, err
		}
		if service != nil && service.Status == infra.StatusStopped {
			order = append(order, name)
		}
	}
	if len(order) == 0 {
		return false, nil
	}
	s.RestoreRecovery = &restoreRecoveryState{
		OperationID: "interrupted-restore", Order: order, SourceFinished: true,
	}
	return true, nil
}
