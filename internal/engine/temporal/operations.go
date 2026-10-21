// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type operationCheckpoint struct {
	Stage       string    `json:"stage"`
	Order       []string  `json:"order,omitempty"`
	Index       int       `json:"index,omitempty"`
	Waiting     bool      `json:"waiting,omitempty"`
	SettleUntil time.Time `json:"settleUntil,omitempty"`
	SourceID    string    `json:"sourceId,omitempty"`
	Pending     *infra.Op `json:"pending,omitempty"`
	Failure     string    `json:"failure,omitempty"`
}

type operationPayload struct {
	Backup                  string `json:"backup"`
	Restore                 string `json:"restore"`
	Component               string `json:"component"`
	Member                  string `json:"member"`
	AcknowledgeSharedSource bool   `json:"acknowledgeSharedSource"`
}

func operationResult(cp operationCheckpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{
		Done: done, Step: cp.Stage, Message: "Temporal maintenance: " + cp.Stage,
		Checkpoint: raw, RequeueAfter: 2 * time.Second,
	}
}

// SyncDuringOperation keeps imperative maintenance exclusive of normal convergence.
func (*Provider) SyncDuringOperation(*domain.Operation) bool { return false }

// CanClaimOperation defers new maintenance while lifecycle or recovery owns runtimes.
func (*Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, e := loadState(c)
	return e == nil && s.Pending == nil && s.Upgrade == nil && s.RestoreRecovery == nil, e
}

// RunOperation advances checkpointed restart, CA rotation, backup and restore work.
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	var cp operationCheckpoint
	var payload operationPayload
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return result, engine.Invalid("invalid Temporal operation checkpoint")
		}
	}
	if len(op.Payload) > 0 {
		if err = json.Unmarshal(op.Payload, &payload); err != nil {
			return result, engine.Invalid("invalid Temporal operation payload")
		}
	}
	if cp.Stage == "done" {
		return operationResult(cp, true), nil
	}
	switch op.Kind {
	case domain.OperationBackup, domain.OperationRestore, domain.OperationDeleteBackup:
		return p.runBackupOperation(ctx, rc, s, op, cp, payload)
	case domain.OperationRestart, domain.OperationRotateTLS:
	default:
		return result, engine.ErrNotSupported
	}
	if cp.Stage == "" {
		if s.Pending != nil || s.Upgrade != nil || s.RestoreRecovery != nil || s.BindingID == "" {
			return result, fmt.Errorf("%w: Temporal maintenance requires a formed cluster", engine.ErrConflict)
		}
		for _, name := range tspec.RollOrder {
			m := s.Members[name]
			if m == nil {
				continue
			}
			if payload.Component != "" && payload.Component != name || payload.Member != "" && payload.Member != m.Name {
				continue
			}
			st, e := componentStatus(ctx, rc, m)
			if e != nil || !st.Ready {
				return result, engine.ErrUnavailable
			}
			cp.Order = append(cp.Order, name)
		}
		if len(cp.Order) == 0 {
			return result, engine.Invalid("restart target does not exist")
		}
		if op.Kind == domain.OperationRotateTLS {
			if payload.Member != "" || payload.Component != "" {
				return result, engine.Invalid("CA rotation must include every component")
			}
			if err = p.stageRotation(ctx, rc, op.ID); err != nil {
				return result, err
			}
			cp.Stage = "trust"
		} else {
			cp.Stage = "restart"
		}
		return operationResult(cp, false), nil
	}
	var ca *pki.CA
	if op.Kind == domain.OperationRotateTLS {
		ca, err = p.rotationCA(ctx, rc, cp.Stage)
	} else {
		ca, err = ensureCA(ctx, rc)
	}
	if err != nil {
		return operationResult(cp, false), err
	}
	done, err := p.rollOperation(ctx, rc, s, op, &cp, ca)
	if err != nil || !done {
		return operationResult(cp, false), err
	}
	if op.Kind == domain.OperationRestart {
		cp.Stage = "done"
		return operationResult(cp, true), nil
	}
	switch cp.Stage {
	case "trust":
		cp.Stage = "issue"
	case "issue":
		// Keep the completed issue cursor until trust deletion succeeds.
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), rotationTrustKey); err != nil && !missing(err) {
			return operationResult(cp, false), err
		}
		cp.Stage = "prune"
		cp.Index = 0
		cp.Waiting = false
	case "prune":
		if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt", ca.CertPEM); err != nil {
			return operationResult(cp, false), err
		}
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), rotationStageKey); err != nil && !missing(err) {
			return operationResult(cp, false), err
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), engine.Invalid("unknown rotation checkpoint")
	}
	cp.Index = 0
	cp.Waiting = false
	return operationResult(cp, false), nil
}

func (p *Provider) rollOperation(ctx context.Context, rc *engine.Context, s *componentState, op *domain.Operation, cp *operationCheckpoint, ca *pki.CA) (bool, error) {
	if !cp.SettleUntil.IsZero() {
		if clockNow(rc).Before(cp.SettleUntil) {
			return false, nil
		}
		cp.SettleUntil = time.Time{}
	}
	if cp.Index >= len(cp.Order) {
		return true, nil
	}
	m := s.Members[cp.Order[cp.Index]]
	if m == nil {
		return false, engine.ErrNotFound
	}
	if cp.Waiting {
		if cp.Pending != nil {
			status, e := rc.Backend.OpStatus(ctx, *cp.Pending)
			if e != nil {
				return false, e
			}
			if !status.Done {
				return false, nil
			}
			if status.Failed {
				return false, engine.Invalid("Temporal maintenance runtime start failed")
			}
			cp.Pending = nil
		}
		st, e := componentStatus(ctx, rc, m)
		if e != nil {
			return false, e
		}
		m.Observed = st
		if !st.Ready {
			reloadComponent(ctx, rc, m)
			return false, nil
		}
		m.Stage = engine.ComponentJoined
		cp.Waiting = false
		cp.Index++
		if m.Component == tspec.History || m.Component == tspec.Server {
			cp.SettleUntil = clockNow(rc).Add(time.Minute)
		}
		return false, nil
	}
	if busy, e := syncUIExposure(ctx, rc, s, m); busy || e != nil {
		return false, e
	}
	conn, e := p.options.Bindings.Resolve(ctx, s.BindingID)
	if e != nil {
		return false, engine.ErrUnavailable
	}
	// A deterministic token makes replay after publication harmless and keeps Sync from undoing this restart.
	m.RestartNonce = op.ID + "/" + cp.Stage
	sp, e := p.desiredSpec(ctx, rc, m, conn, ca)
	if e != nil {
		return false, e
	}
	if sp.RestartToken != m.ConfigHash {
		if e = p.publishConfig(ctx, rc, m, sp, ca); e != nil {
			return false, e
		}
	}
	if m.Stage == engine.ComponentImported {
		pending, e := rc.Backend.Start(ctx, m.Name)
		if e != nil {
			return false, e
		}
		cp.Pending = &pending
	}
	m.Stage = engine.ComponentDeployed
	reloadComponent(ctx, rc, m)
	cp.Waiting = true
	return false, nil
}

const rotationStageKey = "temporal/tls-rotation"
const rotationTrustKey = "temporal/tls-rotation-trust"

type stagedRotation struct {
	Operation                        string `json:"operation"`
	OldCert, OldKey, NewCert, NewKey []byte
}

func (p *Provider) stageRotation(ctx context.Context, rc *engine.Context, id string) error {
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), rotationStageKey)
	if e != nil && !missing(e) {
		return e
	}
	var stage stagedRotation
	if len(raw) > 0 {
		if e = json.Unmarshal(raw, &stage); e != nil {
			return e
		}
		if stage.Operation != id {
			return fmt.Errorf("%w: another CA rotation is staged", engine.ErrConflict)
		}
	} else {
		old, e := ensureCA(ctx, rc)
		if e != nil {
			return e
		}
		next, e := pki.NewCA(rc.Cluster.Name+" Temporal CA", 0)
		if e != nil {
			return e
		}
		stage = stagedRotation{Operation: id, OldCert: old.CertPEM, OldKey: old.KeyPEM, NewCert: next.CertPEM, NewKey: next.KeyPEM}
		raw, e = json.Marshal(stage)
		if e != nil {
			return e
		}
		if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), rotationStageKey, raw); e != nil {
			return e
		}
	}
	bundle := append(append([]byte(nil), stage.OldCert...), stage.NewCert...)
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), rotationTrustKey, bundle); e != nil {
		return e
	}
	// The shared agent dialer must trust both old and new transport identities throughout the roll.
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt", bundle)
}

func (*Provider) rotationCA(ctx context.Context, rc *engine.Context, phase string) (*pki.CA, error) {
	if phase == "prune" {
		if e := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), rotationTrustKey); e != nil && !missing(e) {
			return nil, e
		}
	}
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), rotationStageKey)
	if missing(e) && phase == "prune" {
		// The final CA write and staging deletion may precede checkpoint persistence.
		return ensureCA(ctx, rc)
	}
	if e != nil {
		return nil, e
	}
	var stage stagedRotation
	if e = json.Unmarshal(raw, &stage); e != nil {
		return nil, e
	}
	if phase == "trust" {
		return pki.LoadCA(stage.OldCert, stage.OldKey)
	}
	ca, e := pki.LoadCA(stage.NewCert, stage.NewKey)
	if e != nil {
		return nil, e
	}
	// Replay repairs a crash between these writes using the single sealed staging record.
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key", stage.NewKey); e != nil {
		return nil, e
	}
	bundle := append(append([]byte(nil), stage.NewCert...), stage.OldCert...)
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt", bundle); e != nil {
		return nil, e
	}
	return ca, nil
}
