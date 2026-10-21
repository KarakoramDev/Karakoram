// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"time"
)

// operationCheckpoint deliberately excludes passwords. Staged credentials live
// only in the encrypted secret store until the native receipt is acknowledged.
type operationCheckpoint struct {
	Step     string              `json:"step"`
	Manifest *neo4jspec.Manifest `json:"manifest,omitempty"`
}

func checkpoint(cp operationCheckpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Step, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if s.Member == nil {
		return engine.OpResult{}, engine.ErrUnavailable
	}
	if s.Maintenance != "" && s.Maintenance != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	cp := operationCheckpoint{}
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	if cp.Step == "done" {
		return checkpoint(cp, true), nil
	}
	switch op.Kind {
	case domain.OperationBackup:
		return p.runBackup(ctx, rc, op, cp)
	case domain.OperationRestore, "initial-restore":
		return p.runRestore(ctx, rc, op, cp)
	case domain.OperationDeleteBackup:
		return p.runDeleteBackup(ctx, rc, op, cp)
	case domain.OperationRestart:
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), s.Member.Name)
		if e != nil {
			return checkpoint(cp, false), e
		}
		defer a.Close()
		if cp.Step == "" {
			if e = a.Process(ctx, agentapi.ProcessAction{Process: neo4jspec.Process, Action: "restart", RequestID: op.ID}); e != nil {
				return checkpoint(cp, false), e
			}
			cp.Step = "await-ready"
			return checkpoint(cp, false), nil
		}
	case domain.OperationRotateTLS:
		if cp.Step == "" {
			cp.Step = "issue"
			return checkpoint(cp, false), nil
		}
		if cp.Step == "issue" {
			if s.TLSRotationID != op.ID {
				if err = ensureCertificates(ctx, rc, s, true); err != nil {
					return checkpoint(cp, false), err
				}
				s.TLSRotationID = op.ID
				if err = save(rc.Cluster, s); err != nil {
					return checkpoint(cp, false), err
				}
			}
			cp.Step = "await-ready"
			return checkpoint(cp, false), nil
		}
	case domain.OperationRotateSystemUser:
		var payload struct {
			User string `json:"user"`
		}
		if err = json.Unmarshal(op.Payload, &payload); err != nil {
			return engine.OpResult{}, engine.Invalid("invalid system user rotation")
		}
		if payload.User != neo4jspec.AdminUser {
			return engine.OpResult{}, engine.Invalid("only neo4j is a managed system user")
		}
		key := "operations/" + op.ID + "/password"
		if cp.Step == "" {
			if _, e := secret(ctx, rc, key); missing(e) {
				pw, e := secrets.Password()
				if e != nil {
					return checkpoint(cp, false), e
				}
				if e = putSecret(ctx, rc, key, []byte(pw)); e != nil {
					return checkpoint(cp, false), e
				}
			} else if e != nil {
				return checkpoint(cp, false), e
			}
			cp.Step = "apply-password"
			return checkpoint(cp, false), nil
		}
		if cp.Step == "recover-admin" || cp.Step == "record-admin-recovery" {
			return recoverRotatedAdmin(ctx, rc, op, cp)
		}
		if cp.Step == "apply-password" {
			next, e := secret(ctx, rc, key)
			if e != nil {
				return checkpoint(cp, false), e
			}
			current, e := secret(ctx, rc, "users/neo4j")
			if e != nil {
				return checkpoint(cp, false), e
			}
			if e = action(ctx, rc, neo4jspec.ActionAdminPassword, neo4jspec.AdminPasswordRequest{Receipt: op.ID, Current: string(current), Next: string(next)}, nil); e != nil {
				if adminCredentialsRejected(e) {
					cp.Step = "recover-admin"
					return checkpoint(cp, false), nil
				}
				return checkpoint(cp, false), e
			}
			cp.Step = "commit-password"
			return checkpoint(cp, false), nil
		}
		if cp.Step == "commit-password" {
			next, e := secret(ctx, rc, key)
			if e != nil {
				return checkpoint(cp, false), e
			}
			if e = putSecret(ctx, rc, "users/neo4j", next); e != nil {
				return checkpoint(cp, false), e
			}
			if s.PasswordRotationID != op.ID {
				s.ConfigVersion++
				s.PasswordRotationID = op.ID
				if e = save(rc.Cluster, s); e != nil {
					return checkpoint(cp, false), e
				}
			}
			cp.Step = "await-ready"
			return checkpoint(cp, false), nil
		}
	default:
		return engine.OpResult{}, engine.ErrNotSupported
	}
	if err = reload(ctx, rc); err != nil {
		return checkpoint(cp, false), err
	}
	r, err := readiness(ctx, rc)
	if err != nil {
		return checkpoint(cp, false), err
	}
	cfg, err := p.MemberConfig(ctx, rc, s.Member.Name)
	if err != nil {
		return checkpoint(cp, false), err
	}
	var spec neo4jspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		return checkpoint(cp, false), err
	}
	if !r.Ready || r.CredentialMismatch || r.ConfigVersion < s.ConfigVersion || r.AppliedRestartToken != spec.RestartToken || r.Version != spec.Version {
		return checkpoint(cp, false), nil
	}
	if op.Kind == domain.OperationRotateSystemUser {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "operations/"+op.ID+"/password"); err != nil && !missing(err) {
			return checkpoint(cp, false), err
		}
	}
	cp.Step = "done"
	return checkpoint(cp, true), nil
}
