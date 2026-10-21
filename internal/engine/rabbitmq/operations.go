// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"slices"
	"time"
)

type operationState struct {
	Done      []string `json:"done"`
	Waiting   string   `json:"waiting,omitempty"`
	Stage     string   `json:"stage"`
	NewMember string   `json:"newMember,omitempty"`
}

func opResult(cp operationState, done bool, step string) (engine.OpResult, error) {
	b, err := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: step, Checkpoint: b, RequeueAfter: time.Second}, err
}
func decodeJSON(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	recoveryState, recoveryErr := loadState(rc.Cluster)
	if recoveryErr != nil {
		return result, recoveryErr
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, recoveryState); busy || e != nil {
		if saveErr := recoveryState.save(rc.Cluster); e == nil {
			e = saveErr
		}
		return engine.OpResult{RequeueAfter: 5 * time.Second, Checkpoint: op.Checkpoint, Message: "Waiting for accepted runtime recovery"}, e
	}
	switch op.Kind {
	case domain.OperationBackup:
		return p.backupOp(ctx, rc, op)
	case domain.OperationDeleteBackup:
		return p.deleteBackupOp(ctx, rc, op)
	case domain.OperationRestore:
		return p.restoreOp(ctx, rc, op)
	}
	s, e := loadState(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if op.Kind == domain.OperationRestart || op.Kind == domain.OperationRotateTLS || op.Kind == domain.OperationUpgrade || op.Kind == domain.OperationUpdate {
			for _, m := range s.Members {
				if len(m.QuorumBlockers) > 0 {
					result.Message = quorumMessage(m)
					break
				}
			}
		}
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
		projectQuorumStatus(rc, s, &rc.Cluster.Status)
	}()
	var cp operationState
	if e = decodeJSON(op.Checkpoint, &cp); e != nil {
		return result, e
	}
	payload := map[string]string{}
	if e = decodeJSON(op.Payload, &payload); e != nil {
		return result, invalid("invalid operation payload")
	}
	if s.Pending != nil || s.Retry != nil {
		busy, e := p.observePending(ctx, rc, s)
		if !busy && e == nil {
			_, e = p.retryPending(ctx, rc, s)
		}
		r, _ := opResult(cp, false, "waiting-platform")
		return r, e
	}
	if busy, e := p.observeResumeDeployments(ctx, rc, s); busy || e != nil {
		r, _ := opResult(cp, false, "waiting-resume-platform")
		return r, e
	}
	if cp.Waiting == "" {
		for _, m := range ordered(s) {
			if m.ChangeAction != "" && m.ChangeAction != "password" {
				_, e := p.changeStep(ctx, rc, s, m)
				r, checkpointErr := opResult(cp, false, "waiting-member-change")
				if e != nil {
					return r, e
				}
				if len(m.QuorumBlockers) > 0 {
					r.RequeueAfter = quorumBlockedRequeueAfter
				}
				return r, checkpointErr
			}
		}
	}
	switch op.Kind {
	case domain.OperationRestart, domain.OperationRotateTLS:
		return p.rollingOperation(ctx, rc, s, cp, op.Kind == domain.OperationRotateTLS)
	case domain.OperationReplaceMember:
		old := s.find(payload["member"])
		if cp.Stage == "" {
			if old == nil || old.Removing || old.Stage != "ready" {
				return result, invalid("replacement requires a ready member")
			}
			m, e := allocateNewMember(ctx, rc, s)
			if e != nil {
				return result, e
			}
			m.ReplacesVolume = old.Volume
			cp.NewMember = m.Name
			cp.Stage = "join"
			s.ReplaceActive = true
			return opResult(cp, false, "replacement-planned")
		}
		m := s.find(cp.NewMember)
		if m == nil {
			return result, fmt.Errorf("replacement checkpoint member missing")
		}
		if cp.Stage == "join" {
			if m.Stage != "ready" {
				return opResult(cp, false, "replacement-sync")
			}
			if old != nil {
				old.Removing = true
			}
			cp.Stage = "remove"
			return opResult(cp, false, "replacement-joined")
		}
		if old != nil {
			return opResult(cp, false, "replacement-remove")
		}
		s.ReplaceActive = false
		return opResult(cp, true, "member-replaced")
	case domain.OperationRotateSystemUser:
		user := payload["user"]
		if user != "karakoram" && user != "admin" && !slices.Contains(dataAccessUsers(rc.Cluster), user) {
			return result, invalid("rotatable system user must be karakoram, admin or an enabled data-access user")
		}
		key := "rotations/" + op.ID
		if cp.Stage == "" {
			if e = ensureSecret(ctx, rc, key, secrets.Password); e != nil {
				return result, e
			}
			if !slices.Contains(s.RotationSecrets, key) {
				s.RotationSecrets = append(s.RotationSecrets, key)
			}
			members := s.runningMembers()
			if len(members) == 0 {
				return result, engine.ErrConflict
			}
			cp.Waiting = members[0].Name
			cp.Stage = "reload-password"
			m := members[0]
			m.ChangeAction = "password"
			m.TargetConfig = user
			m.TargetVersion = key
			s.ConfigVersion++
			m.ConfigVersion = s.ConfigVersion
			return opResult(cp, false, "password-staged")
		}
		m := s.find(cp.Waiting)
		if m == nil {
			return result, engine.ErrNotFound
		}
		switch cp.Stage {
		case "reload-password":
			if e = s.save(rc.Cluster); e != nil {
				return result, e
			}
			a, e := p.agent(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			defer a.Close()
			if e = a.Reload(ctx); e != nil {
				return result, e
			}
			cp.Stage = "set-password"
			return opResult(cp, false, "password-config-loaded")
		case "set-password":
			st, e := p.agentStatus(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if st.ConfigVersion < m.ConfigVersion {
				return opResult(cp, false, "waiting-agent")
			}
			if e = p.action(ctx, rc, m.Name, "set-password", rmqspec.SetPasswordRequest{User: user}); e != nil {
				return result, e
			}
			cp.Stage = "persist-password"
			return opResult(cp, false, "password-set")
		case "persist-password":
			pw, e := secret(ctx, rc, key)
			if e != nil {
				return result, e
			}
			if e = putSecret(ctx, rc, "users/"+user, pw); e != nil {
				return result, e
			}
			if user == "admin" {
				s.AdminCredentialVersion = s.ConfigVersion
				// Publish the committed credential epoch before reloading peers.
				// Their previous snapshots must not be mistaken for drift.
				for _, peer := range s.runningMembers() {
					peer.ConfigVersion = s.ConfigVersion
				}
			}
			m.ChangeAction = ""
			m.TargetConfig = ""
			m.TargetVersion = ""
			cp.Stage = "reload-members"
			cp.Done = nil
			return opResult(cp, false, "password-persisted")
		case "reload-members":
			for _, peer := range s.runningMembers() {
				if slices.Contains(cp.Done, peer.Name) {
					continue
				}
				a, e := p.agent(ctx, rc, peer.Name)
				if e != nil {
					return result, e
				}
				e = a.Reload(ctx)
				a.Close()
				if e != nil {
					return result, e
				}
				cp.Done = append(cp.Done, peer.Name)
				return opResult(cp, false, "credentials-reloaded")
			}
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return result, e
			}
			return opResult(cp, true, "password-rotated")
		default:
			return result, fmt.Errorf("unknown credential checkpoint %q", cp.Stage)
		}
	default:
		return result, fmt.Errorf("%w: unknown operation %q", engine.ErrNotSupported, op.Kind)
	}
}
func (p *Provider) rollingOperation(ctx context.Context, rc *engine.Context, s *internalState, cp operationState, rotate bool) (engine.OpResult, error) {
	if cp.Waiting != "" {
		m := s.find(cp.Waiting)
		if m == nil {
			return engine.OpResult{}, engine.ErrNotFound
		}
		done, err := p.changeStep(ctx, rc, s, m)
		if err != nil {
			r, _ := opResult(cp, false, "member-change")
			return r, err
		}
		if done {
			cp.Done = append(cp.Done, m.Name)
			cp.Waiting = ""
		}
		result, err := opResult(cp, false, "member-change")
		if !done && len(m.QuorumBlockers) > 0 {
			result.RequeueAfter = quorumBlockedRequeueAfter
		}
		return result, err
	}
	// Do not begin a rolling change until ordinary Sync has finished the entire
	// desired membership. This checkpoint alone permits provisioning Sync; once
	// a broker is being changed, the operation remains isolated.
	active := 0
	provisioning := s.Suspended
	for _, m := range s.Members {
		if m.Removing {
			provisioning = true
			continue
		}
		active++
		provisioning = provisioning || m.Stage != "ready"
	}
	if provisioning || active != rc.Cluster.Spec.Components.Engine.Replicas {
		cp.Stage = "provision"
		return opResult(cp, false, "waiting-provisioning")
	}
	cp.Stage = ""
	for _, m := range ordered(s) {
		if m.Removing || slices.Contains(cp.Done, m.Name) {
			continue
		}
		if rotate {
			if err := rotateCertificate(ctx, rc, m); err != nil {
				return engine.OpResult{}, err
			}
			beginChange(rc, s, m, "tls")
		} else {
			beginChange(rc, s, m, "restart")
		}
		cp.Waiting = m.Name
		return opResult(cp, false, "member-planned")
	}
	s.Phase = "ready"
	return opResult(cp, true, "rolling-complete")
}
func (*Provider) SyncDuringOperation(op *domain.Operation) bool {
	if op == nil {
		return false
	}
	var cp operationState
	if decodeJSON(op.Checkpoint, &cp) != nil {
		return false
	}
	switch op.Kind {
	case domain.OperationReplaceMember:
		return cp.Stage == "join" || cp.Stage == "remove"
	case domain.OperationRestart, domain.OperationRotateTLS:
		return cp.Stage == "provision" && cp.Waiting == ""
	default:
		return false
	}
}

var _ engine.OperationSyncPolicy = (*Provider)(nil)
