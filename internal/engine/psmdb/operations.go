// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type operationState struct {
	Done          []string `json:"done"`
	Waiting       string   `json:"waiting,omitempty"`
	Stage         string   `json:"stage"`
	NewMember     string   `json:"newMember,omitempty"`
	ConfigVersion int64    `json:"configVersion"`
}

func decodeJSON(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}
func setInternal(c *domain.Cluster, key string, v any) error {
	m := map[string]json.RawMessage{}
	if e := decodeJSON(c.Internal, &m); e != nil {
		return e
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	m[key] = b
	c.Internal, e = json.Marshal(m)
	return e
}
func opResult(cp operationState, done bool, step string) (engine.OpResult, error) {
	b, e := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: step, Checkpoint: b, RequeueAfter: 5 * time.Second}, e
}

// RunOperation resumes imperative operations from durable, secret-free checkpoints.
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		// Operations directly reconfigure PBM; never leave the primary hash
		// claiming a destination is still applied after an alternate operation.
		if op.Kind == domain.OperationBackup || op.Kind == domain.OperationDeleteBackup || op.Kind == domain.OperationRestore {
			s.PBMHash = ""
		}
		if op.Kind == domain.OperationRestore && result.Done && err == nil {
			s.Phase = "ready"
		}
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	payload := map[string]string{}
	if e = decodeJSON(op.Payload, &payload); e != nil {
		return result, invalid("invalid operation payload")
	}
	if op.Kind == domain.OperationBackup || op.Kind == domain.OperationDeleteBackup || op.Kind == domain.OperationRestore {
		storage := rc.Cluster.Spec.Backup.Storage
		if name := payload["backup"]; name != "" {
			b, err := rc.Store.GetBackup(ctx, rc.Cluster.Name, name)
			if op.Kind == domain.OperationDeleteBackup && missing(err) {
				return engine.OpResult{Done: true, Step: "done", Checkpoint: json.RawMessage(`{"step":"done"}`)}, nil
			}
			if err != nil {
				return result, err
			}
			storage = b.Storage
		}
		if name := payload["restore"]; name != "" {
			r, err := rc.Store.GetRestore(ctx, rc.Cluster.Name, name)
			if err != nil {
				return result, err
			}
			if r.Source.Backup != "" {
				source := r.Source.SourceCluster
				if source == "" {
					source = rc.Cluster.Name
				}
				b, err := rc.Store.GetBackup(ctx, source, r.Source.Backup)
				if err != nil {
					return result, err
				}
				storage = b.Storage
			}
		}
		env, e := p.pbmEnvForStorage(ctx, rc, s, storage)
		if e != nil {
			return result, e
		}
		if storage != rc.Cluster.Spec.Backup.Storage {
			env.PITR.Enabled = false
		}
		switch op.Kind {
		case domain.OperationBackup:
			return pbm.RunBackup(ctx, env, op)
		case domain.OperationDeleteBackup:
			return pbm.RunDeleteBackup(ctx, env, op)
		default:
			s.Phase = "restore"
			result, e = pbm.RunRestore(ctx, env, op)
			if e != nil || !result.Done {
				return result, e
			}
			// Every successful restore needs a new base when the target desires
			// PITR, regardless of the temporarily disabled source configuration.
			if rc.Cluster.Spec.Backup.PITR.Enabled {
				if e = setInternal(rc.Cluster, "freshBaseBackupPending", true); e != nil {
					return result, e
				}
				name := "restore-base-" + secrets.Hash(op.ID)[:12]
				payload, _ := json.Marshal(map[string]string{"backup": name})
				base := &domain.Operation{ID: op.ID + "-base", Cluster: rc.Cluster.Name, Kind: domain.OperationBackup, Payload: payload, Phase: domain.PhasePending}
				if _, e = rc.Store.GetOperation(ctx, base.ID); missing(e) {
					e = rc.Store.EnqueueBackup(ctx, &domain.Backup{Name: name, Cluster: rc.Cluster.Name, Storage: rc.Cluster.Spec.Backup.Storage, Type: "logical", Trigger: "restore", Phase: domain.PhasePending}, base)
				}
				if e != nil {
					return result, e
				}
				if e = setInternal(rc.Cluster, "freshBaseBackupName", name); e != nil {
					return result, e
				}
			}
			s.PBMHash = ""
			return result, nil
		}
	}
	cp := operationState{}
	if e = decodeJSON(op.Checkpoint, &cp); e != nil {
		return result, e
	}
	switch op.Kind {
	case domain.OperationRaiseFCV:
		version := payload["version"]
		if version != major(rc.Cluster.Spec.Version) {
			return result, invalid("FCV must equal the deployed engine major")
		}
		for _, m := range s.Members {
			st, e := agentStatus(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if major(st.EngineVersion) != version || major(m.Version) != version {
				return result, fmt.Errorf("%w: all members must run %s before FCV raise", engine.ErrConflict, version)
			}
		}
		sets := []string{"rs0"}
		if rc.Cluster.Spec.Topology.Type == "sharded" {
			sets = []string{"router"}
		}
		for _, set := range sets {
			c, e := p.dial(ctx, rc, s, set, "clusterAdmin")
			if e != nil {
				return result, e
			}
			e = mongoadmin.SetFCV(ctx, c, version)
			_ = c.Disconnect(ctx)
			if e != nil {
				return result, e
			}
		}
		return opResult(cp, true, "fcv-raised")
	case domain.OperationRestart:
		return p.rollingOperation(ctx, rc, s, cp, false)
	case domain.OperationRotateTLS:
		if cp.Stage == "" {
			cp.Stage = "rotate-certificates"
			s.ConfigVersion++
			cp.ConfigVersion = s.ConfigVersion
			cp.Stage = "rotate-certificates"
			return opResult(cp, false, "certificates-issued")
		}
		return p.rollingOperation(ctx, rc, s, cp, true)
	case domain.OperationRotateSystemUser:
		user := payload["user"]
		if !users.IsSystemUser(user) {
			return result, invalid("unknown system user")
		}
		key := "rotations/" + op.ID
		if e = ensureSecret(ctx, rc, key, secrets.Password); e != nil {
			return result, e
		}
		if !slices.Contains(s.RotationSecrets, key) {
			s.RotationSecrets = append(s.RotationSecrets, key)
		}
		pw, e := secret(ctx, rc, key)
		if e != nil {
			return result, e
		}
		if cp.Stage == "" || cp.Stage == "passwords" {
			cp.Stage = "passwords"
			for set, st := range s.Sets {
				if st.Stage != "ready" || slices.Contains(cp.Done, set) {
					continue
				}
				c, e := p.dial(ctx, rc, s, set, "userAdmin")
				if e != nil && user == "userAdmin" {
					ca, _ := secret(ctx, rc, "tls/ca.crt")
					c, e = mongoadmin.Dial(ctx, mongoadmin.DialOptions{Hosts: hosts(s.setMembers(set)), ReplicaSet: set, Username: "userAdmin", Password: string(pw), TLS: s.TLSMode != "disabled", CAPEM: ca})
				}
				if e != nil {
					return result, e
				}
				e = users.SetPassword(ctx, c, "admin", user, string(pw))
				_ = c.Disconnect(ctx)
				if e != nil {
					return result, e
				}
				cp.Done = append(cp.Done, set)
				return opResult(cp, false, "password-set")
			}
			if e = putSecret(ctx, rc, "users/"+user, pw); e != nil {
				return result, e
			}
			cp.Stage = "reload"
			cp.Done = nil
			s.ConfigVersion++
			cp.ConfigVersion = s.ConfigVersion
			return opResult(cp, false, "password-persisted")
		}
		if user == "backup" || user == "clusterMonitor" {
			result, e = p.rollingOperation(ctx, rc, s, cp, true)
		} else {
			result, e = opResult(cp, true, "password-rotated")
		}
		if e == nil && result.Done {
			e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Name, key)
		}
		return result, e
	case domain.OperationReplaceMember:
		old := s.find(payload["member"])
		if cp.Stage == "" {
			if old == nil || old.Role == agentapi.RoleRouter {
				return result, invalid("replacement requires a data/config member")
			}
			m := s.add(rc.Cluster.Name, old.Set, old.Role, old.Shard)
			if e := allocateNewPlans(ctx, rc, s, []*member{m}); e != nil {
				removeRecord(s, m.Name)
				return result, e
			}
			cp.NewMember = m.Name
			cp.Stage = "join"
			s.ReplaceActive = true
			return opResult(cp, false, "replacement-planned")
		}
		if cp.Stage == "join" {
			m := s.find(cp.NewMember)
			if m == nil {
				return result, fmt.Errorf("replacement state lost")
			}
			if m.Stage != "active" && m.Stage != "replacement-caught-up" {
				return opResult(cp, false, "replacement-sync")
			}
			if m.Stage == "replacement-caught-up" && component(rc.Cluster.Spec, m).Replicas == 1 {
				c, err := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
				if err != nil {
					return result, err
				}
				err = mongoadmin.Promote(ctx, c, m.Name+":27017")
				_ = c.Disconnect(ctx)
				if err != nil {
					return result, err
				}
				m.Stage = "active"
				return opResult(cp, false, "singleton-replacement-promoted")
			}
			if old != nil {
				old.Removing = true
			}
			cp.Stage = "remove"
			return opResult(cp, false, "replacement-caught-up")
		}
		if old != nil {
			return opResult(cp, false, "replacement-remove")
		}
		m := s.find(cp.NewMember)
		if m != nil && m.Stage != "active" {
			m.ReplacingReady = true
			m.Stage = "syncing"
			return opResult(cp, false, "replacement-promote")
		}
		s.ReplaceActive = false
		return opResult(cp, true, "member-replaced")
	default:
		return result, fmt.Errorf("%w: unknown operation %q", engine.ErrNotSupported, op.Kind)
	}
}
func (p *Provider) rollingOperation(ctx context.Context, rc *engine.Context, s *internalState, cp operationState, reload bool) (engine.OpResult, error) {
	// An operation may start while convergence still owns a platform action.
	// Preserve its checkpoint until that action (including any retry) completes.
	if s.Pending != nil || s.Retry != nil {
		busy, err := p.observePending(ctx, rc, s)
		if !busy && err == nil {
			_, err = p.retryPending(ctx, rc, s)
		}
		result, checkpointErr := opResult(cp, false, "waiting-platform")
		if err != nil {
			return result, err
		}
		return result, checkpointErr
	}
	if cp.Waiting != "" {
		m := s.find(cp.Waiting)
		if m == nil {
			return engine.OpResult{}, engine.ErrNotFound
		}
		st, e := agentStatus(ctx, rc, m.Name)
		if e != nil || !running(st, m.Role) || reload && st.ConfigVersion < cp.ConfigVersion {
			return opResult(cp, false, "waiting-agent")
		}
		if m.Role != agentapi.RoleRouter {
			rs, e := p.replication(ctx, rc, s, m)
			if e != nil || !caught(rs, m.Name) {
				return opResult(cp, false, "waiting-replication")
			}
		}
		cp.Done = append(cp.Done, m.Name)
		cp.Waiting = ""
		return opResult(cp, false, "member-healthy")
	}
	pending := []*member{}
	for _, m := range ordered(s) {
		if !m.Removing && !slices.Contains(cp.Done, m.Name) {
			pending = append(pending, m)
		}
	}
	if len(pending) == 0 {
		return opResult(cp, true, "rolling-complete")
	}
	candidate := pending[0]
	if candidate.Role != agentapi.RoleRouter {
		rs, e := p.replication(ctx, rc, s, candidate)
		if e != nil {
			return engine.OpResult{}, e
		}
		for _, m := range pending {
			if m.Set != candidate.Set {
				continue
			}
			for _, obs := range rs.Members {
				if obs.Name == m.Name+":27017" && obs.State == "SECONDARY" {
					candidate = m
					break
				}
			}
		}
	}
	stepped, e := p.prepareDisruption(ctx, rc, s, candidate)
	if e != nil {
		return engine.OpResult{}, e
	}
	if stepped {
		return opResult(cp, false, "primary-stepped-down")
	}
	if reload {
		if cp.Stage == "rotate-certificates" {
			if err := rotateCertificate(ctx, rc, candidate); err != nil {
				return engine.OpResult{}, err
			}
		}
		for _, user := range []string{"backup", "clusterMonitor"} {
			pw, err := secret(ctx, rc, "users/"+user)
			if err != nil {
				return engine.OpResult{}, err
			}
			if err = putSecret(ctx, rc, "members/"+candidate.Name+"/users/"+user, pw); err != nil {
				return engine.OpResult{}, err
			}
		}
		activateConfig(rc.Cluster, s, candidate)
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
	}
	a, e := agent(ctx, rc, candidate.Name)
	if e != nil {
		return engine.OpResult{}, e
	}
	defer a.Close()
	if reload {
		e = a.Reload(ctx)
	} else {
		process := "mongod"
		if candidate.Role == agentapi.RoleRouter {
			process = "mongos"
		}
		e = a.Process(ctx, agentapi.ProcessAction{Process: process, Action: "restart"})
	}
	if e != nil {
		return engine.OpResult{}, e
	}
	cp.Waiting = candidate.Name
	return opResult(cp, false, "member-restarted")
}

// RefreshFreshBase clears the PITR gate only after the newly queued base snapshot succeeds.
func refreshFreshBase(ctx context.Context, rc *engine.Context) error {
	var raw struct {
		Pending bool   `json:"freshBaseBackupPending"`
		Name    string `json:"freshBaseBackupName"`
	}
	if e := decodeJSON(rc.Cluster.Internal, &raw); e != nil {
		return e
	}
	if !raw.Pending {
		return nil
	}
	backup, e := rc.Store.GetBackup(ctx, rc.Cluster.Name, raw.Name)
	if e != nil {
		return e
	}
	if backup.Phase == domain.PhaseSucceeded {
		return setInternal(rc.Cluster, "freshBaseBackupPending", false)
	}
	if backup.Phase == domain.PhaseFailed {
		return fmt.Errorf("fresh PITR base backup failed: %s", strings.TrimSpace(backup.Error))
	}
	return nil
}

// SyncDuringOperation permits only the convergence explicitly required by
// replacement. Restore/restart/password recovery must never run ordinary Sync.
func (*Provider) SyncDuringOperation(op *domain.Operation) bool {
	if op.Kind != domain.OperationReplaceMember {
		return false
	}
	var cp operationState
	if decodeJSON(op.Checkpoint, &cp) != nil {
		return false
	}
	return cp.Stage == "join" || cp.Stage == "remove"
}
