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
	return engine.OpResult{Done: done, Step: step, Message: "Executing " + strings.ReplaceAll(step, "-", " "), Checkpoint: b, RequeueAfter: 5 * time.Second}, e
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
		storageNamespace := rc.Cluster.Ref().Namespace
		if name := payload["backup"]; name != "" {
			b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
			if op.Kind == domain.OperationDeleteBackup && missing(err) {
				return engine.OpResult{Done: true, Step: "done", Checkpoint: json.RawMessage(`{"step":"done"}`)}, nil
			}
			if err != nil {
				return result, err
			}
			storage = b.Storage
		}
		if name := payload["restore"]; name != "" {
			r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
			if err != nil {
				return result, err
			}
			sourceRef := domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}
			if sourceRef.Namespace == "" {
				sourceRef.Namespace = rc.Cluster.Ref().Namespace
			}
			if sourceRef.Name == "" {
				sourceRef.Name = rc.Cluster.Name
			}
			storageNamespace = sourceRef.Namespace
			if r.Source.Backup != "" {
				b, err := rc.Store.GetBackup(ctx, sourceRef, r.Source.Backup)
				if err != nil {
					return result, err
				}
				storage = b.Storage
			}
			if r.Source.Backup == "" {
				source, err := rc.Store.GetCluster(ctx, sourceRef)
				if err != nil {
					return result, err
				}
				storage = source.Spec.Backup.Storage
			}
		}
		env, e := p.pbmEnvForStorage(ctx, rc, s, storageNamespace, storage)
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
				name := "restore-base-" + secrets.Hash(op.ID)[:12]
				payload, _ := json.Marshal(map[string]string{"backup": name, "trigger": "restore"})
				base := &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, ID: op.ID + "-base", Cluster: rc.Cluster.Name, Kind: domain.OperationBackup, Payload: payload, Phase: domain.PhasePending}
				if _, e = rc.Store.GetOperation(ctx, base.ID); missing(e) {
					e = rc.Store.EnqueueBackup(ctx, &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Name: name, Cluster: rc.Cluster.Name, Storage: rc.Cluster.Spec.Backup.Storage, Type: "logical", Trigger: "restore", Phase: domain.PhasePending, CreatedAt: now(rc)}, base)
				}
				if e != nil {
					return result, e
				}
				// Preserve the original freshness boundary when completion is replayed.
				var fresh freshBaseState
				if e = decodeJSON(rc.Cluster.Internal, &fresh); e != nil {
					return result, e
				}
				if fresh.Root != name {
					fresh = freshBaseState{Pending: true, Name: name, Root: name, RequiredAt: now(rc)}
				}
				if e = fresh.save(rc.Cluster); e != nil {
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
		return p.raiseFCV(ctx, rc, s, cp, payload["version"], p.setFCV)
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
					c, e = mongoadmin.Dial(ctx, mongoadmin.DialOptions{Hosts: hosts(s.setMembers(set)), Dialer: rc.Dialer, ReplicaSet: set, Username: "userAdmin", Password: string(pw), TLS: s.TLSMode != "disabled", CAPEM: ca})
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
			e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key)
			if e == nil {
				if s.LastRotatedAt == nil {
					s.LastRotatedAt = map[string]*time.Time{}
				}
				stamp := now(rc).UTC()
				s.LastRotatedAt[user] = &stamp
			}
		}
		return result, e
	case domain.OperationReplaceMember:
		old := s.find(payload["member"])
		if cp.Stage == "" {
			if old == nil || old.Role == agentapi.RoleRouter {
				return result, invalid("replacement requires a data/config member")
			}
			m := s.add(rc.Cluster.Name, old.Set, old.Role, old.Shard)
			m.ReplacesVolume = old.Volume
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

// raiseFCV separates prerequisite validation from the irreversible database call.
func (p *Provider) raiseFCV(ctx context.Context, rc *engine.Context, s *internalState, cp operationState, version string,
	apply func(context.Context, *engine.Context, *internalState, string) error,
) (engine.OpResult, error) {
	if err := p.ValidateFCV(ctx, rc, version); err != nil {
		var field *domain.FieldError
		var resource *domain.ResourceError
		if errors.As(err, &field) || errors.As(err, &resource) {
			return engine.OpResult{Done: true, Step: "fcv-rejected", Message: err.Error()}, err
		}
		return engine.OpResult{Step: "fcv-preflight", Message: err.Error()}, err
	}
	if cp.Stage != "apply-fcv" {
		cp.Stage = "apply-fcv"
		return opResult(cp, false, "fcv-ready")
	}
	if err := apply(ctx, rc, s, version); err != nil {
		return engine.OpResult{}, err
	}
	return opResult(cp, true, "fcv-raised")
}

func (p *Provider) setFCV(ctx context.Context, rc *engine.Context, s *internalState, version string) error {
	set := "rs0"
	if rc.Cluster.Spec.Topology.Type == "sharded" {
		set = "router"
	}
	c, err := p.dial(ctx, rc, s, set, "clusterAdmin")
	if err != nil {
		return err
	}
	defer c.Disconnect(ctx)
	return mongoadmin.SetFCV(ctx, c, version)
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

const freshBaseRetryLimit = 3

type freshBaseState struct {
	Pending    bool      `json:"freshBaseBackupPending"`
	Name       string    `json:"freshBaseBackupName"`
	Root       string    `json:"freshBaseBackupRoot"`
	Retries    int       `json:"freshBaseBackupRetries"`
	RequiredAt time.Time `json:"freshBaseBackupRequiredAt"`
	Exhausted  bool      `json:"freshBaseBackupExhausted"`
}

func (s freshBaseState) save(c *domain.Cluster) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	var fresh map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fresh); err != nil {
		return err
	}
	fields := map[string]json.RawMessage{}
	if err = decodeJSON(c.Internal, &fields); err != nil {
		return err
	}
	for key, value := range fresh {
		fields[key] = value
	}
	c.Internal, err = json.Marshal(fields)
	return err
}

// refreshFreshBase keeps PITR gated, but never wedges convergence on a lost base.
func refreshFreshBase(ctx context.Context, rc *engine.Context) error {
	var fresh freshBaseState
	if err := decodeJSON(rc.Cluster.Internal, &fresh); err != nil {
		return err
	}
	if !fresh.Pending {
		return nil
	}
	backup, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), fresh.Name)
	if err != nil && !missing(err) {
		return err
	}
	if fresh.Root == "" {
		fresh.Root = fresh.Name
	}
	if fresh.RequiredAt.IsZero() {
		// Older pending gates have no boundary. A known base was enqueued after
		// restore; without its record, only snapshots queued from now are safe.
		fresh.RequiredAt = now(rc)
		if backup != nil && !backup.CreatedAt.IsZero() {
			fresh.RequiredAt = backup.CreatedAt
		}
	}
	all, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, candidate := range all {
		if candidate.Phase == domain.PhaseSucceeded && candidate.Storage == rc.Cluster.Spec.Backup.Storage &&
			(candidate.Name == fresh.Name || !candidate.CreatedAt.Before(fresh.RequiredAt)) {
			fresh.Pending = false
			return fresh.save(rc.Cluster)
		}
	}
	if backup != nil && backup.Phase != domain.PhaseFailed && backup.Phase != domain.PhaseCancelled {
		return fresh.save(rc.Cluster)
	}
	reason, message := "FreshBaseBackupRetry", ""
	if fresh.Retries >= freshBaseRetryLimit {
		if fresh.Exhausted {
			return nil
		}
		fresh.Exhausted = true
		reason = "FreshBaseBackupRequired"
		message = fmt.Sprintf("PITR remains disabled after %d automatic base backup retries; take a successful backup on primary storage %q to resume PITR. Cluster convergence continues.", freshBaseRetryLimit, rc.Cluster.Spec.Backup.Storage)
	} else {
		attempt := fresh.Retries + 1
		name := fmt.Sprintf("%s-retry-%d", fresh.Root, attempt)
		id := rc.Cluster.Ref().Key() + "-" + name
		payload, _ := json.Marshal(map[string]string{"backup": name, "trigger": "restore"})
		message = fmt.Sprintf("Post-restore base backup %q failed, was cancelled or is missing; queued %q (retry %d/%d). PITR remains disabled until a fresh primary-storage snapshot succeeds.", fresh.Name, name, attempt, freshBaseRetryLimit)
		op := &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, ID: id, Cluster: rc.Cluster.Name, Kind: domain.OperationBackup, Payload: payload, Phase: domain.PhasePending, Message: message}
		if _, err = rc.Store.GetOperation(ctx, id); missing(err) {
			err = rc.Store.EnqueueBackup(ctx, &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Name: name, Cluster: rc.Cluster.Name, Storage: rc.Cluster.Spec.Backup.Storage, Type: "logical", Trigger: "restore", Phase: domain.PhasePending, CreatedAt: now(rc)}, op)
		}
		if err != nil {
			return err
		}
		fresh.Name, fresh.Retries = name, attempt
	}
	if rc.Events != nil {
		if err = rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: reason, Message: message, CreatedAt: now(rc)}); err != nil {
			return err
		}
	}
	return fresh.save(rc.Cluster)
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
