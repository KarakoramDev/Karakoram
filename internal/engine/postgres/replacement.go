// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// replacementState reserves both identities until the replacement is streaming
// and its predecessor has been retired. Every transition is persisted with the
// cluster, so a lost operation response cannot allocate a second replacement.
type replacementState struct {
	Operation string `json:"operation"`
	Old       string `json:"old"`
	New       string `json:"new"`
	Stage     string `json:"stage"`
	Recovery  string `json:"recovery,omitempty"`
}

// SyncDuringOperation permits provisioning only while a replacement joins.
// Maintenance operations must retain exclusive ownership of the data plane.
func (*Core) SyncDuringOperation(op *domain.Operation) bool {
	if op.Kind != domain.OperationReplaceMember {
		return false
	}
	var cp replacementState
	return json.Unmarshal(op.Checkpoint, &cp) == nil && cp.Stage == "join"
}

func (p *Core) replaceMember(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if !patroniTopology(s.Topology) || rc.Cluster.Spec.Components.Engine.Replicas < 2 {
		return result, engine.Invalid("member replacement requires a replicated group")
	}
	if s.Replacement == nil {
		// A completed checkpoint is authoritative if the final response was lost.
		var cp replacementState
		if len(op.Checkpoint) > 0 {
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
				return result, fmt.Errorf("decode replacement checkpoint: %w", err)
			}
			if cp.Stage == "done" {
				return engine.OpResult{Done: true}, nil
			}
		}
		var payload struct {
			Member string `json:"member"`
		}
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return result, engine.Invalid("invalid replacement request")
		}
		old := s.find(payload.Member)
		if old == nil || old.Role != "data" {
			return result, engine.Invalid("replacement target must be a data member")
		}
		if !s.Formed || s.Pending != nil || len(s.group(old.Group).data()) != rc.Cluster.Spec.Components.Engine.Replicas {
			return result, engine.ErrUnavailable
		}
		m := s.newGroupMember(rc.Cluster, old.Group)
		s.Replacement = &replacementState{Operation: op.ID, Old: old.Name, New: m.Name, Stage: "join"}
	}
	cp := s.Replacement
	if cp.Operation != op.ID {
		return result, engine.ErrConflict
	}
	return p.advanceReplacement(ctx, rc, s)
}

func (p *Core) advanceReplacement(ctx context.Context, rc *engine.Context, s *state) (engine.OpResult, error) {
	var result engine.OpResult
	cp := s.Replacement
	if cp.Recovery == "rollback" {
		return p.rollbackReplacement(ctx, rc, s)
	}
	old, next := s.find(cp.Old), s.find(cp.New)
	if next == nil {
		return result, fmt.Errorf("replacement identity missing: %w", engine.ErrNotFound)
	}
	switch cp.Stage {
	case "join":
		if next.Stage == "joined" {
			st, e := live(ctx, rc, next.Name)
			if e != nil {
				return result, e
			}
			if st.Running && st.Role == "replica" && st.Streaming {
				cp.Stage = "fence"
			}
		}
	case "fence", "switchover":
		if old == nil {
			return result, engine.ErrNotFound
		}
		fenced, e := fenceRetirement(ctx, rc, s, old)
		if e != nil {
			return result, e
		}
		if fenced {
			cp.Stage = "remove-etcd"
		}
	case "remove-etcd":
		if old == nil {
			return result, engine.ErrNotFound
		}
		fenced, e := fenceRetirement(ctx, rc, s, old)
		if e != nil {
			return result, e
		}
		if !fenced {
			break
		}
		if e := removeEtcdMember(ctx, rc, s, old); e != nil {
			return result, e
		}
		old.Removing = true
		old.Retirement = "removed-etcd"
		cp.Stage = "promote"
	case "promote":
		if old != nil && old.ServiceID != "" {
			fenced, e := fenceRetirement(ctx, rc, s, old)
			if e != nil || !fenced {
				return replacementResult(cp), e
			}
		}
		if e := p.promoteLearner(ctx, rc, s, next); e != nil {
			return result, e
		}
		cp.Stage = "retire"
	case "retire":
		if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
			return replacementResult(cp), e
		}
		if old != nil {
			if old.ServiceID != "" {
				fenced, e := fenceRetirement(ctx, rc, s, old)
				if e != nil || !fenced {
					return replacementResult(cp), e
				}
			}
			done, e := p.retireMember(ctx, rc, s, old)
			if e != nil {
				return result, e
			}
			if !done {
				break
			}
		}
		cp.Stage = "done"
	case "done":
		s.Replacement = nil
		return engine.OpResult{Done: true, Checkpoint: mustReplacementCheckpoint(cp)}, nil
	default:
		return result, fmt.Errorf("unknown replacement checkpoint %q", cp.Stage)
	}
	return replacementResult(cp), nil
}

func mustReplacementCheckpoint(cp *replacementState) json.RawMessage {
	// This structure contains only strings; encoding cannot fail.
	raw, _ := json.Marshal(cp)
	return raw
}

func replacementResult(cp *replacementState) engine.OpResult {
	return engine.OpResult{
		Checkpoint: mustReplacementCheckpoint(cp), Step: cp.Stage,
		Message: "Replacing PostgreSQL member", RequeueAfter: 2 * time.Second,
	}
}

// Failure cleanup hands a durable recovery owner to Sync. Once retirement has
// started, completing that same cutover is safer than trying to undo stop holds
// or an etcd removal whose response may have been lost.
func (*Core) SupportsFailureCleanup(op *domain.Operation) bool {
	return op.Kind == domain.OperationReplaceMember || op.Kind == "rebalance" || op.Kind == domain.OperationBackup || restoreOperation(op)
}

func (*Core) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationReplaceMember && op.Kind != "rebalance" {
		return engine.Invalid("operation does not support cancellation")
	}
	return nil
}

func (p *Core) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind == "rebalance" {
		return p.runCitusRebalance(ctx, rc, op, true)
	}
	if op.Kind == domain.OperationBackup {
		return handoffCitusBackupRecovery(rc, op)
	}
	if restoreOperation(op) {
		return handoffRestoreRecovery(rc, op)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	cp := s.Replacement
	if cp == nil {
		return engine.OpResult{Done: true}, nil
	}
	if cp.Operation != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	if cp.Recovery == "" {
		old := s.find(cp.Old)
		if cp.Stage == "join" || cp.Stage == "fence" && old != nil && old.Retirement == "" {
			cp.Recovery = "rollback"
		} else {
			cp.Recovery = "complete"
		}
	}
	if err := save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: true, Step: "replacement-recovery", Checkpoint: mustReplacementCheckpoint(cp)}, nil
}

func (p *Core) rollbackReplacement(ctx context.Context, rc *engine.Context, s *state) (engine.OpResult, error) {
	cp := s.Replacement
	if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
		return replacementResult(cp), err
	}
	next := s.find(cp.New)
	if next == nil {
		s.Replacement = nil
		return engine.OpResult{Done: true}, nil
	}
	if err := verifyMember(ctx, rc, next); err != nil {
		return replacementResult(cp), err
	}
	if next.DeployOp != nil {
		status, err := rc.Backend.OpStatus(ctx, *next.DeployOp)
		if err != nil || !status.Done && !status.Failed {
			return replacementResult(cp), err
		}
		next.DeployOp = nil
	}
	if next.ServiceID != "" {
		// A partially bootstrapped learner need not ever become a streaming
		// replica. Fence its runtime directly, but only with another proven
		// quorum-backed writer. Never stop the retained predecessor.
		if next.Retirement == "" {
			if st, err := live(ctx, rc, next.Name); err == nil && retirementLeader(st, next.Name) {
				return replacementResult(cp), switchAway(ctx, rc, s, next.Name)
			}
			if ok, err := survivingPrimary(ctx, rc, s, next.Name); err != nil || !ok {
				return replacementResult(cp), err
			}
			next.Retirement = "runtime-stop"
			return replacementResult(cp), nil
		}
		if fenced, err := fenceRetirement(ctx, rc, s, next); err != nil || !fenced {
			return replacementResult(cp), err
		}
	}
	// Observe membership even when add-learner did not return its receipt.
	if err := removeEtcdMember(ctx, rc, s, next); err != nil {
		return replacementResult(cp), err
	}
	done, err := p.retireMember(ctx, rc, s, next)
	if done {
		s.Replacement = nil
	}
	return engine.OpResult{Done: done, RequeueAfter: 2 * time.Second}, err
}

// switchAway reobserves both ends of a handoff on every reconciliation. A
// missing safe candidate is a convergence wait, not an operation failure.
func switchAway(ctx context.Context, rc *engine.Context, s *state, old string) error {
	return switchAwayConfigured(ctx, rc, s, old, false)
}

func switchAwayConfigured(ctx context.Context, rc *engine.Context, s *state, old string, currentConfig bool) error {
	writer, err := live(ctx, rc, old)
	if err != nil {
		return err
	}
	if writer.Role != "primary" || !writer.Running || writer.Leader != old && (writer.Leader != "" || writer.HealthCapabilities != nil) {
		return nil // Never ask a demoted member to switch the new writer.
	}
	// Legacy agents omit local DCS observations. Patroni still validates the
	// handoff; only an advertised probe can authoritatively block it here.
	if writer.HealthCapabilities != nil && writer.HealthCapabilities.LocalEtcd && !writer.EtcdHealthy {
		return nil
	}
	// Previous-release status also omits Leader. A legacy primary role may
	// request a Patroni-validated handoff, but an explicit mismatched leader
	// (or missing leader from an agent with modern observations) never may.
	if writer.Paused {
		return blockedSwitchover(s, "PostgreSQL switchover is blocked: resume Patroni before rolling this primary")
	}
	// PostgreSQL flavours share this field but may have additional options.
	// Full schema validation belongs to the provider's flavour-aware parser.
	var options struct {
		SynchronousMode string `json:"synchronousMode"`
	}
	if len(rc.Cluster.Spec.EngineOptions) > 0 {
		err = json.Unmarshal(rc.Cluster.Spec.EngineOptions, &options)
	}
	if err != nil {
		return engine.Invalid("invalid synchronous replication options")
	}
	candidate := ""
	for _, m := range s.memberGroup(old).data() {
		if m.Name == old || m.Removing || m.Retirement != "" || m.EtcdLearner || m.DeployOp != nil || m.Stage != "joined" || currentConfig && m.ConfigVersion != s.ConfigVersion {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil || !st.Running || st.Role != "replica" || !st.Streaming || st.FailoverFenced || st.Paused || st.Leader != old && (st.Leader != "" || st.HealthCapabilities != nil) {
			continue
		}
		if options.SynchronousMode != "" && options.SynchronousMode != "off" && st.SyncState != "sync" {
			continue
		}
		if candidate == "" || st.SyncState == "sync" {
			candidate = m.Name
		}
		if st.SyncState == "sync" {
			break
		}
	}
	if candidate == "" {
		return nil
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), old)
	if err != nil {
		return err
	}
	defer a.Close()
	err = a.Engine(ctx, "postgres", "switchover", pgspec.SwitchoverRequest{Candidate: candidate}, nil)
	var rejection *agentapi.HTTPError
	if !errors.As(err, &rejection) || rejection.StatusCode != 409 {
		return err
	}
	// A concurrent graceful stop/failover can win between discovery and POST.
	// Success here only means reobserve; callers still prove demotion before
	// fencing, stopping, or resizing the former writer.
	current, observeErr := live(ctx, rc, old)
	if observeErr == nil && current.Running && current.Role == "replica" && current.Streaming && current.Leader != "" && current.Leader != old {
		return nil
	}
	switch rejection.Message {
	case "switchover_leader_changed", "switchover_candidate_changed", "switchover_in_progress":
		return nil // Re-derive topology rather than spend the operation retry budget.
	case "switchover_unavailable":
		return err
	case "switchover_paused":
		return blockedSwitchover(s, "PostgreSQL switchover is blocked: resume Patroni before rolling this primary")
	case "switchover_rejected":
		return blockedSwitchover(s, "PostgreSQL switchover was rejected; inspect Patroni candidate health, synchronous membership and DCS state before retrying")
	default:
		// Older agents return plaintext 409 for transient failures too. An
		// absent or unknown classification cannot prove a permanent rejection.
		return err
	}
}

func blockedSwitchover(s *state, message string) error {
	s.SwitchoverBlocked = message
	return engine.Invalid("%s", message)
}
