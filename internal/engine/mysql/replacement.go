// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"time"
)

type replacementState struct {
	Operation       string `json:"operation"`
	Old             string `json:"old"`
	New             string `json:"new"`
	Stage           string `json:"stage"`
	Recovery        string `json:"recovery,omitempty"`
	IdentityBarrier bool   `json:"identityBarrier,omitempty"`
}

func replacementResult(cp *replacementState) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: cp.Stage, RequeueAfter: 2 * time.Second}
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
	if s.LastReplacement == op.ID {
		return engine.OpResult{Done: true}, nil
	}
	if !clustered(s.Topology) {
		return result, engine.Invalid("MySQL member replacement requires group topology")
	}
	if s.Replacement == nil {
		for _, m := range s.Members {
			if m.ProxyDraining || m.ProxyDrainOperation != "" {
				return result, fmt.Errorf("member replacement waits for agent rollout drain settlement of %s: %w", m.Name, engine.ErrUnavailable)
			}
		}
		var previous replacementState
		if len(op.Checkpoint) != 0 {
			if err := json.Unmarshal(op.Checkpoint, &previous); err != nil {
				return result, err
			}
			if previous.Stage == "done" {
				return engine.OpResult{Done: true, Checkpoint: op.Checkpoint}, nil
			}
		}
		if err := maintenanceAllowed(ctx, rc, s); err != nil {
			return result, err
		}
		if s.Rolling != nil || s.Pending != nil || len(s.data()) != rc.Cluster.Spec.Components.Engine.Replicas {
			return result, engine.ErrUnavailable
		}
		var payload struct {
			Member string `json:"member"`
		}
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return result, engine.Invalid("invalid MySQL replacement request")
		}
		old := s.find(payload.Member)
		if old == nil || old.Role != "data" {
			return result, engine.Invalid("replacement target must be a MySQL data member")
		}
		identityBarrier, err := preflightIdentityBarrier(ctx, rc, s, old.Name, old.ConfirmedVolumeLost)
		if err != nil {
			return result, err
		}
		if old.ConfirmedVolumeLost {
			if err := p.volumeRecoverySafe(ctx, rc, s, old); err != nil {
				old.RuntimeRecoveryReason = err.Error()
				return result, err
			}
		}
		var survivors []string
		for _, m := range s.data() {
			if m.Name != old.Name && !m.Removing {
				survivors = append(survivors, m.Name)
			}
		}
		avoid, err := engine.ReplacementPlacementAvoid(ctx, rc, survivors, nil)
		if err != nil {
			return result, err
		}
		if s.Topology == "group" && !identityBarrier {
			if rc.Events != nil {
				if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: old.Name, Type: "Warning", Reason: "LegacyGRRetirement", Message: "Admitting confirmed-loss GR replacement using the released ONLINE/count proof because surviving agents lack full GroupView capability; upgrade agents after recovery with karactl -n " + rc.Cluster.Ref().Namespace + " clusters upgrade-agents " + rc.Cluster.Name + " --wait", CreatedAt: now(rc)}); err != nil {
					return result, err
				}
				old.LegacyRetirementWarned = true
			}
		}
		index := s.NextIndex
		s.NextIndex++
		name := fmt.Sprintf("%sm%d", rc.Cluster.Name, index)
		s.Members = append(s.Members, &member{Name: name, Index: index, Role: "data", Volume: name + "v", ReplacesVolume: old.Volume, PlacementAvoid: avoid, Stage: "planned", ConfigVersion: s.ConfigVersion})
		s.Replacement = &replacementState{Operation: op.ID, Old: old.Name, New: name, Stage: "join", IdentityBarrier: identityBarrier}
		if rc.Cluster.Spec.Provider == "pxc" {
			preparePXCFormationCertification(s)
		}
		publishRevision(s)
	}
	if s.Replacement.Operation != op.ID {
		return result, engine.ErrConflict
	}
	return p.advanceReplacement(ctx, rc, s)
}

// replacementNeverDeployed recognizes only the untouched allocation checkpoint.
// Import intent and resource settings are persisted before backend submission;
// deploy requires the later imported checkpoint. Missing receipts or inventory
// after a restart therefore cannot turn a possibly submitted deploy into proof.
func replacementNeverDeployed(s *state, m *member) bool {
	cp := s.Replacement
	return cp != nil && cp.Recovery == "rollback" && cp.New == m.Name &&
		m.Role == "data" && m.Stage == "planned" &&
		m.ServiceID == "" && m.VolumeID == "" && m.ServerUUID == "" &&
		m.ImportBatch == nil && m.DeployOp == nil && m.RetirementOp == nil &&
		(s.Pending == nil || s.Pending.Member != m.Name) &&
		m.Resources == (domain.Resources{}) && m.Replicas == 0 && m.Version == "" &&
		!m.Initialized && m.NativeReadiness == nil && m.Clone == nil && !m.CloneDone &&
		!m.CloneReplacesData && m.LastGTIDExecuted == "" && !m.ConfirmedVolumeLost &&
		m.RuntimeRecovery == (engine.RuntimeRecovery{})
}

func (p *Core) advanceReplacement(ctx context.Context, rc *engine.Context, s *state) (engine.OpResult, error) {
	cp := s.Replacement
	switch cp.Stage {
	case "join":
		next := s.find(cp.New)
		if next == nil {
			if cp.Recovery == "rollback" {
				// The formation lifecycle may already have retired the entire
				// unadmitted cohort before this replacement can resume.
				cp.Stage = "done"
				return replacementResult(cp), nil
			}
			return engine.OpResult{}, engine.ErrNotFound
		}
		if cp.Recovery == "rollback" {
			if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
				return replacementResult(cp), err
			}
			if next.ImportBatch != nil {
				op, err := resumePlannedImport(ctx, rc, next)
				if err != nil {
					return replacementResult(cp), err
				}
				if op != nil {
					p.track(s, next, "import", *op)
					return replacementResult(cp), nil
				}
				next.ImportBatch = nil
			}
			if next.DeployOp != nil {
				status, err := rc.Backend.OpStatus(ctx, *next.DeployOp)
				if err != nil || !status.Done && !status.Failed {
					return replacementResult(cp), err
				}
				next.DeployOp = nil
			}
			next.Removing, next.Offline = true, true
			next.Retirement = "runtime-stop"
			next.IdentityBarrier = cp.IdentityBarrier
			if replacementNeverDeployed(s, next) {
				next.Retirement = "credentials"
			}
			cp.Stage = "retire"
			publishRevision(s)
			return replacementResult(cp), nil
		}
		if next.Stage != "joined" {
			return replacementResult(cp), nil
		}
		st, err := live(ctx, rc, next.Name)
		if err != nil {
			return replacementResult(cp), err
		}
		if st.MemberState == "ONLINE" {
			// PXC join certification intentionally withholds routing until the
			// replacement retires its predecessor and final membership certifies.
			// Retirement still requires an already admitted surviving candidate.
			if !pxcJoinRoutingFenced(s, next) {
				if err := proxyRejoinReady(ctx, rc, s, next.Name); err != nil {
					return replacementResult(cp), err
				}
			}
			if rc.Operation != nil && rc.Operation.Actor != "system" {
				if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
					result := replacementResult(cp)
					result.RequeueAfter = wait.RequeueAfter
					return result, nil
				}
			}
			cp.Stage = "retire"
		}
	case "retire":
		target := cp.Old
		if cp.Recovery == "rollback" {
			target = cp.New
		}
		old := s.find(target)
		if old != nil {
			if old.Retirement == "" {
				old.Retirement = "handoff"
				old.IdentityBarrier = cp.IdentityBarrier
				return replacementResult(cp), nil
			}
			done, err := p.retireMember(ctx, rc, s, old)
			if err != nil || !done {
				return replacementResult(cp), err
			}
		}
		cp.Stage = "done"
	case "done":
		result := replacementResult(cp)
		result.Done = true
		s.LastReplacement = cp.Operation
		s.Replacement = nil
		return result, nil
	default:
		return replacementResult(cp), engine.Invalid("unknown MySQL replacement checkpoint")
	}
	return replacementResult(cp), nil
}

func cancelReplacement(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
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
		if cp.Stage == "join" || cp.Stage == "retire" && old != nil && old.Retirement == "" {
			cp.Recovery = "rollback"
		} else {
			cp.Recovery = "complete"
		}
	}
	if err := save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	result := replacementResult(cp)
	result.Done = true
	return result, nil
}

// Resume pre-freeze allocations written by an older control plane. Only the
// persisted predecessor volume is evidence; a member name is not a volume ID.
func prepareReplacementPlacement(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	cp := s.Replacement
	if cp == nil || cp.New != m.Name || m.ImportBatch != nil {
		return false, nil
	}
	_, authoritative := rc.Backend.(infra.PlacementNodes)
	if m.ReplacesVolume != "" && (!authoritative || m.PlacementAvoid != nil) {
		return false, nil
	}
	if m.ReplacesVolume == "" {
		old := s.find(cp.Old)
		if old == nil || old.Volume == "" {
			return false, engine.Invalid("replacement predecessor volume identity is unavailable; recover the persisted predecessor record before importing %s", m.Name)
		}
		m.ReplacesVolume = old.Volume
	}
	var survivors []string
	for _, member := range s.data() {
		if member.Name != cp.Old && member.Name != cp.New && !member.Removing {
			survivors = append(survivors, member.Name)
		}
	}
	avoid, err := engine.ReplacementPlacementAvoid(ctx, rc, survivors, nil)
	if err != nil {
		return false, err
	}
	m.PlacementAvoid = avoid
	return true, nil
}
