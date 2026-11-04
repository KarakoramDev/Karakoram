// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// operationState contains only durable, non-secret operation progress.
type operationState struct {
	Stage         string                      `json:"stage"`
	Names         []string                    `json:"names,omitempty"`
	Index         int                         `json:"index,omitempty"`
	NewMember     string                      `json:"newMember,omitempty"`
	ConfigVersion int64                       `json:"configVersion,omitempty"`
	OldTokenName  string                      `json:"oldTokenName,omitempty"`
	Failure       string                      `json:"failure,omitempty"`
	Pending       *pending                    `json:"pending,omitempty"`
	AgentTarget   *engine.AgentUpgradePayload `json:"agentTarget,omitempty"`
	Waiting       string                      `json:"waiting,omitempty"`
}

func operationResult(cp operationState, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Stage, Checkpoint: raw, RequeueAfter: 5 * time.Second}
}

func operationMissing(err error) bool {
	return errors.Is(err, engine.ErrNotFound) || errors.Is(err, store.ErrNotFound)
}
func operationInvalid(message string) error { return fmt.Errorf("%w: %s", engine.ErrInvalid, message) }
func operationNow(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
func operationEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	kind := "Normal"
	if reason == "BackupFailed" {
		kind = "Warning"
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: kind, Reason: reason, Message: message})
}

func (p *Provider) runOperation(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation) (engine.OpResult, error) {
	var cp operationState
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	if cp.Stage == "done" {
		if cp.Failure != "" {
			return operationResult(cp, true), errors.New(cp.Failure)
		}
		return operationResult(cp, true), nil
	}
	payload := map[string]string{}
	var relocation struct {
		Member     string `json:"member"`
		Relocate   bool   `json:"relocate"`
		AvoidGroup string `json:"avoidGroup"`
	}
	if op.Kind == domain.OperationReplaceMember {
		if err := json.Unmarshal(op.Payload, &relocation); err != nil {
			return operationResult(cp, true), operationInvalid("invalid operation payload")
		}
		payload["member"] = relocation.Member
	} else if len(op.Payload) > 0 && op.Kind != domain.OperationUpgradeAgents {
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return operationResult(cp, false), operationInvalid("invalid operation payload")
		}
	}
	switch op.Kind {
	case domain.OperationBackup, domain.OperationRestore, domain.OperationRotateTLS, domain.OperationRotateSystemUser:
		if cp.Stage == "" {
			if err := disruptiveOperationReady(rc, s); err != nil {
				return operationResult(cp, true), err
			}
		}
	}
	switch op.Kind {
	case domain.OperationBackup:
		return p.runBackup(ctx, rc, s, op, cp, payload["backup"])
	case domain.OperationDeleteBackup:
		return p.runDeleteBackup(ctx, rc, s, op, cp, payload["backup"])
	case domain.OperationRestore:
		return p.runRestore(ctx, rc, s, op, cp, payload["restore"])
	case domain.OperationRotateTLS:
		return p.runRotateTLS(ctx, rc, s, cp)
	case domain.OperationRotateSystemUser:
		return p.runRotateUser(ctx, rc, s, op, cp, payload["user"])
	case domain.OperationRestart, domain.OperationUpgradeAgents:
		if cp.Stage == "" {
			if err := disruptiveOperationReady(rc, s); err != nil {
				return operationResult(cp, true), err
			}
		}
		reason := "restart"
		if op.Kind == domain.OperationUpgradeAgents {
			reason = "agents"
			if cp.AgentTarget == nil {
				target, err := engine.AgentUpgradeTarget(op)
				if err != nil {
					return operationResult(cp, true), err
				}
				cp.AgentTarget = &target
			}
			if cp.AgentTarget.TargetBuild != engine.CurrentAgentBuild().Build && (s.Rolling == nil || s.Rolling.OperationID != op.ID) {
				cp.Stage, cp.Waiting, cp.Failure = "done", "", agentBuildChangedError().Error()
				return operationResult(cp, true), agentBuildChangedError()
			}
		}
		if cp.Stage == "" {
			if s.Rolling != nil && (s.Rolling.Reason != reason || s.Rolling.OperationID != op.ID) {
				_, _, err := p.rollingStep(ctx, rc, s)
				return operationResult(cp, false), err
			}
			p.rollingStart(rc, s, reason, rc.Cluster.Spec.Version, op.ID)
			s.Rolling.AgentTarget = cp.AgentTarget
			cp.Stage = "rolling"
			return operationResult(cp, false), nil
		}
		if wait := rollingWindowWait(rc, s); wait.RequeueAfter > 0 && !agentTargetChanged(s.Rolling) {
			result := operationResult(cp, false)
			result.RequeueAfter = wait.RequeueAfter
			result.Message = "Waiting for maintenance window"
			return result, nil
		}
		done, msg, err := p.rollingStep(ctx, rc, s)
		cp.Waiting = ""
		if r := s.Rolling; r != nil && r.Current != "" && r.Step != "prepare" {
			cp.Waiting = r.Current
		}
		if done {
			cp.Stage = "done"
			if err != nil {
				cp.Failure = err.Error()
			}
		}
		result := operationResult(cp, done)
		result.Message = msg
		return result, err
	case domain.OperationReplaceMember:
		var avoid []string
		if relocation.Relocate && relocation.AvoidGroup != "" {
			avoid = []string{relocation.AvoidGroup}
		}
		return p.runReplaceMember(ctx, rc, s, cp, payload["member"], avoid...)
	default:
		return operationResult(cp, true), engine.ErrNotSupported
	}
}

func syncDuringOperation(op *domain.Operation) bool {
	if op.Kind != domain.OperationReplaceMember {
		return false
	}
	var cp operationState
	if json.Unmarshal(op.Checkpoint, &cp) != nil {
		return false
	}
	return cp.Stage == "join" || cp.Stage == "remove"
}

func (p *Provider) runReplaceMember(ctx context.Context, rc *engine.Context, s *state, cp operationState, name string, avoid ...string) (engine.OpResult, error) {
	switch cp.Stage {
	case "":
		if err := disruptiveOperationReady(rc, s); err != nil {
			return operationResult(cp, true), err
		}
		if err := validateReplacementMember(s, name); err != nil {
			return operationResult(cp, true), err
		}
		old := s.find(name)
		replacement, err := p.allocate(rc, s, old.Kind)
		if err != nil {
			return operationResult(cp, false), err
		}
		replacement.Replaces = old.Name
		replacement.PlacementAvoid = avoid
		s.ReplacementFailure = ""
		cp.NewMember = replacement.Name
		cp.Stage = "join"
	case "join":
		if s.ReplacementFailure != "" {
			if s.find(cp.NewMember) != nil {
				return operationResult(cp, false), nil
			}
			cp.Stage = "done"
			cp.Failure = s.ReplacementFailure
			return operationResult(cp, true), fmt.Errorf("%s: %w", cp.Failure, infra.ErrPlacementUnsatisfiable)
		}
		replacement := s.find(cp.NewMember)
		if replacement == nil || replacement.Stage != stageJoined {
			return operationResult(cp, false), nil
		}
		client, err := p.es(ctx, rc, s)
		if err != nil {
			return operationResult(cp, false), err
		}
		defer client.Close()
		nodes, err := client.Nodes(ctx)
		if err != nil {
			return operationResult(cp, false), err
		}
		joined := false
		for _, node := range nodes {
			if node.Name == replacement.Name {
				joined = true
			}
		}
		health, err := client.Health(ctx, esapi.HealthOptions{})
		if err != nil {
			return operationResult(cp, false), err
		}
		if !joined || health.Status != "green" || health.RelocatingShards != 0 || health.InitializingShards != 0 {
			return operationResult(cp, false), nil
		}
		if old := s.find(name); old != nil {
			old.Removing = true
		}
		cp.Stage = "remove"
	case "remove":
		if s.find(name) != nil {
			return operationResult(cp, false), nil
		}
		if replacement := s.find(cp.NewMember); replacement != nil {
			replacement.Replaces = ""
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), operationInvalid("unknown replacement checkpoint")
	}
	return operationResult(cp, false), nil
}

func disruptiveOperationReady(rc *engine.Context, s *state) error {
	if s.ClusterUUID == "" || s.Suspended || rc.Cluster.Deleting || rc.Cluster.Status.Phase == domain.PhaseDeleting || rc.Cluster.Status.Phase == domain.PhaseSuspended {
		return fmt.Errorf("%w: operation requires a formed, active cluster", engine.ErrConflict)
	}
	for _, m := range s.esNodes() {
		if m.Stage != stageJoined {
			return fmt.Errorf("%w: Elasticsearch member %s has not joined", engine.ErrConflict, m.Name)
		}
	}
	return nil
}

func (p *Provider) operationRepository(ctx context.Context, rc *engine.Context, s *state, storage *domain.BackupStorage, readOnly bool, basePath string) (string, error) {
	repo, err := p.ensureRepository(ctx, rc, s, storage, readOnly, basePath)
	if errors.Is(err, errWaiting) && s.Rolling != nil {
		_, _, stepErr := p.rollingStep(ctx, rc, s)
		if stepErr != nil {
			return "", stepErr
		}
	}
	return repo, err
}

// ValidateMemberReplacement rejects components and singleton native clusters.
func (p *Provider) ValidateMemberReplacement(c *domain.Cluster, name string) error {
	s, err := load(c)
	if err != nil {
		return err
	}
	return validateReplacementMember(s, name)
}

func validateReplacementMember(s *state, name string) error {
	m := s.find(name)
	if m == nil {
		return operationInvalid("member does not exist")
	}
	if m.Kind != kindNode && m.Kind != kindMaster {
		return operationInvalid("component services cannot be replaced; use restart")
	}
	if len(s.esNodes()) < 2 {
		return operationInvalid("single-node clusters cannot replace members; use snapshot restore")
	}
	if m.Removing {
		return operationInvalid("member is already being removed")
	}
	return nil
}
