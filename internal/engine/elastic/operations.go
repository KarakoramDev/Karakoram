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
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// operationState contains only durable, non-secret operation progress.
type operationState struct {
	Stage         string   `json:"stage"`
	Names         []string `json:"names,omitempty"`
	Before        []string `json:"before,omitempty"`
	Index         int      `json:"index,omitempty"`
	NewMember     string   `json:"newMember,omitempty"`
	ConfigVersion int64    `json:"configVersion,omitempty"`
	OldTokenName  string   `json:"oldTokenName,omitempty"`
	Failure       string   `json:"failure,omitempty"`
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
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: "Normal", Reason: reason, Message: message})
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
	if len(op.Payload) > 0 {
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return operationResult(cp, false), operationInvalid("invalid operation payload")
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
	case domain.OperationRestart:
		if cp.Stage == "" {
			if s.Rolling != nil && (s.Rolling.Reason != "restart" || s.Rolling.OperationID != op.ID) {
				_, _, err := p.rollingStep(ctx, rc, s)
				return operationResult(cp, false), err
			}
			p.rollingStart(rc, s, "restart", rc.Cluster.Spec.Version, op.ID)
			cp.Stage = "rolling"
			return operationResult(cp, false), nil
		}
		done, msg, err := p.rollingStep(ctx, rc, s)
		if done {
			cp.Stage = "done"
		}
		result := operationResult(cp, done)
		result.Message = msg
		return result, err
	case domain.OperationReplaceMember:
		return p.runReplaceMember(ctx, rc, s, cp, payload["member"])
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

func (p *Provider) runReplaceMember(ctx context.Context, rc *engine.Context, s *state, cp operationState, name string) (engine.OpResult, error) {
	switch cp.Stage {
	case "":
		old := s.find(name)
		if old == nil {
			return operationResult(cp, true), operationInvalid("member does not exist")
		}
		if old.Kind != kindNode && old.Kind != kindMaster {
			return operationResult(cp, true), operationInvalid("component services cannot be replaced; use restart")
		}
		if len(s.esNodes()) < 2 {
			return operationResult(cp, true), operationInvalid("single-node clusters cannot replace members; use snapshot restore")
		}
		if old.Removing {
			return operationResult(cp, true), operationInvalid("member is already being removed")
		}
		replacement, err := p.allocate(rc, s, old.Kind)
		if err != nil {
			return operationResult(cp, false), err
		}
		replacement.Replaces = old.Name
		cp.NewMember = replacement.Name
		cp.Stage = "join"
	case "join":
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
