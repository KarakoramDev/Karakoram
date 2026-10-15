// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// checkpoint contains no credentials or YAML. Steps are committed before non-idempotent starts.
type checkpoint struct {
	Step                    string       `json:"step"`
	Cancelling              bool         `json:"cancelling,omitempty"`
	CancelIssued            bool         `json:"cancelIssued,omitempty"`
	Member                  string       `json:"member,omitempty"`
	PBMName                 string       `json:"pbmName,omitempty"`
	Snapshot                string       `json:"snapshot,omitempty"`
	Physical                bool         `json:"physical,omitempty"`
	Source                  string       `json:"source,omitempty"`
	SourceNamespace         string       `json:"sourceNamespace,omitempty"`
	TargetTime              *time.Time   `json:"targetTime,omitempty"`
	Index                   int          `json:"index,omitempty"`
	Failure                 string       `json:"failure,omitempty"`
	FreshBase               bool         `json:"freshBaseBackupRequired,omitempty"`
	Destination             string       `json:"destination,omitempty"`
	Storage                 string       `json:"storage,omitempty"`
	Prefix                  string       `json:"prefix,omitempty"`
	RecoveryIdentity        string       `json:"recoveryIdentity,omitempty"`
	RouterOp                *infra.Op    `json:"routerOp,omitempty"`
	RouterRetry             *routerRetry `json:"routerRetry,omitempty"`
	RestoreMetadataDeadline *time.Time   `json:"restoreMetadataDeadline,omitempty"`
}

type routerRetry struct {
	Failures   int       `json:"failures"`
	RetryAfter time.Time `json:"retryAfter"`
	Message    string    `json:"message"`
}

func loadCheckpoint(op *domain.Operation) (checkpoint, error) {
	var cp checkpoint
	if len(op.Checkpoint) != 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return cp, fmt.Errorf("decode PBM checkpoint: %w", err)
		}
	}
	return cp, nil
}

// PrepareCancellation switches restore to compensating recovery and backup
// polling to its durable cancel receipt. User requests are accepted only at safe
// checkpoints; expired work may require recovery after processes were stopped.
func PrepareCancellation(op *domain.Operation) (bool, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return false, err
	}
	if cp.Step == "" {
		return true, nil
	}
	if op.Kind == domain.OperationRestore && !cp.Cancelling {
		failure := "Restore cancelled before data mutation"
		if op.Step == "failure-cleanup" && op.LastError != "" {
			failure = op.LastError
		}
		switch cp.Step {
		case "configure-source", "source-resync", "source-inventory", "disable-pitr":
			cp.Failure = failure
			cp.Step, cp.Index = "configure-target", 0
		case "routers-off":
			cp.Failure = failure
			// Finish an already-submitted stop before starting routers again.
			if cp.RouterOp == nil {
				cp.Step, cp.Index, cp.RouterRetry = "mongod-start", 0, nil
			}
		case "balancer-off", "maintenance-on":
			cp.Failure = failure
			cp.Step, cp.Index, cp.RouterRetry = "mongod-start", 0, nil
		case "start", "poll":
			// The start receipt may already represent an active physical
			// restore. Replay it and observe PBM before restarting processes.
			// Only PBM's terminal outcome determines restored data success.
		}
	}
	cp.Cancelling = true
	op.Checkpoint, err = json.Marshal(cp)
	return false, err
}

func result(cp checkpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	message := cp.Step
	if cp.FreshBase {
		message = "restore completed; enqueue a fresh base backup before resuming PITR"
	}
	return engine.OpResult{Done: done, Step: cp.Step, Message: message, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}

func payloadName(op *domain.Operation, field string) (string, error) {
	var payload map[string]string
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return "", fmt.Errorf("decode operation payload: %w", err)
	}
	if payload[field] == "" {
		return "", fmt.Errorf("operation payload requires %s", field)
	}
	return payload[field], nil
}

func now(env Env) time.Time {
	if env.RC.Clock != nil {
		return env.RC.Clock.Now().UTC()
	}
	return time.Now().UTC()
}

func healthyMember(ctx context.Context, env Env) (string, error) {
	var last error
	for _, n := range env.Nodes {
		if env.Sharded && n.ReplSet != "cfg" {
			continue
		}
		c, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), n.Member)
		if err != nil {
			last = err
			continue
		}
		s, err := c.Status(ctx)
		c.Close()
		if err != nil {
			last = err
			continue
		}
		if s.Processes["mongod"].State == "running" && s.Processes["pbm-agent"].State == "running" {
			return n.Member, nil
		}
	}
	if last != nil {
		return "", fmt.Errorf("no healthy PBM command node: %w", last)
	}
	return "", errors.New("no healthy mongod/PBM agent available")
}

func command(ctx context.Context, env Env, member, requestID, name string, cfg []byte, args ...string) ([]byte, error) {
	if member == "" {
		var err error
		member, err = healthyMember(ctx, env)
		if err != nil {
			return nil, err
		}
	}
	c, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), member)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req := agentapi.PBMRequest{Command: name, Args: args, RequestID: requestID}
	if cfg != nil {
		req.Config, err = json.Marshal(string(cfg))
		if err != nil {
			return nil, err
		}
	}
	out, err := c.PBM(ctx, req)
	if err != nil {
		return nil, err
	}
	data := []byte(out.Output)
	var text string
	if json.Unmarshal(data, &text) == nil {
		data = []byte(text)
	}
	if out.ExitCode != 0 {
		return nil, fmt.Errorf("pbm %s failed (exit %d): %s %s", name, out.ExitCode, out.Stderr, data)
	}
	return data, nil
}

// decodeLast handles PBM progress text followed by JSON, without mistaking nested objects for the result.
func decodeLast(data []byte, value any) error {
	var last json.RawMessage
	for i := 0; i < len(data); i++ {
		if data[i] != '{' && data[i] != '[' {
			continue
		}
		d := json.NewDecoder(bytes.NewReader(data[i:]))
		var raw json.RawMessage
		if d.Decode(&raw) == nil {
			last = raw
			i += int(d.InputOffset()) - 1
		}
	}
	if len(last) == 0 {
		return errors.New("PBM returned no JSON object or array")
	}
	if err := json.Unmarshal(last, value); err != nil {
		return fmt.Errorf("decode PBM output: %w", err)
	}
	return nil
}

func event(ctx context.Context, env Env, op *domain.Operation, reason, message string) error {
	if env.RC.Events == nil {
		return nil
	}
	if err := env.RC.Events.Record(ctx, domain.Event{Namespace: env.RC.Cluster.Ref().Namespace, Cluster: env.RC.Cluster.Name, Operation: op.ID, Type: "Normal", Reason: reason, Message: message, CreatedAt: now(env)}); err != nil {
		return err
	}
	if reason == "RestoreStarted" || reason == "RestoreCompleted" {
		name, err := payloadName(op, "restore")
		if err != nil {
			return err
		}
		restore, err := env.RC.Store.GetRestore(ctx, env.RC.Cluster.Ref(), name)
		if err != nil {
			return err
		}
		if restore.SourceNamespace != "" && restore.SourceNamespace != env.RC.Cluster.Ref().Namespace {
			source := restore.Source.SourceCluster
			if source == "" {
				source = env.RC.Cluster.Name
			}
			return env.RC.Events.Record(ctx, domain.Event{Namespace: restore.SourceNamespace, Cluster: source, Operation: op.ID, Type: "Normal", Reason: reason, Message: message + " into " + env.RC.Cluster.Ref().Namespace + "/" + env.RC.Cluster.Name, CreatedAt: now(env)})
		}
	}
	return nil
}

func process(ctx context.Context, env Env, member, name, action string) error {
	c, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), member)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Process(ctx, agentapi.ProcessAction{Process: name, Action: action})
}

func maintenance(ctx context.Context, env Env, member string, enabled bool) error {
	c, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), member)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Maintenance(ctx, agentapi.Maintenance{Enabled: enabled})
}

// desiredMaintenance commits desired state before an agent command so config pulls cannot undo it.
func desiredMaintenance(env Env, member string, enabled bool) (bool, error) {
	state := map[string]json.RawMessage{}
	if len(env.RC.Cluster.Internal) != 0 {
		if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
			return false, err
		}
	}
	desired := map[string]bool{}
	if raw := state["pbmMaintenance"]; len(raw) != 0 {
		if err := json.Unmarshal(raw, &desired); err != nil {
			return false, err
		}
	}
	if desired[member] == enabled {
		return false, nil
	}
	if enabled {
		desired[member] = true
	} else {
		delete(desired, member)
	}
	state["pbmMaintenance"], _ = json.Marshal(desired)
	env.RC.Cluster.Internal, _ = json.Marshal(state)
	return true, nil
}

// desiredStopped commits router supervisor suppression before stopping or releasing a process.
func desiredStopped(env Env, member, process string, stopped bool) (bool, error) {
	state := map[string]json.RawMessage{}
	if len(env.RC.Cluster.Internal) != 0 {
		if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
			return false, err
		}
	}
	desired := map[string][]string{}
	if raw := state["pbmStopped"]; len(raw) != 0 {
		if err := json.Unmarshal(raw, &desired); err != nil {
			return false, err
		}
	}
	index := slices.Index(desired[member], process)
	if (index >= 0) == stopped {
		return false, nil
	}
	if stopped {
		desired[member] = append(desired[member], process)
	} else {
		desired[member] = slices.Delete(desired[member], index, index+1)
		if len(desired[member]) == 0 {
			delete(desired, member)
		}
	}
	state["pbmStopped"], _ = json.Marshal(desired)
	env.RC.Cluster.Internal, _ = json.Marshal(state)
	return true, nil
}
