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
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// checkpoint contains no credentials or YAML. Steps are committed before non-idempotent starts.
type checkpoint struct {
	Step                        string                           `json:"step"`
	Cancelling                  bool                             `json:"cancelling,omitempty"`
	CancelIssued                bool                             `json:"cancelIssued,omitempty"`
	Member                      string                           `json:"member,omitempty"`
	PBMName                     string                           `json:"pbmName,omitempty"`
	BackupReadyStartedElapsed   *time.Duration                   `json:"backupReadyStartedElapsed,omitempty"`
	BackupCleanupStartedElapsed *time.Duration                   `json:"backupCleanupStartedElapsed,omitempty"`
	BackupCleanupCandidates     []string                         `json:"backupCleanupCandidates,omitempty"`
	Snapshot                    string                           `json:"snapshot,omitempty"`
	Physical                    bool                             `json:"physical,omitempty"`
	Source                      string                           `json:"source,omitempty"`
	SourceNamespace             string                           `json:"sourceNamespace,omitempty"`
	TargetTime                  *time.Time                       `json:"targetTime,omitempty"`
	Index                       int                              `json:"index,omitempty"`
	Failure                     string                           `json:"failure,omitempty"`
	FreshBase                   bool                             `json:"freshBaseBackupRequired,omitempty"`
	Storage                     string                           `json:"storage,omitempty"`
	Prefix                      string                           `json:"prefix,omitempty"`
	RecoveryIdentity            string                           `json:"recoveryIdentity,omitempty"`
	RouterOp                    *infra.Op                        `json:"routerOp,omitempty"`
	RouterRetry                 *routerRetry                     `json:"routerRetry,omitempty"`
	RestoreMetadataDeadline     *time.Time                       `json:"restoreMetadataDeadline,omitempty"`
	RestoreAbsentSince          *time.Time                       `json:"restoreAbsentSince,omitempty"`
	RestoreCommandTS            int64                            `json:"restoreCommandTS,omitempty"`
	RestoreFenceUnknown         bool                             `json:"restoreFenceUnknown,omitempty"`
	RestoreConsistencyUncertain bool                             `json:"restoreConsistencyUncertain,omitempty"`
	RestoreNativeSucceeded      bool                             `json:"restoreNativeSucceeded,omitempty"`
	RestoreCleanupDeadline      *time.Time                       `json:"restoreCleanupDeadline,omitempty"`
	RestoreWorkers              map[string]RestoreWorkerIdentity `json:"restoreWorkers,omitempty"`
	RestoreWorkerUnconfirmed    bool                             `json:"restoreWorkerUnconfirmed,omitempty"`
	PBMGoMemoryBudgetObserved   bool                             `json:"pbmGoMemoryBudgetObserved,omitempty"`
	PBMGoMemoryBudgetUnknown    bool                             `json:"pbmGoMemoryBudgetUnknown,omitempty"`
	PITRIdentityPrepared        bool                             `json:"pitrIdentityPrepared,omitempty"`
	BalancerWasEnabled          *bool                            `json:"balancerWasEnabled,omitempty"`
}

type routerRetry struct {
	engine.RetrySchedule
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
	cp.Failure = redact.NativeError(cp.Failure)
	return cp, nil
}

// RestoreOutcome exposes only the durable consistency evidence needed by the
// provider. Success means native success followed by completed target recovery.
type RestoreOutcome struct {
	NativeName            string
	Failure               string
	ConsistencyUncertain  bool
	RecoveredSuccessfully bool
	WorkerUnconfirmed     bool
	MemoryBudgetUnknown   bool
}

func ReadRestoreOutcome(raw json.RawMessage) (RestoreOutcome, error) {
	cp, err := loadCheckpoint(&domain.Operation{Checkpoint: raw})
	if err != nil {
		return RestoreOutcome{}, err
	}
	return RestoreOutcome{
		NativeName:            cp.PBMName,
		Failure:               cp.Failure,
		ConsistencyUncertain:  cp.RestoreConsistencyUncertain,
		WorkerUnconfirmed:     cp.RestoreWorkerUnconfirmed,
		MemoryBudgetUnknown:   cp.PBMGoMemoryBudgetUnknown,
		RecoveredSuccessfully: cp.Step == "done" && cp.PBMName != "" && cp.RestoreNativeSucceeded && cp.Failure == "" && !cp.RestoreConsistencyUncertain && !cp.Cancelling,
	}, nil
}

// MigrateRestoreCheckpoint recognizes the old state machine's durable native
// success receipt: a nonempty native name and a recovery checkpoint reachable
// only after native terminal observation, with no failure/cancellation evidence.
// A completed legacy receipt additionally needs the matching successful Restore.
// It never infers success from process health or from an operation phase alone.
func MigrateRestoreCheckpoint(op *domain.Operation, r *domain.Restore) (json.RawMessage, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return nil, err
	}
	if cp.RestoreNativeSucceeded || cp.PBMName == "" || cp.Failure != "" ||
		cp.RestoreConsistencyUncertain || cp.Cancelling || op.CancelRequested ||
		op.Step == "failure-cleanup" || op.Kind != domain.OperationRestore ||
		(op.Phase != domain.PhaseRunning && op.Phase != domain.PhaseSucceeded) ||
		r.OperationID != op.ID || r.Cluster != op.Cluster || domain.NormalizedNamespace(r.Namespace) != domain.NormalizedNamespace(op.Namespace) ||
		(r.Phase == domain.PhaseSucceeded && r.Error != "") || (r.Phase != domain.PhaseRunning && r.Phase != domain.PhaseSucceeded) {
		return nil, nil
	}
	// Running Restore.Error may contain a retry diagnostic. Only the native
	// checkpoint above proves outcome; a completed receipt must still be clean.
	name, err := payloadName(op, "restore")
	if err != nil {
		return nil, err
	}
	if name != r.Name {
		return nil, nil
	}
	switch cp.Step {
	case "done":
		if r.Phase != domain.PhaseSucceeded || r.CompletedAt == nil {
			return nil, nil
		}
	case "mongod-start", "pbm-start", "maintenance-off", "reset-users",
		"release-pitr-identity", "configure-target", "target-resync",
		"target-inventory", "routers-on", "balancer-on", "complete":
		if op.Phase != domain.PhaseRunning {
			return nil, nil
		}
	default:
		return nil, nil
	}
	cp.RestoreNativeSucceeded = true
	return json.Marshal(cp)
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
		case "balancer-off", "maintenance-on", "prepare-pitr-identity":
			cp.Failure = failure
			cp.Step, cp.Index, cp.RouterRetry = "mongod-start", 0, nil
		case "start", "poll", "fence-pbm", "verify-fence":
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
	if !done && cp.PBMGoMemoryBudgetUnknown {
		message = MemoryBudgetUnknownMessage + "; " + message
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
		s, err := engine.ObserveAgentStatus(ctx, c, "PSMDB", n.Member)
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

type commandFailure struct {
	command  string
	exitCode int
	stderr   string
	message  string
}

func (e *commandFailure) Error() string { return e.message }

func command(ctx context.Context, env Env, member, requestID, name string, cfg []byte, args ...string) ([]byte, error) {
	// Only starts/cancellation have member-local receipts. Observations and
	// idempotent configuration must survive that member's loss, including
	// physical restore while mongod and pbm-agent are intentionally stopped.
	if requestID == "" {
		var last error
		for _, node := range env.Nodes {
			if env.Sharded && node.ReplSet != "cfg" {
				continue
			}
			out, err := commandMember(ctx, env, node.Member, requestID, name, cfg, args...)
			if err == nil {
				return out, nil
			}
			last = err
		}
		if last != nil {
			return nil, fmt.Errorf("PBM %s unavailable through eligible members: %w", name, last)
		}
		return nil, errors.New("no eligible PBM command member")
	}
	return commandMember(ctx, env, member, requestID, name, cfg, args...)
}

func commandMember(ctx context.Context, env Env, member, requestID, name string, cfg []byte, args ...string) ([]byte, error) {
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
		if safe := redact.NativeError(err.Error()); safe != err.Error() {
			err = errors.New(safe)
		}
		if requestID != "" && strings.Contains(err.Error(), "PBM initiation outcome unknown after interrupted execution") {
			return nil, fmt.Errorf("PBMReceiptUnknown: %s initiation receipt %s on member %s has no attributable native operation name; preserve receipts and restore holds, inspect PBM list/logs/storage through surviving members, and recover into a new cluster from a verified backup if attribution cannot be established; do not retry with another request ID or member: %w", name, requestID, member, err)
		}
		return nil, err
	}
	data := []byte(out.Output)
	var text string
	if json.Unmarshal(data, &text) == nil {
		data = []byte(text)
	}
	if out.ExitCode != 0 {
		return nil, &commandFailure{
			command: name, exitCode: out.ExitCode, stderr: redact.NativeError(out.Stderr),
			message: fmt.Sprintf("pbm %s failed (exit %d): %s %s", name, out.ExitCode, redact.NativeError(out.Stderr), redact.NativeError(string(data))),
		}
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
	kind := "Normal"
	if reason == "BackupFailed" {
		kind = "Warning"
	}
	if err := env.RC.Events.Record(ctx, domain.Event{Namespace: env.RC.Cluster.Ref().Namespace, Cluster: env.RC.Cluster.Name, Operation: op.ID, Type: kind, Reason: reason, Message: message, CreatedAt: now(env)}); err != nil {
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
