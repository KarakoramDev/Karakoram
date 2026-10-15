// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type operationEventKey struct{}

// failureCleanupStep keeps cleanup ownership durable without changing the
// provider's checkpoint. LastError carries the original terminal reason.
const failureCleanupStep = "failure-cleanup"

type operationFailureCleanup interface {
	engine.OperationCanceller
	SupportsFailureCleanup(*domain.Operation) bool
}

func (m *Manager) failQueueOperation(p engine.Provider, op *domain.Operation, err error) bool {
	if !m.operationFailure(op, err) {
		return false
	}
	if cleanup, ok := p.(operationFailureCleanup); ok && cleanup.SupportsFailureCleanup(op) {
		op.LastError = op.Message
		now := m.opts.Context.Clock.Now()
		op.FailureCleanupStartedAt = &now
		op.Phase, op.CompletedAt, op.Step = domain.PhaseRunning, nil, failureCleanupStep
		return false
	}
	return true
}

// DefaultOperationTimeout bounds wall-clock queue ownership, including scheduled
// waits and successful-but-incomplete checkpoints. Restore gets a larger window.
const DefaultOperationTimeout = 2 * time.Hour
const DefaultRestoreTimeout = 6 * time.Hour
const DefaultOperationRetryLimit = 20
const DefaultFailureCleanupTimeout = 6 * time.Hour

func compensationStep(op *domain.Operation) string {
	var cp struct {
		Step  string `json:"step"`
		Stage string `json:"stage"`
	}
	if json.Unmarshal(op.Checkpoint, &cp) == nil {
		if cp.Step != "" {
			return cp.Step
		}
		if cp.Stage != "" {
			return cp.Stage
		}
	}
	return op.Step
}

func (m *Manager) operationTimeout(op *domain.Operation) time.Duration {
	if d := m.opts.OperationTimeouts[op.Kind]; d > 0 {
		return d
	}
	if op.Kind == domain.OperationRestore {
		return DefaultRestoreTimeout
	}
	return DefaultOperationTimeout
}

func (m *Manager) operationFailure(op *domain.Operation, err error) bool {
	if err != nil {
		op.Message = err.Error()
		op.LastError = op.Message
		if scheduledPlatformWait(err) == nil {
			op.Failures++
		}
	}
	var field *domain.FieldError
	permanent := errors.As(err, &field) || errors.Is(err, engine.ErrInvalid) || errors.Is(err, engine.ErrNotSupported)
	expired := !m.opts.Context.Clock.Now().Before(op.CreatedAt.Add(m.operationTimeout(op)))
	if !permanent && !expired && op.Failures < m.opts.OperationRetryLimit {
		return false
	}
	last := op.LastError
	if last == "" {
		last = op.Message
	}
	if last == "" {
		last = "No terminal result was observed"
	}
	if expired {
		op.Message = fmt.Sprintf("%s exceeded its %s deadline: %s", op.Kind, m.operationTimeout(op), last)
	}
	if !expired && !permanent {
		op.Message = fmt.Sprintf("%s exhausted its retry budget (%d failures): %s", op.Kind, op.Failures, last)
	}
	op.Phase = domain.PhaseFailed
	now := m.opts.Context.Clock.Now()
	op.CompletedAt = &now
	return true
}

// trackingOperations do not own the imperative queue: ordinary Sync converges
// desired state while these records observe the target generation. Failed tracking
// does not discard desired state or stop eventual cleanup.
func (m *Manager) trackingOperations(ctx context.Context, c *domain.Cluster, cause error, convergenceAttempted bool) error {
	ops, err := m.opts.Store.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
	if err != nil {
		return err
	}
	now := m.opts.Context.Clock.Now()
	for i := range ops {
		op := &ops[i]
		if !domain.IsConvergenceOperation(op.Kind) {
			// Expire Pending work even if ordinary convergence keeps failing before claim.
			if op.Phase == domain.PhasePending && m.operationFailure(op, nil) {
				if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
					return err
				}
			}
			continue
		}
		if op.Phase == domain.PhasePending {
			op.Phase, op.StartedAt = domain.PhaseRunning, &now
		}
		if convergenceAttempted && cause == nil {
			op.Failures = 0
		}
		op.Attempts++
		if c.Generation > op.TargetGeneration {
			op.Phase, op.Message, op.CompletedAt = domain.PhaseCancelled, "Superseded by a newer cluster generation", &now
		} else if !m.operationFailure(op, cause) {
			op.Step = string(c.Status.Phase)
			if op.Kind == domain.OperationUpgrade {
				for _, condition := range c.Status.Conditions {
					if condition.Type == "RollingProgress" && condition.Status == "True" {
						op.Step = condition.Reason
						break
					}
				}
			}
			if c.Status.Message != "" {
				op.Message = c.Status.Message
			} else {
				op.Message = fmt.Sprintf("%s: waiting for generation %d", c.Status.Phase, op.TargetGeneration)
			}
			ready := c.Status.Phase == domain.PhaseReady || (c.Status.Phase == domain.PhaseDegraded && c.Status.Message != "")
			if op.Kind != domain.OperationDelete && c.Status.ObservedGeneration >= op.TargetGeneration && ready {
				op.Phase, op.Message, op.CompletedAt = domain.PhaseSucceeded, fmt.Sprintf("%s applied generation %d (%s)", op.Kind, op.TargetGeneration, c.Status.Phase), &now
			}
		}
		if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
			return err
		}
	}
	return nil
}

// preserveTransitions compares status and reason, not observation time or message.
func preserveTransitions(previous, next []domain.Condition) {
	for i := range next {
		for _, old := range previous {
			if old.Type == next[i].Type && old.Status == next[i].Status && old.Reason == next[i].Reason {
				next[i].LastTransitionTime = old.LastTransitionTime
				break
			}
		}
	}
}

// observedVersion never substitutes desired state for an unavailable observation.
// Catalog versions use numeric components, including vendor build suffixes.
func observedVersion(members []domain.MemberStatus) string {
	version := ""
	for _, member := range members {
		if member.Version != "" && (version == "" || olderVersion(member.Version, version)) {
			version = member.Version
		}
	}
	return version
}

func olderVersion(a, b string) bool {
	for a != "" || b != "" {
		left, restA := versionPart(a)
		right, restB := versionPart(b)
		if left != right {
			return left < right
		}
		a, b = restA, restB
	}
	return false
}

func versionPart(version string) (uint64, string) {
	i := 0
	for i < len(version) && (version[i] < '0' || version[i] > '9') {
		i++
	}
	var part uint64
	for i < len(version) && version[i] >= '0' && version[i] <= '9' {
		part = part*10 + uint64(version[i]-'0')
		i++
	}
	return part, version[i:]
}
