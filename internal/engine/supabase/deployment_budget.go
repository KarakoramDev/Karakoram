// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const componentDeploymentAttempts = 4

// The CP budget is independent of backend runtime/probe fingerprints: changing
// readiness during recovery must not mint another budget for the same software.
func componentDeploymentIntent(a *nativeartifacts.Artifact) string {
	return a.Version + "@" + a.SHA256 + "#" + engine.CurrentAgentBuild().Build
}

func sameComponentDeploymentBudget(rc *engine.Context, failure *componentDeploymentFailure, intent string) bool {
	return failure != nil && failure.Intent == intent && rc.Cluster.Spec.RetryEpoch <= failure.RetryEpoch
}

func componentDeploymentBudgetError(ctx context.Context, rc *engine.Context, m *componentMember, runtime infra.RuntimeSpec) error {
	failure := m.DeploymentFailure
	err := &infra.DeploymentExhaustedError{Service: m.Name, Version: failure.Op.ID, Attempts: failure.Attempts, LastError: failure.Detail}
	return engine.RecordDeploymentExhausted(ctx, rc, m.Name, runtime, err)
}

func (p *Provider) recordComponentDeploymentFailure(ctx context.Context, rc *engine.Context, m *componentMember, op infra.Op, status, detail, intent string) error {
	if intent == "" { // A receipt persisted before the CP budget was introduced.
		a, err := nativeartifacts.Get(componentArtifact(m.Component))
		if err != nil {
			return err
		}
		intent = componentDeploymentIntent(&a)
	}
	attempts := 1
	if sameComponentDeploymentBudget(rc, m.DeploymentFailure, intent) {
		attempts += m.DeploymentFailure.Attempts
	}
	failure := &componentDeploymentFailure{Op: op, Status: status, Detail: detail, Intent: intent, RetryEpoch: rc.Cluster.Spec.RetryEpoch, Attempts: attempts}
	m.DeploymentFailure = failure
	if attempts >= componentDeploymentAttempts {
		failure.Status = "DeploymentRetriesExhausted"
		runtime, err := p.runtimeSpec(rc, m)
		if err != nil {
			return err
		}
		return componentDeploymentBudgetError(ctx, rc, m, runtime)
	}
	return errors.New(failure.message())
}

func componentDeploymentWaiting(err error) bool {
	var waiting interface{ DeploymentWaiting() bool }
	return !infra.RetryAt(err).IsZero() || errors.As(err, &waiting) && waiting.DeploymentWaiting()
}

func newComponentExhaustedFailure(rc *engine.Context, op infra.Op, intent string, err error) *componentDeploymentFailure {
	attempts := componentDeploymentAttempts
	var exhausted *infra.DeploymentExhaustedError
	if errors.As(err, &exhausted) && exhausted.Attempts > 0 {
		attempts = exhausted.Attempts
	}
	return &componentDeploymentFailure{Op: op, Status: "DeploymentRetriesExhausted", Detail: infra.SafeDetail(err), Intent: intent, RetryEpoch: rc.Cluster.Spec.RetryEpoch, Attempts: attempts}
}
