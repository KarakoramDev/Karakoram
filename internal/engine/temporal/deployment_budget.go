// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const componentDeploymentAttempts = 4

type componentDeploymentFailure struct {
	Op         infra.Op `json:"op"`
	Status     string   `json:"status"`
	Detail     string   `json:"detail,omitempty"`
	Intent     string   `json:"intent"`
	RetryEpoch int64    `json:"retryEpoch"`
	Attempts   int      `json:"attempts"`
}

func componentDeploymentIntent(version, component string) (string, error) {
	var a nativeartifacts.Artifact
	var err error
	if component == tspec.UI {
		a, err = nativeartifacts.Get("temporal-ui-server")
	} else {
		a, err = serverArtifact(version)
	}
	if err != nil {
		return "", err
	}
	// Recovery probe changes do not grant another budget for the same software.
	return a.Version + "@" + a.SHA256 + "#" + engine.CurrentAgentBuild().Build, nil
}

func sameComponentDeploymentBudget(rc *engine.Context, failure *componentDeploymentFailure, intent string) bool {
	return failure != nil && failure.Intent == intent && rc.Cluster.Spec.RetryEpoch <= failure.RetryEpoch
}

func componentDeploymentBudgetError(ctx context.Context, rc *engine.Context, m *componentMember, runtime infra.RuntimeSpec) error {
	f := m.DeploymentFailure
	err := &infra.DeploymentExhaustedError{Service: m.Name, Version: f.Op.ID, Attempts: f.Attempts, LastError: f.Detail}
	return engine.RecordDeploymentExhausted(ctx, rc, m.Name, runtime, err)
}

func (p *Provider) recordComponentDeploymentFailure(ctx context.Context, rc *engine.Context, m *componentMember, op infra.Op, status, detail, intent string, cause error) error {
	if intent == "" {
		var err error
		intent, err = componentDeploymentIntent(rc.Cluster.Spec.Version, m.Component)
		if err != nil {
			return err
		}
	}
	attempts := 1
	if sameComponentDeploymentBudget(rc, m.DeploymentFailure, intent) {
		attempts += m.DeploymentFailure.Attempts
	}
	var exhausted *infra.DeploymentExhaustedError
	terminal := errors.As(cause, &exhausted) || status == "DeploymentRetriesExhausted"
	if terminal {
		attempts = componentDeploymentAttempts
		if exhausted != nil && exhausted.Attempts > 0 {
			attempts = exhausted.Attempts
		}
	}
	f := &componentDeploymentFailure{Op: op, Status: status, Detail: detail, Intent: intent, RetryEpoch: rc.Cluster.Spec.RetryEpoch, Attempts: attempts}
	m.DeploymentFailure = f
	if terminal || attempts >= componentDeploymentAttempts {
		f.Status = "DeploymentRetriesExhausted"
		runtime, err := p.runtimeSpec(rc, m)
		if err != nil {
			return err
		}
		return componentDeploymentBudgetError(ctx, rc, m, runtime)
	}
	return fmt.Errorf("temporal component %s deployment failed (%s); %d of %d attempts consumed: %s", m.Name, status, attempts, componentDeploymentAttempts, detail)
}

func (p *Provider) deployPreparedRuntime(ctx context.Context, rc *engine.Context, m *componentMember, runtime infra.RuntimeSpec, version string) (infra.Op, string, error) {
	intent, err := componentDeploymentIntent(version, m.Component)
	if err != nil {
		return infra.Op{}, intent, err
	}
	if sameComponentDeploymentBudget(rc, m.DeploymentFailure, intent) && m.DeploymentFailure.Status == "DeploymentRetriesExhausted" {
		return infra.Op{}, intent, componentDeploymentBudgetError(ctx, rc, m, runtime)
	}
	op, err := engine.Deploy(ctx, rc, m.Name, runtime)
	if err != nil {
		var waiting interface{ DeploymentWaiting() bool }
		if !infra.RetryAt(err).IsZero() || errors.As(err, &waiting) && waiting.DeploymentWaiting() {
			return op, intent, err
		}
		status := "DeploymentSubmissionFailed"
		if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			status = "DeploymentRetriesExhausted"
		}
		failure := p.recordComponentDeploymentFailure(ctx, rc, m, op, status, infra.SafeDetail(err), intent, err)
		return op, intent, errors.Join(err, failure)
	}
	return op, intent, nil
}
