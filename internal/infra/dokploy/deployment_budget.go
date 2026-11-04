// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import "github.com/KarakoramDev/Karakoram/internal/infra"

const deploymentAttemptLimit = 4

// Persist the reservation with Pending before delivering an upload. Neither
// successful submission/polling nor adapter reconstruction grants a new budget.
// A different rendered desired runtime or an explicit RetryEpoch does.
type deploymentBudget struct {
	Digest     string `json:"digest"`
	RetryEpoch int64  `json:"retryEpoch,omitempty"`
	Attempts   int    `json:"attempts"`
	LastError  string `json:"lastError,omitempty"`
}

func prepareDeploymentBudget(m *metadata, digest string, epoch int64) error {
	if m.DeploymentBudget == nil {
		m.DeploymentBudget = &deploymentBudget{Digest: digest, RetryEpoch: epoch}
		// Pre-budget receipts already consumed one delivered attempt. Preserve
		// it when upgrading a failed deployment instead of granting it for free.
		if m.Pending != nil && m.Pending.Action == "deploy" && m.Pending.Digest == digest && m.Pending.Failed && !m.Pending.Uncertain {
			m.DeploymentBudget.Attempts = 1
			m.DeploymentBudget.LastError = "Dokploy deployment failed; consult the deployment logs"
		}
	} else if m.DeploymentBudget.Digest != digest || epoch > m.DeploymentBudget.RetryEpoch {
		m.DeploymentBudget = &deploymentBudget{Digest: digest, RetryEpoch: epoch}
	}
	budget := m.DeploymentBudget
	if budget.Attempts >= deploymentAttemptLimit {
		return &infra.DeploymentExhaustedError{Service: m.Hostname, Version: budget.Digest, Attempts: budget.Attempts, LastError: budget.LastError}
	}
	return nil
}
