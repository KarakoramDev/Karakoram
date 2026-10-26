// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// New and retained brokers deploy independently of native formation readiness.
// Retained Khepri voters cannot become healthy until a majority is online.
// This supplements backend admission, never the serial rolling checkpoint.
const resumeDeploymentLimit = 2

func resumeMembers(s *internalState) []*member {
	members := ordered(s)
	// stopForSuspension stops in ascending order. Submit the last-stopped broker
	// first (important for Mnesia); never reset membership or force_boot a node.
	slices.Reverse(members)
	return members
}

func resumeDeployments(s *internalState) int {
	count := 0
	for _, m := range s.Members {
		if m.ResumeDeploy != nil {
			count++
		}
	}
	return count
}

// Observe every provisioning receipt without monopolizing Pending. Reuse the
// ordinary completion/retry accounting, including exact-version exhaustion.
// A failed receipt is transferred to the existing serial retry checkpoint.
func (p *Provider) observeResumeDeployments(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.Pending != nil || s.Retry != nil {
		return resumeDeployments(s) > 0, nil
	}
	for _, m := range resumeMembers(s) {
		if m.ResumeDeploy == nil {
			continue
		}
		s.Pending = m.ResumeDeploy
		_, err := p.observePending(ctx, rc, s)
		m.ResumeDeploy = s.Pending
		s.Pending = nil
		if err != nil {
			return true, err
		}
	}
	return resumeDeployments(s) > 0, nil
}
