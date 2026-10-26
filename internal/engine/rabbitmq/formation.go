// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

// A capability read never needs a running broker. Transport failures leave the
// decision unknown; a legacy route's 404 positively selects serial formation.
func (p *Provider) formationCapability(ctx context.Context, rc *engine.Context, name string) (supported, observed bool) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var caps rmqspec.FormationCapabilities
	err := p.call(ctx, rc, name, "formation-capabilities", rmqspec.EmptyRequest{}, &caps)
	if err == nil {
		return caps.FixedSeed, true
	}
	var response *agentapi.HTTPError
	return false, errors.As(err, &response) && response.StatusCode == http.StatusNotFound
}

// prepareCreation checkpoints the deterministic seed and supervisor holds
// without contacting agents that may still be provisioning.
func prepareCreation(s *internalState) {
	if s.Bootstrapped {
		return
	}
	members := ordered(s)
	if len(members) < 2 {
		return
	}
	seed := members[0]
	if !seed.Resume && s.CreationSeed == "" {
		s.CreationSeed = seed.Name
	}
	if s.CreationSeed == "" {
		return
	}
	for _, m := range members {
		if !m.Removing && !m.Resume && !m.FormationVerified && m.Stage != "ready" {
			m.InitialFormation = true
		}
	}
}

func (p *Provider) observeCreationCapabilities(ctx context.Context, rc *engine.Context, s *internalState) {
	if s.Bootstrapped {
		pending := false
		for _, m := range s.Members {
			if m.InitialFormation && !m.FormationChecked && !m.FormationVerified && !m.Removing && !m.Resume && m.Stage != "ready" {
				pending = true
				break
			}
		}
		if !pending {
			return
		}
	}
	members := ordered(s)
	if len(members) < 2 {
		return
	}
	seed := members[0]
	// Election and holds were checkpointed before provisioning. Capability
	// reads run only after available platform work has been submitted.
	if !s.CreationGateChecked && !seed.Resume && (seed.ResumeDeploy != nil || seed.Stage == "deployed" || seed.Stage == "ready") {
		if supported, observed := p.formationCapability(ctx, rc, seed.Name); observed {
			s.CreationGateChecked = true
			seed.FormationChecked, seed.FormationGate = true, supported
		}
	}
	if s.CreationSeed == "" {
		return
	}
	for _, m := range members {
		if m == seed || !s.CreationGateChecked || !m.InitialFormation || m.Removing || m.Resume || m.FormationVerified || m.Stage == "ready" || m.FormationChecked || m.ResumeDeploy == nil && m.Stage != "deployed" {
			continue
		}
		if supported, observed := p.formationCapability(ctx, rc, m.Name); observed {
			m.FormationChecked, m.FormationGate = true, supported
		}
	}
}
