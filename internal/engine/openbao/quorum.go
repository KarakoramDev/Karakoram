// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"sync"
	"time"
)

// Observation failure is local to a member. Never reuse stale health or seal state.
func (p *Provider) observeMembers(ctx context.Context, rc *engine.Context, s *internalState) {
	var probes sync.WaitGroup
	for _, m := range ordered(s) {
		m.Observed, m.Healthy, m.Active, m.Standby = false, false, false, false
		if m.Stage != "configured" && m.Stage != "ready" {
			continue
		}
		probes.Go(func() {
			bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			c, err := p.nativeClient(bounded, rc, m.Name, "")
			if err != nil {
				return
			}
			seal, err := c.SealStatus(bounded)
			if err != nil {
				return
			}
			m.Initialized, m.Sealed, m.Version = seal.Initialized, seal.Sealed, seal.Version
			if !seal.Sealed && seal.Initialized {
				leader, err := c.Leader(bounded)
				if err != nil {
					return
				}
				m.Active, m.Standby, m.LeaderAddress = leader.IsSelf, !leader.IsSelf, leader.LeaderAddress
			}
			m.Observed = true
		})
	}
	probes.Wait()
}

func removalQuorum(s *internalState, target string) bool {
	voters, live := 0, 0
	for _, m := range s.Members {
		if m.Name == target || !m.Voter {
			continue
		}
		voters++
		if m.Observed && !m.Sealed && m.Healthy {
			live++
		}
	}
	return voters > 0 && live >= voters/2+1
}
