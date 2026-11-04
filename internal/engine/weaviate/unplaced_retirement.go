// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"slices"
)

// advanceUnplacedRetirement drains a persisted scale-down owner before any
// provisioning. Admission requires RuntimeUnplaced proof that an uninitialized
// member never received a placement.
func advanceUnplacedRetirement(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	// A never-placed deployment may remain pending forever. Admit scale-back
	// before polling that obsolete receipt, but never discard import ownership
	// or a deployment for an initialized/fixed-voter member.
	target := rc.Cluster.Spec.Components.Engine.Replicas
	if q := s.Pending; q != nil && q.Action == "deploy" && target >= 3 && target >= len(s.Voters) && len(s.Members) > target && engine.CleanupSupersedesDeployment(rc.Backend, infra.Op{Kind: "deploy"}) {
		m := s.find(q.Member)
		if m != nil && !m.RuntimeRecovery.Initialized && !slices.Contains(s.Voters, m.Name) {
			unplaced, err := engine.RuntimeUnplaced(ctx, rc, m.Name)
			if err != nil {
				return true, err
			}
			if unplaced {
				m.Removing = true
				m.UnplacedRetirement = &engine.UnplacedRetirement{}
				s.Pending = nil
				return true, nil
			}
		}
	}
	for i, m := range s.Members {
		if m.UnplacedRetirement == nil {
			continue
		}
		done, e := engine.AdvanceUnplacedRetirement(ctx, rc, m.UnplacedRetirement, m.Name, m.ServiceID, m.Volume, m.VolumeID)
		if e != nil {
			return true, e
		}
		if done {
			if e = deleteMemberSecrets(ctx, rc, m.Name); e != nil {
				return true, e
			}
			s.Members = append(s.Members[:i], s.Members[i+1:]...)
		}
		return true, nil
	}
	return false, nil
}
