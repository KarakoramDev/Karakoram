// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (p *Core) retireUnplacedScaleDown(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range s.Members {
		if m.UnplacedRetirement != nil {
			// A learner can have been added by a surviving voter before its runtime
			// was placed. Remove that exact native identity before storage retirement.
			if m.Role == "data" && s.Topology == "ha" && m.Retirement != "removed-etcd" {
				if err := removeEtcdMember(ctx, rc, s, m); err != nil {
					return true, err
				}
				m.Retirement = "removed-etcd"
				return true, nil
			}
			done, err := engine.AdvanceUnplacedRetirement(ctx, rc, m.UnplacedRetirement, m.Name, m.ServiceID, m.Volume, m.VolumeID)
			if err != nil || !done {
				return true, err
			}
			m.ServiceID = ""
			m.VolumeID = ""
			m.Volume = ""
			_, err = p.retireMember(ctx, rc, s, m)
			return true, err
		}
	}
	if s.Replacement != nil || s.Rolling != nil || s.RestoreRecovery != nil || s.Upgrade != nil || !engine.CleanupSupersedesDeployment(rc.Backend, infra.Op{Kind: "deploy"}) {
		return false, nil
	}
	rank := 0
	for _, m := range s.Members {
		unwanted := false
		if m.Role == "data" {
			rank++
			unwanted = rank > rc.Cluster.Spec.Components.Engine.Replicas
		} else if m.Role == "proxy" {
			unwanted = desiredComponent(rc, m).Replicas == 0
		}
		if !unwanted || m.Removing || m.RuntimeRecovery.Initialized || m.ImportBatch != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || (m.Stage != "planned" && m.Stage != "imported" && m.Stage != "deploying") {
			continue
		}
		if s.Pending != nil && (s.Pending.Member != m.Name || !engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op)) {
			continue
		}
		unplaced, err := engine.RuntimeUnplaced(ctx, rc, m.Name)
		if err != nil {
			return true, err
		}
		if !unplaced {
			continue
		}
		m.Removing = true
		m.UnplacedRetirement = &engine.UnplacedRetirement{}
		m.DeployOp = nil
		s.Pending = nil
		rc.Cluster.Status.Message = "Scaling down: retiring unplaced member " + m.Name
		return true, nil
	}
	return false, nil
}
