// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (p *Provider) retireUnplacedScaleDown(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range s.Members {
		if m.UnplacedRetirement != nil {
			done, err := engine.AdvanceUnplacedRetirement(ctx, rc, m.UnplacedRetirement, m.Name, m.ServiceID, m.Volume, m.VolumeID)
			if err != nil || !done {
				return true, err
			}
			if err = deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return true, err
			}
			removeRecord(s, m.Name)
			delete(rc.Cluster.Status.DeploymentFailures, m.Name)
			return true, nil
		}
	}
	if !engine.CleanupSupersedesDeployment(rc.Backend, infra.Op{Kind: "deploy"}) {
		return false, nil
	}
	for rank, m := range ordered(s) {
		if rank < rc.Cluster.Spec.Components.Engine.Replicas || m.Name == s.InitOwner || m.Version != "" || m.Resume || m.RuntimeRecovery.Initialized || m.ImportPlan != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || (m.Stage != "planned" && m.Stage != "imported") {
			continue
		}
		if s.Pending != nil && (s.Pending.Member != m.Name || s.Pending.Action != "deploy" || !engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op)) {
			continue
		}
		if s.Retry != nil && (s.Retry.Member != m.Name || s.Retry.Action != "deploy") {
			continue
		}
		if m.ResumeDeploy != nil && !engine.CleanupSupersedesDeployment(rc.Backend, m.ResumeDeploy.Op) {
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
		s.resetPlatformRetry(&pending{Member: m.Name, Action: "deploy"})
		s.Pending = nil
		m.ResumeDeploy = nil
		s.Phase = "remove"
		return true, nil
	}
	return false, nil
}
