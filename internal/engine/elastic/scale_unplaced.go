// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (p *Provider) retireUnplacedScaleDown(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range s.Members {
		if m.UnplacedRetirement != nil {
			done, err := engine.AdvanceUnplacedRetirement(ctx, rc, m.UnplacedRetirement, m.Name, m.ServiceID, m.Volume, m.VolumeID)
			if err != nil || !done {
				return true, err
			}
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return true, err
			}
			removeRecord(s, m.Name)
			delete(rc.Cluster.Status.DeploymentFailures, m.Name)
			p.bumpConfig(s)
			return true, nil
		}
	}
	if s.Rolling != nil || !engine.CleanupSupersedesDeployment(rc.Backend, infra.Op{Kind: "deploy"}) {
		return false, nil
	}
	for _, m := range s.esNodes() {
		if m.Replaces != "" {
			return false, nil
		}
	}
	for _, kind := range []string{kindMaster, kindNode, kindKibana, kindLogstash, kindAPM} {
		desired := rc.Cluster.Spec.Components.Extra[kind].Replicas
		if kind == kindNode {
			desired = rc.Cluster.Spec.Components.Engine.Replicas
		}
		for rank, m := range s.members(kind) {
			if rank < desired || m.Removing || m.Version != "" || m.RuntimeRecovery.Initialized || m.ImportPlan != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || (m.Stage != stagePlanned && m.Stage != stageImported) {
				continue
			}
			if s.Pending != nil && (s.Pending.Member != m.Name || s.Pending.Action != "deploy" || !engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op)) {
				continue
			}
			if s.Retry != nil && (s.Retry.Member != m.Name || s.Retry.Action != "deploy") {
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
			delete(s.PlatformRetries, retryKey(&pending{Member: m.Name, Action: "deploy"}))
			s.Pending = nil
			s.Retry = nil
			s.Phase = "scaling"
			rc.Cluster.Status.Message = "Scaling down: retiring unplaced member " + m.Name
			return true, nil
		}
	}
	return false, nil
}
