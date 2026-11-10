// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (s *Server) memberConfigInputs(ctx context.Context, rc *engine.Context, member string) (string, bool) {
	if generation, ok := s.Trigger.(interface {
		ConfigInvalidationGeneration(domain.ClusterRef) uint64
	}); ok {
		rc.ConfigInvalidationGeneration = generation.ConfigInvalidationGeneration(rc.Cluster.Ref())
	}
	provider, err := s.Registry.Get(rc.Cluster.Spec.Provider)
	if err != nil {
		return "", false
	}
	if fresh, ok := provider.(engine.FreshMemberConfigSource); ok {
		required, err := fresh.RequiresFreshMemberConfig(ctx, rc, member)
		if err != nil || required {
			return "", false
		}
	}
	current, err := s.Store.GetCluster(ctx, rc.Cluster.Ref())
	if err != nil || current.Generation != rc.Cluster.Generation || !current.CreatedAt.Equal(rc.Cluster.CreatedAt) || !bytes.Equal(current.Internal, rc.Cluster.Internal) || current.Status.Phase != rc.Cluster.Status.Phase {
		return "", false
	}
	secrets, err := s.Store.ConfigSecretRevision(ctx, current.Ref())
	if err != nil {
		return "", false
	}
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok || !target.PlatformSnapshot().EnsureListing(ctx) {
		return "", false
	}
	inputs := engine.ConfigPullRevision(rc, member, rc.AgentConfigCapabilities, secrets)
	baseline, ok := s.Trigger.(interface {
		SteadyConfigBaseline(context.Context, *domain.Cluster) bool
	})
	return inputs, inputs != "" && ok && baseline.SteadyConfigBaseline(ctx, current)
}

func (s *Server) memberConfigChangeRevision(ctx context.Context, rc *engine.Context, member string) string {
	if generation, ok := s.Trigger.(interface {
		ConfigInvalidationGeneration(domain.ClusterRef) uint64
	}); ok {
		rc.ConfigInvalidationGeneration = generation.ConfigInvalidationGeneration(rc.Cluster.Ref())
	}
	secrets, err := s.Store.ConfigSecretRevision(ctx, rc.Cluster.Ref())
	if err != nil {
		return ""
	}
	steady := false
	if baseline, ok := s.Trigger.(interface {
		SteadyConfigBaseline(context.Context, *domain.Cluster) bool
	}); ok {
		steady = baseline.SteadyConfigBaseline(ctx, rc.Cluster)
	}
	return engine.ConfigChangeRevision(rc, member, secrets, steady)
}
