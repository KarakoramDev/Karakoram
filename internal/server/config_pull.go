// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (s *Server) memberConfigRevision(ctx context.Context, rc *engine.Context, member string) string {
	if generation, ok := s.Trigger.(interface {
		ConfigInvalidationGeneration(domain.ClusterRef) uint64
	}); ok {
		rc.ConfigInvalidationGeneration = generation.ConfigInvalidationGeneration(rc.Cluster.Ref())
	}
	provider, err := s.Registry.Get(rc.Cluster.Spec.Provider)
	if err != nil {
		return ""
	}
	if fresh, ok := provider.(engine.FreshMemberConfigSource); ok {
		required, err := fresh.RequiresFreshMemberConfig(ctx, rc, member)
		if err != nil || required {
			return ""
		}
	}
	baseline, ok := s.Trigger.(interface {
		SteadyConfigBaseline(context.Context, *domain.Cluster) bool
	})
	if !ok {
		return ""
	}
	current, err := s.Store.GetCluster(ctx, rc.Cluster.Ref())
	if err != nil || current.Generation != rc.Cluster.Generation || !current.CreatedAt.Equal(rc.Cluster.CreatedAt) || !bytes.Equal(current.Internal, rc.Cluster.Internal) || !baseline.SteadyConfigBaseline(ctx, current) {
		return ""
	}
	secrets, err := s.Store.ConfigSecretRevision(ctx, current.Ref())
	if err != nil || !baseline.SteadyConfigBaseline(ctx, current) {
		return ""
	}
	return engine.ConfigPullRevision(rc, member, rc.AgentConfigCapabilities, secrets)
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
