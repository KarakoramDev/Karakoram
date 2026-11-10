// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type configInputProof struct {
	inputs, change, content string
	eligible                bool
}

func (s *Server) configInvalidation(rc *engine.Context) {
	if generation, ok := s.Trigger.(interface {
		ConfigInvalidationGeneration(domain.ClusterRef) uint64
	}); ok {
		rc.ConfigInvalidationGeneration = generation.ConfigInvalidationGeneration(rc.Cluster.Ref())
	}
}

func (s *Server) configSteady(ctx context.Context, current *domain.Cluster) bool {
	baseline, ok := s.Trigger.(interface {
		SteadyConfigBaseline(context.Context, *domain.Cluster) bool
	})
	return ok && baseline.SteadyConfigBaseline(ctx, current)
}

func (s *Server) memberConfigInputs(ctx context.Context, rc *engine.Context, member string) configInputProof {
	s.configInvalidation(rc)
	provider, err := s.Registry.Get(rc.Cluster.Spec.Provider)
	if err != nil {
		return configInputProof{}
	}
	if fresh, ok := provider.(engine.FreshMemberConfigSource); ok {
		required, err := fresh.RequiresFreshMemberConfig(ctx, rc, member)
		if err != nil || required {
			return configInputProof{}
		}
	}
	current, err := s.Store.GetCluster(ctx, rc.Cluster.Ref())
	if err != nil || current.Generation != rc.Cluster.Generation || !current.CreatedAt.Equal(rc.Cluster.CreatedAt) || !bytes.Equal(current.Internal, rc.Cluster.Internal) || current.Status.Phase != rc.Cluster.Status.Phase {
		return configInputProof{}
	}
	secrets, err := s.Store.ConfigSecretRevision(ctx, current.Ref())
	if err != nil {
		return configInputProof{}
	}
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok || !target.PlatformSnapshot().EnsureListing(ctx) {
		return configInputProof{}
	}
	steady := s.configSteady(ctx, current)
	inputs, change, content := engine.ConfigRevisions(rc, member, rc.AgentConfigCapabilities, secrets, steady)
	change, content = s.configNoticeContent(ctx, rc, provider, member, steady, change, content)
	return configInputProof{inputs: inputs, change: change, content: content, eligible: inputs != "" && steady}
}

func (s *Server) memberConfigNotice(ctx context.Context, rc *engine.Context, member string) (change, content string, steady bool) {
	s.configInvalidation(rc)
	secrets, err := s.Store.ConfigSecretRevision(ctx, rc.Cluster.Ref())
	if err != nil {
		return "", "", false
	}
	steady = s.configSteady(ctx, rc.Cluster)
	_, change, content = engine.ConfigRevisions(rc, member, nil, secrets, steady)
	if provider, err := s.Registry.Get(rc.Cluster.Spec.Provider); err == nil {
		change, content = s.configNoticeContent(ctx, rc, provider, member, steady, change, content)
	}
	return change, content, steady
}

func (s *Server) configNoticeContent(ctx context.Context, rc *engine.Context, provider engine.Provider, member string, steady bool, change, content string) (string, string) {
	if member == companion.Member(rc.Cluster.Name) {
		cfg, err := companion.MemberConfig(ctx, rc, member)
		if err == nil {
			if payload, err := engine.MemberConfigContentRevision(cfg); err == nil {
				return engine.ConfigNoticeRevisions(rc, payload, steady)
			}
		}
		return change, content
	}
	if source, ok := provider.(engine.ConfigNoticeContentSource); ok {
		payload, err := source.MemberConfigNoticeContent(ctx, rc, member)
		if err == nil && payload != "" {
			return engine.ConfigNoticeRevisions(rc, payload, steady)
		}
	}
	return change, content
}
