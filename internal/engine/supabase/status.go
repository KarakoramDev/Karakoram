// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	out, err := p.Core.Status(ctx, rc)
	if err != nil {
		return out, err
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return out, err
	}
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return out, err
	}
	ready := s.Pending == nil
	for _, def := range stackComponents {
		m := s.Members[def.Name]
		desired := rc.Cluster.Spec.Components.Extra[def.Name]
		if m == nil {
			ready = ready && desired.Replicas == 0
			continue
		}
		if def.Name == "gateway" && m.Public != options.Gateway.PublicHTTPS {
			ready = false
		}
		status, err := componentStatus(ctx, rc, m)
		if err != nil {
			return out, err
		}
		healthy := status.Ready
		if def.Name == "gateway" && desired.Replicas > 0 {
			ready = gatewayInstancesReady(ctx, rc, m) && ready
		}
		member := domain.MemberStatus{
			Name: m.Name, Set: def.Name, Role: def.Name, State: string(m.Stage),
			ComponentVersion: m.Version, Health: healthy,
		}
		if healthy {
			member.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: m.ConfigVersion, LastSeen: clockNow(rc)}
		}
		containers, err := rc.Backend.Containers(ctx, m.Name)
		if err != nil {
			if def.Name != "gateway" {
				return out, err
			}
			ready = false
		}
		for _, container := range containers {
			if container.PlacementGroup != "" {
				member.PlacementGroup = container.PlacementGroup
				out.Placement.Groups[container.PlacementGroup] = append(out.Placement.Groups[container.PlacementGroup], m.Name)
			}
		}
		// These members deliberately leave Version empty: the reconciler aggregates
		// that field as PostgreSQL's observed version across the entire cluster.
		out.Members = append(out.Members, member)
		ready = ready && healthy && m.Stage == engine.ComponentJoined && m.Replicas == desired.Replicas && desired.Replicas > 0
	}
	if !ready && out.Phase == domain.PhaseReady {
		out.Phase = domain.PhaseUpdating
		if rc.Cluster.Status.ObservedGeneration == 0 {
			out.Phase = domain.PhaseCreating
		}
		out.ObservedGeneration = rc.Cluster.Status.ObservedGeneration
		out.Conditions = []domain.Condition{{Type: "Ready", Status: "False", Reason: "ComponentsStarting", LastTransitionTime: clockNow(rc)}}
	}
	if ready {
		api, err := p.apiURL(ctx, rc)
		if err != nil {
			return out, err
		}
		if api != "" {
			out.Connection.Endpoints = append(out.Connection.Endpoints, domain.Endpoint{Name: "supabase", URL: api})
		}
		if rc.Cluster.Spec.Components.Extra["studio"].Replicas > 0 {
			out.Connection.Endpoints = append(out.Connection.Endpoints, domain.Endpoint{Name: "studio", URL: api})
		}
	}
	return out, nil
}

// Final readiness requires local observations from every live gateway. Bootstrap
// readiness remains listener-only so sequential upstream creation can proceed.
func gatewayInstancesReady(ctx context.Context, rc *engine.Context, m *componentMember) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	all, count, err := engine.VisitLiveInstances(ctx, rc, m.Name, func(client *agentapi.Client) (bool, error) {
		applied, err := client.Status(ctx)
		if err != nil || applied.ConfigVersion < m.ConfigVersion {
			return false, err
		}
		var status sbspec.Status
		if err := client.Engine(ctx, "supabase", "status", struct{}{}, &status); err != nil {
			return false, err
		}
		if !status.Ready {
			return false, nil
		}
		for _, upstream := range sbspec.GatewayUpstreams {
			if rc.Cluster.Spec.Components.Extra[upstream].Replicas > 0 && status.GatewayUpstreamHosts[upstream] < 1 {
				return false, nil
			}
		}
		return true, nil
	})
	return err == nil && all && count > 0
}
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if string(op.Kind) != "rotate-jwt-secret" {
		return p.Core.RunOperation(ctx, rc, op)
	}
	credentials, err := loadCredentials(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	// The operation identity is sealed with the new keys before rolling starts.
	// Retrying a lost operation checkpoint cannot generate a second secret.
	if credentials.RotationID != op.ID {
		keys, err := sbkeys.Generate(rand.Reader, clockNow(rc))
		if err != nil {
			return engine.OpResult{}, err
		}
		credentials.Keys.JWTSecret = keys.JWTSecret
		credentials.Keys.AnonKey = keys.AnonKey
		credentials.Keys.ServiceRoleKey = keys.ServiceRoleKey
		credentials.Keys.PublishableKey = keys.PublishableKey
		credentials.Keys.SecretKey = keys.SecretKey
		credentials.Keys.AnonJWT = keys.AnonJWT
		credentials.Keys.ServiceRoleJWT = keys.ServiceRoleJWT
		credentials.Keys.JWTKeys = keys.JWTKeys
		credentials.Keys.JWKS = keys.JWKS
		credentials.RotationID = op.ID
		if err = storeCredentials(ctx, rc, credentials); err != nil {
			return engine.OpResult{}, err
		}
	}
	busy, err := p.syncComponents(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: !busy, Step: "rotate-jwt-secret", Message: "Applying rotated API signing keys", RequeueAfter: 2 * time.Second}, nil
}
