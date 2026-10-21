// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentrollout"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (s *Server) UpgradeClusterAgents(ctx context.Context, request api.UpgradeClusterAgentsRequestObject) (api.UpgradeClusterAgentsResponseObject, error) {
	return s.handle(ctx, "UpgradeClusterAgents", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitUpgradeClusterAgentsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) GetAgentRolloutSettings(ctx context.Context, request api.GetAgentRolloutSettingsRequestObject) (api.GetAgentRolloutSettingsResponseObject, error) {
	return s.handle(ctx, "GetAgentRolloutSettings", input{}), nil
}
func (r response) VisitGetAgentRolloutSettingsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) UpdateAgentRolloutSettings(ctx context.Context, request api.UpdateAgentRolloutSettingsRequestObject) (api.UpdateAgentRolloutSettingsResponseObject, error) {
	return s.handle(ctx, "UpdateAgentRolloutSettings", input{Body: request.Body}), nil
}
func (r response) VisitUpdateAgentRolloutSettingsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) upgradeAgents(ctx context.Context, c *domain.Cluster, in input) response {
	include := false
	if body, ok := in.Body.(*api.UpgradeAgentsRequest); ok && body != nil {
		include = body.IncludeCurrent != nil && *body.IncludeCurrent
	}
	if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
		return fail(409, "conflict", "Cluster is deleting or suspended; resume it before requesting this action")
	}
	status := engine.FreshStatus(c.Status, time.Now())
	target := engine.CurrentAgentBuild()
	engine.ObserveAgentVersions(&status, target, time.Now())
	if !include && len(status.Agents.Outdated) == 0 && len(status.Agents.Unknown) == 0 {
		return fail(409, "AgentsCurrent", "All member agents already report the current build")
	}
	return s.enqueue(ctx, c.Ref(), domain.OperationUpgradeAgents, engine.AgentUpgradePayload{IncludeCurrent: include, TargetBuild: target.Build, TargetVersion: target.Version})
}

func (s *Server) agentRolloutSettings(ctx context.Context, name string, in input) response {
	if name == "UpdateAgentRolloutSettings" {
		body := in.Body.(*api.AgentRolloutSettings)
		settings := domain.AgentRolloutSettings{Automatic: body.Automatic, MaxConcurrentClusters: body.MaxConcurrentClusters}
		if err := agentrollout.Validate(settings); err != nil {
			return fail(422, "validation_failed", err.Error())
		}
		raw, _ := json.Marshal(settings)
		if err := s.Store.SetSetting(ctx, agentrollout.SettingsKey, raw); err != nil {
			return s.failure(err)
		}
	}
	settings, err := agentrollout.Settings(ctx, s.Store)
	if err != nil {
		return s.failure(err)
	}
	return ok(settings)
}
