// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestUpgradeAgentsAPI(t *testing.T) {
	s, _, token := testServer(t)
	path := "/api/v1/clusters/prod/upgrade-agents"
	if w := call(s, "POST", path, "{}", token); w.Code != 409 {
		t.Fatal("unsupported", w.Code, w.Body.String())
	}
	provider, _ := s.Registry.Get("fake")
	fake := provider.(*fakeProvider)
	info := fake.Describe()
	info.Operations = append(info.Operations, domain.OperationUpgradeAgents)
	fake.info = &info
	ctx := context.Background()
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	target := engine.CurrentAgentBuild()
	c.Status.ObservedAt = time.Now()
	c.Status.Members[0].Agent = domain.AgentStatus{Healthy: true, Build: target.Build, Version: target.Version, LastSeen: time.Now()}
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "POST", path, "", token); w.Code != 409 || !strings.Contains(w.Body.String(), "AgentsCurrent") {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call(s, "POST", path+"?ignoreMaintenanceWindow=true", `{"includeCurrent":true}`, token)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	op, err := s.Store.GetOperation(ctx, w.Header().Get("Karakoram-Operation-Id"))
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := engine.AgentUpgradeTarget(op)
	if err != nil || frozen.TargetBuild != target.Build || frozen.TargetVersion != target.Version || !frozen.IncludeCurrent || !op.IgnoreMaintenanceWindow {
		t.Fatal(op, frozen, err)
	}
	c.Status.Members[0].Agent.Build = "old"
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	if w = call(s, "POST", path, "", token); w.Code != 202 {
		t.Fatal("optional body", w.Code, w.Body.String())
	}
	c.Status.Phase = domain.PhaseSuspended
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	if w = call(s, "POST", path, `{"includeCurrent":true}`, token); w.Code != 409 {
		t.Fatal("suspended", w.Code, w.Body.String())
	}
	if w = call(s, "POST", "/api/v1/clusters/missing/upgrade-agents", "", token); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestAgentRolloutSettingsAPI(t *testing.T) {
	s, _, token := testServer(t)
	path := "/api/v1/settings/agent-rollout"
	w := call(s, "GET", path, "", token)
	var settings domain.AgentRolloutSettings
	if err := json.Unmarshal(w.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || settings.Automatic || settings.MaxConcurrentClusters != 1 || settings.CurrentBuild == nil || settings.CurrentBuild.Build != engine.CurrentAgentBuild().Build {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, body := range []string{`{"automatic":true,"maxConcurrentClusters":0}`, `{"automatic":true,"maxConcurrentClusters":6}`} {
		w = call(s, "PUT", path, body, token)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w = call(s, "PUT", path, `{"automatic":true,"maxConcurrentClusters":5}`, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "GET", path, "", token)
	if !strings.Contains(w.Body.String(), `"automatic":true`) || !strings.Contains(w.Body.String(), `"maxConcurrentClusters":5`) {
		t.Fatal(w.Body.String())
	}
}
