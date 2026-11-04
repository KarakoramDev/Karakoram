// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAgentBuildInventory(t *testing.T) {
	f := coreSetup(t, 1)
	coreConverge(t, f)
	target := engine.CurrentAgentBuild()
	for _, build := range []agentapi.AgentBuild{{Version: target.Version, Commit: target.Commit, Build: target.Build}, {Version: "old-version", Commit: "old-commit", Build: "old-build"}, {}} {
		f.agentBuild = build
		status, err := f.p.Status(t.Context(), f.rc)
		if err != nil || len(status.Members) != 1 {
			t.Fatal(status, err)
		}
		agent := status.Members[0].Agent
		if !agent.Healthy || agent.Version != build.Version || agent.Commit != build.Commit || agent.Build != build.Build {
			t.Fatal("generic executable identity lost", agent, build)
		}
		engine.ObserveAgentVersions(&status, target, time.Now())
		if (len(status.Agents.Unknown) == 1) != (build.Build == "") || (len(status.Agents.Outdated) == 1) != (build.Build != "" && build.Build != target.Build) {
			t.Fatal("incorrect fleet classification", status.Agents, build)
		}
	}
}
