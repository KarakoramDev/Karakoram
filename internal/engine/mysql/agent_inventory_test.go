// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAgentBuildInventory(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "single")
			}
			converge(t, p, rc, b)
			target := engine.CurrentAgentBuild()
			for _, build := range []agentapi.AgentBuild{{Version: target.Version, Commit: target.Commit, Build: target.Build}, {Version: "old-version", Commit: "old-commit", Build: "old-build"}, {}} {
				b.agentBuild = build
				status, err := p.Status(t.Context(), rc)
				if err != nil || len(status.Members) == 0 {
					t.Fatal(status, err)
				}
				for _, member := range status.Members {
					if !member.Agent.Healthy || member.Agent.Version != build.Version || member.Agent.Commit != build.Commit || member.Agent.Build != build.Build {
						t.Fatal("generic executable identity lost", member.Agent, build)
					}
				}
				engine.ObserveAgentVersions(&status, target, time.Now())
				if (len(status.Agents.Unknown) == len(status.Members)) != (build.Build == "") || (len(status.Agents.Outdated) == len(status.Members)) != (build.Build != "" && build.Build != target.Build) {
					t.Fatal("incorrect fleet classification", status.Agents, build)
				}
			}
		})
	}
}
