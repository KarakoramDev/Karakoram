// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAgentBuildInventory(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	status, err := p.Status(t.Context(), rc)
	if err != nil || len(status.Members) != 1 {
		t.Fatal(status, err)
	}
	agent := status.Members[0].Agent
	if !agent.Healthy || agent.Version != "agent-version" || agent.Commit != "agent-commit" || agent.Build != "agent-build" {
		t.Fatalf("generic executable identity lost: %+v", agent)
	}
	engine.ObserveAgentVersions(&status, engine.CurrentAgentBuild(), status.ObservedAt)
	if len(status.Agents.Unknown) != 0 || len(status.Agents.Outdated) != 1 {
		t.Fatal(status.Agents)
	}
}
