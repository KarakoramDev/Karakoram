// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

func TestAgentBuildArtifactIdentityParity(t *testing.T) {
	reported := agentapi.AgentBuild{Version: version.Version, Commit: version.Commit, Build: version.BuildIdentity()}
	if reported.Build == "" || reported.Build != (ExecutableArtifact{}).ArtifactIdentity() || CurrentAgentBuild().Build != reported.Build {
		t.Fatalf("different executable identities: %+v", reported)
	}
}

func TestAgentInventoryInformational(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := domain.AgentBuild{Build: "target", Version: "v2"}
	status := domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: at, Conditions: []domain.Condition{{Type: "Healthy", Status: "True"}}, Members: []domain.MemberStatus{
		{Name: "z", Agent: domain.AgentStatus{Healthy: true, Build: "old"}},
		{Name: "b", Agent: domain.AgentStatus{Healthy: true}},
		{Name: "a", Agent: domain.AgentStatus{Healthy: false, Build: "target"}},
		{Name: "c", Agent: domain.AgentStatus{Healthy: true, Build: "target"}},
		{Name: "d", Agent: domain.AgentStatus{Healthy: true, Build: "old"}},
	}}
	ObserveAgentVersions(&status, target, at)
	if !reflect.DeepEqual(status.Agents.Outdated, []string{"d", "z"}) || !reflect.DeepEqual(status.Agents.Unknown, []string{"a", "b"}) {
		t.Fatal(status.Agents)
	}
	if status.Phase != domain.PhaseReady || status.Conditions[0].Status != "True" || readinessCondition(status.Conditions[1]) {
		t.Fatal("inventory degraded health", status)
	}
	if status.Conditions[1].Status != "False" || status.Conditions[1].Reason != "Outdated" {
		t.Fatal(status.Conditions)
	}
	status.Members = status.Members[1:4]
	ObserveAgentVersions(&status, target, at)
	if status.Conditions[1].Reason != "Unreported" {
		t.Fatal(status.Conditions)
	}
	status.Members = status.Members[2:]
	ObserveAgentVersions(&status, target, at)
	if status.Conditions[1].Reason != "Current" {
		t.Fatal(status.Conditions)
	}
	stale := FreshStatus(status, at.Add(time.Hour))
	if len(stale.Agents.Unknown) != 1 || len(stale.Agents.Outdated) != 0 {
		t.Fatal(stale.Agents)
	}
}

func TestAgentUpgradeTargetFrozenAndSelection(t *testing.T) {
	payload := AgentUpgradePayload{TargetBuild: "frozen", TargetVersion: "v1"}
	raw, _ := json.Marshal(payload)
	got, err := AgentUpgradeTarget(&domain.Operation{Payload: raw})
	if err != nil || got != payload {
		t.Fatal(got, err)
	}
	current := domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: "frozen"}}
	if MemberNeedsAgentUpgrade(current, got) {
		t.Fatal("current selected")
	}
	current.Agent.Build = "older"
	if !MemberNeedsAgentUpgrade(current, got) {
		t.Fatal("outdated skipped")
	}
	current.Agent.Build = ""
	if !MemberNeedsAgentUpgrade(current, got) {
		t.Fatal("legacy skipped")
	}
	current.Agent.Build, current.Agent.Healthy = "frozen", false
	if !MemberNeedsAgentUpgrade(current, got) {
		t.Fatal("unreachable skipped")
	}
	got.IncludeCurrent = true
	current.Agent.Healthy = true
	if !MemberNeedsAgentUpgrade(current, got) {
		t.Fatal("include-current skipped")
	}
	if _, err = AgentUpgradeTarget(&domain.Operation{Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("missing target accepted")
	}
}
