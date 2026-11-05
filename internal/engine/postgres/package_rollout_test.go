// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestAgentRolloutUpdatesInstalledProfileExtensionsAfterAllMembers(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.actions = nil
	p.options.ProfileExtensions = func(*engine.Context) []string { return []string{"vector", "set_user"} }
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) {
		return []string{"vector", "set_user"}, nil
	}
	op := rolloutOperation(true)
	order := []string{"demop1", "demop2", "demopx", "demop0"}
	// An in-flight member has not acknowledged its new binaries yet.
	op.Checkpoint = mustJSON(agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}, Order: order, Index: len(order) - 1, Waiting: true})
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(b.actions, pgspec.ActionUpdateExtensions+":demop0:vector,set_user") {
		t.Fatal("updated SQL before every member acknowledged its binaries", b.actions)
	}
	// The persisted completed order is the binary rollout barrier.
	op.Checkpoint = mustJSON(agentUpgradeCheckpoint{Order: order, Index: len(order)})
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal(result, err)
	}
	if !slices.Contains(b.actions, pgspec.ActionUpdateExtensions+":demop0:vector,set_user") {
		t.Fatal("apt-only rollout omitted per-database extension update", b.actions)
	}
	after, _ := load(rc.Cluster)
	if after.ExtensionsPending || after.find("demop0").Version != s.find("demop0").Version {
		t.Fatal("extension completion changed server release or stayed pending")
	}
}

func TestAgentRolloutExtensionFailureRetainsPendingAndRetries(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	op := rolloutOperation(true)
	op.Checkpoint = mustJSON(agentUpgradeCheckpoint{Order: []string{"demop0"}, Index: 1})
	p.options.ProfileExtensions = func(*engine.Context) []string { return []string{"vector"} }
	sentinel := errors.New("extension package unavailable")
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) { return nil, sentinel }
	result, err := p.RunOperation(t.Context(), rc, op)
	if !errors.Is(err, sentinel) || result.Done {
		t.Fatal("rollout completed despite extension failure", result, err)
	}
	s, _ := load(rc.Cluster)
	if !s.ExtensionsPending {
		t.Fatal("failed SQL update lost pending state")
	}
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) { return []string{"vector"}, nil }
	result, err = New(p.options).RunOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal("completed rollout did not retry SQL", result, err)
	}
	s, _ = load(rc.Cluster)
	if s.ExtensionsPending {
		t.Fatal("successful SQL retry stayed pending")
	}
}

type packageRolloutSupabaseFlavour struct{}

func (packageRolloutSupabaseFlavour) Name() string { return "supabase" }
func (packageRolloutSupabaseFlavour) PostReady(context.Context, *engine.Context) error {
	return nil
}
func (packageRolloutSupabaseFlavour) RelayTargets(*domain.Cluster) []string { return nil }
func (packageRolloutSupabaseFlavour) SelectedExtensions(json.RawMessage) ([]string, error) {
	return nil, nil
}

func TestSupabasePackageRolloutIncludesFormerlySelectedExtensions(t *testing.T) {
	p, rc, _ := setup(t, "single")
	p.options.Flavour = packageRolloutSupabaseFlavour{}
	names := p.selectedProfileExtensions(rc)
	for _, name := range []string{"vector", "set_user", "pg_cron"} {
		if !slices.Contains(names, name) {
			t.Fatal("formerly selected installed extension omitted from updates", name, names)
		}
	}
}

func TestAgentRolloutSQLFailureRetriesWithoutRedeployingCompletedMembers(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	p.options.ProfileExtensions = func(*engine.Context) []string { return []string{"vector"} }
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) {
		return []string{"vector"}, nil
	}
	b.extensionFailures = 1
	b.actions = nil
	op := rolloutOperation(true)
	op.Checkpoint = mustJSON(agentUpgradeCheckpoint{Order: []string{"demop0"}, Index: 1})
	completedCheckpoint := string(op.Checkpoint)
	result, err := p.RunOperation(t.Context(), rc, op)
	if err == nil || result.Done {
		t.Fatal("completed despite failed update SQL", result, err)
	}
	s, _ := load(rc.Cluster)
	if !s.ExtensionsPending || string(op.Checkpoint) != completedCheckpoint {
		t.Fatal("failed SQL lost pending state or completed member checkpoint")
	}
	result, err = New(p.options).RunOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal("SQL retry did not complete", result, err)
	}
	s, _ = load(rc.Cluster)
	if s.ExtensionsPending || string(op.Checkpoint) != completedCheckpoint {
		t.Fatal("SQL retry retained pending state or changed completed checkpoint")
	}
	if !slices.Equal(b.actions, []string{"update-extensions:demop0:vector", "update-extensions:demop0:vector"}) {
		t.Fatal("SQL retry redeployed a completed member or omitted an update attempt", b.actions)
	}
}
