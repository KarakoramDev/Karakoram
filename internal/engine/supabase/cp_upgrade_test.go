// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
)

// Captured from the real pre-bundle engine at 1cb6bd98 with the released
// 17.11.1-1 extension artifact and pgvector 0.8.6 APT preparation.
func TestCPUpgradePreservesPreparedPostgresExtensions(t *testing.T) {
	p, rc, backend := setupProvider(t)
	fixture := enginetest.RestorePreparedCPUpgrade(t, rc, "testdata/cp-upgrade-17.11.1.json")
	legacyPrepare := strings.Join(fixture.Runtime.Prepare, "\n")
	for _, want := range []string{"supabase-pg17-extensions-17.11.1-1", "pgvector='1:0.8.6-1.noble'"} {
		if !strings.Contains(legacyPrepare, want) {
			t.Fatal("fixture lacks genuine old-bundle preparation", want)
		}
	}
	backend.services = enginetest.PreparedServices(fixture)
	backend.deployed = map[string]bool{}
	for name := range fixture.Configs {
		backend.deployed[name] = true
	}
	backend.prepares = map[string][]string{}
	for range 3 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.prepares) != 0 {
		t.Fatal("CP-only upgrade redeployed existing PostgreSQL members", backend.prepares)
	}
	for name, before := range fixture.Configs {
		after, err := p.MemberConfig(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		enginetest.RequirePreparedConfig(t, name, before.EngineSpec, after.EngineSpec)
	}

	backend.agentBuilds = map[string]string{}
	wrapped := &agentRolloutBackend{testBackend: backend}
	rc.Backend = wrapped
	payload, err := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: payload}
	done := false
	for range 100 {
		p = New(p.options)
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Checkpoint != nil {
			op.Checkpoint = result.Checkpoint
		}
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("prepared extension rollout did not complete")
	}
	artifact, err := nativeartifacts.Get("supabase-pg17-extensions")
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Version != "17.11.1-2" {
		t.Fatal("test requires the upgraded PG17 extension bundle", artifact.Version)
	}
	prepare := strings.Join(backend.prepares["demop0"], "\n")
	for _, want := range []string{artifact.File, "pgvector-pg17-0.8.7"} {
		if !strings.Contains(prepare, want) {
			t.Fatal("explicit rollout omitted matching preparation", want)
		}
	}
}
