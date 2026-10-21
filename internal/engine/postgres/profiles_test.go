// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestProfileAdmission(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	p, rc, _ := setup(t, "single")
	for _, version := range []string{"16.15.1", "17.11.1", "18.6.1"} {
		for _, ids := range [][]string{{"postgis"}, {"pgvector"}, {"pgvector", "postgis", "postgis"}} {
			rc.Cluster.Spec.Version = version
			rc.Cluster.Spec.EngineOptions = mustJSON(map[string]any{"profiles": ids})
			if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
				t.Fatal(version, ids, err)
			}
		}
		rc.Cluster.Spec.EngineOptions = mustJSON(map[string]any{"profiles": []string{"timescaledb"}})
		err := p.Validate(t.Context(), nil, rc.Cluster)
		if _, artifactErr := nativeartifacts.Get("timescaledb-pg" + strings.Split(version, ".")[0]); artifactErr != nil {
			if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "timescaledb-pg"+strings.Split(version, ".")[0]) {
				t.Fatal("missing artifact not rejected clearly", err)
			}
		} else if err != nil {
			t.Fatal("qualified TimescaleDB artifact rejected", version, err)
		}
	}
	for _, id := range []string{"unknown", "supabase"} {
		rc.Cluster.Spec.EngineOptions = mustJSON(map[string]any{"profiles": []string{id}})
		if err := p.Validate(t.Context(), nil, rc.Cluster); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(id, err)
		}
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["postgis","pgvector"]}`)
	old := *rc.Cluster
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["postgis"]}`)
	if err := p.Validate(t.Context(), &old, rc.Cluster); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "cannot remove profile pgvector") {
		t.Fatal(err)
	}
}
func TestAddingProfileRepreparesAndBootstraps(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	b.actions = nil
	rc.Cluster.Generation++
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["pgvector","postgis","postgis"]}`)
	state := converge(t, p, rc)
	if !slices.Contains(b.actions, "deploy:demop0") {
		t.Fatal("profile addition did not reprepare", b.actions)
	}
	if !slices.Contains(b.actions, "profile-bootstrap:demop0:postgis,pgvector") {
		t.Fatal("curated bootstrap missing", b.actions)
	}
	runtime, err := p.runtimeSpec(rc, state.find("demop0"))
	if err != nil {
		t.Fatal(err)
	}
	prepare := strings.Join(runtime.Prepare, "\n")
	if strings.Count(prepare, "percona-postgresql-17-postgis-3='") != 1 || !strings.Contains(prepare, "percona-postgresql-17-pgvector='1:0.8.6-1.noble'") {
		t.Fatal("packages missing or duplicated", prepare)
	}
	config, err := p.MemberConfig(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var wire pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &wire); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(wire.Profiles, "pgvector") {
		t.Fatal("member cannot authorize bootstrap", wire.Profiles)
	}
	available, err := p.availableExtensions(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"postgis", "postgis_topology", "postgis_raster", "vector", "pgcrypto"} {
		if !slices.Contains(available, name) {
			t.Fatal("not available", name)
		}
	}
	want := []string{"postgis", "postgis_topology", "postgis_raster", "vector"}
	if got := p.options.ProfileExtensions(rc); !reflect.DeepEqual(got, want) {
		t.Fatal("upgrade update set", got)
	}
	before := len(b.actions)
	if err = p.postReadyProfiles(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(b.actions) != before {
		t.Fatal("bootstrap digest did not suppress replay")
	}
	state.BootstrapGeneration++
	if err = save(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	if err = p.postReadyProfiles(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(b.actions) != before+1 {
		t.Fatal("restore generation did not reapply bootstrap")
	}
	if p.profilesAdded(json.RawMessage(`{"profiles":["postgis","pgvector"]}`), rc.Cluster.Spec.EngineOptions) {
		t.Fatal("set reordering needs no preparation")
	}
}
func TestNoProfilesRetainsLegacyWire(t *testing.T) {
	p, rc, b := setup(t, "single")
	state := converge(t, p, rc)
	config, err := p.MemberConfig(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(config.EngineSpec), `"profiles"`) {
		t.Fatal("legacy wire gained a field")
	}
	runtime, err := p.runtimeSpec(rc, state.find("demop0"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(runtime.Prepare, PrepareCommands(rc.Cluster.Spec.Version)) {
		t.Fatal("legacy prepare changed")
	}
	for _, action := range b.actions {
		if strings.HasPrefix(action, "profile-bootstrap:") {
			t.Fatal("legacy cluster bootstrapped profiles")
		}
	}
}
func TestDescribeProfilesPerMajor(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	metadata := profileDescription()
	items := metadata["items"].([]any)
	if len(items) != 3 {
		t.Fatal(items)
	}
	for _, item := range items {
		entry := item.(map[string]any)
		majors := entry["majors"].(map[string]any)
		for _, major := range []string{"16", "17", "18"} {
			capability := majors[major].(map[string]any)
			wantAvailable := true
			if entry["id"] == "timescaledb" {
				_, err := nativeartifacts.Get("timescaledb-pg" + major)
				wantAvailable = err == nil
			}
			if capability["available"] != wantAvailable {
				t.Fatal("profile availability differs from qualified catalog", entry["id"], major, capability)
			}
		}
	}
}
