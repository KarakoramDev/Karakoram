// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pgprofile

import (
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"reflect"
	"strings"
	"testing"
)

func TestPinnedPackagesAndPlans(t *testing.T) {
	for _, major := range []int{16, 17, 18} {
		for _, id := range []string{"postgis", "pgvector"} {
			d, err := Get(id)
			if err != nil {
				t.Fatal(err)
			}
			commands, err := d.PrepareCommands(infra.Layout{User: "app"}, major)
			if err != nil {
				t.Fatal(err)
			}
			if len(commands) != 1 || !strings.Contains(commands[0], "percona-postgresql-") {
				t.Fatal(commands)
			}
			for _, pkg := range d.Packages(major) {
				if !strings.Contains(pkg, "='") || !strings.HasSuffix(pkg, ".noble'") {
					t.Fatal("unpinned package", pkg)
				}
			}
			steps := d.Plan(nil, nil, major)
			if len(steps) != len(d.ExtensionNames(major)) {
				t.Fatal("missing extension plan", id)
			}
			for i, name := range d.ExtensionNames(major) {
				want := "CREATE EXTENSION IF NOT EXISTS \"" + name + "\" WITH SCHEMA public CASCADE;\n"
				if name == "postgis_topology" {
					want = "CREATE EXTENSION IF NOT EXISTS \"postgis_topology\" CASCADE;\n"
				}
				if steps[i].SQL != want {
					t.Fatal(steps[i])
				}
			}
		}
	}
}
func TestRegistryAvailabilityAndComposition(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	for _, major := range []int{16, 17, 18} {
		_, err := Resolve([]string{"timescaledb"}, major)
		if _, artifactErr := nativeartifacts.ExtensionArtifact("timescaledb", major); artifactErr != nil {
			if err == nil || !strings.Contains(err.Error(), "timescaledb-pg") {
				t.Fatal("missing artifact not named", err)
			}
		} else if err != nil {
			t.Fatal("qualified TimescaleDB artifact rejected", major, err)
		}
	}
	if _, err := Resolve([]string{"typo"}, 17); err == nil {
		t.Fatal("unknown profile admitted")
	}
	if _, err := Resolve([]string{"supabase"}, 17); err == nil {
		t.Fatal("wrapper profile selected on plain postgres")
	}
	if _, err := Resolve([]string{"pgvector"}, 15); err == nil {
		t.Fatal("unsupported major admitted")
	}
	profiles, err := Resolve([]string{"pgvector", "postgis", "postgis"}, 17)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || profiles[0].ID() != "postgis" || profiles[1].ID() != "pgvector" {
		t.Fatal("not canonical", profiles)
	}
}
func TestDescriptorsReturnOwnedData(t *testing.T) {
	d, _ := Get("supabase")
	pins := d.Packages(17)
	pins[0] = "changed"
	params := d.Parameters()
	params["wal_level"] = "changed"
	majors := d.Majors()
	majors[0] = 16
	preloads := d.BasePreloads()
	preloads[0] = "changed"
	if d.Packages(17)[0] != "ca-certificates" || d.Parameters()["wal_level"] != "logical" || d.Majors()[0] != 17 || d.BasePreloads()[0] != "pg_stat_statements" {
		t.Fatal("descriptor mutated")
	}
	if err := d.Validate(16); err == nil {
		t.Fatal("supabase accepted PG16")
	}
}
func TestSupabasePlanAndPreloadOrder(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	d, _ := Get("supabase")
	selected := []string{"pg_net", "pg_cron", "pgsodium", "pg_net", "pgaudit"}
	if !reflect.DeepEqual(d.Plan([]string{"auth"}, selected, 17), initsql.Plan([]string{"auth"}, selected)) {
		t.Fatal("legacy SQL changed")
	}
	if want := []string{"pg_stat_statements", "pg_net", "pg_cron", "pgaudit"}; !reflect.DeepEqual(d.Preloads(selected), want) {
		t.Fatal(d.Preloads(selected))
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	if err := d.Validate(17); err == nil || !strings.Contains(err.Error(), "supabase-pg17-extensions") {
		t.Fatal("unresolved artifact accepted", err)
	}
}

func TestProfileExtensionSchemaTargets(t *testing.T) {
	for _, id := range []string{"postgis", "pgvector", "timescaledb"} {
		d, _ := Get(id)
		for _, major := range d.Majors() {
			for _, step := range d.Plan(nil, nil, major) {
				if !strings.Contains(step.SQL, " WITH SCHEMA public CASCADE") &&
					step.SQL != "CREATE EXTENSION IF NOT EXISTS \"postgis_topology\" CASCADE;\n" {
					t.Fatalf("%s PG%d has no extension schema target: %q", id, major, step.SQL)
				}
			}
		}
	}
}
