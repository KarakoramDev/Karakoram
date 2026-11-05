// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestApplyDefaultsUsesCatalogDefault(t *testing.T) {
	var s domain.ClusterSpec
	(&Provider{}).ApplyDefaults(&s)
	if s.Version != "1.40.3" {
		t.Fatalf("default create version = %s, want catalog default 1.40.3", s.Version)
	}
}

func TestPatchCatalogArtifacts(t *testing.T) {
	defaults := 0
	for _, v := range catalog {
		if v.Default {
			defaults++
			if v.Version != "1.40.3" {
				t.Fatalf("unexpected default %s", v.Version)
			}
		}
		a, err := nativeartifacts.Get(ArtifactName(v.Version))
		if err != nil {
			t.Fatal(err)
		}
		if a.Version != v.Version {
			t.Fatalf("%s resolves to %s", v.Version, a.Version)
		}
		if _, err := PrepareCommands(v.Version, infra.Layout{User: "ubuntu"}); err != nil {
			t.Fatal(err)
		}
		wantStatus := "supported"
		if v.Version == "1.40.1" || v.Version == "1.39.12" {
			wantStatus = "deprecated"
		}
		if v.Status != wantStatus {
			t.Fatalf("%s: %s", v.Version, v.Status)
		}
	}
	if defaults != 1 {
		t.Fatalf("got %d defaults", defaults)
	}
}

func TestPatchUpgradePaths(t *testing.T) {
	paths := catalogUpgradePaths()
	for _, pair := range [][2]string{{"1.39.12", "1.39.15"}, {"1.40.1", "1.40.3"}, {"1.39.12", "1.40.3"}, {"1.39.15", "1.40.3"}, {"1.39.12", "1.40.1"}} {
		found := false
		for _, p := range paths {
			if p.From == pair[0] && p.To == pair[1] {
				found = true
			}
		}
		if !found {
			t.Errorf("missing upgrade %s -> %s", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{{"1.39.15", "1.39.12"}, {"1.40.3", "1.40.1"}, {"1.40.3", "1.39.15"}, {"1.39.12", "1.41.0"}, {"1.40.3", "2.0.0"}} {
		if UpgradeAllowed(pair[0], pair[1]) {
			t.Errorf("accepted upgrade %s -> %s", pair[0], pair[1])
		}
	}
}
