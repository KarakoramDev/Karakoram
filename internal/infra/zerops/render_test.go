// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type renderFixture struct {
	Hostname string            `json:"hostname"`
	Binary   string            `json:"binary"`
	Runtime  infra.RuntimeSpec `json:"runtime"`
}

func TestRenderYAMLGolden(t *testing.T) {
	paths, err := filepath.Glob("testdata/render/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no render fixtures")
	}
	for _, filename := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(filename), ".json"), func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			var fixture renderFixture
			if err = json.Unmarshal(data, &fixture); err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(strings.TrimSuffix(filename, ".json") + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			got, err := RenderYAML(fixture.Hostname, zeropsLayout, fixture.Binary, fixture.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Fatalf("rendered YAML differs from baseline\nwant:\n%s\ngot:\n%s", want, got)
			}
		})
	}
}

func TestRenderYAMLOptionalFields(t *testing.T) {
	got, err := RenderYAML("member", zeropsLayout, "karakoram", infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"prepareCommands:", "readinessCheck:", "volume:"} {
		if strings.Contains(got, key) {
			t.Errorf("unexpected %s", key)
		}
	}
	if strings.Count(got, "httpSupport: true") != 1 || strings.Contains(got, "httpSupport: false") {
		t.Fatalf("incorrect HTTP flags:\n%s", got)
	}
}
