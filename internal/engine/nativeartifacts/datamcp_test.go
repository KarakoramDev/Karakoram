// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nativeartifacts

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/version"
)

func TestDataMCPArtifacts(t *testing.T) {
	old := version.Version
	t.Cleanup(func() { version.Version = old })
	version.Version = "dev"
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	for _, pin := range []struct{ name, version, license, build string }{
		{"pgedge-postgres-mcp", "1.1.0", "PostgreSQL", "upstream-release"},
		{"mongodb-mcp-server", "3.1.0", "Apache-2.0", "native-v1"},
	} {
		a, err := Get(pin.name)
		if err != nil {
			t.Fatal(err)
		}
		if a.Version != pin.version || a.License != pin.license || a.Build != pin.build {
			t.Fatalf("unexpected pin: %+v", a)
		}
		url, err := ResolveURL(a)
		if pin.build == "upstream-release" {
			if err != nil || url != a.UpstreamURL {
				t.Fatalf("official resolution: %q, %v", url, err)
			}
		} else {
			var missing *MissingMirrorError
			if !errors.As(err, &missing) {
				t.Fatalf("development bundle requires mirror, got %v", err)
			}
		}
	}
}
