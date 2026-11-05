// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
)

// TestRealtimeNativeFixture exports the actual renderer and curated bootstrap
// for the isolated native artifact runner. It contains only test credentials.
func TestRealtimeNativeFixture(t *testing.T) {
	s := testSpec("realtime")
	s.Database = "smoke"
	s.Version = "2.153.3"
	s.InstallDir = "/opt/artifact"
	p, _, err := Render(s, "/tmp/native-render")
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{}
	for _, entry := range p.Env {
		key, value, _ := strings.Cut(entry, "=")
		environment[key] = value
	}
	var bootstrap strings.Builder
	for _, step := range initsql.Plan([]string{"realtime"}, nil) {
		if step.Database != "" {
			t.Fatal("unexpected alternate database")
		}
		bootstrap.WriteString(step.SQL)
		bootstrap.WriteByte('\n')
	}
	fixture := map[string]any{"environment": environment, "bootstrap": bootstrap.String(), "seed": realtimeSeed, "tenantJWTSecret": s.JWTSecret}
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("KARAKORAM_REALTIME_SMOKE_FIXTURE"); path != "" {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
