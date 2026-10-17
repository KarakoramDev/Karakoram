// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"testing"
)

func TestRenderGatewayCORSAllowlist(t *testing.T) {
	s := testSpec("gateway")
	s.CORSOrigins = []string{"https://app.example.test"}
	_, files, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config := files["supabase/envoy.json"]
	if !bytes.Contains(config, []byte(`"exact": "https://app.example.test"`)) || !bytes.Contains(config, []byte(`"allow_credentials": false`)) {
		t.Fatal("gateway did not receive the CORS allowlist without credentials")
	}
}

func TestRenderPostgRESTUpstreamSchemas(t *testing.T) {
	s := testSpec("rest")
	process, _, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !contains(process.Env, "PGRST_DB_SCHEMAS=public,graphql_public") {
		t.Fatal("PostgREST fallback diverges from the pinned upstream schema list")
	}
	s.REST.Schemas = []string{"custom", "graphql_public"}
	process, _, err = Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !contains(process.Env, "PGRST_DB_SCHEMAS=custom,graphql_public") {
		t.Fatal("effective custom GraphQL schema list was not rendered")
	}
}
