// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// These fixtures were recorded before the profile cutover. Inputs use fixed
// credentials, certificates and incarnation so every hashed byte is reproducible.
func TestLegacyProfileBytes(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	var core map[string]any
	if err := json.Unmarshal(root["postgres"], &core); err != nil {
		t.Fatal(err)
	}
	core["uid"] = "golden-incarnation"
	for _, name := range []string{"demop0", "demopx"} {
		for _, kind := range []string{"servers", "agents"} {
			for _, suffix := range []string{"crt", "key"} {
				key := "tls/" + kind + "/" + name + "." + suffix
				if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture-"+key)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, key := range []string{"users/postgres", "users/kk_replicator", "users/kk_rewind", "users/kk_monitor", "users/kk_admin", "users/kk_proxy_admin"} {
		if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	// A real, checked-in public CA keeps validation and every compared byte intact.
	const fixtureCA = `-----BEGIN CERTIFICATE-----
MIIBWTCCAQugAwIBAgIUKml0U6EMHdpce6Fv3QRvK0z/nOcwBQYDK2VwMCExHzAd
BgNVBAMMFnByb2ZpbGUtZ29sZGVuLWZpeHR1cmUwIBcNMjYxMDIxMDcxNjQ3WhgP
MjEyNjA5MjcwNzE2NDdaMCExHzAdBgNVBAMMFnByb2ZpbGUtZ29sZGVuLWZpeHR1
cmUwKjAFBgMrZXADIQBhwN0EUJt8aoOO1mI3i9cm6VpADN9L2JyuvCJXhKllM6NT
MFEwHQYDVR0OBBYEFMx4KZYbkCIoCmrm5qWxRv9tG412MB8GA1UdIwQYMBaAFMx4
KZYbkCIoCmrm5qWxRv9tG412MA8GA1UdEwEB/wQFMAMBAf8wBQYDK2VwA0EApg/j
y0YAD9cjhjQ/Bg6AmeMgqfTYNeqyM0mF4XSx/LTnz4BRKR0UyvJDqEFQXDCWAbfp
iAIaJqbqp0310Zg0Dw==
-----END CERTIFICATE-----
`
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.crt", []byte(fixtureCA)); err != nil {
		t.Fatal(err)
	}
	credentials, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	keys := reflect.ValueOf(&credentials.Keys).Elem()
	for i := 0; i < keys.NumField(); i++ {
		f := keys.Field(i)
		if f.Kind() == reflect.String {
			f.SetString("fixture-" + keys.Type().Field(i).Name)
		} else {
			f.SetBytes([]byte(`{"keys":[]}`))
		}
	}
	credentials.Keys.JWTSecret = "fixture-jwt-secret-at-least-thirty-two-bytes"
	for name := range credentials.Roles {
		credentials.Roles[name] = "fixture-" + name
	}
	credentials.VectorToken = "fixture-vector-token"
	credentials.RealtimeAdminSecret = "fixture-realtime-admin-secret"
	if err := storeCredentials(t.Context(), rc, credentials); err != nil {
		t.Fatal(err)
	}
	type golden struct {
		Prepare         map[string][]string
		Members         map[string]json.RawMessage
		Plan            []initsql.Step
		BootstrapDigest string
		RestartTokens   map[string]string
		CoreSpecDigest  string
	}
	cases := []struct {
		name       string
		extensions []string
		generation int
	}{
		{"default", nil, 1}, {"ordered", []string{"pg_net", "pg_cron", "pgsodium", "pgaudit", "pg_stat_monitor"}, 1},
		{"reordered-duplicates", []string{"pg_stat_monitor", "pg_cron", "pg_net", "pg_cron", "pgsodium"}, 1},
		{"restore-generation", []string{"pg_net", "pg_cron"}, 7},
	}
	presets := p.Describe().Presets
	for _, preset := range presets {
		cases = append(cases, struct {
			name       string
			extensions []string
			generation int
		}{preset.Name, nil, 1})
	}
	original := rc.Cluster.Spec
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc.Cluster.Spec = original
			for _, preset := range presets {
				if preset.Name == tc.name {
					rc.Cluster.Spec.Components = preset.Components
				}
			}
			raw, _ := json.Marshal(map[string]any{"extensions": tc.extensions})
			rc.Cluster.Spec.EngineOptions = raw
			core["bootstrapGeneration"] = tc.generation
			// Member options deliberately remain the applied baseline: the historical
			// wrapper appends selected preloads from the desired spec, not member options.
			root["postgres"], _ = json.Marshal(core)
			rc.Cluster.Internal, _ = json.Marshal(root)
			out := golden{Prepare: backend.prepares, Members: map[string]json.RawMessage{}, RestartTokens: map[string]string{}}
			for _, name := range []string{"demop0", "demopx"} {
				config, err := p.MemberConfig(t.Context(), rc, name)
				if err != nil {
					t.Fatal(err)
				}
				out.Members[name] = config.EngineSpec
			}
			request := pgspec.SQLBootstrapRequest{Components: stackComponents.Enabled(rc.Cluster.Spec.Components), Extensions: tc.extensions, Passwords: credentials.Roles}
			out.Plan = initsql.Plan(request.Components, request.Extensions)
			out.BootstrapDigest = configDigest(struct {
				Request    pgspec.SQLBootstrapRequest
				Generation int
				Plan       []initsql.Step
			}{request, tc.generation, out.Plan})
			for _, component := range stackComponents.Enabled(rc.Cluster.Spec.Components) {
				sp, err := p.desiredSpec(t.Context(), rc, &componentMember{Component: component})
				if err != nil {
					t.Fatal(err)
				}
				out.RestartTokens[component] = sp.RestartToken
			}
			spec := rc.Cluster.Spec
			spec.Components.Extra = nil
			out.CoreSpecDigest = configDigest(struct {
				Spec               domain.ClusterSpec
				Proxy              domain.ComponentSpec
				AccessRevision     string
				ProxyBudgetVersion int
			}{spec, rc.Cluster.Spec.Components.Extra["proxy"], "", 1})
			got, err := json.MarshalIndent(out, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join("testdata", "profile-"+tc.name+".golden.json")
			if os.Getenv("UPDATE_PROFILE_GOLDEN") == "1" {
				if err := os.MkdirAll("testdata", 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				for i := range min(len(got), len(want)) {
					if got[i] != want[i] {
						t.Fatalf("legacy bytes changed: %s at %d: got %q; want %q", path, i, got[max(0, i-80):min(len(got), i+160)], want[max(0, i-80):min(len(want), i+160)])
					}
				}
				t.Fatalf("legacy byte length changed: %s: got %d; want %d", path, len(got), len(want))
			}
		})
	}
}
