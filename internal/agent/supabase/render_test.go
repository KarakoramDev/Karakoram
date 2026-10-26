// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func testSpec(component string) sbspec.Spec {
	return sbspec.Spec{
		Component: component, Version: "1.2.3", InstallDir: "/opt/karakoram/supabase-" + component + "-1.2.3",
		RestartToken: "revision", Topology: "single", DataPeers: []string{"data.example"}, Database: "postgres",
		DBRoles: map[string]string{"postgres": "p@ss:/word", "supabase_auth_admin": "auth-secret", "authenticator": "rest-secret", "supabase_admin": "admin-secret", "supabase_storage_admin": "storage-secret", "pgbouncer": "pool-secret"},
		SiteURL: "https://app.example", APIExternalURL: "https://api.example", GatewayURL: "http://gateway.example:8000",
		JWTSecret: strings.Repeat("s", 32), AnonKey: "anon-key", ServiceRoleKey: "service-key",
		SecretKeyBase: strings.Repeat("b", 64), RealtimeDBEncKey: "0123456789abcdef", VaultEncKey: strings.Repeat("f", 32),
		RealtimeAdminSecret: strings.Repeat("a", 43),
		Upstreams:           map[string]string{"auth": "auth.example:9999", "rest": "rest.example:3000", "meta": "meta.example:8080", "analytics": "analytics.example:4000"},
		LogflarePublicToken: "ingest", LogflarePrivateToken: "private",
		Studio:  sbspec.Studio{MetaCryptoKey: "crypto", DashboardUsername: "admin", DashboardPassword: "dashboard"},
		Storage: sbspec.Storage{Endpoint: "https://s3.example", TLS: true, Bucket: "objects", Region: "us-east-1", PathStyle: true, AccessKey: "access", SecretKey: "secret", TenantID: "project"},
		Pooler:  sbspec.Pooler{TenantID: "project"},
	}
}
func TestRenderComponents(t *testing.T) {
	expected := map[string]string{"gateway": "", "auth": "GOTRUE_DB_DATABASE_URL=postgres://supabase_auth_admin:", "rest": "PGRST_ADMIN_SERVER_PORT=3001", "realtime": "DB_ENC_KEY=0123456789abcdef", "storage": "STORAGE_BACKEND=s3", "imgproxy": "IMGPROXY_BIND=:5001", "meta": "PG_META_DB_HOST=127.0.0.1", "studio": "HOSTNAME=127.0.0.1", "functions": "FUNCTIONS_ROOT=/data/karakoram/functions", "pooler": "POSTGRES_PORT=15432", "analytics": "LOGFLARE_SUPABASE_MODE=true", "vector": ""}
	for _, component := range sbspec.Components {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := validateService(s); err != nil {
				t.Fatal(err)
			}
			p, files, err := Render(s, "/data")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(p.Binary, s.InstallDir+"/") {
				t.Fatal("binary outside artifact")
			}
			if !strings.Contains(strings.Join(p.Env, "\n"), expected[component]) {
				t.Fatalf("missing expected environment for %s", component)
			}
			if !contains(p.Env, "HOME=/data/karakoram") || !contains(p.Env, "LANG=C.UTF-8") {
				t.Fatal("missing writable home or portable UTF-8 locale")
			}
			if files["supabase/environment.json"] == nil {
				t.Fatal("missing protected environment")
			}
			if component == "gateway" && !json.Valid(files["supabase/envoy.json"]) {
				t.Fatal("invalid Envoy JSON")
			}
			if component == "vector" && !json.Valid(files["supabase/vector.json"]) {
				t.Fatal("invalid Vector JSON")
			}
		})
	}
}

func TestBEAMDistributionIsPrivate(t *testing.T) {
	cookies := map[string]bool{}
	expectedCookies := map[string]string{
		"realtime":  "e43bf41360e81ac8ce09fb30f93251281e2a3c21d98addbdd3c60ea1ace59c8d",
		"pooler":    "2b3b0c1b1670902312b46b2d0d3b350212ef23510279efe4387ce5a92f33bf55",
		"analytics": "585dc198f3abfcb9763a46f4bdc54dd784fef3e831724d04788fb4ec7e13c34b",
	}
	cookieFromEnv := func(env []string) string {
		for _, entry := range env {
			if cookie, ok := strings.CutPrefix(entry, "RELEASE_COOKIE="); ok {
				return cookie
			}
		}
		return ""
	}
	for _, component := range []string{"realtime", "pooler", "analytics"} {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			p, files, err := Render(s, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var environment map[string]string
			if err := json.Unmarshal(files["supabase/environment.json"], &environment); err != nil {
				t.Fatal(err)
			}
			cookie := cookieFromEnv(p.Env)
			if len(cookie) != 64 || strings.Trim(cookie, "0123456789abcdef") != "" || cookies[cookie] {
				t.Fatal("distribution cookie must be secret-derived and component-specific")
			}
			cookies[cookie] = true
			if cookie != expectedCookies[component] {
				t.Fatal("distribution cookie does not match versioned HMAC-SHA256 derivation")
			}
			if _, present := environment["RELEASE_COOKIE"]; present || strings.Contains(string(files["supabase/environment.json"]), cookie) {
				t.Fatal("distribution cookie leaked into diagnostic environment dump")
			}
			if !contains(p.Env, "ERL_EPMD_ADDRESS=127.0.0.1") ||
				!strings.Contains(environment["ERL_AFLAGS"], "-kernel inet_dist_use_interface {127,0,0,1}") {
				t.Fatal("distribution or epmd is not bound to loopback")
			}
			if !strings.Contains(environment["ERL_AFLAGS"], "+S 2:2 +SDcpu 1 +SDio 1") {
				t.Fatal("BEAM scheduler limits lost")
			}
			projected, _, err := Render(s.ProjectSecrets(), t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if cookieFromEnv(projected.Env) != cookie {
				t.Fatal("cookie changed across rendering or secret projection")
			}
			s.DBRoles["supabase_admin"] = "another-cluster-password"
			other, _, err := Render(s, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if cookieFromEnv(other.Env) == cookie {
				t.Fatal("different cluster secret reused distribution cookie")
			}
			delete(s.DBRoles, "supabase_admin")
			if _, _, err := Render(s, t.TempDir()); err == nil {
				t.Fatal("missing secret material produced a guessable cookie")
			}
		})
	}
}

func TestComponentListenerAddressFamily(t *testing.T) {
	for _, component := range []string{"storage", "studio", "rest", "gateway", "vector"} {
		for _, ipv6 := range []bool{false, true} {
			s := testSpec(component)
			s.DatabaseIPv6 = ipv6
			p, files, err := Render(s, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			name, host := "HOST", "0.0.0.0"
			switch component {
			case "studio":
				name = "HOSTNAME"
				host = "127.0.0.1"
			case "rest":
				name = "PGRST_SERVER_HOST"
			}
			if ipv6 && component != "studio" {
				host = "::"
			}
			if component == "vector" {
				var config struct {
					API     struct{ Address string } `json:"api"`
					Sources struct {
						Logs struct{ Address string } `json:"member_logs"`
					} `json:"sources"`
				}
				if err := json.Unmarshal(files["supabase/vector.json"], &config); err != nil {
					t.Fatal(err)
				}
				address := "0.0.0.0:9002"
				if ipv6 {
					address = "[::]:9002"
				}
				if config.Sources.Logs.Address != address || config.API.Address != "127.0.0.1:9001" {
					t.Fatalf("Vector IPv6=%v ingestion or health listener has wrong family", ipv6)
				}
				continue
			}
			if component == "gateway" {
				if !strings.Contains(string(files["supabase/envoy.json"]), `"address": "`+host+`"`) {
					t.Fatalf("gateway IPv6=%v listener does not match backend address family", ipv6)
				}
			} else if !contains(p.Env, name+"="+host) {
				t.Fatalf("%s IPv6=%v listener does not match backend address family", component, ipv6)
			}
		}
	}
}
func TestCredentialURLEscaping(t *testing.T) {
	s := testSpec("functions")
	p, _, err := Render(s, "/data")
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range p.Env {
		if strings.HasPrefix(env, "SUPABASE_DB_URL=") {
			if !strings.Contains(env, "p%40ss%3A%2Fword@") {
				t.Fatal("password not URL escaped")
			}
			return
		}
	}
	t.Fatal("missing database URL")
}
func TestFunctionsSourcesAndJWT(t *testing.T) {
	s := testSpec("functions")
	s.VerifyJWT = true
	s.Functions = map[string]map[string]string{"hello": {"index.ts": "Deno.serve(() => new Response('hello'))"}}
	p, files, err := Render(s, "/data")
	if err != nil {
		t.Fatal(err)
	}
	if string(files["functions/hello/index.ts"]) == "" {
		t.Fatal("missing function")
	}
	var importMap struct {
		Imports map[string]string `json:"imports"`
	}
	if err := json.Unmarshal(files["functions/deno.jsonc"], &importMap); err != nil || importMap.Imports == nil {
		t.Fatal("missing shared function import map")
	}
	if !contains(p.Env, "VERIFY_JWT=true") {
		t.Fatal("JWT verification disabled")
	}
}
func TestInvalidServiceCredentials(t *testing.T) {
	s := testSpec("auth")
	delete(s.DBRoles, "supabase_auth_admin")
	if validateService(s) == nil {
		t.Fatal("missing service credential accepted")
	}
}

func TestProjectedSecretsPreserveComponentRendering(t *testing.T) {
	for _, component := range sbspec.Components {
		t.Run(component, func(t *testing.T) {
			full := testSpec(component)
			full.JWTKeys = "private-signing-keys"
			full.JWKS = `{"keys":[]}`
			full.PublishableKey, full.SecretKey = "sb_publishable_test", "sb_secret_test"
			full.AnonAsymmetricJWT, full.ServiceAsymmetricJWT = "anon-jwt", "service-jwt"
			full.Auth.SMTP = sbspec.SMTP{Host: "smtp.example", Password: "smtp-password"}
			full.Functions = map[string]map[string]string{"hello": {"index.ts": "Deno.serve(() => new Response('hello'))"}}
			full.VectorSinkURL, full.VectorSinkToken = "http://vector.example:9002", "vector-token"
			projected := full.ProjectSecrets()
			if err := projected.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := validateService(projected); err != nil {
				t.Fatal(err)
			}
			wantProcess, wantFiles, err := Render(full, "/data")
			if err != nil {
				t.Fatal(err)
			}
			gotProcess, gotFiles, err := Render(projected, "/data")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(wantProcess, gotProcess) || !reflect.DeepEqual(wantFiles, gotFiles) {
				t.Fatal("secret projection removed a credential consumed by component rendering")
			}
		})
	}
}

func TestRealtimeClusterStrategy(t *testing.T) {
	s := testSpec("realtime")
	process, files, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !contains(process.Env, "CLUSTER_STRATEGIES=") {
		t.Fatal("Realtime must explicitly disable DB-dependent discovery")
	}
	var environment map[string]string
	if err := json.Unmarshal(files["supabase/environment.json"], &environment); err != nil {
		t.Fatal(err)
	}
	if strategy, present := environment["CLUSTER_STRATEGIES"]; !present || strategy != "" {
		t.Fatalf("protected environment must preserve the explicit empty strategy: present=%v, strategy=%q", present, strategy)
	}
	if _, present := environment["DNS_NODES"]; present {
		t.Fatal("unused DNS discovery configuration retained")
	}
}

func TestRealtimeGenRPCIsPrivate(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		s := testSpec("realtime")
		s.DatabaseIPv6 = ipv6
		process, files, err := Render(s, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if !contains(process.Env, "GEN_RPC_SOCKET_IP=127.0.0.1") {
			t.Fatalf("IPv6=%v: Realtime's internal RPC listener must be bound to loopback", ipv6)
		}
		var environment map[string]string
		if err := json.Unmarshal(files["supabase/environment.json"], &environment); err != nil {
			t.Fatal(err)
		}
		if environment["GEN_RPC_SOCKET_IP"] != "127.0.0.1" {
			t.Fatal("non-secret internal RPC listener bind configuration missing from diagnostics")
		}
		for _, name := range []string{"GEN_RPC_TCP_SERVER_PORT", "GEN_RPC_SSL_SERVER_PORT"} {
			if _, present := environment[name]; present {
				t.Fatalf("unexpected override of native internal RPC port: %s", name)
			}
		}
	}
	for _, component := range []string{"pooler", "analytics"} {
		process, _, err := Render(testSpec(component), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range process.Env {
			if strings.HasPrefix(entry, "GEN_RPC_") {
				t.Fatalf("%s must not receive Realtime internal RPC settings", component)
			}
		}
	}
}
