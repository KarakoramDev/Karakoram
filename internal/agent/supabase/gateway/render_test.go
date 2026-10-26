// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

var update = flag.Bool("update-golden", false, "update renderer golden fingerprints")

func fixture() Input {
	return Input{PublicURL: "https://api.example.test", AnonKey: "legacy-anon", ServiceRoleKey: "legacy-service", DashboardUsername: "dashboard", DashboardPassword: "dashboard-password", GraphQL: true, OpaqueKeys: map[string]OpaqueKey{"sb_publishable_test": {JWT: "internal-anon"}, "sb_secret_test": {JWT: "internal-service", Admin: true}}}
}
func TestRenderGolden(t *testing.T) {
	sets := map[string][]string{"disabled": {}, "minimal": {"auth", "rest"}, "full": {"auth", "rest", "realtime", "storage", "functions", "meta", "studio"}}
	for _, name := range []string{"auth", "rest", "realtime", "storage", "functions", "meta", "studio"} {
		sets[name] = []string{name}
	}
	for name, components := range sets {
		t.Run(name, func(t *testing.T) {
			in := fixture()
			in.Upstreams = map[string]string{}
			for _, c := range components {
				in.Upstreams[c] = c + ".internal:8080"
			}
			if in.Upstreams["studio"] == "" {
				in.DashboardUsername, in.DashboardPassword = "", ""
			}
			got, err := Render(in)
			if err != nil {
				t.Fatal(err)
			}
			again, err := Render(in)
			if err != nil || !bytes.Equal(got, again) {
				t.Fatal("nondeterministic renderer")
			}
			var config object
			if err := json.Unmarshal(got, &config); err != nil {
				t.Fatal(err)
			}
			admin := config["admin"].(map[string]any)["address"].(map[string]any)["socket_address"].(map[string]any)
			if admin["address"] != "127.0.0.1" || admin["port_value"] != float64(AdminPort) {
				t.Fatal("admin must remain on IPv4 loopback at the agent's observation port")
			}
			if bytes.Contains(got, []byte("access_log")) {
				t.Fatal("credential logging risk")
			}
			if in.Upstreams["studio"] == "" && bytes.Contains(got, []byte("envoy.filters.http.basic_auth")) {
				t.Fatal("disabled Studio must not install a basic-auth filter")
			}
			sum := sha256.Sum256(got)
			fingerprint := []byte(hex.EncodeToString(sum[:]) + "\n")
			path := filepath.Join("testdata", name+".golden")
			if *update {
				if err := os.MkdirAll("testdata", 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, fingerprint, 0644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want, fingerprint) {
				t.Fatal("bootstrap golden mismatch; inspect renderer change before updating golden")
			}
		})
	}
}

func TestDiscoveryRefreshBounds(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{"auth": "auth.internal:9999", "studio": "studio.internal:3002"}
	raw, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	clusters := config["static_resources"].(map[string]any)["clusters"].([]any)
	for _, entry := range clusters {
		cluster := entry.(map[string]any)
		backoff := cluster["dns_failure_refresh_rate"].(map[string]any)
		if cluster["type"] != "STRICT_DNS" || cluster["dns_refresh_rate"] != "1s" || cluster["respect_dns_ttl"] != false || backoff["base_interval"] != "0.250s" || backoff["max_interval"] != "1s" {
			t.Fatalf("unbounded discovery convergence: %v", cluster)
		}
		if _, exists := cluster["health_checks"]; exists {
			t.Fatal("discovery readiness must not introduce a bootstrap health dependency")
		}
	}
}

func TestRolloutTransportFailurePolicy(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{}
	for _, name := range []string{"auth", "rest", "realtime", "storage", "functions", "meta", "studio"} {
		in.Upstreams[name] = name + ".internal:8080"
	}
	raw, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	resources := config["static_resources"].(map[string]any)
	wantOutliers := map[string]any{
		"split_external_local_origin_errors": true,
		"consecutive_local_origin_failure":   float64(1), "enforcing_consecutive_local_origin_failure": float64(100),
		"consecutive_5xx": float64(0), "enforcing_consecutive_5xx": float64(0),
		"consecutive_gateway_failure": float64(0), "enforcing_consecutive_gateway_failure": float64(0),
		"enforcing_success_rate": float64(0), "enforcing_local_origin_success_rate": float64(0),
		"interval": "1s", "base_ejection_time": "5s", "max_ejection_time": "30s",
		"max_ejection_percent": float64(50), "always_eject_one_host": false,
	}
	clusters := resources["clusters"].([]any)
	if len(clusters) != len(in.Upstreams) {
		t.Fatal("missing upstream clusters")
	}
	for _, entry := range clusters {
		cluster := entry.(map[string]any)
		if !reflect.DeepEqual(cluster["outlier_detection"], wantOutliers) {
			t.Fatalf("%s: unexpected outlier detection: %v", cluster["name"], cluster["outlier_detection"])
		}
	}
	hcm := resources["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	if !reflect.DeepEqual(hcm["upgrade_configs"], []any{map[string]any{"upgrade_type": "websocket"}}) {
		t.Fatal("websocket upgrades must remain enabled")
	}
	wantRetry := map[string]any{
		"retry_on": "connect-failure,refused-stream,reset-before-request", "num_retries": float64(2),
		"retry_host_predicate": []any{map[string]any{
			"name":         "envoy.retry_host_predicates.previous_hosts",
			"typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate"},
		}},
		"host_selection_retry_max_attempts": float64(3),
	}
	routes := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)["routes"].([]any)
	seen := map[string]bool{}
	websockets := 0
	for _, entry := range routes {
		route := entry.(map[string]any)
		action, ok := route["route"].(map[string]any)
		if !ok {
			if _, exists := route["retry_policy"]; exists {
				t.Fatal("direct responses must not retry")
			}
			continue
		}
		if !reflect.DeepEqual(action["retry_policy"], wantRetry) {
			t.Fatalf("%s: unsafe retry policy: %v", route["name"], action["retry_policy"])
		}
		component := action["cluster"].(string)
		seen[component] = true
		timeout, idle := "30s", any(nil)
		if component == "functions" {
			timeout, idle = "410s", "160s"
		}
		if component == "storage" || route["name"] == "realtime-ws" {
			timeout, idle = "0s", "0s"
		}
		if action["timeout"] != timeout || action["idle_timeout"] != idle {
			t.Fatalf("%s: streaming/route timeouts changed: %v", route["name"], action)
		}
		if route["name"] == "realtime-ws" {
			websockets++
			if action["host_rewrite_literal"] != "realtime-dev.supabase-realtime" {
				t.Fatal("websocket tenant rewrite changed")
			}
		}
	}
	if len(seen) != len(in.Upstreams) || websockets != 2 {
		t.Fatal("missing upstream or websocket/longpoll routes")
	}
}

func TestIPv6WildcardListenerRetainsIPv4(t *testing.T) {
	for _, address := range []string{"0.0.0.0", "::", "::1"} {
		in := fixture()
		in.ListenAddress = address
		raw, err := Render(in)
		if err != nil {
			t.Fatal(err)
		}
		var config struct {
			Resources struct {
				Listeners []struct {
					Address struct {
						Socket struct {
							Address    string `json:"address"`
							IPv4Compat bool   `json:"ipv4_compat"`
						} `json:"socket_address"`
					} `json:"address"`
				} `json:"listeners"`
			} `json:"static_resources"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			t.Fatal(err)
		}
		listener := config.Resources.Listeners[0].Address.Socket
		if listener.Address != address || listener.IPv4Compat != (address == "::") {
			t.Fatalf("unexpected listener family for %s", address)
		}
	}
}
func TestInvalidInput(t *testing.T) {
	cases := []func(*Input){func(i *Input) { i.ServiceRoleKey = i.AnonKey }, func(i *Input) { i.PublicURL = "https://user:pass@example.test" }, func(i *Input) { i.Upstreams = map[string]string{"other": "host:80"} }, func(i *Input) { i.Upstreams = map[string]string{"auth": "host:0"} }, func(i *Input) { i.Upstreams = map[string]string{"auth": "unbracketed:ipv6:80"} }, func(i *Input) { i.ListenAddress = "hostname" }, func(i *Input) { i.CORS = CORS{Origins: []string{"*"}, AllowCredentials: true} }, func(i *Input) { i.RealtimeHost = "invalid\nhost" }, func(i *Input) { i.Upstreams = map[string]string{"studio": "host:80"}; i.DashboardPassword = "" }, func(i *Input) { i.OpaqueKeys = map[string]OpaqueKey{"invalid": {JWT: "jwt"}} }}
	for n, change := range cases {
		i := fixture()
		change(&i)
		if _, err := Render(i); err == nil {
			t.Errorf("case %d accepted", n)
		}
	}
}
func TestLuaQuoting(t *testing.T) {
	if got := luaQuote("a\"\\\n\x00"); got != `"a\034\092\010\000"` {
		t.Fatalf("unsafe Lua quoting %q", got)
	}
}

func TestCORSDefaultsAndAllowlistBeforeAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name    string
		origins []string
	}{
		{"default", nil},
		{"allowlist", []string{"https://app.example.test", "http://localhost:3000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := fixture()
			in.Upstreams = map[string]string{"rest": "rest.example.test:3000", "studio": "studio.example.test:3000"}
			in.CORS.Origins = tc.origins
			data, err := Render(in)
			if err != nil {
				t.Fatal(err)
			}
			var config object
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			hcm := config["static_resources"].(map[string]any)["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
			filters := hcm["http_filters"].([]any)
			if filters[0].(map[string]any)["name"] != "envoy.filters.http.cors" {
				t.Fatal("preflight must precede basic auth, API key authorization and RBAC")
			}
			host := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)
			policy := host["typed_per_filter_config"].(map[string]any)["envoy.filters.http.cors"].(map[string]any)
			if policy["allow_credentials"] != false {
				t.Fatal("browser cookie credentials must not be enabled")
			}
			matches := policy["allow_origin_string_match"].([]any)
			if tc.origins == nil {
				if len(matches) != 1 || matches[0].(map[string]any)["safe_regex"].(map[string]any)["regex"] != ".*" {
					t.Fatal("default must allow browser apps on any origin")
				}
			} else {
				if len(matches) != len(tc.origins) {
					t.Fatal("allowlist was changed")
				}
				for i, origin := range tc.origins {
					if matches[i].(map[string]any)["exact"] != origin {
						t.Fatal("allowlist must match exact origins")
					}
				}
			}
		})
	}
	for _, origin := range []string{"null", "https://user:pass@example.test", "https://app.example.test/path", "https://app.example.test?x=1"} {
		in := fixture()
		in.CORS.Origins = []string{origin}
		if _, err := Render(in); err == nil {
			t.Fatal("invalid CORS origin accepted", origin)
		}
	}
}

func TestGraphQLProfilesForGETAndPOST(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{"rest": "rest.example.test:3000"}
	data, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	hcm := config["static_resources"].(map[string]any)["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	routes := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)["routes"].([]any)
	for _, item := range routes {
		route := item.(map[string]any)
		if route["name"] != "graphql" {
			continue
		}
		headers := map[string]string{}
		for _, item := range route["request_headers_to_add"].([]any) {
			entry := item.(map[string]any)
			header := entry["header"].(map[string]any)
			if entry["append_action"] != "OVERWRITE_IF_EXISTS_OR_ADD" {
				t.Fatal("client profiles must not override the GraphQL schema")
			}
			headers[header["key"].(string)] = header["value"].(string)
		}
		if headers["content-profile"] != "graphql_public" || headers["accept-profile"] != "graphql_public" {
			t.Fatal("GraphQL POST and GET require their respective profile headers")
		}
		return
	}
	t.Fatal("missing GraphQL route")
}

func TestRealtimeCanonicalAllowlist(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{"realtime": "realtime.example.test:4000", "storage": "storage.example.test:5000"}
	raw, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	hcm := config["static_resources"].(map[string]any)["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	if hcm["normalize_path"] != false || hcm["merge_slashes"] != false || hcm["path_with_escaped_slashes_action"] != "KEEP_UNCHANGED" {
		t.Fatal("Realtime hardening must not normalize Storage SigV4 paths")
	}
	routes := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)["routes"].([]any)
	selectRoute := func(path string) map[string]any {
		t.Helper()
		path, _, _ = strings.Cut(path, "?")
		for _, item := range routes {
			route := item.(map[string]any)
			match := route["match"].(map[string]any)
			matches := false
			if prefix, ok := match["prefix"].(string); ok {
				matches = strings.HasPrefix(path, prefix)
			} else if exact, ok := match["path"].(string); ok {
				matches = path == exact
			} else if regex, ok := match["safe_regex"].(map[string]any); ok {
				matches = regexp.MustCompile("^(?:" + regex["regex"].(string) + ")$").MatchString(path)
			}
			if matches {
				return route
			}
		}
		t.Fatal("missing route", path)
		return nil
	}
	for _, path := range []string{"/realtime/v1/websocket", "/realtime/v1/longpoll", "/realtime/v1/api/ping", "/realtime/v1/healthcheck", "/realtime/v1/api/broadcast", "/realtime/v1/api/broadcast/topic/events/event", "/realtime/v1/api/broadcast/topic%20name/events/event?x=//"} {
		if route := selectRoute(path); route["name"] == "deny" {
			t.Error("tenant endpoint denied", path)
		}
	}
	for _, path := range []string{"/realtime/v1/api/broadcast/topic/events/event", "/realtime/v1/api/broadcast/topic%20name/events/event?private=false"} {
		route := selectRoute(path)["route"].(map[string]any)
		rewrite := route["regex_rewrite"].(map[string]any)
		pattern := regexp.MustCompile(rewrite["pattern"].(map[string]any)["regex"].(string))
		replacement := strings.ReplaceAll(rewrite["substitution"].(string), `\1`, "${1}")
		if got, want := pattern.ReplaceAllString(path, replacement), strings.TrimPrefix(path, "/realtime/v1"); got != want {
			t.Errorf("broadcast rewrite produced %q, want origin-form URI %q", got, want)
		}
	}
	blocked := []string{"/realtime/v1/api/tenants", "/realtime/v1/api/tenants/realtime-dev", "/realtime/v1/api/openapi", "/realtime/v1/api/unknown", "/realtime/v1/api/broadcast/../tenants", "/realtime/v1/api/broadcast/./events/event", "/realtime/v1/api/broadcast/%2e/events/event", "/realtime/v1/api/broadcast/%2E%2e/events/event", "/realtime/v1/api/broadcast/topic%2fadmin/events/event", "/realtime/v1/api/broadcast/topic%2Fadmin/events/event", "/realtime/v1/api/broadcast/topic%5cadmin/events/event", "/realtime/v1/api/broadcast/topic%252fadmin/events/event", "/realtime/v1/websocket/extra"}
	for _, path := range []string{"/realtime/v1/api/tenants/realtime-dev", "/realtime/v1/api/broadcast/topic/events/event"} {
		for i, c := range path {
			if c == '/' && i > 0 {
				blocked = append(blocked, path[:i]+"/"+path[i:])
			}
		}
	}
	for _, path := range blocked {
		route := selectRoute(path)
		if route["name"] != "deny" || route["direct_response"].(map[string]any)["status"] != float64(403) {
			t.Error("noncanonical or administrative route admitted", path)
		}
	}
	if selectRoute("/storage/v1/s3/a%2Fb//c/../d?X-Amz-Signature=test")["name"] != "storage" {
		t.Fatal("Storage raw path was affected")
	}
}

func TestStudioInjectsCredentialOnlyBehindBasicAuth(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{"studio": "studio.example.test:3002"}
	raw, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	hcm := config["static_resources"].(map[string]any)["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	routes := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)["routes"].([]any)
	studio := routes[len(routes)-1].(map[string]any)
	if _, disabled := studio["typed_per_filter_config"].(map[string]any)["envoy.filters.http.basic_auth"]; disabled {
		t.Fatal("Studio transport credential is available without dashboard authentication")
	}
	found := false
	for _, entry := range studio["request_headers_to_add"].([]any) {
		header := entry.(map[string]any)
		value := header["header"].(map[string]any)
		if value["key"] == "apikey" {
			found = value["value"] == in.ServiceRoleKey && header["append_action"] == "OVERWRITE_IF_EXISTS_OR_ADD"
		}
	}
	if !found {
		t.Fatal("Studio requires an overwritten service credential on the private hop")
	}
}
