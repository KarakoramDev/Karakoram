// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package gateway

import (
	"encoding/json"
	"strings"
	"testing"
)

// Evaluate the rendered route's actual RBAC principals, rather than duplicating
// its configured key lists. Lua key translation is covered by native conformance.
func TestRESTAndAdminRouteKeyPolicy(t *testing.T) {
	in := fixture()
	in.Upstreams = map[string]string{"rest": "rest.example.test:3001", "meta": "meta.example.test:8081", "studio": "studio.example.test:3002"}
	raw, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	var config object
	if err = json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	hcm := config["static_resources"].(map[string]any)["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	routes := hcm["route_config"].(map[string]any)["virtual_hosts"].([]any)[0].(map[string]any)["routes"].([]any)
	for _, path := range []string{"/rest/v1/", "/rest/v1/?apikey=sb_secret_test", "/rest/v1/messages", "/rest/v1/rpc/list_messages", "/pg/tables", "/"} {
		t.Run(path, func(t *testing.T) {
			target, _, _ := strings.Cut(path, "?")
			var route map[string]any
			for _, item := range routes {
				candidate := item.(map[string]any)
				match := candidate["match"].(map[string]any)
				exact, hasExact := match["path"].(string)
				prefix, hasPrefix := match["prefix"].(string)
				if hasExact && target == exact || hasPrefix && strings.HasPrefix(target, prefix) {
					route = candidate
					break
				}
			}
			if route == nil {
				t.Fatal("missing route")
			}
			match := route["match"].(map[string]any)
			if len(match) != 1 {
				t.Fatal("unexpected method/header restriction on API route")
			}
			filters := route["typed_per_filter_config"].(map[string]any)
			if target == "/" {
				if _, disabled := filters["envoy.filters.http.basic_auth"]; disabled {
					t.Fatal("API keys must not bypass Studio dashboard authentication")
				}
				return
			}
			rules := filters["envoy.filters.http.rbac"].(map[string]any)["rbac"].(map[string]any)["rules"].(map[string]any)
			if rules["action"] != "ALLOW" {
				t.Fatal("unexpected RBAC action")
			}
			principals := rules["policies"].(map[string]any)["configured_keys"].(map[string]any)["principals"].([]any)
			admin := target == "/rest/v1/" || strings.HasPrefix(target, "/pg/")
			for _, key := range []string{"", "invalid", in.AnonKey, in.ServiceRoleKey, "sb_publishable_test", "sb_secret_test"} {
				allowed := false
				for _, p := range principals {
					header := p.(map[string]any)["header"].(map[string]any)
					if header["name"] != "apikey" {
						t.Fatal("authorization must use the configured API key")
					}
					allowed = allowed || header["string_match"].(map[string]any)["exact"] == key
				}
				want := key == in.ServiceRoleKey || key == "sb_secret_test" || !admin && (key == in.AnonKey || key == "sb_publishable_test")
				if allowed != want {
					t.Errorf("key class admitted=%t want=%t (admin=%t)", allowed, want, admin)
				}
			}
		})
	}
}
