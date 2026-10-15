// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestSecurityRestrictionsSurviveRoleAndAPIKeyRequests(t *testing.T) {
	for _, query := range []string{`"{\"term\":{\"tenant\":\"mine\"}}"`, `{"term":{"tenant":"mine"}}`} {
		t.Run(query, func(t *testing.T) {
			const wantQuery = `{"term":{"tenant":"mine"}}`
			calls := 0
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "GET" {
					w.Write([]byte(`{"restricted":{"indices":[{"names":["logs-*"],"privileges":["read"],"query":` + query + `,"field_security":{"grant":[],"except":["secret"]}}]}}`))
					return
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if r.URL.Path == "/_security/api_key" {
					var descriptors map[string]map[string]json.RawMessage
					if err := json.Unmarshal(body["role_descriptors"], &descriptors); err != nil {
						t.Error(err)
					}
					body = descriptors["__proto__"]
				}
				var indices []struct {
					Query  string              `json:"query"`
					Fields map[string][]string `json:"field_security"`
				}
				if err := json.Unmarshal(body["indices"], &indices); err != nil || len(indices) != 1 {
					t.Error("missing index restrictions", string(body["indices"]), err)
				} else if indices[0].Query != wantQuery || !reflect.DeepEqual(indices[0].Fields, map[string][]string{"grant": {}, "except": {"secret"}}) {
					t.Error("weakened restrictions", indices[0])
				}
				w.Write([]byte(`{"id":"scoped","api_key":"secret","encoded":"encoded"}`))
			})
			roles, err := c.GetRoles(t.Context(), "restricted")
			if err != nil {
				t.Fatal(err)
			}
			role := roles["restricted"]
			if err := c.PutRole(t.Context(), "restricted", role); err != nil {
				t.Fatal(err)
			}
			if _, err := c.CreateAPIKey(t.Context(), APIKeyRequest{Name: "scoped", RoleDescriptors: map[string]Role{"__proto__": role}}); err != nil {
				t.Fatal(err)
			}
			if calls != 3 {
				t.Fatal("expected role read, role update and key creation", calls)
			}
		})
	}
}

func TestCreateAPIKeyRejectsMissingOrEmptyDescriptors(t *testing.T) {
	for _, roles := range []map[string]Role{nil, {}} {
		// No client configuration: rejection must precede any HTTP request.
		if _, err := (&Client{}).CreateAPIKey(t.Context(), APIKeyRequest{Name: "unscoped", RoleDescriptors: roles}); err == nil {
			t.Fatal("unscoped key accepted")
		}
	}
}
