// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAccessPreservesDocumentAndFieldRestrictions(t *testing.T) {
	const query = `{"term":{"tenant":"mine"}}`
	const roleJSON = `{"restricted":{"cluster":[],"indices":[{"names":["logs-*"],"privileges":["read"],"allow_restricted_indices":false,"query":"{\"term\":{\"tenant\":\"mine\"}}","field_security":{"grant":[],"except":["secret"]}}]}}`
	writes := 0
	p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			w.Write([]byte(roleJSON))
			return
		}
		writes++
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
			t.Error("missing restrictions", string(body["indices"]), err)
		} else if indices[0].Query != query || !reflect.DeepEqual(indices[0].Fields, map[string][]string{"grant": {}, "except": {"secret"}}) {
			t.Error("restrictions changed", indices[0])
		}
		w.Write([]byte(`{"id":"scoped","api_key":"secret","encoded":"encoded"}`))
	})
	roles, err := p.ListRoles(t.Context(), rc)
	if err != nil || len(roles) != 1 {
		t.Fatal(roles, err)
	}
	role, err := p.GetRole(t.Context(), rc, "restricted")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roles[0], *role) || role.Indices[0].Query == nil || *role.Indices[0].Query != query || role.Indices[0].FieldSecurity == nil || role.Indices[0].FieldSecurity.Grant == nil || len(*role.Indices[0].FieldSecurity.Grant) != 0 {
		t.Fatal("restriction lost on read", role)
	}
	role.Cluster = []string{"monitor"}
	if err := p.PutRole(t.Context(), rc, *role, false); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CreateAPIKey(t.Context(), rc, APIKeyCreate{Name: "scoped", RoleDescriptors: map[string]RoleDescriptor{"__proto__": role.Role}}); err != nil {
		t.Fatal(err)
	}
	if writes != 2 {
		t.Fatal("expected role update and scoped key creation", writes)
	}
}

func TestAccessRejectsUnscopedAPIKeyBeforeConnecting(t *testing.T) {
	for _, roles := range []map[string]RoleDescriptor{nil, {}} {
		// A nil context is intentional: validation must happen before client setup.
		if _, err := New(ProviderOptions{}).CreateAPIKey(t.Context(), nil, APIKeyCreate{Name: "unscoped", RoleDescriptors: roles}); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal("expected invalid unscoped key", err)
		}
	}
}
