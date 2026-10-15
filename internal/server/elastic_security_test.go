// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

func TestElasticSecurityRestrictionsRoundTrip(t *testing.T) {
	s, f, token := elasticTestServer(t)
	const body = `{"name":"restricted","cluster":[],"indices":[{"names":["logs-*"],"privileges":["read"],"allowRestrictedIndices":false,"query":"{\"term\":{\"tenant\":\"mine\"}}","fieldSecurity":{"grant":[],"except":["secret"]}}],"applications":[]}`
	var want api.ElasticRole
	if err := json.Unmarshal([]byte(body), &want); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{{"POST", "roles", 201}, {"PUT", "roles/restricted", 200}} {
		w := call(s, tc.method, "/api/v1/clusters/prod/elasticsearch/"+tc.path, body, token)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		var got api.ElasticRole
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || !reflect.DeepEqual(got.Indices, want.Indices) {
			t.Fatal("restriction lost in response", got, err)
		}
		if !reflect.DeepEqual(elasticRoleAPI(f.role).Indices, want.Indices) {
			t.Fatal("restriction lost at provider boundary", f.role)
		}
	}
	const keyBody = `{"name":"scoped","roleDescriptors":{"__proto__":{"cluster":[],"indices":[{"names":["logs-*"],"privileges":["read"],"allowRestrictedIndices":false,"query":"{\"term\":{\"tenant\":\"mine\"}}","fieldSecurity":{"grant":[],"except":["secret"]}}],"applications":[]}}}`
	w := call(s, "POST", "/api/v1/clusters/prod/elasticsearch/api-keys", keyBody, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	role, exists := f.key.RoleDescriptors["__proto__"]
	if !exists || !reflect.DeepEqual(role.Indices, f.role.Indices) {
		t.Fatal("restriction lost in key descriptor", f.key)
	}
}

func TestElasticAPIKeyRejectsMissingOrEmptyDescriptors(t *testing.T) {
	s, f, token := elasticTestServer(t)
	for _, body := range []string{`{"name":"unscoped"}`, `{"name":"unscoped","roleDescriptors":{}}`, `{"name":"unscoped","roleDescriptors":null}`} {
		w := call(s, "POST", "/api/v1/clusters/prod/elasticsearch/api-keys", body, token)
		var failure api.Error
		if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if w.Code != 400 || failure.Code != "invalid_request" {
			t.Fatal("expected invalid request", w.Code, w.Body.String())
		}
		if f.key.Name != "" {
			t.Fatal("unscoped key reached provider", f.key)
		}
	}
}

func TestElasticResponseArraysAreNeverNull(t *testing.T) {
	user := elasticUserAPI(elastic.User{Username: "app", Enabled: true})
	role := elasticRoleAPI(elastic.AccessRole{Name: "custom", Role: elastic.Role{
		Indices:      []elastic.IndexPrivileges{{}},
		Applications: []elastic.ApplicationPrivileges{{Application: "app"}},
	}})
	if user.Roles == nil || role.Cluster == nil || role.Indices == nil || role.Applications == nil ||
		role.Indices[0].Names == nil || role.Indices[0].Privileges == nil ||
		role.Applications[0].Privileges == nil || role.Applications[0].Resources == nil {
		t.Fatal("required response arrays must serialize as [], not null", user, role)
	}
}

func TestElasticRequestsRejectUnknownNestedFields(t *testing.T) {
	s, f, token := elasticTestServer(t)
	for _, body := range []string{
		`{"name":"custom","cluster":[],"indices":[],"applications":[],"unexpected":true}`,
		`{"name":"custom","cluster":[],"indices":[{"names":[],"privileges":[],"allowRestrictedIndices":false,"unexpected":true}],"applications":[]}`,
		`{"name":"custom","cluster":[],"indices":[{"names":[],"privileges":[],"allowRestrictedIndices":false,"fieldSecurity":{"unexpected":true}}],"applications":[]}`,
	} {
		w := call(s, "POST", "/api/v1/clusters/prod/elasticsearch/roles", body, token)
		var failure api.Error
		if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if w.Code != 400 || failure.Details == nil || (*failure.Details)["reason"] != "unknown_field" || f.createRole {
			t.Fatal("unknown request field reached provider", w.Code, w.Body.String())
		}
	}
}

func TestExtensionResourceFloorRoundTrip(t *testing.T) {
	s, _, token := elasticTestServer(t)
	body := `{"name":"copy","spec":{"provider":"fake","components":{"extra":{"apm":{"replicas":1,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":0.25,"maxRam":0.25,"minDisk":0,"maxDisk":0}}}}}}`
	w := call(s, "POST", "/api/v1/clusters", body, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var cluster api.Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &cluster); err != nil {
		t.Fatal(err)
	}
	component := (*cluster.Spec.Components.Extra)["apm"]
	if component.Resources.MinRam != 0.25 || component.Resources.MinDisk != 0 {
		t.Fatal("extension resource floor was lost", component)
	}
}
