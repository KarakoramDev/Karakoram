// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestAllPermissionScopes(t *testing.T) {
	// Each published Permission scope has its own typed model. Cluster access is
	// represented by action read_cluster without a nested cluster scope.
	input := `{"action":"read_data","backups":{"collection":"Article"},"data":{"collection":"Article","tenant":"one","object":"*"},"collections":{"collection":"Article"},"nodes":{"collection":"Article","verbosity":"verbose"},"roles":{"role":"reader","scope":"match"},"users":{"users":"alice"},"tenants":{"collection":"Article","tenant":"one"},"replicate":{"collection":"Article","shard":"shard-a"},"aliases":{"collection":"Article","alias":"articles"},"groups":{"group":"team","groupType":"oidc"},"namespaces":{"namespace":"default"}}`
	var permission Permission
	if err := json.Unmarshal([]byte(input), &permission); err != nil {
		t.Fatal(err)
	}
	if permission.Backups == nil || permission.Data == nil || permission.Collections == nil || permission.Nodes == nil || permission.Roles == nil || permission.Users == nil || permission.Tenants == nil || permission.Replicate == nil || permission.Aliases == nil || permission.Groups == nil || permission.Namespaces == nil {
		t.Fatal("missing typed permission scope")
	}
	output, err := json.Marshal(permission)
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err = json.Unmarshal(output, &got); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal([]byte(input), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scope round-trip: %s", output)
	}
}

func TestRequestEchoIsRedacted(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		w.WriteHeader(422)
		json.NewEncoder(w).Encode(map[string]any{"error": []map[string]string{{"message": "invalid request " + string(body)}}})
	})
	err := c.CreateRole(context.Background(), Role{Name: "private-role-name", Permissions: []Permission{{Action: "read_cluster"}}})
	if err == nil || strings.Contains(err.Error(), "private-role-name") {
		t.Fatalf("request echo not redacted: %v", err)
	}
}

func TestMalformedResponseDoesNotEchoBody(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"apikey":"secret-in-malformed-response"`)
	})
	_, err := c.CreateDBUser(context.Background(), "alice")
	if err == nil || strings.Contains(err.Error(), "secret-in-malformed-response") {
		t.Fatal("malformed JSON leaked response")
	}
}

func TestEscapedPathSegments(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/users/db/name%2Fwith%20space" {
			t.Errorf("unescaped segment: %s", r.URL.EscapedPath())
		}
		w.WriteHeader(204)
	})
	if err := c.DeleteDBUser(context.Background(), "name/with space"); err != nil {
		t.Fatal(err)
	}
}
