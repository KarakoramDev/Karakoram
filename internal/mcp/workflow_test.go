// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestWaitWorkflowAndCancellation(t *testing.T) {
	server, _ := testEndpoint(t, "readonly", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/operations/op-123" {
			t.Errorf("wrong wait target %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"op-123","phase":"Succeeded"}`))
	}), nil)
	session := connectClient(t, server, "2026-07-28", nil)
	result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "wait", Arguments: map[string]any{"operation": "op-123"}})
	if err != nil || result.IsError {
		t.Fatalf("wait: %#v %v", result, err)
	}
	for _, arguments := range []map[string]any{{"operation": "op-123", "timeout": 601}, {"operation": "op-123", "backup": "b"}, {"backup": "b"}} {
		result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "wait", Arguments: arguments})
		if err != nil || !result.IsError {
			t.Fatalf("invalid wait: %#v %v", result, err)
		}
	}
}
func TestSchemaReferencesAndPatchBody(t *testing.T) {
	operations, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range operations {
		raw, err := json.Marshal(o.Tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "#/components/") {
			t.Fatalf("unresolved reference in %s", o.ID)
		}
		if o.ID == "patchCluster" {
			schema := o.Tool.InputSchema.(map[string]any)
			props := schema["properties"].(map[string]any)
			if props["body"] == nil || o.ContentType != "application/merge-patch+json" {
				t.Fatal("merge patch body missing")
			}
		}
	}
}
