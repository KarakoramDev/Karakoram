// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestNestedConfirmationNamesAndNamespace(t *testing.T) {
	for _, test := range []struct {
		namespace, typed string
		executes         bool
	}{{"team-a", "snapshot", true}, {"team-b", "snapshot", true}, {"team-a", "prod", false}} {
		t.Run(test.namespace+"/"+test.typed, func(t *testing.T) {
			key, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			executions := 0
			server, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions++
				if r.URL.Path != "/api/v1/clusters/prod/backups/snapshot" || r.URL.Query().Get("namespace") != test.namespace {
					t.Errorf("wrong target %s", r.URL)
				}
				_, _ = w.Write([]byte(`{}`))
			}), &Confirmer{Sign: key.MCPConfirmationHash})
			session := connectClient(t, server, "2026-07-28", &sdk.ClientOptions{ElicitationHandler: func(_ context.Context, req *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
				if !strings.Contains(req.Params.Message, "/clusters/prod/backups/snapshot") || !strings.Contains(req.Params.Message, "namespace "+test.namespace) {
					t.Errorf("imprecise confirmation: %s", req.Params.Message)
				}
				schema, _ := json.Marshal(req.Params.RequestedSchema)
				if !strings.Contains(string(schema), "snapshot") {
					t.Errorf("typed target missing: %s", schema)
				}
				return &sdk.ElicitResult{Action: "accept", Content: map[string]any{"name": test.typed}}, nil
			}})
			result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["deleteBackup"].Tool, Arguments: map[string]any{"namespace": test.namespace, "cluster": "prod", "backup": "snapshot"}})
			if err != nil {
				t.Fatal(err)
			}
			if result.IsError == test.executes {
				t.Fatalf("unexpected result: %#v", result)
			}
			expected := 0
			if test.executes {
				expected = 1
			}
			if executions != expected {
				t.Fatalf("executed %d, expected %d", executions, expected)
			}
		})
	}
}

func TestIdempotencyKeysSeparatePrincipals(t *testing.T) {
	key, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	h, err := New(Options{Mode: "readwrite", SignIdempotency: key.MCPIdempotencyHash, REST: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		_, _ = w.Write([]byte(`{}`))
	})})
	if err != nil {
		t.Fatal(err)
	}
	var op operation
	for _, candidate := range h.operations {
		if candidate.ID == "createCluster" {
			op = candidate
			break
		}
	}
	args := map[string]any{"body": map[string]any{"name": "prod"}}
	canonical, _ := json.Marshal(args)
	for _, principal := range []string{"alice\x00token-a", "bob\x00token-b", "alice\x00token-a"} {
		result := h.execute(context.Background(), "Bearer ignored", principal, op, args, canonical)
		if result.IsError {
			t.Fatalf("execute failed: %#v", result)
		}
	}
	if keys[0] == "" || keys[0] != keys[2] || keys[0] == keys[1] {
		t.Fatalf("principal key isolation failed: %v", keys)
	}
}

func TestConfirmationUsesPathOrder(t *testing.T) {
	op := operation{Path: "/clusters/{cluster}/database-users/{database}/{username}", Parameters: []parameter{{Name: "username", In: "path"}, {Name: "cluster", In: "path"}, {Name: "database", In: "path"}}}
	name, description := confirmationTarget(op, map[string]any{"cluster": "prod", "database": "app", "username": "alice", "namespace": "tenant"})
	if name != "alice" || description != "/clusters/prod/database-users/app/alice (namespace tenant)" {
		t.Fatalf("wrong target %q %q", name, description)
	}
}
