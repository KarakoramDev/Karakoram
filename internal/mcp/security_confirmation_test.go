// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSecurityMutationsRequireElicitation(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-06-18"} {
		for _, operation := range []struct {
			id        string
			arguments map[string]any
		}{
			{"grantPostgresPrivileges", map[string]any{"cluster": "prod", "body": map[string]any{"role": "app", "database": "app", "privileges": []string{"CONNECT"}}}},
			{"replaceDatabaseUser", map[string]any{"cluster": "prod", "database": "app", "username": "app", "body": map[string]any{"roles": []any{}}}},
			{"putSupabaseFunction", map[string]any{"cluster": "prod", "name": "hook", "body": map[string]any{"index.ts": "Deno.serve(() => new Response('ok'));"}}},
		} {
			for _, action := range []string{"accept", "decline", "unsupported"} {
				t.Run(version+"/"+operation.id+"/"+action, func(t *testing.T) {
					var executed, elicited atomic.Int32
					sealer, err := secrets.New(strings.Repeat("x", 32))
					if err != nil {
						t.Fatal(err)
					}
					endpoint, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executed.Add(1); _, _ = w.Write([]byte(`{"ok":true}`)) }), &Confirmer{Sign: sealer.MCPConfirmationHash})
					var options *sdk.ClientOptions
					if action != "unsupported" {
						options = &sdk.ClientOptions{ElicitationHandler: func(context.Context, *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
							elicited.Add(1)
							return &sdk.ElicitResult{Action: action}, nil
						}}
					}
					session := connectClient(t, endpoint, version, options)
					result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent[operation.id].Tool, Arguments: operation.arguments})
					if err != nil {
						t.Fatal(err)
					}
					want := int32(0)
					if action == "accept" {
						want = 1
					}
					if executed.Load() != want || result.IsError != (want == 0) {
						t.Fatalf("execution bypassed confirmation: executed=%d result=%+v", executed.Load(), result)
					}
					if action != "unsupported" && elicited.Load() != 1 {
						t.Fatal("security operation did not request human confirmation")
					}
				})
			}
		}
	}
}
