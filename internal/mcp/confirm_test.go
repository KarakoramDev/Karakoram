// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestConfirmationRejectsReplayBinding(t *testing.T) {
	key, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	audit := []bool{}
	gate := &Confirmer{Sign: key.MCPConfirmationHash, Now: func() time.Time { return now }, Audit: func(_ Binding, accepted bool) error { audit = append(audit, accepted); return nil }}
	executed := 0
	server, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executed++; _, _ = w.Write([]byte(`{}`)) }), gate)
	opts := &sdk.ClientOptions{MultiRoundTrip: &sdk.MultiRoundTripOptions{Disabled: true}, ElicitationHandler: func(context.Context, *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
		return &sdk.ElicitResult{Action: "accept"}, nil
	}}
	session := connectClient(t, server, "2026-07-28", opts)
	initial, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(initial.InputRequests) != 1 || initial.RequestState == "" {
		t.Fatalf("missing confirmation: %#v", initial)
	}
	for _, test := range []struct {
		name, cluster, state, action string
		advance                      time.Duration
	}{{"different arguments", "dev", initial.RequestState, "accept", 0}, {"missing state", "prod", "", "accept", 0}, {"decline", "prod", initial.RequestState, "decline", 0}, {"name mismatch", "prod", initial.RequestState, "accept", 0}, {"expired", "prod", initial.RequestState, "accept", 6 * time.Minute}} {
		t.Run(test.name, func(t *testing.T) {
			now = now.Add(test.advance)
			result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": test.cluster}, RequestState: test.state, InputResponses: sdk.InputResponseMap{"confirmation": &sdk.ElicitResult{Action: test.action, Content: map[string]any{"name": "not-prod"}}}})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError {
				t.Fatalf("replay accepted: %#v", result)
			}
			if executed != 0 {
				t.Fatal("rejected confirmation executed")
			}
		})
	}
	if len(audit) != 5 {
		t.Fatalf("missing decline audits: %v", audit)
	}
}

func TestConfirmationRejectsPrincipalAndTool(t *testing.T) {
	key, _ := secrets.New(strings.Repeat("x", 32))
	binding := Binding{Principal: "alice", Tool: "delete", Arguments: []byte(`{"cluster":"prod"}`)}
	// Verify the signed payload separates principals, tools and canonical args.
	original := string(confirmationPayload(binding, "123"))
	for _, change := range []Binding{{Principal: "bob", Tool: binding.Tool, Arguments: binding.Arguments}, {Principal: binding.Principal, Tool: "other", Arguments: binding.Arguments}, {Principal: binding.Principal, Tool: binding.Tool, Arguments: []byte(`{"cluster":"dev"}`)}} {
		if key.MCPConfirmationHash([]byte(original)) == key.MCPConfirmationHash(confirmationPayload(change, "123")) {
			t.Fatal("binding collision")
		}
	}
}
