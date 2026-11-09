// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestReconcileClusterTool(t *testing.T) {
	rule := api.OperationAgent["reconcileCluster"]
	if rule.Tool != "cluster_reconcile" || rule.Risk != "write" || rule.Async != "none" || !rule.Idempotent {
		t.Fatal(rule)
	}
	for _, mode := range []string{"readonly", "readwrite"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server, _ := testEndpoint(t, mode, "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/orders/reconcile" || r.URL.Query().Get("namespace") != "dev" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, `{"name":"orders","namespace":"dev","generation":2,"status":{"phase":"Ready"}}`)
			}), nil)
			session := connectClient(t, server, "2026-07-28", nil)
			list, err := session.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, tool := range list.Tools {
				if tool.Name == "cluster_reconcile" {
					found = true
					if tool.Annotations != nil && tool.Annotations.ReadOnlyHint {
						t.Fatal("mutating tool marked readonly")
					}
				}
			}
			if found != (mode == "readwrite") {
				t.Fatal("incorrect mode exposure", found)
			}
			result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "cluster_reconcile", Arguments: map[string]any{"cluster": "orders", "namespace": "dev"}})
			if mode == "readonly" {
				if err == nil || calls.Load() != 0 {
					t.Fatal("readonly mode allowed mutation", result, err)
				}
			} else if err != nil || result.IsError || calls.Load() != 1 {
				t.Fatal(result, err, calls.Load())
			}
		})
	}
}
