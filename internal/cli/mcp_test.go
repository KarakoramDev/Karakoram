// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Delay the cancelled call's final error response until after the peer closes.
// This forces the shutdown ordering that otherwise depends on host scheduling.
type mcpDelayedErrorTransport struct {
	sdk.Transport
	ready   chan struct{}
	release chan struct{}
}

func (t *mcpDelayedErrorTransport) Connect(ctx context.Context) (sdk.Connection, error) {
	connection, err := t.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &mcpDelayedErrorConnection{Connection: connection, ready: t.ready, release: t.release}, nil
}

type mcpDelayedErrorConnection struct {
	sdk.Connection
	ready   chan struct{}
	release chan struct{}
}

func (c *mcpDelayedErrorConnection) Write(ctx context.Context, message jsonrpc.Message) error {
	if response, ok := message.(*jsonrpc.Response); ok && response.Error != nil {
		close(c.ready)
		<-c.release
	}
	return c.Connection.Write(ctx, message)
}

func TestMCPBridge(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-11-25"} {
		t.Run(version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			upstream := sdk.NewServer(&sdk.Implementation{Name: "test", Version: "1"}, &sdk.ServerOptions{Instructions: "upstream instructions"})
			var calls atomic.Int32
			upstream.AddTool(&sdk.Tool{Name: "confirm", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
				calls.Add(1)
				if req.Params.RequestState == "" {
					return &sdk.CallToolResult{RequestState: "opaque-state", InputRequests: sdk.InputRequestMap{"approval": &sdk.ElicitParams{Mode: "form", Message: "Approve exact action?", RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{}}}}}, nil
				}
				approval, ok := req.Params.InputResponses["approval"].(*sdk.ElicitResult)
				if !ok || approval.Action != "accept" || req.Params.RequestState != "opaque-state" {
					return nil, fmt.Errorf("approval not forwarded")
				}
				if err := req.Session.NotifyProgress(ctx, &sdk.ProgressNotificationParams{ProgressToken: req.Params.GetProgressToken(), Progress: 1, Total: 1}); err != nil {
					return nil, err
				}
				return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(req.Params.Arguments)}}}, nil
			})
			started, cancelled := make(chan struct{}), make(chan struct{})
			upstream.AddTool(&sdk.Tool{Name: "block", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, _ *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
				close(started)
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			})
			legacy := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return upstream }, nil)
			modern := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return upstream }, &sdk.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true})
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer saved-secret" || r.Header.Get("Origin") != "" || r.URL.Path != "/base/mcp" || r.URL.Query().Get("toolsets") != "Clusters,Workflows" {
					t.Errorf("incorrect upstream request: %s %s %v", r.Method, r.URL, r.Header)
				}
				// Match the control plane's separate HTTP protocol eras.
				if r.Header.Get("MCP-Protocol-Version") >= "2026-07-28" {
					modern.ServeHTTP(w, r)
				} else {
					legacy.ServeHTTP(w, r)
				}
			}))
			defer httpServer.Close()
			serverTransport, clientTransport := sdk.NewInMemoryTransports()
			errorResponseReady, releaseErrorResponse := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseErrorResponse) })
			defer release()
			delayed := &mcpDelayedErrorTransport{Transport: serverTransport, ready: errorResponseReady, release: releaseErrorResponse}
			bridge := &app{server: httpServer.URL + "/base/api/v1", token: "saved-secret", http: httpServer.Client()}
			done := make(chan error, 1)
			go func() { done <- bridge.mcpBridge(ctx, "Clusters,Workflows", delayed) }()
			approvals, progress := make(chan struct{}, 1), make(chan struct{}, 1)
			changed := make(chan struct{}, 8)
			client := sdk.NewClient(&sdk.Implementation{Name: "local", Version: "1"}, &sdk.ClientOptions{
				ToolListChangedHandler: func(context.Context, *sdk.ToolListChangedRequest) {
					changed <- struct{}{}
				},
				ElicitationHandler: func(_ context.Context, req *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
					if req.Params.Message != "Approve exact action?" {
						return nil, fmt.Errorf("wrong prompt")
					}
					approvals <- struct{}{}
					return &sdk.ElicitResult{Action: "accept", Content: map[string]any{}}, nil
				},
				ProgressNotificationHandler: func(_ context.Context, req *sdk.ProgressNotificationClientRequest) {
					if req.Params.ProgressToken != "progress-1" {
						t.Errorf("progress token: %v", req.Params.ProgressToken)
					}
					progress <- struct{}{}
				},
			})
			session, err := client.Connect(ctx, clientTransport, &sdk.ClientSessionOptions{ProtocolVersion: version})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if session.InitializeResult().Instructions != "upstream instructions" {
				t.Fatal("instructions not forwarded")
			}
			tools, err := session.ListTools(ctx, nil)
			if err != nil || len(tools.Tools) != 2 {
				t.Fatalf("list: %+v, %v", tools, err)
			}
			result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "confirm", Arguments: map[string]any{"target": "orders"}, Meta: sdk.Meta{"progressToken": "progress-1"}})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Content) != 1 || !strings.Contains(result.Content[0].(*sdk.TextContent).Text, "orders") || calls.Load() != 2 {
				t.Fatalf("call result: %+v, calls %d", result, calls.Load())
			}
			for _, signal := range []<-chan struct{}{approvals, progress} {
				select {
				case <-signal:
				case <-ctx.Done():
					t.Fatal("missing forwarded notification")
				}
			}
			if version < "2026-07-28" {
				if err := session.Ping(ctx, nil); err != nil {
					t.Fatal(err)
				}
			}
			upstream.AddTool(&sdk.Tool{Name: "added", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
				return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "new tool"}}}, nil
			})
			select {
			case <-changed:
			case <-ctx.Done():
				t.Fatal("tool-list-changed not forwarded")
			}
			tools, err = session.ListTools(ctx, nil)
			if err != nil || len(tools.Tools) != 3 {
				t.Fatalf("updated list: %+v %v", tools, err)
			}
			if _, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "added"}); err != nil {
				t.Fatalf("new tool not callable: %v", err)
			}
			callCtx, stop := context.WithCancel(ctx)
			callDone := make(chan error, 1)
			go func() { _, err := session.CallTool(callCtx, &sdk.CallToolParams{Name: "block"}); callDone <- err }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("call did not start")
			}
			stop()
			select {
			case <-callDone:
			case <-ctx.Done():
				t.Fatal("call cancellation blocked")
			}
			select {
			case <-cancelled:
			case <-ctx.Done():
				t.Fatal("cancellation not forwarded")
			}
			select {
			case <-errorResponseReady:
			case <-ctx.Done():
				t.Fatal("cancelled call did not finish its response")
			}
			session.Close()
			release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("bridge did not close")
			}
		})
	}
}

func TestMCPConfigClients(t *testing.T) {
	for _, client := range []string{"claude-code", "cursor", "vscode", "codex", "json"} {
		for _, stdio := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stdio=%t", client, stdio), func(t *testing.T) {
				var body map[string]any
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "POST" || r.URL.Path != "/api/v1/api-tokens" {
						t.Errorf("unexpected token endpoint %s %s", r.Method, r.URL)
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"token":{"id":"new-id"},"secret":"new-secret"}`)
				}))
				defer server.Close()
				args := []string{"mcp", "config", "--client", client, "--namespace", "dev", "--namespace", "staging"}
				if stdio {
					args = append(args, "--stdio", "--write")
				}
				out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
				if err != nil {
					t.Fatal(err)
				}
				restrictions := body["restrictions"].(map[string]any)
				if restrictions["readOnly"] != !stdio || fmt.Sprint(restrictions["namespaces"]) != "[dev staging]" {
					t.Fatalf("restrictions: %+v", restrictions)
				}
				if !strings.HasPrefix(body["name"].(string), "mcp-"+client+"-") {
					t.Fatalf("name: %v", body["name"])
				}
				expires, err := time.Parse(time.RFC3339, body["expiresAt"].(string))
				if err != nil || time.Until(expires) < 89*24*time.Hour || time.Until(expires) > 91*24*time.Hour {
					t.Fatalf("expiry: %v %v", body["expiresAt"], err)
				}
				if !strings.Contains(stderr, "WARNING") || strings.Count(out+stderr, "new-secret") != 1 {
					t.Fatalf("secret/warning output: %q %q", out, stderr)
				}
				if stdio {
					if !strings.Contains(out, "karactl") || strings.Contains(out, "new-secret") || strings.Contains(out, server.URL) {
						t.Fatalf("stdio output: %s", out)
					}
				} else if !strings.Contains(out, server.URL+"/mcp") || !strings.Contains(out, "Bearer new-secret") {
					t.Fatalf("remote output: %s", out)
				}
				if client == "vscode" || client == "cursor" || client == "json" {
					var config map[string]map[string]map[string]any
					if err := json.Unmarshal([]byte(out), &config); err != nil {
						t.Fatal(err)
					}
					key := "mcpServers"
					if client == "vscode" {
						key = "servers"
					}
					entry := config[key]["karakoram"]
					if entry == nil {
						t.Fatalf("wrong shape: %s", out)
					}
					if stdio && entry["command"] != "karactl" || !stdio && entry["url"] != server.URL+"/mcp" {
						t.Fatalf("entry: %+v", entry)
					}
				} else if client == "codex" && !strings.Contains(out, "[mcp_servers.karakoram]") {
					t.Fatalf("codex config: %s", out)
				}
			})
		}
	}
}

func TestMCPConfigPrivateOutputAndValidation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "agent-token" {
			t.Errorf("token name: %v", body["name"])
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"secret":"private-secret"}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	for _, flags := range [][]string{{"--client", "unknown"}, {"--expires-in", "0d"}, {"--expires-in", "forever"}, {"--expires-in", "999999999999d"}, {"--cluster", "orders"}} {
		_, _, err := execute(t, server.URL, config, "", append([]string{"mcp", "config"}, flags...)...)
		if err == nil || ExitCode(err) != 2 {
			t.Fatalf("expected usage error for %v: %v", flags, err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid configuration minted tokens")
	}
	output := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(output, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := execute(t, server.URL, config, "", "mcp", "config", "--token-name", "agent-token", "--expires-in", "24h", "--output", output)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode: %v %v", info, err)
	}
	content, err := os.ReadFile(output)
	if err != nil || !bytes.Contains(content, []byte("private-secret")) || stdout != "" || strings.Contains(stderr, "private-secret") {
		t.Fatalf("private output: %q %q %q %v", stdout, stderr, content, err)
	}
}

func TestMCPBridgeAuthenticationExit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Bearer required", http.StatusUnauthorized)
	}))
	defer server.Close()
	for _, token := range []string{"", "invalid-token"} {
		var stdout, stderr bytes.Buffer
		code := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(""), Out: &stdout, Err: &stderr}, []string{"--server", server.URL, "--token", token, "mcp"})
		if code != 5 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "token") {
			t.Fatalf("auth exit: %d %q %q", code, stdout.String(), stderr.String())
		}
	}
}

func TestMCPBridgeSavedContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer selected-secret" {
			t.Errorf("context token: %v", r.Header)
		}
		http.Error(w, "Bearer required", http.StatusUnauthorized)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := SaveConfig(path, Config{CurrentContext: "other", Contexts: map[string]Context{"other": {Server: "http://127.0.0.1:1", Token: "wrong"}, "selected": {Server: server.URL, Token: "selected-secret"}}}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	code := Run(context.Background(), Options{ConfigPath: path, In: strings.NewReader(""), Out: io.Discard, Err: &stderr}, []string{"--context", "selected", "mcp"})
	if code != 5 {
		t.Fatalf("context exit: %d %s", code, stderr.String())
	}
}
