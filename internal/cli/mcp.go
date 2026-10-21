// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

func addMCPCommands(a *app, root *cobra.Command) {
	bridge := &cobra.Command{Use: "mcp", Short: "Bridge a stdio MCP client to the control plane", Args: cobra.NoArgs}
	bridge.Flags().String("toolsets", "", "Comma-separated server toolsets (OpenAPI tags)")
	bridge.RunE = func(cmd *cobra.Command, _ []string) error {
		sets, _ := cmd.Flags().GetString("toolsets")
		reader, ok := a.opts.In.(io.ReadCloser)
		if !ok {
			reader = io.NopCloser(a.opts.In)
		}
		return a.mcpBridge(cmd.Context(), sets, &sdk.IOTransport{Reader: reader, Writer: mcpWriter{a.opts.Out}})
	}
	config := &cobra.Command{Use: "config", Short: "Mint a restricted token and print MCP client configuration", Args: cobra.NoArgs}
	config.Flags().String("cluster", "", "Configure a cluster data MCP gateway instead of management MCP")
	config.Flags().Bool("write", false, "Permit writes (still subject to server mode and owner permissions)")
	config.Flags().String("client", "json", "Client: claude-code, cursor, vscode, codex, json")
	config.Flags().String("token-name", "", "Dedicated API token name (default mcp-CLIENT-DATE)")
	config.Flags().String("expires-in", "90d", "Token lifetime (for example 90d or 24h)")
	config.Flags().StringArrayP("namespace", "n", nil, "Restrict token to a namespace (repeatable)")
	config.Flags().Bool("stdio", false, "Print a karactl mcp command configuration using the saved context")
	config.Flags().String("output", "", "Write configuration to this file with mode 0600 instead of stdout")
	annotate(config, "createAPIToken")
	config.RunE = func(cmd *cobra.Command, _ []string) error {
		cluster, _ := cmd.Flags().GetString("cluster")
		write, _ := cmd.Flags().GetBool("write")
		client, _ := cmd.Flags().GetString("client")
		if cluster != "" {
			for _, flag := range []string{"namespace", "stdio", "output"} {
				if cmd.Flags().Changed(flag) {
					return usageError("--%s cannot be combined with --cluster", flag)
				}
			}
			return dataMCPConfig(cmd, a, cluster, write, client)
		}
		return a.managementMCPConfig(cmd, client, write)
	}
	bridge.AddCommand(config)
	root.AddCommand(bridge)
}

type mcpWriter struct{ io.Writer }

func (mcpWriter) Close() error { return nil }

func (a *app) mcpEndpoint(toolsets string) string {
	endpoint := strings.TrimSuffix(a.server, "/api/v1") + "/mcp"
	if toolsets != "" {
		endpoint += "?" + url.Values{"toolsets": {toolsets}}.Encode()
	}
	return endpoint
}

// Only the bearer credential is forwarded; browser Origin and cookie/CSRF
// credentials have no role on the MCP endpoint.
type mcpBearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t mcpBearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	response, err := t.base.RoundTrip(r)
	if err == nil && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		response.Body.Close()
		return nil, apiFailure(response.StatusCode, "MCP authentication failed: check the API token and its permissions (karactl login)")
	}
	return response, err
}

// The SDK exposes progress and elicitation, but not a public tool-list-changed
// sender. Retain its concurrency-safe connection for that notification only.
type mcpBridgeTransport struct {
	sdk.Transport
	connection *mcpBridgeConnection
}

func (t *mcpBridgeTransport) Connect(ctx context.Context) (sdk.Connection, error) {
	c, err := t.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	t.connection = &mcpBridgeConnection{Connection: c}
	return t.connection, nil
}

type mcpBridgeConnection struct {
	sdk.Connection
	peerEOF atomic.Bool
}

func (c *mcpBridgeConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	message, err := c.Connection.Read(ctx)
	if errors.Is(err, io.EOF) {
		c.peerEOF.Store(true)
	}
	return message, err
}

func (a *app) mcpBridge(ctx context.Context, toolsets string, transport sdk.Transport) error {
	if a.token == "" {
		return apiFailure(http.StatusUnauthorized, "MCP requires an API token: use karactl login or KARAKORAM_TOKEN")
	}
	var local atomic.Pointer[sdk.ServerSession]
	downstream := &mcpBridgeTransport{Transport: transport}
	remoteClient := sdk.NewClient(&sdk.Implementation{Name: "karactl", Version: "1"}, &sdk.ClientOptions{
		MultiRoundTrip: &sdk.MultiRoundTripOptions{Disabled: true},
		ElicitationHandler: func(ctx context.Context, req *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
			if session := local.Load(); session != nil {
				return session.Elicit(ctx, req.Params)
			}
			return nil, fmt.Errorf("local MCP client is not connected")
		},
		ProgressNotificationHandler: func(ctx context.Context, req *sdk.ProgressNotificationClientRequest) {
			if session := local.Load(); session != nil {
				_ = session.NotifyProgress(ctx, req.Params)
			}
		},
		ToolListChangedHandler: func(ctx context.Context, _ *sdk.ToolListChangedRequest) {
			if local.Load() != nil {
				_ = downstream.connection.Write(ctx, &jsonrpc.Request{Method: "notifications/tools/list_changed"})
			}
		},
	})
	httpClient := *a.http
	httpClient.Jar = nil
	httpClient.Transport = mcpBearerTransport{base: a.http.Transport, token: a.token}
	remote, err := remoteClient.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: a.mcpEndpoint(toolsets), HTTPClient: &httpClient}, nil)
	if err != nil {
		return err
	}
	defer remote.Close()
	server := sdk.NewServer(&sdk.Implementation{Name: "karactl", Version: "1"}, &sdk.ServerOptions{
		Instructions: remote.InitializeResult().Instructions,
		Capabilities: &sdk.ServerCapabilities{Tools: &sdk.ToolCapabilities{ListChanged: true}},
	})
	// Tool handlers (rather than middleware) let the SDK adapt returned MRTR
	// input requests to legacy elicitation while retaining native MRTR results.
	var toolsMu sync.Mutex
	known := map[string]bool{}
	install := func(tools []*sdk.Tool) {
		toolsMu.Lock()
		defer toolsMu.Unlock()
		for _, tool := range tools {
			if known[tool.Name] {
				continue
			}
			server.AddTool(tool, func(ctx context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
				p := req.Params
				return remote.CallTool(ctx, &sdk.CallToolParams{Meta: p.Meta, Name: p.Name, Arguments: p.Arguments, InputResponses: p.InputResponses, RequestState: p.RequestState})
			})
			known[tool.Name] = true
		}
	}
	for cursor := ""; ; {
		page, err := remote.ListTools(ctx, &sdk.ListToolsParams{Cursor: cursor})
		if err != nil {
			return err
		}
		install(page.Tools)
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	server.AddReceivingMiddleware(func(next sdk.MethodHandler) sdk.MethodHandler {
		return func(ctx context.Context, method string, req sdk.Request) (sdk.Result, error) {
			switch method {
			case "tools/list":
				page, err := remote.ListTools(ctx, req.GetParams().(*sdk.ListToolsParams))
				if err == nil {
					install(page.Tools)
				}
				return page, err
			case "ping":
				// Ping was removed in 2026-07-28. Forward legacy clients'
				// liveness checks over a short-lived legacy upstream session.
				pingRemote := remote
				if remote.InitializeResult().ProtocolVersion >= "2026-07-28" {
					var err error
					pingRemote, err = remoteClient.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: a.mcpEndpoint(toolsets), HTTPClient: &httpClient}, &sdk.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
					if err != nil {
						return nil, err
					}
					defer pingRemote.Close()
				}
				if err := pingRemote.Ping(ctx, req.GetParams().(*sdk.PingParams)); err != nil {
					return nil, err
				}
			}
			return next(ctx, method, req)
		}
	})
	session, err := server.Connect(ctx, downstream, nil)
	if err != nil {
		return err
	}
	local.Store(session)
	defer session.Close()
	stop := context.AfterFunc(ctx, func() { _ = session.Close() })
	defer stop()
	err = session.Wait()
	// A client may close stdin while a cancelled call's final response is
	// still being written. Once its EOF is observed this is an orderly peer
	// disconnect, not a bridge failure. Keep genuine read/write errors.
	if downstream.connection.peerEOF.Load() && errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}
