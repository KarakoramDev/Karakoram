package datamcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type protocolFactory struct {
	*testFactory
	endpoint string
	native   bool
}

func (f *protocolFactory) Open(ctx context.Context, _ *domain.Cluster, _ engine.DataAccessLevel) (Upstream, error) {
	if f.native {
		return protocolNative{}, nil
	}
	return NewMCPUpstream(ctx, &mcp.StreamableClientTransport{Endpoint: f.endpoint})
}

// Native adapters implement Upstream directly, without an MCP client handshake.
type protocolNative struct{}

func (protocolNative) ListTools(context.Context) ([]*mcp.Tool, error) {
	return []*mcp.Tool{{Name: "query", InputSchema: map[string]any{"type": "object"}}}, nil
}
func (protocolNative) CallTool(context.Context, string, map[string]any) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "native result"}}}, nil
}
func (protocolNative) Close() error { return nil }

func TestGatewayProtocolBridge(t *testing.T) {
	for _, native := range []bool{false, true} {
		kind := "companion"
		if native {
			kind = "native"
		}
		for _, version := range []string{"2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"} {
			t.Run(kind+"/"+version, func(t *testing.T) {
				x := setup(t)
				f := &protocolFactory{testFactory: x.f, native: native}
				var mu sync.Mutex
				var methods []string
				if !native {
					s := mcp.NewServer(&mcp.Implementation{Name: "legacy-companion", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2025-11-25", "2025-06-18", "2025-03-26"}})
					s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
						return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
							mu.Lock()
							methods = append(methods, method)
							mu.Unlock()
							return next(ctx, method, req)
						}
					})
					s.AddTool(&mcp.Tool{Name: "query", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
						return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "companion result"}}}, nil
					})
					h := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, nil))
					t.Cleanup(h.Close)
					t.Cleanup(func() { _ = x.g.Close() })
					f.endpoint = h.URL
				}
				x.g.o.Upstreams = f
				if version == "2026-07-28" {
					// Exercise a genuinely stateless client: SDK Connect may silently
					// negotiate a legacy revision when discovery excludes modern MCP.
					for _, method := range []string{"tools/list", "tools/call"} {
						params := `"name":"query",`
						if method == "tools/list" {
							params = ""
						}
						body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":{` + params + `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"stateless-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}`
						req := httptest.NewRequest(http.MethodPost, "/mcp/clusters/team/db", strings.NewReader(body))
						req.Header.Set("Content-Type", "application/json")
						req.Header.Set("Accept", "application/json, text/event-stream")
						req.Header.Set("MCP-Protocol-Version", version)
						req.Header.Set("Mcp-Method", method)
						if method == "tools/call" {
							req.Header.Set("Mcp-Name", "query")
						}
						response := httptest.NewRecorder()
						x.g.ServeHTTP(response, req)
						if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"error"`) || !strings.Contains(response.Body.String(), `"result"`) {
							t.Fatalf("stateless %s: HTTP %d: %s", method, response.Code, response.Body.String())
						}
						if method == "tools/list" {
							var wire struct {
								Result struct {
									CacheScope string `json:"cacheScope"`
									TTLMs      *int   `json:"ttlMs"`
								} `json:"result"`
							}
							payload := response.Body.String()
							if strings.HasPrefix(response.Header().Get("Content-Type"), "text/event-stream") {
								_, data, found := strings.Cut(payload, "data: ")
								if !found {
									t.Fatalf("missing SSE data: %s", payload)
								}
								payload, _, _ = strings.Cut(data, "\n")
							}
							if err := json.Unmarshal([]byte(payload), &wire); err != nil {
								t.Fatal(err)
							}
							if wire.Result.CacheScope != "private" || wire.Result.TTLMs == nil || *wire.Result.TTLMs != 0 {
								t.Fatalf("caller-specific tool list must be private and immediately stale: %s", response.Body.String())
							}
						}
					}
				}
				client := x.client(t, version, nil)
				if got := names(t, client); !slices.Equal(got, []string{"query", "query_write"}) {
					t.Fatalf("tools: %v", got)
				}
				result, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"})
				if err != nil || result == nil || result.IsError || len(result.Content) == 0 {
					t.Fatalf("call: result=%+v err=%v", result, err)
				}
				wantEvents := 1
				if version == "2026-07-28" {
					wantEvents++
				}
				if len(x.events) != wantEvents {
					t.Fatalf("audit events: got %d, want %d", len(x.events), wantEvents)
				}
				if version == "2026-07-28" && len(x.g.sessions) != 0 {
					t.Fatalf("stateless client created %d sessions", len(x.g.sessions))
				}
				if !native {
					mu.Lock()
					defer mu.Unlock()
					if len(methods) == 0 || methods[0] != "initialize" || slices.Contains(methods, "server/discover") {
						t.Fatalf("companion must use legacy initialize, got %v", methods)
					}
				}
			})
		}
	}
}
