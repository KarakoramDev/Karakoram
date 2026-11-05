package datamcp

import (
	"context"
	"errors"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpUpstream struct {
	mu      sync.Mutex
	session *mcp.ClientSession
	closed  bool
}

// Companions use initialize-based MCP independently of the downstream protocol.
// Starting with this revision skips modern discovery while still allowing the
// companion to negotiate an older supported initialize-based revision.
const companionProtocolVersion = "2025-11-25"

// A downstream SDK handler context carries private protocol metadata. Before
// initialization the SDK transport would reuse that version as the upstream
// HTTP header, even when Connect explicitly requests a legacy revision.
// Preserve cancellation and deadlines, but do not inherit downstream values.
type upstreamConnectContext struct{ context.Context }

func (upstreamConnectContext) Value(any) any { return nil }

// NewMCPUpstream connects without advertising sampling, roots, or elicitation.
func NewMCPUpstream(ctx context.Context, t mcp.Transport) (Upstream, error) {
	if t == nil {
		return nil, errors.New("nil MCP transport")
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "karakoram-data-gateway", Version: "1"}, &mcp.ClientOptions{Capabilities: &mcp.ClientCapabilities{}, MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true}})
	client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "roots/list", "sampling/createMessage", "elicitation/create":
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "upstream client input is disabled"}
			}
			return next(ctx, method, req)
		}
	})
	s, e := client.Connect(upstreamConnectContext{ctx}, t, &mcp.ClientSessionOptions{ProtocolVersion: companionProtocolVersion})
	if e != nil {
		return nil, e
	}
	return &mcpUpstream{session: s}, nil
}
func (u *mcpUpstream) current() (*mcp.ClientSession, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return nil, errors.New("upstream closed")
	}
	return u.session, nil
}
func (u *mcpUpstream) ListTools(ctx context.Context) ([]*mcp.Tool, error) {
	s, e := u.current()
	if e != nil {
		return nil, e
	}
	var tools []*mcp.Tool
	for t, err := range s.Tools(ctx, nil) {
		if err != nil {
			return nil, err
		}
		tools = append(tools, t)
	}
	return tools, nil
}
func (u *mcpUpstream) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	s, e := u.current()
	if e != nil {
		return nil, e
	}
	r, e := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if r != nil && r.InputRequests != nil {
		return nil, errors.New("upstream client input is disabled")
	}
	return r, e
}
func (u *mcpUpstream) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return nil
	}
	u.closed = true
	return u.session.Close()
}
