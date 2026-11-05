// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package datamcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type reconnectUpstream struct {
	list                 func(context.Context) ([]*mcp.Tool, error)
	callErr              error
	lists, calls, closed atomic.Int32
}

func (u *reconnectUpstream) ListTools(ctx context.Context) ([]*mcp.Tool, error) {
	u.lists.Add(1)
	if u.list != nil {
		return u.list(ctx)
	}
	return []*mcp.Tool{{Name: "query", InputSchema: map[string]any{"type": "object"}}}, nil
}
func (u *reconnectUpstream) CallTool(context.Context, string, map[string]any) (*mcp.CallToolResult, error) {
	u.calls.Add(1)
	if u.callErr != nil {
		return nil, u.callErr
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
}
func (u *reconnectUpstream) Close() error { u.closed.Add(1); return nil }

type reconnectFactory struct {
	UpstreamFactory
	upstreams []*reconnectUpstream
	opens     atomic.Int32
	openErr   error
}

func (f *reconnectFactory) Open(context.Context, *domain.Cluster, engine.DataAccessLevel) (Upstream, error) {
	n := int(f.opens.Add(1)) - 1
	if n >= len(f.upstreams) {
		return nil, f.openErr
	}
	return f.upstreams[n], nil
}
func failedListing(err error) *reconnectUpstream {
	return &reconnectUpstream{list: func(context.Context) ([]*mcp.Tool, error) { return nil, err }}
}
func reconnectFixture(t *testing.T, upstreams ...*reconnectUpstream) (*fixture, *reconnectFactory) {
	t.Helper()
	x := setup(t)
	x.write = false
	f := &reconnectFactory{UpstreamFactory: x.f, upstreams: upstreams, openErr: errors.New(diagnosticCanary)}
	x.g.o.Upstreams = f
	return x, f
}
func listing(x *fixture) (map[string]exposedTool, error) {
	return x.g.tools(context.Background(), domain.ClusterRef{Namespace: "team", Name: "db"}, x.c, AccessDecision{}, x.g.o.Manifests["test"])
}

func TestListToolsReconnectOnce(t *testing.T) {
	for _, sentinel := range []error{mcp.ErrConnectionClosed, mcp.ErrSessionMissing} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			old, fresh := failedListing(fmt.Errorf("%s: %w", diagnosticCanary, sentinel)), &reconnectUpstream{}
			x, f := reconnectFixture(t, old, fresh)
			var logs bytes.Buffer
			x.g.o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			tools, err := listing(x)
			if err != nil || tools["query"].tool == nil {
				t.Fatalf("recovery failed: %v", err)
			}
			if f.opens.Load() != 2 || old.lists.Load() != 1 || fresh.lists.Load() != 1 || old.closed.Load() != 1 {
				t.Fatal("listing did not reopen exactly once")
			}
			if logs.Len() != 0 {
				t.Fatal("recovered listing logged a terminal failure")
			}
		})
	}
}

func TestListToolsPersistentFailureRetriesOnce(t *testing.T) {
	for _, sentinel := range []error{mcp.ErrConnectionClosed, mcp.ErrSessionMissing} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			err := fmt.Errorf("%s: %w", diagnosticCanary, sentinel)
			old, fresh := failedListing(err), failedListing(err)
			x, f := reconnectFixture(t, old, fresh)
			var logs bytes.Buffer
			x.g.o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			_, got := listing(x)
			if got == nil || got.Error() != "tool listing unavailable" {
				t.Fatalf("unmasked failure: %v", got)
			}
			if f.opens.Load() != 2 || old.lists.Load() != 1 || fresh.lists.Load() != 1 || old.closed.Load() != 1 || fresh.closed.Load() != 1 {
				t.Fatal("retry or close count changed")
			}
			var record map[string]any
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			flag := "connectionClosed"
			if sentinel == mcp.ErrSessionMissing {
				flag = "sessionMissing"
			}
			if record[flag] != true || record["msg"] != "Data MCP upstream tool listing failed" {
				t.Fatal("safe diagnostic missing")
			}
			if strings.Contains(logs.String(), "synthetic") {
				t.Fatal("raw upstream error leaked")
			}
		})
	}
}

func TestListToolsReconnectOpenFailureMasked(t *testing.T) {
	old := failedListing(fmt.Errorf("%s: %w", diagnosticCanary, mcp.ErrConnectionClosed))
	x, f := reconnectFixture(t, old)
	_, err := listing(x)
	if err == nil || err.Error() != "tool listing unavailable" || f.opens.Load() != 2 || old.closed.Load() != 1 {
		t.Fatalf("reopen failure not bounded/masked: %v", err)
	}
}

func TestListToolsOtherFailureDoesNotReconnect(t *testing.T) {
	old := failedListing(fmt.Errorf("%s: %w", diagnosticCanary, net.ErrClosed))
	x, f := reconnectFixture(t, old)
	_, err := listing(x)
	if err == nil || err.Error() != "tool listing unavailable" || f.opens.Load() != 1 || old.closed.Load() != 0 {
		t.Fatalf("unrelated error reconnected: %v", err)
	}
}

func TestCallToolFailureReconnectsNextRequestWithoutReplay(t *testing.T) {
	for _, sentinel := range []error{mcp.ErrConnectionClosed, mcp.ErrSessionMissing} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			old := &reconnectUpstream{callErr: fmt.Errorf("%s: %w", diagnosticCanary, sentinel)}
			fresh := &reconnectUpstream{}
			x, f := reconnectFixture(t, old, fresh)
			s := x.client(t, "2025-11-25", nil)
			_, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"})
			if err == nil || !strings.Contains(err.Error(), "upstream tool call failed") || strings.Contains(err.Error(), "synthetic") {
				t.Fatalf("unexpected call error: %v", err)
			}
			if f.opens.Load() != 1 || old.calls.Load() != 1 || fresh.calls.Load() != 0 || old.closed.Load() != 1 {
				t.Fatal("failed call replayed or not retired")
			}
			if _, err = s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"}); err != nil {
				t.Fatal(err)
			}
			if f.opens.Load() != 2 || old.calls.Load() != 1 || fresh.calls.Load() != 1 {
				t.Fatal("next call did not use fresh upstream")
			}
		})
	}
}

func TestConcurrentLateFailureDoesNotCloseActiveOrEvictReplacement(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	old := &reconnectUpstream{list: func(ctx context.Context) ([]*mcp.Tool, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-finish:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, fmt.Errorf("%s: %w", diagnosticCanary, mcp.ErrConnectionClosed)
	}}
	fresh := &reconnectUpstream{}
	x, f := reconnectFixture(t, old, fresh)
	// Bound the blocked operation and unblock it even if an assertion fails.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := x.g.tools(ctx, domain.ClusterRef{Namespace: "team", Name: "db"}, x.c, AccessDecision{}, x.g.o.Manifests["test"])
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err := listing(x)
	if err != nil {
		t.Fatal(err)
	}
	if f.opens.Load() != 2 || old.closed.Load() != 0 {
		t.Fatal("active stale upstream closed or no replacement opened")
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.opens.Load() != 2 || old.closed.Load() != 1 || fresh.closed.Load() != 0 {
		t.Fatal("late failure evicted replacement or closed upstream twice")
	}
	if _, err := listing(x); err != nil {
		t.Fatal(err)
	}
	if f.opens.Load() != 2 {
		t.Fatal("replacement not retained")
	}
}

func TestInvalidatedUpstreamShutdownClosesOnce(t *testing.T) {
	old := &reconnectUpstream{}
	x, _ := reconnectFixture(t, old)
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	entry, release, err := x.g.upstream(context.Background(), ref, x.c, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	x.g.invalidateUpstream(ref, engine.DataAccessRead, entry, mcp.ErrConnectionClosed)
	if err := x.g.Close(); err != nil {
		t.Fatal(err)
	}
	x.g.invalidateUpstream(ref, engine.DataAccessRead, entry, mcp.ErrSessionMissing)
	release()
	if old.closed.Load() != 1 {
		t.Fatal("shutdown and release closed retired upstream twice")
	}
}

func TestGatewayBoundsCyclicUpstreamFailures(t *testing.T) {
	for _, operation := range []string{"list", "call"} {
		t.Run(operation, func(t *testing.T) {
			old := failedListing(&diagnosticCycle{})
			if operation == "call" {
				old = &reconnectUpstream{callErr: &diagnosticCycle{}}
			}
			x, f := reconnectFixture(t, old)
			var logs bytes.Buffer
			x.g.o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			done := make(chan error, 1)
			if operation == "list" {
				go func() { _, err := listing(x); done <- err }()
			} else {
				session := x.client(t, "2025-11-25", nil)
				go func() {
					_, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"})
					done <- err
				}()
			}
			select {
			case err := <-done:
				message := "tool listing unavailable"
				if operation == "call" {
					message = "upstream tool call failed"
				}
				if err == nil || !strings.Contains(err.Error(), message) {
					t.Fatalf("unexpected masked failure: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cyclic upstream error blocked the request")
			}
			if f.opens.Load() != 1 || old.closed.Load() != 0 {
				t.Fatal("cycle reconnected without a terminal sentinel")
			}
			x.g.mu.Lock()
			active := x.g.upstreams["team/db:"+string(engine.DataAccessRead)].active
			x.g.mu.Unlock()
			if active != 0 {
				t.Fatal("cyclic failure leaked an active lease")
			}
			if operation == "list" {
				var record map[string]any
				if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				if record["errorChainTruncated"] != true {
					t.Fatal("missing bounded-chain diagnostic")
				}
			}
		})
	}
}

// Exercise HTTP 404 through the real MCP adapter, not a sentinel-returning fake.
// The SDK translates a missing initialized HTTP session to ErrSessionMissing.
func missingSessionCompanion(t *testing.T, x *fixture, failMethod string, persistent bool) (*atomic.Int32, *atomic.Int32) {
	t.Helper()
	var attempts, initializes atomic.Int32
	server := mcp.NewServer(&mcp.Implementation{Name: "companion", Version: "1"}, nil)
	server.AddTool(&mcp.Tool{Name: "query", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "request read failed", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var request struct{ Method string }
			if err := json.Unmarshal(body, &request); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if request.Method == "initialize" {
				initializes.Add(1)
			}
			if request.Method == failMethod {
				n := attempts.Add(1)
				if persistent || n == 1 {
					http.Error(w, "session missing", http.StatusNotFound)
					return
				}
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)
	// Close upstream sessions before the companion server during cleanup.
	t.Cleanup(func() { _ = x.g.Close() })
	x.g.o.Upstreams = &protocolFactory{testFactory: x.f, endpoint: httpServer.URL}
	return &attempts, &initializes
}

func TestMCPAdapterListingReconnectBound(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			x := setup(t)
			x.write = false
			attempts, initializes := missingSessionCompanion(t, x, "tools/list", persistent)
			tools, err := listing(x)
			if persistent {
				if err == nil || err.Error() != "tool listing unavailable" {
					t.Fatalf("unexpected terminal error: %v", err)
				}
			} else if err != nil || tools["query"].tool == nil {
				t.Fatalf("listing did not recover: %v", err)
			}
			if attempts.Load() != 2 || initializes.Load() != 2 {
				t.Fatalf("adapter/gateway retry exceeded bound: listing=%d initialize=%d", attempts.Load(), initializes.Load())
			}
		})
	}
}

func TestMCPAdapterCallMissingSessionNeverReplayed(t *testing.T) {
	x := setup(t)
	x.write = false
	attempts, initializes := missingSessionCompanion(t, x, "tools/call", false)
	session := x.client(t, "2025-11-25", nil)
	_, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"})
	if err == nil || !strings.Contains(err.Error(), "upstream tool call failed") {
		t.Fatalf("missing session was replayed or unmasked: %v", err)
	}
	if attempts.Load() != 1 || initializes.Load() != 1 {
		t.Fatal("adapter replayed a failed call")
	}
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || initializes.Load() != 2 {
		t.Fatal("next request did not reconnect exactly once")
	}
}
