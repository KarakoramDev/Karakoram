package datamcp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type lifecycleUpstream struct{ closed atomic.Int32 }

func (u *lifecycleUpstream) ListTools(context.Context) ([]*mcp.Tool, error) { return nil, nil }
func (u *lifecycleUpstream) CallTool(context.Context, string, map[string]any) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{}, nil
}
func (u *lifecycleUpstream) Close() error { u.closed.Add(1); return nil }

type blockingFactory struct {
	started, release chan struct{}
	opens            atomic.Int32
	changed          atomic.Bool
	late             lifecycleUpstream
}

func (f *blockingFactory) Manifest(context.Context, *domain.Cluster) (string, error) {
	return "test", nil
}
func (f *blockingFactory) Fingerprint(context.Context, *domain.Cluster, engine.DataAccessLevel) (string, error) {
	if f.changed.Load() {
		return "changed", nil
	}
	return "initial", nil
}
func (f *blockingFactory) Open(_ context.Context, c *domain.Cluster, _ engine.DataAccessLevel) (Upstream, error) {
	if c.Name == "aa" {
		f.opens.Add(1)
		select {
		case f.started <- struct{}{}:
		default:
		}
		<-f.release
		return &f.late, nil
	}
	return &lifecycleUpstream{}, nil
}
func TestBlockedOpenDoesNotBlockOtherClustersOrClose(t *testing.T) {
	x := setup(t)
	f := &blockingFactory{started: make(chan struct{}, 2), release: make(chan struct{})}
	x.g.o.Upstreams = f
	done := make(chan error, 1)
	go func() {
		_, release, e := x.g.upstream(context.Background(), domain.ClusterRef{Namespace: "team", Name: "aa"}, &domain.Cluster{Name: "aa"}, engine.DataAccessRead)
		if release != nil {
			release()
		}
		done <- e
	}()
	select {
	case <-f.started:
	case <-time.After(time.Second):
		t.Fatal("open did not start")
	}
	b := make(chan error, 1)
	go func() {
		_, release, e := x.g.upstream(context.Background(), domain.ClusterRef{Namespace: "team", Name: "bb"}, &domain.Cluster{Name: "bb"}, engine.DataAccessRead)
		if release != nil {
			release()
		}
		b <- e
	}()
	select {
	case e := <-b:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked open serialized another cluster")
	}
	closed := make(chan error, 1)
	go func() { closed <- x.g.Close() }()
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("close blocked on opening upstream")
	}
	close(f.release)
	if e := <-done; e == nil {
		t.Fatal("late result published after close")
	}
	if f.late.closed.Load() != 1 {
		t.Fatal("late upstream not closed")
	}
}
func TestOpeningFingerprintRechecked(t *testing.T) {
	x := setup(t)
	f := &blockingFactory{started: make(chan struct{}, 2), release: make(chan struct{})}
	x.g.o.Upstreams = f
	done := make(chan error, 1)
	go func() {
		_, release, e := x.g.upstream(context.Background(), domain.ClusterRef{Namespace: "team", Name: "aa"}, &domain.Cluster{Name: "aa"}, engine.DataAccessRead)
		if release != nil {
			release()
		}
		done <- e
	}()
	<-f.started
	f.changed.Store(true)
	close(f.release)
	if e := <-done; e == nil {
		t.Fatal("stale fingerprint published")
	}
	if f.late.closed.Load() != 1 {
		t.Fatal("stale upstream not closed")
	}
}
func TestOpeningWaitersShareResult(t *testing.T) {
	x := setup(t)
	f := &blockingFactory{started: make(chan struct{}, 2), release: make(chan struct{})}
	x.g.o.Upstreams = f
	ref := domain.ClusterRef{Namespace: "team", Name: "aa"}
	c := &domain.Cluster{Name: "aa"}
	done := make(chan Upstream, 2)
	open := func() {
		u, release, e := x.g.upstream(context.Background(), ref, c, engine.DataAccessRead)
		if e != nil {
			done <- nil
			return
		}
		release()
		done <- u
	}
	go open()
	<-f.started
	go open()
	close(f.release)
	first, second := <-done, <-done
	if first == nil || first != second || f.opens.Load() != 1 {
		t.Fatal("same-key opens not shared")
	}
}
func TestMCPUpstreamRefusesRootsRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ct, st := mcp.NewInMemoryTransports()
	server := mcp.NewServer(&mcp.Implementation{Name: "untrusted-upstream", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2025-11-25"}})
	session, e := server.Connect(ctx, st, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	upstream, e := NewMCPUpstream(ctx, ct)
	if e != nil {
		t.Fatal(e)
	}
	defer upstream.Close()
	//lint:ignore SA1019 Intentionally exercise refusal of legacy roots requests.
	_, e = session.ListRoots(ctx, nil)
	var rpc *jsonrpc.Error
	if !errors.As(e, &rpc) || rpc.Code != jsonrpc.CodeMethodNotFound {
		t.Fatalf("roots/list not explicitly refused: %v", e)
	}
	caps := session.InitializeParams().Capabilities
	//lint:ignore SA1019 Assert deprecated roots and sampling capabilities are not advertised.
	if caps.RootsV2 != nil || caps.Sampling != nil || caps.Elicitation != nil {
		t.Fatalf("advertised upstream client input: %+v", caps)
	}
}
func TestMCPUpstreamRefusesMRTRInput(t *testing.T) {
	for _, kind := range []string{"roots", "sampling", "elicitation"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ct, st := mcp.NewInMemoryTransports()
			server := mcp.NewServer(&mcp.Implementation{Name: "untrusted-upstream", Version: "1"}, nil)
			var calls atomic.Int32
			server.AddTool(&mcp.Tool{Name: "input", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				calls.Add(1)
				var input mcp.InputRequest
				switch kind {
				case "roots":
					//lint:ignore SA1019 Intentionally exercise refusal of legacy roots input.
					input = &mcp.ListRootsParams{}
				case "sampling":
					//lint:ignore SA1019 Intentionally exercise refusal of legacy sampling input.
					input = &mcp.CreateMessageParams{MaxTokens: 1}
				case "elicitation":
					input = &mcp.ElicitParams{Message: "approve"}
				}
				return &mcp.CallToolResult{InputRequests: mcp.InputRequestMap{"request": input}}, nil
			})
			session, e := server.Connect(ctx, st, nil)
			if e != nil {
				t.Fatal(e)
			}
			defer session.Close()
			upstream, e := NewMCPUpstream(ctx, ct)
			if e != nil {
				t.Fatal(e)
			}
			defer upstream.Close()
			if _, e = upstream.CallTool(ctx, "input", nil); e == nil {
				t.Fatal("upstream client input accepted")
			}
			if calls.Load() != 1 {
				t.Fatal("input request automatically fulfilled")
			}
		})
	}
}
