package datamcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type testClusters struct{ c *domain.Cluster }

func (c testClusters) GetCluster(context.Context, domain.ClusterRef) (*domain.Cluster, error) {
	return c.c, nil
}

type testFactory struct {
	mu          sync.Mutex
	calls       []engine.DataAccessLevel
	args        []map[string]any
	fp          string
	closed      int
	discoveries int
}

func (f *testFactory) Manifest(context.Context, *domain.Cluster) (string, error) {
	f.mu.Lock()
	f.discoveries++
	f.mu.Unlock()
	return "test", nil
}
func (f *testFactory) Fingerprint(context.Context, *domain.Cluster, engine.DataAccessLevel) (string, error) {
	return f.fp, nil
}
func (f *testFactory) Open(ctx context.Context, _ *domain.Cluster, l engine.DataAccessLevel) (Upstream, error) {
	ct, st := mcp.NewInMemoryTransports()
	s := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	for _, name := range []string{"query", "write", "destroy", "unknown", "hidden"} {
		s.AddTool(&mcp.Tool{Name: name, Description: string(l), InputSchema: map[string]any{"type": "object"}}, func(_ context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args map[string]any
			_ = json.Unmarshal(r.Params.Arguments, &args)
			f.mu.Lock()
			f.calls = append(f.calls, l)
			f.args = append(f.args, args)
			f.mu.Unlock()
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("data", 1000)}}, StructuredContent: map[string]any{"value": "unsafe"}}, nil
		})
	}
	ss, e := s.Connect(ctx, st, nil)
	if e != nil {
		return nil, e
	}
	u, e := NewMCPUpstream(ctx, ct)
	if e != nil {
		_ = ss.Close()
		return nil, e
	}
	return &testUpstream{Upstream: u, server: ss, f: f}, nil
}

type testUpstream struct {
	Upstream
	server *mcp.ServerSession
	f      *testFactory
}

func (u *testUpstream) Close() error {
	u.f.mu.Lock()
	u.f.closed++
	u.f.mu.Unlock()
	return errors.Join(u.Upstream.Close(), u.server.Close())
}

type fixture struct {
	g          *Gateway
	c          *domain.Cluster
	caller     Caller
	f          *testFactory
	events     []domain.Event
	write      bool
	allowWrite bool
	enabled    bool
}

func setup(t *testing.T) *fixture {
	t.Helper()
	x := &fixture{c: &domain.Cluster{Spec: domain.ClusterSpec{DataMcp: &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}}}, caller: Caller{Actor: "alice", TokenID: "token", Scope: &TokenScope{Namespaces: []string{"team"}, Permissions: []string{"cluster-data:write"}}}, f: &testFactory{}, write: true, allowWrite: true, enabled: true}
	max, def := 10.0, 5.0
	g, e := New(Options{Authenticate: func(r *http.Request) (Caller, error) {
		c := x.caller
		if v := r.Header.Get("Authorization"); v != "" {
			c.TokenID = v
		}
		return c, nil
	}, Authorize: func(_ []string, _ string, a string, _ string) bool { return a != "write" || x.allowWrite }, Writes: func() bool { return x.write }, Enabled: func() bool { return x.enabled }, Clusters: testClusters{x.c}, Upstreams: x.f, Manifests: map[string]Manifest{"test": {Upstream: "test", Tools: map[string]ToolRule{"query": {Class: ClassRead, WriteAlias: "query_write", Args: []ArgRule{{Name: "limit", Default: &def, Max: &max}, {Name: "secret", Forbidden: true}, {Name: "mode", Allow: []string{"safe"}, Deny: []string{"bad"}}}}, "write": {Class: ClassWrite}, "destroy": {Class: ClassDestructive}, "hidden": {Class: ClassDeny}}}}, PublicURL: "https://example.com", Limits: Limits{MaxResultBytes: 1024, Burst: 100}, Audit: func(_ context.Context, e domain.Event) error { x.events = append(x.events, e); return nil }})
	if e != nil {
		t.Fatal(e)
	}
	x.g = g
	t.Cleanup(func() { _ = g.Close() })
	return x
}
func (x *fixture) client(t *testing.T, version string, confirm *bool) *mcp.ClientSession {
	t.Helper()
	h := httptest.NewServer(x.g)
	t.Cleanup(h.Close)
	o := &mcp.ClientOptions{}
	if confirm != nil {
		o.ElicitationHandler = func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			action := "decline"
			if *confirm {
				action = "accept"
			}
			return &mcp.ElicitResult{Action: action, Content: map[string]any{"confirm": *confirm}}, nil
		}
	}
	c := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, o)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	s, e := c.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL + "/mcp/clusters/team/db"}, &mcp.ClientSessionOptions{ProtocolVersion: version})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func names(t *testing.T, s *mcp.ClientSession) []string {
	t.Helper()
	r, e := s.ListTools(context.Background(), nil)
	if e != nil {
		t.Fatal(e)
	}
	var out []string
	for _, v := range r.Tools {
		out = append(out, v.Name)
	}
	slices.Sort(out)
	return out
}
func TestAccessDecision(t *testing.T) {
	for _, which := range []string{"all", "cluster-only", "both-matching", "empty-targets", "scope", "namespace", "cluster", "permission", "spec", "token-write", "readonly", "rbac", "install"} {
		t.Run(which, func(t *testing.T) {
			x := setup(t)
			switch which {
			case "empty-targets":
				x.caller.Scope.Namespaces = nil
				x.caller.Scope.Clusters = nil
			case "cluster-only":
				x.caller.Scope.Namespaces = nil
				x.caller.Scope.Clusters = []string{"team/db"}
			case "both-matching":
				x.caller.Scope.Clusters = []string{"team/db"}
			case "scope":
				x.caller.Scope = nil
			case "namespace":
				x.caller.Scope.Namespaces = []string{"other"}
			case "cluster":
				x.caller.Scope.Clusters = []string{"team/other"}
			case "permission":
				x.caller.Scope.Permissions = nil
			case "spec":
				x.c.Spec.DataMcp.Access = "read"
			case "token-write":
				x.caller.Scope.Permissions = []string{"cluster-data:read"}
			case "readonly":
				x.caller.Scope.ReadOnly = true
			case "rbac":
				x.allowWrite = false
			case "install":
				x.write = false
			}
			a := DecideAccess(x.caller, domain.ClusterRef{Namespace: "team", Name: "db"}, x.c, x.g.o.Authorize, x.write)
			wantRead := !slices.Contains([]string{"empty-targets", "scope", "namespace", "cluster", "permission"}, which)
			if a.Read != wantRead || a.Write != slices.Contains([]string{"all", "cluster-only", "both-matching"}, which) {
				t.Fatalf("decision: %+v", a)
			}
		})
	}
}
func TestGatewayListRoutingArgumentsAudit(t *testing.T) {
	x := setup(t)
	x.c.Spec.DataMcp.Tools.Deny = []string{"write"}
	s := x.client(t, "2025-11-25", nil)
	if got := names(t, s); !slices.Equal(got, []string{"destroy", "query", "query_write"}) {
		t.Fatal(got)
	}
	listed, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range listed.Tools {
		if (tool.Name == "query" || tool.Name == "query_write") && tool.Description != string(engine.DataAccessRead) {
			t.Fatalf("read definition came from write upstream: %+v", tool)
		}
	}
	r, e := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query", Arguments: map[string]any{"limit": 100}})
	if e != nil {
		t.Fatal(e)
	}
	if x.f.calls[0] != engine.DataAccessRead || x.f.args[0]["limit"] != float64(10) {
		t.Fatal(x.f.calls, x.f.args)
	}
	b, _ := json.Marshal(r)
	if len(b) > 1024 || !strings.Contains(string(b), "untrusted-data-") || !strings.Contains(string(b), "truncated") || r.StructuredContent != nil {
		t.Fatal(string(b))
	}
	var details map[string]any
	_ = json.Unmarshal(x.events[0].Details, &details)
	hash := sha256.Sum256([]byte(`{"limit":10}`))
	if details["argsHash"] != hex.EncodeToString(hash[:]) || details["args"] != nil || details["outcome"] != "allowed" || x.events[0].Actor != "alice (mcp-data: team/db)" {
		t.Fatal(string(x.events[0].Details))
	}
	for _, name := range []string{"unknown", "hidden", "write"} {
		_, e = s.CallTool(context.Background(), &mcp.CallToolParams{Name: name})
		if e == nil {
			t.Fatalf("allowed %s", name)
		}
	}
	_, e = s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query", Arguments: map[string]any{"secret": "x"}})
	if e == nil {
		t.Fatal("forbidden argument allowed")
	}
	x.write = false
	if got := names(t, s); !slices.Equal(got, []string{"query"}) {
		t.Fatal(got)
	}
}
func TestConfirmation(t *testing.T) {
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		for _, mode := range []string{"accept", "decline", "absent"} {
			t.Run(version+mode, func(t *testing.T) {
				x := setup(t)
				var answer *bool
				if mode != "absent" {
					b := mode == "accept"
					answer = &b
				}
				s := x.client(t, version, answer)
				_, e := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query_write"})
				if (e == nil) != (mode == "accept") {
					t.Fatalf("confirmation %s: %v", mode, e)
				}
				if mode == "accept" {
					if len(x.f.calls) != 1 || x.f.calls[0] != engine.DataAccessReadWrite {
						t.Fatal(x.f.calls)
					}
				} else if len(x.f.calls) != 0 {
					t.Fatal("declined tool executed")
				}
				if mode != "absent" {
					if len(x.events) != 2 || !strings.Contains(string(x.events[0].Details), `"outcome":"pending"`) || !strings.Contains(string(x.events[0].Details), `"confirmation":"requested"`) {
						t.Fatalf("confirmation audit: %+v", x.events)
					}
				}
			})
		}
	}
}
func TestHTTPGuards(t *testing.T) {
	x := setup(t)
	for _, tc := range []struct {
		path, key, value string
		code             int
	}{{"/mcp/clusters/team/db/", "", "", 404}, {"/mcp/clusters/team/db", "Cookie", "session=x", 401}, {"/mcp/clusters/team/db", "Origin", "https://evil.example", 403}, {"/mcp/clusters/team/db", "Mcp-Session-Id", "other", 403}, {"/mcp/clusters/a/db", "", "", 404}} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader("{}"))
		if tc.key != "" {
			r.Header.Set(tc.key, tc.value)
		}
		w := httptest.NewRecorder()
		x.g.ServeHTTP(w, r)
		if w.Code != tc.code {
			t.Fatalf("%+v: %d", tc, w.Code)
		}
	}
	x.enabled = false
	w := httptest.NewRecorder()
	x.g.ServeHTTP(w, httptest.NewRequest("POST", "/mcp/clusters/team/db", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
}
func TestRateLimitAndManifest(t *testing.T) {
	x := setup(t)
	x.g.o.Limits.Burst = 1
	s := x.client(t, "2025-11-25", nil)
	for i := range 2 {
		_, e := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"})
		if (e != nil) != (i == 1) {
			t.Fatal(i, e)
		}
	}
	if !strings.Contains(string(x.events[1].Details), "ratelimited") {
		t.Fatal(string(x.events[1].Details))
	}
	for _, m := range CompanionManifests() {
		if e := validateManifest(m); e != nil {
			t.Fatal(e)
		}
	}
	if validateManifest(Manifest{Tools: map[string]ToolRule{"a": {Class: ClassRead, WriteAlias: "b"}, "b": {Class: ClassRead}}}) == nil {
		t.Fatal("alias collision allowed")
	}
}
func TestArgRules(t *testing.T) {
	d, m := 3.0, 5.0
	rules := []ArgRule{{Name: "n", Default: &d, Max: &m}, {Name: "s", Allow: []string{"yes"}, Deny: []string{"no"}}, {Name: "x", Forbidden: true}}
	for _, tc := range []struct {
		args map[string]any
		bad  bool
	}{{map[string]any{}, false}, {map[string]any{"n": 9.0}, false}, {map[string]any{"n": "nine"}, true}, {map[string]any{"s": "no"}, true}, {map[string]any{"x": nil}, true}} {
		if e := applyRules(tc.args, rules); (e != nil) != tc.bad {
			t.Fatal(tc, e)
		}
	}
}

func TestSessionTokenBindingAndBodyLimit(t *testing.T) {
	x := setup(t)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`
	r := httptest.NewRequest("POST", "/mcp/clusters/team/db", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	x.g.ServeHTTP(w, r)
	id := w.Header().Get("Mcp-Session-Id")
	if w.Code != 200 || id == "" {
		t.Fatalf("initialize: %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("POST", "/mcp/clusters/team/db", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"ping"}`))
	r.Header.Set("Mcp-Session-Id", id)
	r.Header.Set("Authorization", "another-token")
	w = httptest.NewRecorder()
	x.g.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	x.g.o.Limits.MaxBodyBytes = 16
	r = httptest.NewRequest("POST", "/mcp/clusters/team/db", strings.NewReader(strings.Repeat("x", 100)))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w = httptest.NewRecorder()
	x.g.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatalf("body limit: %d %s", w.Code, w.Body.String())
	}
}

func TestCacheReplacementAndClose(t *testing.T) {
	x := setup(t)
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	u, release, err := x.g.upstream(context.Background(), ref, x.c, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	x.f.fp = "rotated"
	v, release2, err := x.g.upstream(context.Background(), ref, x.c, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if u == v || x.f.closed != 0 {
		t.Fatal("active superseded upstream closed")
	}
	release()
	if x.f.closed != 1 {
		t.Fatal(x.f.closed)
	}
	release2()
	if err := x.g.Close(); err != nil {
		t.Fatal(err)
	}
	if x.f.closed != 2 {
		t.Fatal(x.f.closed)
	}
}

func TestAuditOptInAndWrappedBounds(t *testing.T) {
	x := setup(t)
	x.c.Spec.DataMcp.AuditQueryText = true
	s := x.client(t, "2025-11-25", nil)
	if _, e := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query", Arguments: map[string]any{"mode": "safe"}}); e != nil {
		t.Fatal(e)
	}
	var d map[string]any
	_ = json.Unmarshal(x.events[0].Details, &d)
	if d["args"] == nil {
		t.Fatal(string(x.events[0].Details))
	}
	r := x.g.boundResult(&mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("x", 5000)}}}, true)
	b, _ := json.Marshal(r)
	if len(b) > 1024 || strings.Contains(string(b), "untrusted-data-") || !strings.Contains(string(b), "truncated") {
		t.Fatal(string(b))
	}
}

func TestConfirmationBindingExpiryReplay(t *testing.T) {
	x := setup(t)
	yes := true
	_ = x.client(t, "2025-11-25", &yes)
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	var session *mcp.ServerSession
	for s := range x.g.servers[ref].Sessions() {
		session = s
		break
	}
	if session == nil {
		t.Fatal("missing server session")
	}
	binding := ConfirmRequest{Cluster: ref, TokenID: "token", Tool: "destroy", ArgsHash: "hash", Class: ClassDestructive, Summary: "Confirm modification"}
	req := &mcp.CallToolRequest{Session: session, Params: &mcp.CallToolParamsRaw{}}
	challenge, _, err := x.g.confirm(req, binding)
	if err != nil {
		t.Fatal(err)
	}
	req.Params.RequestState = challenge.RequestState
	req.Params.InputResponses = mcp.InputResponseMap{"confirm": &mcp.ElicitResult{Action: "accept", Content: map[string]any{"confirm": true}}}
	changed := binding
	changed.TokenID = "other"
	if _, _, e := x.g.confirm(req, changed); e == nil {
		t.Fatal("changed token accepted")
	}
	if _, accepted, e := x.g.confirm(req, binding); e != nil || !accepted {
		t.Fatal(accepted, e)
	}
	if _, _, e := x.g.confirm(req, binding); e == nil {
		t.Fatal("replay accepted")
	}
	req.Params = &mcp.CallToolParamsRaw{}
	challenge, _, err = x.g.confirm(req, binding)
	if err != nil {
		t.Fatal(err)
	}
	req.Params.RequestState = challenge.RequestState
	x.g.o.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, _, e := x.g.confirm(req, binding); e == nil {
		t.Fatal("expired confirmation accepted")
	}
}

func TestConcurrencyAndRateRefill(t *testing.T) {
	x := setup(t)
	x.g.o.Limits.MaxConcurrentPerCluster = 1
	x.g.o.Limits.Burst = 1
	now := time.Now()
	x.g.o.Now = func() time.Time { return now }
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	if !x.g.admit("a", ref) || x.g.admit("b", ref) {
		t.Fatal("concurrency cap not enforced")
	}
	x.g.concurrent[ref]--
	if x.g.admit("a", ref) {
		t.Fatal("token burst not enforced")
	}
	now = now.Add(time.Minute)
	if !x.g.admit("a", ref) {
		t.Fatal("rate did not refill")
	}
}

func TestWriteToolsRequireEverySwitch(t *testing.T) {
	for _, which := range []string{"spec", "token", "readonly", "rbac", "installation"} {
		t.Run(which, func(t *testing.T) {
			x := setup(t)
			switch which {
			case "spec":
				x.c.Spec.DataMcp.Access = "read"
			case "token":
				x.caller.Scope.Permissions = []string{"cluster-data:read"}
			case "readonly":
				x.caller.Scope.ReadOnly = true
			case "rbac":
				x.allowWrite = false
			case "installation":
				x.write = false
			}
			s := x.client(t, "2025-11-25", nil)
			if got := names(t, s); !slices.Equal(got, []string{"query"}) {
				t.Fatal(got)
			}
			if _, e := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "write"}); e == nil {
				t.Fatal("hidden write callable")
			}
		})
	}
}

func TestLegacyConfirmationTimeout(t *testing.T) {
	x := setup(t)
	x.g.o.Limits.CallTimeout = 30 * time.Millisecond
	h := httptest.NewServer(x.g)
	defer h.Close()
	release := make(chan struct{})
	c := mcp.NewClient(&mcp.Implementation{Name: "timeout-client", Version: "1"}, &mcp.ClientOptions{
		ElicitationHandler: func(ctx context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return &mcp.ElicitResult{Action: "cancel"}, nil
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL + "/mcp/clusters/team/db"}, &mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// The SDK does not cancel an already dispatched elicitation handler when
	// its peer times out. Release the simulated user before closing the client.
	defer close(release)
	start := time.Now()
	_, err = s.CallTool(ctx, &mcp.CallToolParams{Name: "destroy"})
	if err == nil || time.Since(start) > time.Second || len(x.f.calls) != 0 {
		t.Fatalf("confirmation timeout: %v, elapsed %s, calls %v", err, time.Since(start), x.f.calls)
	}
	if len(x.events) != 2 || !strings.Contains(string(x.events[1].Details), `"outcome":"error"`) || !strings.Contains(string(x.events[1].Details), `"confirmation":"declined"`) {
		t.Fatalf("legacy SDK failure missing terminal audit: %+v", x.events)
	}
}

func TestMongoConnectionPinnedByManifest(t *testing.T) {
	m := CompanionManifests()["mongodb-mcp-server"]
	for name, rule := range m.Tools {
		if rule.Class == ClassDeny {
			continue
		}
		args := map[string]any{}
		if err := applyRules(args, rule.Args); err != nil || args["connectionId"] != "preconfigured" {
			t.Fatalf("%s: default %v, %v", name, args, err)
		}
		args["connectionId"] = "another-connection"
		if err := applyRules(args, rule.Args); err == nil {
			t.Fatalf("%s permits connection switching", name)
		}
		args["connectionId"] = "preconfigured"
		if err := applyRules(args, rule.Args); err != nil {
			t.Fatalf("%s rejects pinned connection: %v", name, err)
		}
	}
}

func TestDeniedCallsConsumeAdmissionBeforeDiscovery(t *testing.T) {
	for _, name := range []string{"unknown", "hidden", "query"} {
		t.Run(name, func(t *testing.T) {
			x := setup(t)
			x.g.o.Limits.Burst = 1
			s := x.client(t, "2025-11-25", nil)
			args := map[string]any{}
			if name == "query" {
				args["secret"] = "forbidden"
			}
			if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args}); err == nil {
				t.Fatal("denied call allowed")
			}
			discoveries := x.f.discoveries
			if x.g.concurrent[domain.ClusterRef{Namespace: "team", Name: "db"}] != 0 {
				t.Fatal("denied call leaked concurrency")
			}
			if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"}); err == nil {
				t.Fatal("denied call did not consume rate allowance")
			}
			if x.f.discoveries != discoveries {
				t.Fatal("rate-limited call performed discovery")
			}
			if !strings.Contains(string(x.events[len(x.events)-1].Details), `"outcome":"ratelimited"`) {
				t.Fatal("missing rate-limit audit")
			}
		})
	}
}

func TestConcurrencyAdmissionPrecedesDiscovery(t *testing.T) {
	x := setup(t)
	x.g.o.Limits.MaxConcurrentPerCluster = 1
	s := x.client(t, "2025-11-25", nil)
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	if !x.g.admit("another-token", ref) {
		t.Fatal("failed to occupy cluster slot")
	}
	if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "query"}); err == nil {
		t.Fatal("concurrent call admitted")
	}
	if x.f.discoveries != 0 {
		t.Fatal("concurrency-limited call performed discovery")
	}
	x.g.concurrent[ref]--
}

func TestSessionBindingsFollowSDKLifetime(t *testing.T) {
	x := setup(t)
	first := x.client(t, "2025-11-25", nil)
	second := x.client(t, "2025-11-25", nil)
	if len(x.g.sessions) != 2 {
		t.Fatalf("bindings: %d", len(x.g.sessions))
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if len(x.g.sessions) != 1 {
		t.Fatalf("DELETE retained closed session: %d", len(x.g.sessions))
	}
	ref := domain.ClusterRef{Namespace: "team", Name: "db"}
	for session := range x.g.servers[ref].Sessions() {
		// SDK idle expiry closes the session through this same lifecycle.
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	}
	x.g.mu.Lock()
	x.g.pruneSessionsLocked()
	count := len(x.g.sessions)
	x.g.mu.Unlock()
	if count != 0 {
		t.Fatalf("expiry retained closed session: %d", count)
	}
	_ = second.Close()
}
