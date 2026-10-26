package nativermq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func openTest(t *testing.T, handler http.HandlerFunc, level engine.DataAccessLevel, scope ...string) datamcp.Upstream {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	u, err := Open(context.Background(), datamcp.NativeInput{Level: level, Scope: domain.DataMcpScope{Vhosts: scope}, Credentials: engine.DataAccessCredentials{URI: s.URL, Username: "identity", Password: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Close() })
	return u
}

// vhostCollection mirrors RabbitMQ 4.3.6: pagination fails while injecting
// default queue types into pagination metadata; column selection is ignored
// because the resulting vhost maps bypass proplist column extraction.
func vhostCollection(vhosts []map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("page") != "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(vhosts)
	}
}
func TestManifestAndAccess(t *testing.T) {
	m := Manifest()
	if m.Upstream != "native-rabbitmq" || m.Version != "1" || m.Tools["queue_purge"].Class != datamcp.ClassDestructive || m.Tools["messages_peek"].Class != datamcp.ClassWrite {
		t.Fatal(m)
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		t.Run(string(level), func(t *testing.T) {
			u := openTest(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected request") }, level)
			list, err := u.ListTools(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := 6
			if level == engine.DataAccessReadWrite {
				want = 9
			}
			if len(list) != want {
				t.Fatalf("tools=%d", len(list))
			}
			for _, tool := range list {
				schema := tool.InputSchema.(map[string]any)
				if schema["additionalProperties"] != false {
					t.Fatal(schema)
				}
			}
			if level == engine.DataAccessRead {
				for _, name := range []string{"messages_peek", "message_publish", "queue_purge"} {
					if _, err := u.CallTool(context.Background(), name, map[string]any{"vhost": "/"}); err == nil {
						t.Fatal("write allowed")
					}
				}
			}
		})
	}
}
func TestScopeAndArguments(t *testing.T) {
	calls := 0
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.EscapedPath() != "/api/queues/team%2Fa/q" {
			t.Error(r.URL.EscapedPath())
		}
		user, pass, _ := r.BasicAuth()
		if user != "identity" || pass != "secret" {
			t.Error("missing identity")
		}
		_, _ = w.Write([]byte(`{"messages":2}`))
	}, engine.DataAccessRead, "team/a")
	for _, args := range []map[string]any{{}, {"vhost": "other"}, {"vhost": 1}, {"vhost": "team/a", "unknown": true}} {
		if _, err := u.CallTool(context.Background(), "overview", args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err := u.CallTool(context.Background(), "queue_get", map[string]any{"vhost": "team/a", "queue": "q"}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
	all := openTest(t, vhostCollection([]map[string]any{{"name": "other"}}), engine.DataAccessRead)
	if _, err := all.CallTool(context.Background(), "overview", map[string]any{"vhost": "other"}); err != nil {
		t.Fatal(err)
	}
}

func TestOverviewMonitoringIdentity(t *testing.T) {
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		t.Run(string(level), func(t *testing.T) {
			calls := 0
			u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				user, pass, ok := r.BasicAuth()
				if !ok || user != "identity" || pass != "secret" {
					t.Error("missing data identity")
				}
				if r.Method != http.MethodGet {
					t.Errorf("unexpected method: %s", r.Method)
				}
				switch r.URL.EscapedPath() {
				case "/api/vhosts/%2Fteam":
					// RabbitMQ 4.x requires administrator for the single-vhost
					// resource, even for a monitoring user with vhost permissions.
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(`{"error":"not_authorised","reason":"Not administrator user"}`))
				case "/api/vhosts":
					// Monitoring users can list vhosts with their statistics.
					// Only the exact requested vhost may reach the tool result.
					vhostCollection([]map[string]any{
						{"name": "/", "messages": 99},
						{"name": "/team-other", "messages": 88},
						{"name": "/team", "messages": 2, "message_stats": map[string]any{"publish": 3, "publish_details": map[string]any{"rate": 1.5}}},
					})(w, r)
				default:
					t.Errorf("unexpected path: %s", r.URL.EscapedPath())
					w.WriteHeader(http.StatusNotFound)
				}
			}, level, "/team")
			if _, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/"}); err == nil {
				t.Fatal("out-of-scope overview accepted")
			}
			if calls != 0 {
				t.Fatal("out-of-scope overview reached the broker")
			}
			res, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/team"})
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &got); err != nil {
				t.Fatal(err)
			}
			if got["name"] != "/team" || got["messages"] != float64(2) {
				t.Fatalf("unexpected overview: %v", got)
			}
			stats := got["message_stats"].(map[string]any)
			if stats["publish"] != float64(3) || stats["publish_details"].(map[string]any)["rate"] != 1.5 {
				t.Fatalf("message statistics lost: %v", stats)
			}
			if calls != 1 {
				t.Fatalf("requests=%d", calls)
			}
		})
	}
}

func TestOverviewMissingOrInvalidVhost(t *testing.T) {
	for _, response := range []string{`[]`, `[{"name":"/team-other"}]`, `{}`, `[null]`} {
		t.Run(response, func(t *testing.T) {
			u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(response))
			}, engine.DataAccessRead, "/team")
			if _, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/team"}); err == nil {
				t.Fatal("missing or invalid vhost accepted")
			}
		})
	}
}

func TestOverviewCollectionColumnHint(t *testing.T) {
	// Request only overview columns, but do not assume the broker honors them.
	// Name selection is exact even when case/substring alternatives exist.
	vhosts := []map[string]any{
		{"name": "/", "messages": 99, "description": "Default virtual host"},
		{"name": "/TEAM", "messages": 88},
		{"name": "/team-other", "messages": 77},
		{"name": "/team", "messages": 7},
	}
	calls := 0
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/vhosts" || r.URL.Query().Get("columns") == "" || len(r.URL.Query()) != 1 {
			t.Errorf("unexpected collection request: %s", r.URL)
		}
		vhostCollection(vhosts)(w, r)
	}, engine.DataAccessRead, "/team")
	res, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/team"})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &got); err != nil {
		t.Fatal(err)
	}
	if got["name"] != "/team" || got["messages"] != float64(7) || len(got) != 2 {
		t.Fatalf("unexpected overview: %v", got)
	}
	if calls != 1 {
		t.Fatalf("requests=%d, want 1", calls)
	}
}

func TestOverviewCollectionLimit(t *testing.T) {
	u := openTest(t, vhostCollection([]map[string]any{
		{"name": "/other", "description": strings.Repeat("x", 4<<20)},
		{"name": "/team", "messages": 7},
	}), engine.DataAccessRead, "/team")
	if _, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/team"}); err == nil {
		t.Fatal("oversized collection accepted when broker ignores columns")
	}
}
func TestPeekBounds(t *testing.T) {
	calls := 0
	expected := 5
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.EscapedPath() != "/api/queues/%2F/q/get" {
			t.Error(r.Method, r.URL.EscapedPath())
		}
		var b map[string]any
		if json.NewDecoder(r.Body).Decode(&b) != nil {
			t.Fatal("body")
		}
		if b["count"] != float64(expected) || b["truncate"] != float64(payloadLimit) || b["ackmode"] != "ack_requeue_true" || b["encoding"] != "auto" {
			t.Error(b)
		}
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"payload": strings.Repeat("x", payloadLimit+1)}})
	}, engine.DataAccessReadWrite)
	args := map[string]any{"vhost": "/", "queue": "q"}
	res, err := u.CallTool(context.Background(), "messages_peek", args)
	if err != nil {
		t.Fatal(err)
	}
	var items []map[string]any
	if err = json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &items); err != nil {
		t.Fatal(err)
	}
	if len(items[0]["payload"].(string)) != payloadLimit || items[0]["payload_truncated"] != true {
		t.Fatal("not truncated")
	}
	expected = 20
	args["count"] = 20
	if _, err = u.CallTool(context.Background(), "messages_peek", args); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]any{{"count": 21}, {"count": 0}, {"count": 1.5}, {"count": "5"}, {"ackmode": "ack_requeue_false"}, {"encoding": "base64"}} {
		a := map[string]any{"vhost": "/", "queue": "q"}
		for k, v := range bad {
			a[k] = v
		}
		if _, err = u.CallTool(context.Background(), "messages_peek", a); err == nil {
			t.Fatal(bad)
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestListsAndQueue(t *testing.T) {
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/queues/v":
			if r.URL.Query().Get("page") != "2" || r.URL.Query().Get("page_size") != "3" {
				t.Error(r.URL)
			}
			_, _ = w.Write([]byte(`{"items":[{"messages":4,"consumers":2,"message_stats":{"publish_details":{"rate":1}}}],"page":2}`))
		case "/api/queues/v/q":
			_, _ = w.Write([]byte(`{"messages":4,"consumers":2}`))
		default:
			_, _ = w.Write([]byte(`[1,2,3,4]`))
		}
	}, engine.DataAccessRead)
	for _, name := range []string{"queues_list", "exchanges_list", "bindings_list", "consumers_list"} {
		res, err := u.CallTool(context.Background(), name, map[string]any{"vhost": "v", "page": 2, "page_size": 3})
		if err != nil {
			t.Fatal(err)
		}
		if name != "queues_list" && !strings.Contains(res.Content[0].(*mcp.TextContent).Text, `"items":[4]`) {
			t.Fatal(res)
		}
	}
	if _, err := u.CallTool(context.Background(), "queue_get", map[string]any{"vhost": "v", "queue": "q"}); err != nil {
		t.Fatal(err)
	}
	for _, a := range []map[string]any{{"vhost": "v", "page_size": 101}, {"vhost": "v", "page": 0}} {
		if _, err := u.CallTool(context.Background(), "queues_list", a); err == nil {
			t.Fatal(a)
		}
	}
}
func TestPublishPurgeAndErrors(t *testing.T) {
	calls := 0
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.Method {
		case "POST":
			if r.URL.Path != "/api/exchanges/v/e/publish" {
				t.Error(r.URL)
			}
			var b map[string]any
			_ = json.NewDecoder(r.Body).Decode(&b)
			if b["payload_encoding"] != "string" || b["payload"] != "hello" {
				t.Error(b)
			}
			_, _ = w.Write([]byte(`{"routed":true}`))
		case "DELETE":
			if r.URL.Path != "/api/queues/v/q/contents" {
				t.Error(r.URL)
			}
			w.WriteHeader(204)
		default:
			w.WriteHeader(500)
			_, _ = w.Write([]byte("secret"))
		}
	}, engine.DataAccessReadWrite, "v")
	a := map[string]any{"vhost": "v", "exchange": "e", "routing_key": "q", "payload": "hello"}
	if _, err := u.CallTool(context.Background(), "message_publish", a); err != nil {
		t.Fatal(err)
	}
	a["payload"] = strings.Repeat("x", payloadLimit+1)
	if _, err := u.CallTool(context.Background(), "message_publish", a); err == nil {
		t.Fatal("oversize accepted")
	}
	a["vhost"] = "other"
	a["payload"] = "hello"
	if _, err := u.CallTool(context.Background(), "message_publish", a); err == nil {
		t.Fatal("scope bypass")
	}
	if _, err := u.CallTool(context.Background(), "queue_purge", map[string]any{"vhost": "v", "queue": "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "v"}); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatal(calls)
	}
}

type testDialer struct {
	address string
	called  bool
}

func (d *testDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.called = true
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}
func TestEndpointTLSAndDialer(t *testing.T) {
	s := httptest.NewTLSServer(vhostCollection([]map[string]any{{"name": "/"}}))
	defer s.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
	secure, err := Open(context.Background(), datamcp.NativeInput{Level: engine.DataAccessRead, Credentials: engine.DataAccessCredentials{URI: s.URL, TLS: true, CAPEM: ca}})
	if err != nil {
		t.Fatal(err)
	}
	defer secure.Close()
	if _, err = secure.CallTool(context.Background(), "overview", map[string]any{"vhost": "/"}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []engine.DataAccessCredentials{{URI: "amqp://host"}, {URI: "https://host", CAPEM: []byte("invalid")}, {URI: "http://host", TLS: true}, {URI: "http://host/?x=1"}} {
		if u, err := Open(context.Background(), datamcp.NativeInput{Level: engine.DataAccessRead, Credentials: c}); err == nil {
			_ = u.Close()
			t.Fatal(c.URI)
		}
	}
	plain := httptest.NewServer(vhostCollection([]map[string]any{{"name": "/"}}))
	defer plain.Close()
	d := &testDialer{address: strings.TrimPrefix(plain.URL, "http://")}
	u, err := Open(context.Background(), datamcp.NativeInput{Level: engine.DataAccessRead, Dialer: d, Credentials: engine.DataAccessCredentials{Hosts: []string{"logical.example"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if _, err = u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/"}); err != nil {
		t.Fatal(err)
	}
	if !d.called {
		t.Fatal("dialer unused")
	}
}
