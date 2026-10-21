package nativees

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type testDialer struct{ calls int }

func (d *testDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.calls++
	return (&net.Dialer{}).DialContext(ctx, network, addr)
}

func fake(t *testing.T, level engine.DataAccessLevel, patterns []string, handler http.HandlerFunc) (*upstream, *testDialer) {
	t.Helper()
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "reader" || pass != "secret" {
			t.Error("missing credentials")
		}
		allowed := map[string]bool{}
		switch {
		case strings.HasPrefix(r.URL.Path, "/_resolve/index/"):
			allowed["expand_wildcards"] = true
		case strings.Contains(r.URL.Path, "/_doc/"), strings.Contains(r.URL.Path, "/_update/"), r.URL.Path == "/_query":
			// These endpoints do not consume multi-index selection parameters.
		default:
			for _, key := range []string{"expand_wildcards", "allow_no_indices", "ignore_unavailable"} {
				allowed[key] = true
			}
			if strings.HasSuffix(r.URL.Path, "/_delete_by_query") {
				for _, key := range []string{"timeout", "scroll_size", "max_docs", "wait_for_completion"} {
					allowed[key] = true
				}
			}
		}
		for key := range r.URL.Query() {
			if !allowed[key] {
				t.Errorf("unconsumed parameter %s on %s", key, r.URL.Path)
				http.Error(w, "unconsumed parameter", http.StatusBadRequest)
				return
			}
		}
		handler(w, r)
	}))
	t.Cleanup(s.Close)
	d := &testDialer{}
	raw, err := Open(context.Background(), datamcp.NativeInput{Credentials: engine.DataAccessCredentials{Hosts: []string{s.URL}, Username: "reader", Password: "secret", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}, Scope: domain.DataMcpScope{IndexPatterns: patterns}, Level: level, Dialer: d})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	return raw.(*upstream), d
}
func resolved(w http.ResponseWriter, names ...string) {
	indices := []map[string]any{}
	for _, n := range names {
		indices = append(indices, map[string]any{"name": n, "attributes": []string{"open"}})
	}
	json.NewEncoder(w).Encode(map[string]any{"indices": indices})
}

func TestManifestAndTools(t *testing.T) {
	m := Manifest()
	if m.Upstream != "native-elasticsearch" || m.Version != "1" || len(m.Tools) != 10 || m.Tools["delete_by_query"].Class != datamcp.ClassDestructive {
		t.Fatal(m)
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		t.Run(string(level), func(t *testing.T) {
			u, _ := fake(t, level, nil, func(http.ResponseWriter, *http.Request) { t.Error("unexpected request") })
			tools, err := u.ListTools(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := 7
			if level == engine.DataAccessReadWrite {
				want = 10
			}
			if len(tools) != want {
				t.Fatal(len(tools))
			}
			for _, tool := range tools {
				b, err := json.Marshal(tool.InputSchema)
				if err != nil || !json.Valid(b) {
					t.Fatal("invalid schema")
				}
			}
			if level == engine.DataAccessRead {
				for _, n := range []string{"doc_index", "doc_update", "delete_by_query"} {
					if _, err := u.CallTool(context.Background(), n, nil); err == nil {
						t.Fatal("write allowed")
					}
				}
			}
		})
	}
}

func TestScopeResolution(t *testing.T) {
	tests := []struct {
		name, input, response string
		reject                bool
	}{
		{"explicit hidden", ".security", "", true},
		{"all alias", "_all", "", true},
		{"remote", "remote:logs-*", "", true},
		{"negative", "logs-*,-logs-secret", "", true},
		{"allowed", "logs-*", `{"indices":[{"name":"logs-a","attributes":["open"]}]}`, false},
		{"outside", "*", `{"indices":[{"name":"other"}]}`, true},
		{"hidden expansion", "*", `{"indices":[{"name":".security"}]}`, true},
		{"hidden attribute", "logs-*", `{"indices":[{"name":"logs-a","attributes":["hidden"]}]}`, true},
		{"alias escape", "logs-*", `{"aliases":[{"name":"logs-alias","indices":["other"]}]}`, true},
		{"hidden alias", "logs-*", `{"aliases":[{"name":".alias","indices":["logs-a"]}]}`, true},
		{"allowed alias", "logs-*", `{"aliases":[{"name":"logs-alias","indices":["logs-a"]}]}`, false},
		{"stream escape", "logs-*", `{"data_streams":[{"name":"logs-stream","backing_indices":[".ds-logs"]}]}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := fake(t, engine.DataAccessRead, []string{"logs-*"}, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("expand_wildcards") != "open" {
					t.Error("wildcard policy")
				}
				if tt.name == "allowed alias" && r.URL.Path == "/_resolve/index/logs-a" {
					resolved(w, "logs-a")
					return
				}
				fmt.Fprint(w, tt.response)
			})
			_, err := u.CallTool(context.Background(), "indices_list", map[string]any{"index": tt.input})
			if (err != nil) != tt.reject {
				t.Fatalf("error %v, reject %v", err, tt.reject)
			}
		})
	}
}

func TestSearchBoundsAndScope(t *testing.T) {
	for _, size := range []any{nil, float64(999), float64(0)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			u, d := fake(t, engine.DataAccessRead, []string{"logs-*"}, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/_resolve/") {
					if r.URL.Path != "/_resolve/index/logs-*" {
						t.Error(r.URL.Path)
					}
					resolved(w, "logs-a")
					return
				}
				if r.URL.Path != "/logs-a/_search" || r.Method != "POST" {
					t.Error(r.URL.Path)
				}
				var b map[string]any
				json.NewDecoder(r.Body).Decode(&b)
				want := float64(10)
				if size != nil {
					want = size.(float64)
					if want > 100 {
						want = 100
					}
				}
				if b["size"] != want || b["timeout"] != "5s" || b["track_total_hits"] != float64(10000) {
					t.Error(b)
				}
				if _, ok := b["_source"]; !ok {
					t.Error("missing source filter")
				}
				fmt.Fprint(w, `{"hits":{"hits":[]}}`)
			})
			args := map[string]any{"body": map[string]any{"query": map[string]any{"match_all": map[string]any{}}}, "source": []any{"name"}}
			if size != nil {
				args["size"] = size
			}
			result, err := u.CallTool(context.Background(), "search", args)
			if err != nil || len(result.Content) != 1 {
				t.Fatal(err)
			}
			if d.calls == 0 {
				t.Fatal("dialer not used")
			}
		})
	}
}

func TestSearchRejectsEscapes(t *testing.T) {
	bodies := []map[string]any{
		{"pit": map[string]any{"id": "x"}}, {"scroll": "1m"}, {"runtime_mappings": map[string]any{}},
		{"query": map[string]any{"terms": map[string]any{"user": map[string]any{"index": "other", "id": "1", "path": "users"}}}},
		{"query": map[string]any{"more_like_this": map[string]any{"like": []any{map[string]any{"_index": "other", "_id": "one"}}}}}, {"size": -1}, {"size": 1.5}, {"_source": map[string]any{"bad": "x"}},
		{"query": map[string]any{"wrapper": map[string]any{"query": "encoded"}}},
	}
	for _, body := range bodies {
		t.Run(fmt.Sprint(body), func(t *testing.T) {
			u, _ := fake(t, engine.DataAccessRead, nil, func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/_resolve/") {
					t.Fatal("unsafe search forwarded")
				}
				resolved(w, "logs-a")
			})
			if _, err := u.CallTool(context.Background(), "search", map[string]any{"body": body}); err == nil {
				t.Fatal("unsafe body allowed")
			}
		})
	}
}

func TestESQL(t *testing.T) {
	queries := []struct {
		query  string
		reject bool
	}{
		{"FROM logs-* | WHERE count > 1 | LIMIT 900", false}, {"from logs-a", false},
		{"FROM .security", true}, {"FROM other", true}, {"FROM `logs-*`", true}, {"FROM logs-* METADATA _id", true},
		{"ROW x=1", true}, {"FROM logs-* | LOOKUP JOIN other ON id", true}, {"FROM logs-* | ENRICH secret ON id", true},
		{"FROM logs-* | FROM other", true}, {"FROM logs-* /* comment */", true}, {"FROM logs-*; FROM other", true},
	}
	for _, tt := range queries {
		t.Run(tt.query, func(t *testing.T) {
			called := false
			u, _ := fake(t, engine.DataAccessRead, []string{"logs-*"}, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/_resolve/") {
					if strings.HasSuffix(r.URL.Path, "other") {
						resolved(w, "other")
					} else {
						resolved(w, "logs-a")
					}
					return
				}
				called = true
				var b map[string]any
				json.NewDecoder(r.Body).Decode(&b)
				q, _ := b["query"].(string)
				if !strings.HasPrefix(q, "FROM logs-a") || !strings.HasSuffix(q, "| LIMIT 500") || r.URL.Path != "/_query" {
					t.Error(b)
				}
				fmt.Fprint(w, `{"columns":[],"values":[]}`)
			})
			_, err := u.CallTool(context.Background(), "esql_query", map[string]any{"query": tt.query})
			if (err != nil) != tt.reject || called == tt.reject {
				t.Fatalf("error %v called %v", err, called)
			}
		})
	}
}

func TestOperations(t *testing.T) {
	for _, name := range []string{"mapping_get", "count", "shards_get", "index_stats", "doc_index", "doc_update", "delete_by_query"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			u, _ := fake(t, engine.DataAccessReadWrite, nil, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/_resolve/") {
					resolved(w, "logs-a")
					return
				}
				calls++
				want := map[string]string{"mapping_get": "/logs-a/_mapping", "count": "/logs-a/_count", "shards_get": "/logs-a/_search_shards", "index_stats": "/logs-a/_stats", "doc_index": "/logs-a/_doc/one", "doc_update": "/logs-a/_update/one", "delete_by_query": "/logs-a/_delete_by_query"}[name]
				if r.URL.Path != want {
					t.Error(r.URL.Path)
				}
				if name == "delete_by_query" && r.URL.Query().Get("max_docs") != "500" {
					t.Error("unbounded deletion")
				}
				fmt.Fprint(w, `{"ok":true}`)
			})
			args := map[string]any{"index": "logs-a"}
			if name == "doc_index" || name == "doc_update" {
				args["id"] = "one"
				args["document"] = map[string]any{"message": "hello"}
			}
			if _, err := u.CallTool(context.Background(), name, args); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal(calls)
			}
		})
	}
}

func TestErrorsAreSanitizedAndBounded(t *testing.T) {
	for _, mode := range []string{"status", "large", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			u, _ := fake(t, engine.DataAccessRead, nil, func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "status":
					w.WriteHeader(500)
					fmt.Fprint(w, `{"error":"https://reader:secret@example.invalid"}`)
				case "large":
					fmt.Fprint(w, strings.Repeat("x", maxResponse+1))
				case "invalid":
					fmt.Fprint(w, "not json")
				}
			})
			_, err := u.CallTool(context.Background(), "indices_list", nil)
			if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "https://") {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenRejectsUnsafeEndpoints(t *testing.T) {
	for _, endpoint := range []string{"http://example.invalid", "https://example.invalid/path", "https://example.invalid?secret=x", "https://example.invalid#x", ":bad"} {
		if _, err := Open(context.Background(), datamcp.NativeInput{Credentials: engine.DataAccessCredentials{URI: endpoint}, Level: engine.DataAccessRead}); err == nil {
			t.Fatal(endpoint)
		}
	}
	if _, err := Open(context.Background(), datamcp.NativeInput{Credentials: engine.DataAccessCredentials{URI: "https://example.invalid", CAPEM: []byte("invalid")}, Level: engine.DataAccessRead}); err == nil {
		t.Fatal("invalid CA allowed")
	}
}

func TestTLSVerification(t *testing.T) {
	u, _ := fake(t, engine.DataAccessRead, nil, func(w http.ResponseWriter, r *http.Request) { resolved(w, "logs-a") })
	u.transport.TLSClientConfig.RootCAs = nil
	if _, err := u.CallTool(context.Background(), "indices_list", nil); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestTLSHostnameVerification(t *testing.T) {
	u, _ := fake(t, engine.DataAccessRead, nil, func(w http.ResponseWriter, r *http.Request) { resolved(w, "logs-a") })
	u.transport.TLSClientConfig.ServerName = "wrong.invalid"
	if _, err := u.CallTool(context.Background(), "indices_list", nil); err == nil {
		t.Fatal("certificate hostname mismatch accepted")
	}
}

func TestAliasBackingAttributes(t *testing.T) {
	u, _ := fake(t, engine.DataAccessRead, []string{"logs-*"}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_resolve/index/logs-alias" {
			fmt.Fprint(w, `{"aliases":[{"name":"logs-alias","indices":["logs-hidden"]}]}`)
			return
		}
		fmt.Fprint(w, `{"indices":[{"name":"logs-hidden","attributes":["hidden"]}]}`)
	})
	if _, err := u.CallTool(context.Background(), "mapping_get", map[string]any{"index": "logs-alias"}); err == nil {
		t.Fatal("hidden alias target accepted")
	}
}

func TestInvalidArguments(t *testing.T) {
	u, _ := fake(t, engine.DataAccessReadWrite, nil, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/_resolve/") {
			t.Error("invalid arguments forwarded")
		}
		resolved(w, "logs-a")
	})
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"unknown", nil},
		{"search", map[string]any{"scroll": "1m"}},
		{"search", map[string]any{"index": 42}},
		{"search", map[string]any{"body": "bad"}},
		{"esql_query", nil},
		{"doc_index", map[string]any{"index": "logs-a", "id": "../secret", "document": map[string]any{}}},
		{"doc_update", map[string]any{"index": "logs-*", "id": "one", "document": map[string]any{}}},
	} {
		if _, err := u.CallTool(context.Background(), tc.name, tc.args); err == nil {
			t.Fatalf("accepted %s %v", tc.name, tc.args)
		}
	}
}

func TestAliasExecutionPreservesFiltersAndRouting(t *testing.T) {
	for _, name := range []string{"search", "count", "delete_by_query", "esql_query", "doc_index", "doc_update"} {
		t.Run(name, func(t *testing.T) {
			executed := false
			u, _ := fake(t, engine.DataAccessReadWrite, []string{"logs-*"}, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/_resolve/index/logs-tenant":
					fmt.Fprint(w, `{"aliases":[{"name":"logs-tenant","indices":["logs-shared"]}]}`)
				case "/_resolve/index/logs-shared":
					resolved(w, "logs-shared")
				default:
					executed = true
					if name == "esql_query" {
						var b map[string]any
						json.NewDecoder(r.Body).Decode(&b)
						if !strings.HasPrefix(b["query"].(string), "FROM logs-tenant ") {
							t.Error(b)
						}
					} else if !strings.HasPrefix(r.URL.Path, "/logs-tenant/") {
						t.Errorf("alias replaced with backing index: %s", r.URL.Path)
					}
					fmt.Fprint(w, `{"ok":true}`)
				}
			})
			args := map[string]any{"index": "logs-tenant"}
			if name == "esql_query" {
				args = map[string]any{"query": "FROM logs-tenant"}
			}
			if name == "doc_index" || name == "doc_update" {
				args["id"] = "one"
				args["document"] = map[string]any{"message": "hello"}
			}
			if _, err := u.CallTool(context.Background(), name, args); err != nil {
				t.Fatal(err)
			}
			if !executed {
				t.Fatal("operation not executed")
			}
		})
	}
}

func TestDSLPositionValidation(t *testing.T) {
	tests := []struct {
		name, body string
		reject     bool
	}{
		{"index field", `{"query":{"term":{"index":"one"}}}`, false},
		{"script field", `{"query":{"match":{"script":"hello"}}}`, false},
		{"indices field", `{"query":{"terms":{"indices":["one","two"]}}}`, false},
		{"sort script field", `{"sort":{"script":"asc"}}`, false},
		{"sort index field", `{"sort":[{"index":"desc"}]}`, false},
		{"aggregation names", `{"aggs":{"indices":{"terms":{"field":"index"},"aggs":{"script":{"max":{"field":"script"}}}}}}`, false},
		{"filter field", `{"aggs":{"index":{"filter":{"term":{"script":"value"}}}}}`, false},
		{"inline similarity", `{"query":{"more_like_this":{"like":["text",{"doc":{"index":"value"}}]}}}`, false},
		{"inline percolate", `{"query":{"percolate":{"field":"query","document":{"index":"one","script":"text"}}}}`, false},
		{"shape fields", `{"query":{"geo_shape":{"index":{"shape":{"type":"point","coordinates":[0,0]}}}}}`, false},
		{"script query", `{"query":{"script":{"script":"return true"}}}`, true},
		{"script score", `{"query":{"script_score":{"query":{"match_all":{}},"script":"1"}}}`, true},
		{"function script", `{"query":{"function_score":{"functions":[{"script_score":{"script":"1"}}]}}}`, true},
		{"terms lookup", `{"query":{"terms":{"user":{"index":"other","id":"one","path":"users"}}}}`, true},
		{"nested lookup", `{"query":{"bool":{"filter":[{"terms":{"user":{"index":"other","id":"one","path":"users"}}}]}}}`, true},
		{"similarity like", `{"query":{"more_like_this":{"like":[{"_index":"other","_id":"one"}]}}}`, true},
		{"similarity unlike", `{"query":{"more_like_this":{"unlike":{"_index":"other","_id":"one"}}}}`, true},
		{"percolate index", `{"query":{"percolate":{"index":"other","id":"one"}}}`, true},
		{"indexed shape", `{"query":{"geo_shape":{"location":{"indexed_shape":{"index":"other","id":"one","path":"location"}}}}}`, true},
		{"encoded wrapper", `{"query":{"wrapper":{"query":"encoded"}}}`, true},
		{"sort script", `{"sort":[{"_script":{"script":"1","type":"number"}}]}`, true},
		{"nested sort lookup", `{"sort":[{"field":{"nested":{"path":"items","filter":{"terms":{"field":{"index":"other","id":"one","path":"field"}}}}}}]}`, true},
		{"aggregation script", `{"aggs":{"script":{"terms":{"script":"1"}}}}`, true},
		{"scripted metric", `{"aggs":{"index":{"scripted_metric":{"map_script":"1"}}}}`, true},
		{"filter lookup", `{"aggs":{"index":{"filter":{"terms":{"field":{"index":"other","id":"one","path":"field"}}}}}}`, true},
		{"filters lookup", `{"aggs":{"index":{"filters":{"filters":{"tenant":{"terms":{"field":{"index":"other","id":"one","path":"field"}}}}}}}}`, true},
		{"background lookup", `{"aggs":{"index":{"significant_terms":{"field":"index","background_filter":{"terms":{"field":{"index":"other","id":"one","path":"field"}}}}}}}`, true},
		{"top hits script", `{"aggs":{"index":{"top_hits":{"script_fields":{"x":{"script":"1"}}}}}}`, true},
		{"runtime mapping", `{"runtime_mappings":{"index":{"type":"keyword","script":"emit('one')"}}}`, true},
		{"script fields", `{"script_fields":{"index":{"script":"1"}}}`, true},
		{"multi terms script", `{"aggs":{"indices":{"multi_terms":{"terms":[{"script":"1"}]}}}}`, true},
		{"composite script", `{"aggs":{"indices":{"composite":{"sources":[{"index":{"terms":{"script":"1"}}}]}}}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(tt.body), &body); err != nil {
				t.Fatal(err)
			}
			if got := unsafeSearch(body); got != tt.reject {
				t.Fatalf("unsafeSearch=%v want %v", got, tt.reject)
			}
			forwarded := false
			u, _ := fake(t, engine.DataAccessRead, nil, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/_resolve/") {
					resolved(w, "logs-a")
					return
				}
				forwarded = true
				fmt.Fprint(w, `{"hits":{"hits":[]}}`)
			})
			_, err := u.CallTool(context.Background(), "search", map[string]any{"body": body})
			if (err != nil) != tt.reject || forwarded == tt.reject {
				t.Fatalf("error=%v forwarded=%v", err, forwarded)
			}
		})
	}
}
