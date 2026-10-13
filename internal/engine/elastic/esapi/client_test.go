// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	s := httptest.NewTLSServer(h)
	t.Cleanup(s.Close)
	c, err := New(Config{Endpoints: []string{s.URL}, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), Username: "admin", Password: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, s
}
func TestRESTMethods(t *testing.T) {
	ctx := context.Background()
	enabled := true
	type testCase struct {
		name, method, path string
		q                  url.Values
		body, response     string
		call               func(*Client) error
	}
	cases := []testCase{
		{name: "info", method: "GET", path: "/", response: `{"name":"n1","cluster_name":"cluster","cluster_uuid":"uuid","version":{"number":"9.5.5"}}`, call: func(c *Client) error {
			v, e := c.Info(ctx)
			if v.Name != "n1" || v.ClusterName != "cluster" || v.ClusterUUID != "uuid" || v.Version.Number != "9.5.5" {
				t.Errorf("info: %+v", v)
			}
			return e
		}},
		{name: "health", method: "GET", path: "/_cluster/health", q: url.Values{"wait_for_status": {"yellow"}, "timeout": {"1500ms"}, "local": {"true"}}, response: `{"status":"yellow","number_of_nodes":3,"number_of_data_nodes":2,"active_primary_shards":4,"active_shards":6,"relocating_shards":1,"initializing_shards":2,"unassigned_shards":3,"timed_out":true}`, call: func(c *Client) error {
			v, e := c.Health(ctx, HealthOptions{"yellow", 1500 * time.Millisecond, true})
			if v != (Health{"yellow", 3, 2, 4, 6, 1, 2, 3, true}) {
				t.Errorf("health: %+v", v)
			}
			return e
		}},
		{name: "nodes", method: "GET", path: "/_nodes", q: url.Values{"filter_path": {"nodes.*.name,nodes.*.version,nodes.*.roles,nodes.*.host,nodes.*.ip"}}, response: `{"nodes":{"id":{"name":"n1","version":"9.5.5","roles":["master","data"]}}}`, call: func(c *Client) error {
			v, e := c.Nodes(ctx)
			if len(v) != 1 || v[0].ID != "id" || v[0].Name != "n1" || len(v[0].Roles) != 2 {
				t.Errorf("nodes: %+v", v)
			}
			return e
		}},
		{name: "master", method: "GET", path: "/_cat/master", q: url.Values{"format": {"json"}}, response: `[{"node":"n1"}]`, call: func(c *Client) error {
			v, e := c.MasterNode(ctx)
			if v != "n1" {
				t.Error(v)
			}
			return e
		}},
		{name: "shards", method: "GET", path: "/_cat/allocation", q: url.Values{"format": {"json"}, "bytes": {"b"}}, response: `[{"node":"n1","shards":"3"},{"node":"UNASSIGNED","shards":"2"}]`, call: func(c *Client) error {
			v, e := c.ShardCounts(ctx)
			if !reflect.DeepEqual(v, map[string]int{"n1": 3}) {
				t.Error(v)
			}
			return e
		}},
		{name: "settings", method: "GET", path: "/_cluster/settings", q: url.Values{"flat_settings": {"true"}}, response: `{"persistent":{"a":"b"},"transient":{"a":"c"}}`, call: func(c *Client) error {
			v, e := c.ClusterSettings(ctx)
			if v.Persistent["a"] != "b" || v.Transient["a"] != "c" {
				t.Error(v)
			}
			return e
		}},
		{name: "put-settings", method: "PUT", path: "/_cluster/settings", body: `{"persistent":{"a":null},"transient":{"b":false}}`, call: func(c *Client) error {
			return c.PutClusterSettings(ctx, map[string]any{"a": nil}, map[string]any{"b": false})
		}},
		{name: "allocation", method: "PUT", path: "/_cluster/settings", body: `{"persistent":{"cluster.routing.allocation.enable":"primaries"}}`, call: func(c *Client) error { v := "primaries"; return c.SetAllocationEnable(ctx, &v) }},
		{name: "reset-allocation", method: "PUT", path: "/_cluster/settings", body: `{"persistent":{"cluster.routing.allocation.enable":null}}`, call: func(c *Client) error { return c.SetAllocationEnable(ctx, nil) }},
		{name: "exclude-get", method: "GET", path: "/_cluster/settings", q: url.Values{"flat_settings": {"true"}}, response: `{"persistent":{"cluster.routing.allocation.exclude._name":"n1"},"transient":{"cluster.routing.allocation.exclude._name":"n2, n3"}}`, call: func(c *Client) error {
			v, e := c.AllocationExcludeNames(ctx)
			if !reflect.DeepEqual(v, []string{"n1", "n2", "n3"}) {
				t.Error(v)
			}
			return e
		}},
		{name: "exclude-set", method: "PUT", path: "/_cluster/settings", body: `{"persistent":{"cluster.routing.allocation.exclude._name":"n1,n2"},"transient":{"cluster.routing.allocation.exclude._name":null}}`, call: func(c *Client) error { return c.SetAllocationExcludeNames(ctx, []string{"n1", "n2"}) }},
		{name: "exclude-reset", method: "PUT", path: "/_cluster/settings", body: `{"persistent":{"cluster.routing.allocation.exclude._name":null},"transient":{"cluster.routing.allocation.exclude._name":null}}`, call: func(c *Client) error { return c.SetAllocationExcludeNames(ctx, nil) }},
		{name: "flush", method: "POST", path: "/_flush", call: func(c *Client) error { return c.Flush(ctx) }},
		{name: "vote", method: "POST", path: "/_cluster/voting_config_exclusions", q: url.Values{"node_names": {"n1,n2"}, "timeout": {"2000ms"}}, call: func(c *Client) error { return c.AddVotingExclusion(ctx, []string{"n1", "n2"}, 2*time.Second) }},
		{name: "clear-vote", method: "DELETE", path: "/_cluster/voting_config_exclusions", q: url.Values{"wait_for_removal": {"false"}}, call: func(c *Client) error { return c.ClearVotingExclusions(ctx, false) }},
		{name: "password", method: "POST", path: "/_security/user/user%2Fname/_password", body: `{"password":"new-secret"}`, call: func(c *Client) error { return c.ChangePassword(ctx, "user/name", "new-secret") }},
		{name: "user-put", method: "PUT", path: "/_security/user/u", body: `{"roles":["r"],"enabled":true}`, call: func(c *Client) error { return c.PutUser(ctx, "u", User{Roles: []string{"r"}, Enabled: &enabled}) }},
		{name: "user-get", method: "GET", path: "/_security/user/u,v", response: `{"u":{"roles":["r"],"enabled":true,"password":"never-return"}}`, call: func(c *Client) error {
			v, e := c.GetUsers(ctx, "u", "v")
			if v["u"].Password != "" || !(*v["u"].Enabled) {
				t.Error(v)
			}
			return e
		}},
		{name: "user-delete", method: "DELETE", path: "/_security/user/u", call: func(c *Client) error { return c.DeleteUser(ctx, "u") }},
		{name: "role-put", method: "PUT", path: "/_security/role/r", body: `{"cluster":["monitor"],"indices":[{"names":["*"],"privileges":["read"],"allow_restricted_indices":false}]}`, call: func(c *Client) error {
			return c.PutRole(ctx, "r", Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: []string{"*"}, Privileges: []string{"read"}}}})
		}},
		{name: "role-get", method: "GET", path: "/_security/role", response: `{"r":{"cluster":["monitor"],"indices":[]}}`, call: func(c *Client) error {
			v, e := c.GetRoles(ctx)
			if len(v["r"].Cluster) != 1 {
				t.Error(v)
			}
			return e
		}},
		{name: "role-delete", method: "DELETE", path: "/_security/role/r", call: func(c *Client) error { return c.DeleteRole(ctx, "r") }},
		{name: "token-create", method: "POST", path: "/_security/service/elastic/kibana/credential/token/test", response: `{"token":{"value":"token-secret"}}`, call: func(c *Client) error {
			v, e := c.CreateServiceToken(ctx, "elastic", "kibana", "test")
			if v != "token-secret" {
				t.Error(v)
			}
			return e
		}},
		{name: "token-delete", method: "DELETE", path: "/_security/service/elastic/kibana/credential/token/test", call: func(c *Client) error { return c.DeleteServiceToken(ctx, "elastic", "kibana", "test") }},
		{name: "authenticate", method: "GET", path: "/_security/_authenticate", response: `{"username":"elastic/kibana","authentication_type":"token"}`, call: func(c *Client) error {
			v, e := c.Authenticate(ctx)
			if v.Username != "elastic/kibana" || v.AuthenticationType != "token" {
				t.Error(v)
			}
			return e
		}},
		{name: "key-create", method: "POST", path: "/_security/api_key", body: `{"name":"key","expiration":"1h"}`, response: `{"id":"id","name":"key","api_key":"secret","encoded":"encoded","expiration":42}`, call: func(c *Client) error {
			v, e := c.CreateAPIKey(ctx, APIKeyRequest{Name: "key", Expiration: "1h"})
			if v.ID != "id" || v.APIKey != "secret" || v.Encoded != "encoded" || v.Expiration != 42 {
				t.Error(v)
			}
			return e
		}},
		{name: "key-list", method: "GET", path: "/_security/api_key", q: url.Values{"owner": {"true"}, "name": {"key*"}}, response: `{"api_keys":[{"id":"id","name":"key","creation":1,"expiration":2,"invalidated":false,"username":"u","realm":"native"}]}`, call: func(c *Client) error {
			v, e := c.GetAPIKeys(ctx, true, "key*")
			if len(v) != 1 || v[0].Creation != 1 || v[0].Realm != "native" {
				t.Error(v)
			}
			return e
		}},
		{name: "key-invalidate", method: "DELETE", path: "/_security/api_key", body: `{"ids":["id"]}`, response: `{"error_count":0}`, call: func(c *Client) error { return c.InvalidateAPIKeys(ctx, []string{"id"}) }},
		{name: "reload", method: "POST", path: "/_nodes/reload_secure_settings", response: `{"nodes":{"id":{"name":"n1"}}}`, call: func(c *Client) error { return c.ReloadSecureSettings(ctx) }},
		{name: "repo-put", method: "PUT", path: "/_snapshot/repo", q: url.Values{"verify": {"false"}}, body: `{"type":"fs","settings":{"location":"backup"}}`, call: func(c *Client) error {
			return c.PutRepository(ctx, "repo", Repository{"fs", map[string]any{"location": "backup"}}, false)
		}},
		{name: "repo-get", method: "GET", path: "/_snapshot/repo", response: `{"repo":{"type":"fs","settings":{"location":"backup"}}}`, call: func(c *Client) error {
			v, e := c.GetRepository(ctx, "repo")
			if v.Type != "fs" {
				t.Error(v)
			}
			return e
		}},
		{name: "repo-verify", method: "POST", path: "/_snapshot/repo/_verify", response: `{"nodes":{"id":{"name":"n1"}}}`, call: func(c *Client) error {
			v, e := c.VerifyRepository(ctx, "repo")
			if !reflect.DeepEqual(v, []string{"n1"}) {
				t.Error(v)
			}
			return e
		}},
		{name: "repo-analyze", method: "POST", path: "/_snapshot/repo/_analyze", q: url.Values{"blob_count": {"10"}, "max_blob_size": {"1mb"}}, response: `{"blob_count":10}`, call: func(c *Client) error {
			v, e := c.AnalyzeRepository(ctx, "repo", map[string]string{"blob_count": "10", "max_blob_size": "1mb"})
			if v["blob_count"] != float64(10) {
				t.Error(v)
			}
			return e
		}},
		{name: "snapshot-create", method: "PUT", path: "/_snapshot/repo/snap", q: url.Values{"wait_for_completion": {"true"}}, body: `{"indices":"docs","include_global_state":false,"feature_states":["none"]}`, call: func(c *Client) error {
			return c.CreateSnapshot(ctx, "repo", "snap", SnapshotRequest{Indices: "docs", FeatureStates: []string{"none"}}, true)
		}},
		{name: "snapshot-get", method: "GET", path: "/_snapshot/repo/snap", response: `{"snapshots":[{"snapshot":"snap","uuid":"uuid","state":"SUCCESS","version":"9.5.5","indices":["docs"],"start_time":"2026-01-02T03:04:05.000Z","end_time":"2026-01-02T03:04:06.000Z","failures":[],"shards":{"total":1,"failed":0,"successful":1}}]}`, call: func(c *Client) error {
			v, e := c.GetSnapshot(ctx, "repo", "snap")
			if v.State != "SUCCESS" || v.EndTime.Sub(v.StartTime) != time.Second || v.Shards.Successful != 1 {
				t.Error(v)
			}
			return e
		}},
		{name: "snapshot-list", method: "GET", path: "/_snapshot/repo/_all", response: `{"snapshots":[]}`, call: func(c *Client) error { _, e := c.ListSnapshots(ctx, "repo"); return e }},
		{name: "snapshot-size", method: "GET", path: "/_snapshot/repo/snap/_status", response: `{"snapshots":[{"stats":{"total":{"size_in_bytes":123}}}]}`, call: func(c *Client) error {
			v, e := c.SnapshotSize(ctx, "repo", "snap")
			if v != 123 {
				t.Error(v)
			}
			return e
		}},
		{name: "restore", method: "POST", path: "/_snapshot/repo/snap/_restore", q: url.Values{"wait_for_completion": {"false"}}, body: `{"indices":"docs","include_global_state":false,"feature_states":["none"],"include_aliases":true,"ignore_unavailable":false}`, call: func(c *Client) error {
			return c.RestoreSnapshot(ctx, "repo", "snap", RestoreRequest{Indices: "docs", FeatureStates: []string{"none"}, IncludeAliases: true})
		}},
		{name: "recovery", method: "GET", path: "/_cat/recovery", q: url.Values{"active_only": {"true"}, "format": {"json"}}, response: `[{"index":"docs"}]`, call: func(c *Client) error {
			v, e := c.RecoveryInProgress(ctx)
			if !v {
				t.Error("no recovery")
			}
			return e
		}},
		{name: "snapshot-delete", method: "DELETE", path: "/_snapshot/repo/snap", call: func(c *Client) error { return c.DeleteSnapshot(ctx, "repo", "snap") }},
		{name: "repo-delete", method: "DELETE", path: "/_snapshot/repo", call: func(c *Client) error { return c.DeleteRepository(ctx, "repo") }},
		{name: "indices", method: "GET", path: "/_cat/indices", q: url.Values{"format": {"json"}, "expand_wildcards": {"all"}, "h": {"index"}}, response: `[{"index":"docs"}]`, call: func(c *Client) error {
			v, e := c.ListIndices(ctx, true)
			if !reflect.DeepEqual(v, []string{"docs"}) {
				t.Error(v)
			}
			return e
		}},
		{name: "system-indices", method: "GET", path: "/_cluster/state/metadata", q: url.Values{"filter_path": {"metadata.indices.*.system"}}, response: `{"metadata":{"indices":{".security-7":{"system":true},".tenant-cache":{"system":false},"hidden-user":{"system":false}}}}`, call: func(c *Client) error {
			v, e := c.SystemIndices(ctx)
			if !reflect.DeepEqual(v, map[string]bool{".security-7": true, ".tenant-cache": false, "hidden-user": false}) {
				t.Error(v)
			}
			return e
		}},
		{name: "streams-delete", method: "DELETE", path: "/_data_stream/%2A", q: url.Values{"expand_wildcards": {"all"}}, call: func(c *Client) error { return c.DeleteDataStreams(ctx, "*") }},
		{name: "indices-delete", method: "DELETE", path: "/docs,logs", call: func(c *Client) error { return c.DeleteIndices(ctx, []string{"docs", "logs"}) }},
		{name: "indices-close", method: "POST", path: "/docs,logs/_close", call: func(c *Client) error { return c.CloseIndices(ctx, []string{"docs", "logs"}) }},
		{name: "index", method: "PUT", path: "/docs/_doc/a%2Fb", q: url.Values{"refresh": {"true"}}, body: `{"hello":"world"}`, call: func(c *Client) error { return c.Index(ctx, "docs", "a/b", map[string]string{"hello": "world"}, true) }},
		{name: "search", method: "POST", path: "/docs/_search", q: url.Values{"track_total_hits": {"true"}}, body: `{"query":{"match_all":{}}}`, response: `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_source":{"hello":"world"}}]}}`, call: func(c *Client) error {
			n, v, e := c.Search(ctx, "docs", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
			if n != 1 || len(v) != 1 || string(v[0]) != `{"hello":"world"}` {
				t.Error(n, v)
			}
			return e
		}},
		{name: "count", method: "GET", path: "/docs/_count", response: `{"count":12}`, call: func(c *Client) error {
			v, e := c.Count(ctx, "docs")
			if v != 12 {
				t.Error(v)
			}
			return e
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called atomic.Bool
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				called.Store(true)
				if r.Method != tc.method || r.URL.EscapedPath() != tc.path || r.URL.Query().Encode() != tc.q.Encode() {
					t.Errorf("request: %s %s, expected %s %s?%s", r.Method, r.URL.String(), tc.method, tc.path, tc.q.Encode())
				}
				user, password, ok := r.BasicAuth()
				if !ok || user != "admin" || password != "test-secret" {
					t.Error("missing basic authentication")
				}
				if r.Header.Get("Accept") != "application/json" {
					t.Error("missing accept")
				}
				data, _ := io.ReadAll(r.Body)
				if tc.body != "" {
					var got, want any
					if json.Unmarshal(data, &got) != nil || json.Unmarshal([]byte(tc.body), &want) != nil || !reflect.DeepEqual(got, want) {
						t.Errorf("body %s, expected %s", data, tc.body)
					}
					if r.Header.Get("Content-Type") != "application/json" {
						t.Error("missing content type")
					}
				} else if len(data) != 0 {
					t.Error("unexpected body")
				}
				response := tc.response
				if response == "" {
					response = "{}"
				}
				fmt.Fprint(w, response)
			})
			if err := tc.call(c); err != nil {
				t.Fatal(err)
			}
			if !called.Load() {
				t.Error("request not made")
			}
		})
	}
}

func TestErrorsAndFailover(t *testing.T) {
	for _, status := range []int{400, 404, 409, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"type":"test_exception","reason":"test-secret admin new-secret"}}`)
			})
			var second atomic.Int32
			other := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { second.Add(1) }))
			defer other.Close()
			u, _ := url.Parse(other.URL)
			c.endpoints = append(c.endpoints, u)
			err := c.ChangePassword(context.Background(), "u", "new-secret")
			var e *Error
			if !errors.As(err, &e) || e.Status != status || e.Type != "test_exception" {
				t.Fatalf("error: %v", err)
			}
			if strings.Contains(e.Reason, "secret") || strings.Contains(e.Reason, "admin") {
				t.Fatal("credential leaked")
			}
			if IsNotFound(err) != (status == 404) || IsConflict(fmt.Errorf("wrapped: %w", err)) != (status == 409) {
				t.Error("classification")
			}
			if second.Load() != 0 {
				t.Error("HTTP error caused failover")
			}
			_ = s
		})
	}
	c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"name":"reachable"}`) })
	first, _ := url.Parse("https://unreachable.example:9200")
	c.endpoints = append([]*url.URL{first}, c.endpoints...)
	tr := c.http.Transport.(*http.Transport)
	realDial := tr.DialContext
	var attempts atomic.Int32
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		attempts.Add(1)
		if strings.HasPrefix(addr, "unreachable.example:") {
			return nil, errors.New("dial failure test-secret")
		}
		return realDial(ctx, network, addr)
	}
	v, err := c.Info(context.Background())
	if err != nil || v.Name != "reachable" || attempts.Load() != 2 {
		t.Fatal(v, err, attempts.Load())
	}
	c.bearer = "bearer-secret"
	c.username = "ignored"
	s.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer bearer-secret" {
			t.Error("bearer did not override basic")
		}
		fmt.Fprint(w, `{"username":"elastic/kibana","authentication_type":"token"}`)
	})
	if _, err = c.Authenticate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTLSAndConfig(t *testing.T) {
	c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) })
	tr := c.http.Transport.(*http.Transport)
	expectedRoots := x509.NewCertPool()
	expectedRoots.AddCert(s.Certificate())
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 || tr.TLSClientConfig.InsecureSkipVerify || tr.TLSClientConfig.RootCAs == nil || !tr.TLSClientConfig.RootCAs.Equal(expectedRoots) {
		t.Error("unpinned TLS")
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
	for _, endpoint := range []string{"http://example.com", "https://u:secret@example.com", "https://example.com/path", "https://example.com?x=y", "https://example.com/#fragment", "invalid"} {
		if _, err := New(Config{Endpoints: []string{endpoint}, CAPEM: ca}); err == nil {
			t.Errorf("accepted %s", endpoint)
		}
	}
	if _, err := New(Config{Endpoints: []string{s.URL}, CAPEM: []byte("bad")}); err == nil {
		t.Error("accepted invalid CA")
	}
	u, _ := url.Parse(s.URL)
	u.Host = net.JoinHostPort("localhost", u.Port())
	c.endpoints = []*url.URL{u}
	if _, err := c.Info(context.Background()); err == nil {
		t.Error("accepted invalid hostname SAN")
	}
}

func TestNodeFailuresAndBatches(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_nodes/reload_secure_settings":
			fmt.Fprint(w, `{"nodes":{"id":{"reload_exception":{"reason":"sensitive-setting"}}}}`)
		case "/_security/api_key":
			fmt.Fprint(w, `{"error_count":1}`)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	})
	if err := c.ReloadSecureSettings(context.Background()); err == nil || strings.Contains(err.Error(), "sensitive-setting") {
		t.Error("reload exception missing or leaked")
	}
	if err := c.InvalidateAPIKeys(context.Background(), []string{"id"}); err == nil {
		t.Error("per-key failure ignored")
	}
	if err := c.DeleteIndices(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	var count atomic.Int32
	var seen []string
	batch, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if len(r.URL.String()) > 4096 {
			t.Error("unbounded URL")
		}
		seen = append(seen, strings.Split(strings.TrimPrefix(r.URL.Path, "/"), ",")...)
		fmt.Fprint(w, `{}`)
	})
	names := make([]string, 100)
	for i := range names {
		names[i] = fmt.Sprintf("i%d-%s", i, strings.Repeat("a", 100))
	}
	if err := batch.DeleteIndices(context.Background(), names); err != nil {
		t.Fatal(err)
	}
	if count.Load() < 2 || !reflect.DeepEqual(seen, names) {
		t.Error("incorrect batches")
	}
}

func TestNoReplayAfterConnection(t *testing.T) {
	var first, second atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	})
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer other.Close()
	u, _ := url.Parse(other.URL)
	c.endpoints = append(c.endpoints, u)
	err := c.ChangePassword(context.Background(), "u", "new-password")
	if err == nil || first.Load() != 1 || second.Load() != 0 {
		t.Fatalf("uncertain mutation replayed: %v, %d, %d", err, first.Load(), second.Load())
	}
}

func TestEmptyPrivilegePayloads(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		want := `{}`
		if strings.HasPrefix(r.URL.Path, "/_security/user/") {
			want = `{"roles":[]}`
		}
		if string(body) != want {
			t.Errorf("payload: %s, expected %s", body, want)
		}
		fmt.Fprint(w, `{}`)
	})
	if err := c.PutUser(context.Background(), "u", User{}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutRole(context.Background(), "r", Role{}); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedAndStringErrors(t *testing.T) {
	for _, body := range []string{`not JSON`, `{"error":"test-secret"}`} {
		c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, body)
		})
		_, err := c.Info(context.Background())
		var e *Error
		if !errors.As(err, &e) || e.Status != http.StatusForbidden || strings.Contains(err.Error(), "test-secret") {
			t.Fatalf("unsafe or untyped HTTP error: %v", err)
		}
	}
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `not JSON`) })
	if _, err := c.Info(context.Background()); err == nil {
		t.Fatal("accepted malformed success response")
	}
}

func TestMaxIndexReplicas(t *testing.T) {
	for _, tc := range []struct {
		name, settings string
		nodes, want    int
		fail           bool
	}{
		{"analyzer-arrays", `"index.number_of_replicas":"2","index.analysis.analyzer.custom.filter":["lowercase","stop"]`, 3, 2, false},
		{"numeric", `"index.number_of_replicas":3`, 4, 3, false},
		{"disabled", `"index.number_of_replicas":"2","index.auto_expand_replicas":false`, 3, 2, false},
		{"disabled-string", `"index.number_of_replicas":"2","index.auto_expand_replicas":"false"`, 3, 2, false},
		{"bounded", `"index.number_of_replicas":"1","index.auto_expand_replicas":"1-5"`, 3, 5, false},
		{"all", `"index.number_of_replicas":"0","index.auto_expand_replicas":"0-all"`, 4, 3, false},
		{"single-all", `"index.number_of_replicas":"0","index.auto_expand_replicas":"0-all"`, 1, 0, false},
		{"missing", `"index.analysis.filter.stop.stopwords":["a","the"]`, 3, 0, true},
		{"negative", `"index.number_of_replicas":"-1"`, 3, 0, true},
		{"invalid-range", `"index.number_of_replicas":"0","index.auto_expand_replicas":"3-1"`, 3, 0, true},
		{"invalid-all", `"index.number_of_replicas":"0","index.auto_expand_replicas":"x-all"`, 3, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/_all/_settings":
					if r.URL.Query().Get("filter_path") != "*.settings.index.number_of_replicas,*.settings.index.auto_expand_replicas" || r.URL.Query().Get("expand_wildcards") != "all" {
						t.Error("replica settings request is not restricted", r.URL)
					}
					fmt.Fprintf(w, `{"logs":{"settings":{%s}},"other":{"settings":{"index.number_of_replicas":"0"}}}`, tc.settings)
				case "/_cluster/health":
					fmt.Fprintf(w, `{"number_of_data_nodes":%d}`, tc.nodes)
				default:
					t.Error("unexpected request", r.URL)
					http.Error(w, "unexpected", 500)
				}
			})
			got, err := c.MaxIndexReplicas(context.Background())
			if (err != nil) != tc.fail || (!tc.fail && got != tc.want) {
				t.Fatalf("replicas=%d err=%v, want %d failure=%v", got, err, tc.want, tc.fail)
			}
		})
	}
}

func TestAllocationExclusionsMigrateTransient(t *testing.T) {
	key := "cluster.routing.allocation.exclude._name"
	persistent, transient := map[string]any{key: "old"}, map[string]any{key: "operator"}
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			var settings ClusterSettings
			if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
				t.Error(err)
			}
			if value, ok := settings.Transient[key]; !ok || value != nil {
				t.Error("transient override was not cleared", settings)
			}
			persistent, transient = settings.Persistent, settings.Transient
			delete(transient, key)
		}
		_ = json.NewEncoder(w).Encode(ClusterSettings{persistent, transient})
	})
	ctx := context.Background()
	names, err := c.AllocationExcludeNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.SetAllocationExcludeNames(ctx, append(names, "draining")); err != nil {
		t.Fatal(err)
	}
	names, err = c.AllocationExcludeNames(ctx)
	if err != nil || !reflect.DeepEqual(names, []string{"old", "operator", "draining"}) {
		t.Fatalf("effective exclusion: %v %v", names, err)
	}
	if err = c.SetAllocationExcludeNames(ctx, []string{"old", "operator"}); err != nil {
		t.Fatal(err)
	}
	names, err = c.AllocationExcludeNames(ctx)
	if err != nil || !reflect.DeepEqual(names, []string{"old", "operator"}) {
		t.Fatalf("operator exclusion lost: %v %v", names, err)
	}
}
