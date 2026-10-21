// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestDCSResetAtomicGroupIsolation(t *testing.T) {
	for _, formation := range []bool{false, true} {
		t.Run(map[bool]string{false: "group", true: "formation"}[formation], func(t *testing.T) {
			s := testSpec()
			s.Citus = &pgspec.Citus{Group: 7, Database: "postgres"}
			s.CAPEM, s.ServerCert = testTLS(t)
			pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var in map[string]any
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
					http.Error(w, "bad payload", 400)
					return
				}
				decode := func(v any) string {
					t.Helper()
					raw, err := base64.StdEncoding.DecodeString(v.(string))
					if err != nil {
						t.Fatal(err)
					}
					return string(raw)
				}
				if r.URL.Path == "/v3/kv/range" {
					if !formation || decode(in["key"]) != "/karakoram/test-scope/" {
						t.Errorf("unexpected namespace observation: %v", in)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"revision": "42"}})
					return
				}
				calls++
				if r.URL.Path != "/v3/kv/txn" {
					t.Errorf("unexpected DCS path %s", r.URL.Path)
				}
				compare := in["compare"].([]any)[0].(map[string]any)
				deletion := in["success"].([]any)[0].(map[string]any)["request_delete_range"].(map[string]any)
				expected := "/karakoram/test-scope/7/"
				if formation {
					expected = "/karakoram/test-scope/"
				}
				if decode(deletion["key"]) != expected || decode(deletion["range_end"]) != strings.TrimSuffix(expected, "/")+"0" {
					t.Errorf("unsafe reset interval %v", deletion)
				}
				if formation {
					if compare["target"] != "MOD" || compare["mod_revision"] != "43" || decode(compare["key"]) != expected {
						t.Errorf("missing formation revision fence: %v", compare)
					}
				} else {
					if compare["target"] != "VERSION" || compare["version"] != "0" || decode(compare["key"]) != expected+"leader" {
						t.Errorf("missing local-group leader fence: %v", compare)
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]bool{"succeeded": true})
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			s.Etcd = &pgspec.Etcd{Endpoints: []string{strings.TrimPrefix(server.URL, "https://")}}
			if err := resetDCSNamespace(context.Background(), s, formation); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("delete transactions=%d", calls)
			}
		})
	}
}

func TestDCSResetRefusesLeader(t *testing.T) {
	s := testSpec()
	s.Citus = &pgspec.Citus{Group: 1, Database: "postgres"}
	s.CAPEM, s.ServerCert = testTLS(t)
	pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/kv/range" {
			_ = json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"revision": "42"}, "kvs": []any{map[string]string{"key": base64.StdEncoding.EncodeToString([]byte("/karakoram/test-scope/9/leader"))}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"succeeded": false})
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	s.Etcd = &pgspec.Etcd{Endpoints: []string{strings.TrimPrefix(server.URL, "https://")}}
	for _, formation := range []bool{false, true} {
		if resetDCSNamespace(context.Background(), s, formation) == nil {
			t.Fatalf("reset accepted a held leader (formation=%v)", formation)
		}
	}
}

func TestEtcdRequestBlackholedFirstEndpoint(t *testing.T) {
	s := testSpec()
	s.Citus = &pgspec.Citus{Group: 1, Database: "postgres"}
	s.CAPEM, s.ServerCert = testTLS(t)
	pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	start := func(handler http.HandlerFunc) *httptest.Server {
		server := httptest.NewUnstartedServer(handler)
		server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
		server.StartTLS()
		t.Cleanup(server.Close)
		return server
	}
	entered := make(chan struct{})
	blackhole := start(func(w http.ResponseWriter, r *http.Request) {
		var input map[string]any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			return
		}
		close(entered)
		// No response until the client cancels this attempt. This is not a
		// timing-dependent sleep: without a per-attempt deadline it consumes
		// the entire parent deadline.
		<-r.Context().Done()
	})
	healthy := start(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-entered:
		default:
			t.Error("healthy endpoint tried before the blackhole")
		}
		if r.URL.Path != "/v3/kv/range" {
			t.Errorf("unexpected quorum path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"revision": "42"}})
	})
	s.Etcd = &pgspec.Etcd{Endpoints: []string{strings.TrimPrefix(blackhole.URL, "https://"), strings.TrimPrefix(healthy.URL, "https://")}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var out map[string]any
	if err := etcdRequest(ctx, s, "/v3/kv/range", map[string]any{"key": "bGVhZGVy", "serializable": false}, &out); err != nil {
		t.Fatalf("healthy coordinator unavailable after blackhole: %v", err)
	}
	if ctx.Err() != nil || out["header"].(map[string]any)["revision"] != "42" {
		t.Fatalf("healthy quorum response not received within status budget: %v", out)
	}
}
