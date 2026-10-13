// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type accessSecrets struct {
	engine.SecretStore
	values map[string][]byte
}

func (s accessSecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, key string) ([]byte, error) {
	v, ok := s.values[key]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
}

type accessDialer struct {
	target    string
	addresses []string
}

func (d *accessDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.addresses = append(d.addresses, address)
	if strings.HasPrefix(address, "unavailable:") {
		return nil, errors.New("unavailable member")
	}
	return (&net.Dialer{}).DialContext(ctx, network, d.target)
}
func accessContext(t *testing.T, srv *httptest.Server, mode, name string) (*engine.Context, *accessDialer) {
	t.Helper()
	u, e := url.Parse(srv.URL)
	if e != nil {
		t.Fatal(e)
	}
	d := &accessDialer{target: u.Host}
	c := &domain.Cluster{Name: "brokers", Spec: domain.ClusterSpec{TLS: domain.TLSConfig{Mode: mode}}, Internal: json.RawMessage(fmt.Sprintf(`{"members":[{"name":%q,"stage":"ready","tlsMode":%q}]}`, name, mode))}
	secrets := accessSecrets{values: map[string][]byte{"users/karakoram": []byte("control-secret"), "users/admin": []byte("customer-secret")}}
	if srv.TLS != nil {
		secrets.values["tls/ca.crt"] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	}
	return &engine.Context{Cluster: c, Dialer: d, Secrets: secrets}, d
}

func TestAccessValidation(t *testing.T) {
	for _, name := range []string{"", "bad\nname", strings.Repeat("x", 256), string([]byte{0xff})} {
		if !errors.Is(accessName(name), engine.ErrInvalid) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"/", "app/events", "app user", "µ"} {
		if e := accessName(name); e != nil {
			t.Fatal(e)
		}
	}
	p := New(Options{})
	rc := &engine.Context{}
	for _, name := range []string{"admin", "karakoram", "guest", "monitor"} {
		if _, e := p.CreateUser(t.Context(), rc, UserWrite{Name: name}); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(e)
		}
		if e := p.UpdateUser(t.Context(), rc, UserWrite{Name: name}); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(e)
		}
		if e := p.DeleteUser(t.Context(), rc, name); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(e)
		}
		if e := p.DeletePermission(t.Context(), rc, "/", name); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(e)
		}
		if e := p.DeleteTopicPermission(t.Context(), rc, "/", name); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(e)
		}
	}
	for _, tag := range []string{"management", "policymaker", "monitoring", "administrator", "impersonator"} {
		if e := userValidation(UserWrite{Name: "app", Tags: []string{tag}}); e != nil {
			t.Fatal(e)
		}
	}
	if e := userValidation(UserWrite{Name: "app", Tags: []string{"root"}}); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
	pw, hash := "password", "hash"
	if e := userValidation(UserWrite{Name: "app", Password: &pw, PasswordHash: &hash}); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
	for _, pattern := range []string{"[", "(", "*"} {
		if e := permissionValidation(Permission{Vhost: "/", User: "app", Configure: pattern}); !errors.Is(e, engine.ErrInvalid) {
			t.Fatal(e)
		}
	}
	for _, v := range []Policy{{Vhost: "/", Name: "p", Pattern: "[", ApplyTo: "queues", Definition: map[string]any{}}, {Vhost: "/", Name: "p", ApplyTo: "invalid", Definition: map[string]any{}}, {Vhost: "/", Name: "p", ApplyTo: "queues", Priority: -1, Definition: map[string]any{}}, {Vhost: "/", Name: "p", ApplyTo: "queues"}, {Vhost: "/", Name: "p", ApplyTo: "queues", Definition: map[string]any{"bad": make(chan int)}}} {
		if e := policyValidation(v); !errors.Is(e, engine.ErrInvalid) {
			t.Fatal(v, e)
		}
	}
	if e := policyValidation(Policy{Vhost: "/", Name: "p", Pattern: ".*", ApplyTo: "queues", Definition: map[string]any{"future-key": true}}); e != nil {
		t.Fatal(e)
	}
	invalid := -2
	if e := vhostValidation(Vhost{Name: "/", Limits: &VhostLimits{MaxQueues: &invalid}}); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
}

func TestAccessUserPasswordLifecycle(t *testing.T) {
	exists := false
	hash := "original-hash"
	puts := []map[string]any{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/overview":
			fmt.Fprint(w, `{}`)
		case r.Method == "GET" && r.URL.Path == "/api/users":
			fmt.Fprintf(w, `[{"name":"app","tags":[],"password_hash":%q}]`, hash)
		case r.Method == "GET" && r.URL.Path == "/api/users/app":
			if !exists {
				w.WriteHeader(404)
				return
			}
			fmt.Fprintf(w, `{"name":"app","tags":[],"password_hash":%q,"hashing_algorithm":"rabbit_password_hashing_sha256"}`, hash)
		case r.Method == "PUT" && r.URL.Path == "/api/users/app":
			var b map[string]any
			if e := json.NewDecoder(r.Body).Decode(&b); e != nil {
				t.Error(e)
			}
			puts = append(puts, b)
			exists = true
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	rc, _ := accessContext(t, srv, "disabled", "broker")
	p := New(Options{})
	generated, e := p.CreateUser(t.Context(), rc, UserWrite{Name: "app", Tags: []string{}})
	if e != nil || generated == "" {
		t.Fatal(generated, e)
	}
	if puts[0]["password"] != generated {
		t.Fatal(puts)
	}
	if _, e = p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); !errors.Is(e, engine.ErrConflict) {
		t.Fatal(e)
	}
	if e = p.UpdateUser(t.Context(), rc, UserWrite{Name: "app", Tags: []string{"monitoring"}}); e != nil {
		t.Fatal(e)
	}
	if puts[1]["password_hash"] != hash || puts[1]["hashing_algorithm"] != "rabbit_password_hashing_sha256" {
		t.Fatal("password was not preserved", puts)
	}
	user, e := p.GetUser(t.Context(), rc, "app")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(user)
	if strings.Contains(string(b), hash) || strings.Contains(string(b), generated) {
		t.Fatal("credentials leaked", string(b))
	}
	items, e := p.ListUsers(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	b, _ = json.Marshal(items)
	if strings.Contains(string(b), hash) {
		t.Fatal("hash leaked")
	}
	exists = false
	supplied := "caller-password"
	generated, e = p.CreateUser(t.Context(), rc, UserWrite{Name: "app", Tags: []string{}, Password: &supplied})
	if e != nil || generated != "" || puts[2]["password"] != supplied {
		t.Fatal(generated, e, puts)
	}
	exists = false
	generated, e = p.CreateUser(t.Context(), rc, UserWrite{Name: "app", Tags: []string{}, PasswordHash: &hash})
	if e != nil || generated != "" || puts[3]["password_hash"] != hash {
		t.Fatal(generated, e, puts)
	}
}

func TestAccessVhostLimitsAndTopicExchanges(t *testing.T) {
	limits := map[string]int{"max-connections": 25, "max-queues": 50}
	writes := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/overview":
			fmt.Fprint(w, `{}`)
		case r.Method == "GET" && r.URL.Path == "/api/vhosts":
			fmt.Fprint(w, `[{"name":"/","default_queue_type":"quorum","tags":["production"]}]`)
		case r.Method == "GET" && r.URL.EscapedPath() == "/api/vhosts/%2F":
			fmt.Fprint(w, `{"name":"/","tags":[]}`)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/vhost-limits"):
			b, _ := json.Marshal([]any{map[string]any{"vhost": "/", "value": limits}})
			w.Write(b)
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/api/vhost-limits/"):
			var b struct {
				Value int `json:"value"`
			}
			json.NewDecoder(r.Body).Decode(&b)
			limits[strings.TrimPrefix(r.URL.EscapedPath(), "/api/vhost-limits/%2F/")] = b.Value
			w.WriteHeader(204)
		case r.Method == "PUT" && r.URL.EscapedPath() == "/api/vhosts/%2F":
			w.WriteHeader(204)
		case r.Method == "GET" && r.URL.EscapedPath() == "/api/topic-permissions/%2F/app":
			fmt.Fprint(w, `[{"vhost":"/","user":"app","exchange":"existing","write":".*","read":".*"}]`)
		case r.Method == "PUT" && r.URL.EscapedPath() == "/api/topic-permissions/%2F/app":
			writes++
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	rc, _ := accessContext(t, srv, "disabled", "broker")
	p := New(Options{})
	v, e := p.GetVhost(t.Context(), rc, "/")
	if e != nil || v.Limits == nil || *v.Limits.MaxQueues != 50 {
		t.Fatal(v, e)
	}
	items, e := p.ListVhosts(t.Context(), rc)
	if e != nil || len(items) != 1 || *items[0].Limits.MaxConnections != 25 {
		t.Fatal(items, e)
	}
	n := 100
	if e = p.UpdateVhost(t.Context(), rc, Vhost{Name: "/", Tags: []string{}, Limits: &VhostLimits{MaxQueues: &n}}); e != nil || limits["max-queues"] != 100 {
		t.Fatal(limits, e)
	}
	topic := TopicPermission{Vhost: "/", User: "app", Exchange: "new", Write: ".*", Read: ".*"}
	if e = p.CreateTopicPermission(t.Context(), rc, topic); e != nil {
		t.Fatal(e)
	}
	topic.Exchange = "existing"
	if e = p.CreateTopicPermission(t.Context(), rc, topic); !errors.Is(e, engine.ErrConflict) {
		t.Fatal(e)
	}
	if e = p.UpdateTopicPermission(t.Context(), rc, topic); e != nil {
		t.Fatal(e)
	}
	topic.Exchange = "missing"
	if e = p.UpdateTopicPermission(t.Context(), rc, topic); !errors.Is(e, engine.ErrNotFound) {
		t.Fatal(e)
	}
	if writes != 2 {
		t.Fatal(writes)
	}
}

func TestAccessAdminRelayTLSAndFailover(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "karakoram" || p != "control-secret" {
			t.Error("admin credentials")
		}
		fmt.Fprint(w, `{}`)
	})
	srv := httptest.NewTLSServer(handler)
	defer srv.Close()
	name := srv.Certificate().DNSNames[0]
	rc, d := accessContext(t, srv, "requireTLS", name)
	p := New(Options{})
	client, e := p.adminClient(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	client.Close()
	if len(d.addresses) != 1 || d.addresses[0] != name+":15671" {
		t.Fatal(d.addresses)
	}
	// A desired TLS disable must not change the observed member's current verified listener.
	rc.Cluster.Spec.TLS.Mode = "disabled"
	client, e = p.adminClient(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	client.Close()
	secrets := rc.Secrets.(accessSecrets)
	delete(secrets.values, "tls/ca.crt")
	if _, e = p.adminClient(t.Context(), rc); e == nil {
		t.Fatal("missing CA accepted")
	}
	plain := httptest.NewServer(handler)
	defer plain.Close()
	rc, d = accessContext(t, plain, "disabled", "broker")
	rc.Cluster.Spec.TLS.Mode = "requireTLS"
	rc.Cluster.Internal = json.RawMessage(`{"members":[{"name":"unavailable","stage":"ready","index":0,"tlsMode":"disabled"},{"name":"broker","stage":"ready","index":1,"tlsMode":"disabled"}]}`)
	client, e = p.adminClient(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	client.Close()
	if len(d.addresses) != 2 || d.addresses[1] != "broker:15672" {
		t.Fatal(d.addresses)
	}
	rc.Cluster.Internal = json.RawMessage(`{"members":[{"name":"broker","stage":"planned"}]}`)
	if _, e = p.adminClient(t.Context(), rc); !errors.Is(e, engine.ErrConflict) {
		t.Fatal(e)
	}
}
