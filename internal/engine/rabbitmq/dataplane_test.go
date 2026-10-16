// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type accessAgentDialer struct {
	url string
	ca  []byte
}

func (d accessAgentDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test-token", CAPEM: d.ca})
}

func TestAccessConnectionDiscovery(t *testing.T) {
	p := New(Options{})
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		t.Run(mode, func(t *testing.T) {
			c := &domain.Cluster{Name: "brokers", Spec: domain.ClusterSpec{TLS: domain.TLSConfig{Mode: mode}}, Status: domain.ClusterStatus{Connection: domain.Connection{PublicHosts: []string{"broker.example.com:5671"}, Endpoints: []domain.Endpoint{{Name: "management", URL: "https://management.example.com", Public: true}}}}}
			state := &internalState{Members: []*member{{Name: "brokersm0", Index: 0, Stage: "ready", TLSMode: mode}, {Name: "brokersm1", Index: 1, Stage: "joined", TLSMode: mode}, {Name: "brokersm2", Index: 2, Stage: "planned", TLSMode: mode}}}
			if e := state.save(c); e != nil {
				t.Fatal(e)
			}
			rc := &engine.Context{Cluster: c, Secrets: accessSecrets{values: map[string][]byte{"users/admin": []byte("p@ss:/?#"), "tls/ca.crt": []byte("test CA")}}}
			v, e := p.Connection(t.Context(), rc)
			if e != nil {
				t.Fatal(e)
			}
			port, scheme := "5671", "amqps"
			if mode == "disabled" {
				port, scheme = "5672", "amqp"
			}
			if len(v.Connection.Hosts) != 2 || v.Connection.Hosts[0] != "brokersm0:"+port || v.Connection.URI != scheme+"://brokersm0:"+port+"/%2F" {
				t.Fatal(v)
			}
			if strings.Contains(v.Connection.URI, "p@ss") {
				t.Fatal("password in discovery")
			}
			if mode == "disabled" && v.CACertificate != "" {
				t.Fatal(v.CACertificate)
			}
			if mode != "disabled" && v.CACertificate != "test CA" {
				t.Fatal(v.CACertificate)
			}
			foundPublic, foundManagement := false, false
			for _, ep := range v.Connection.Endpoints {
				if ep.Public && ep.Name == scheme {
					foundPublic = true
				}
				if ep.Public && ep.Name == "management" {
					foundManagement = true
				}
			}
			if !foundPublic || !foundManagement {
				t.Fatal(v.Connection.Endpoints)
			}
			for _, cr := range v.Credentials {
				u, e := url.Parse(cr.URI)
				if e != nil {
					t.Fatal(e)
				}
				pw, ok := u.User.Password()
				if !ok || pw != "p@ss:/?#" || u.User.Username() != "admin" || u.EscapedPath() != "/%2F" {
					t.Fatal(cr)
				}
				if cr.Password != pw {
					t.Fatal(cr)
				}
			}
			if len(v.Credentials) < 3 {
				t.Fatal(v.Credentials)
			}
		})
	}
	users, e := p.SystemUsers(t.Context(), nil)
	if e != nil || len(users) != 2 || users[0].Name != "karakoram" || users[1].Name != "admin" {
		t.Fatal(users, e)
	}
}

func TestAccessPlacementAndLogs(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("agent auth")
		}
		switch r.URL.Path {
		case "/v1/status":
			fmt.Fprint(w, `{"hostFingerprint":"same-host"}`)
		case "/v1/logs":
			if r.URL.Query().Get("process") != "rabbitmq-server" && r.URL.Query().Get("process") != "agent" {
				t.Error(r.URL.Query())
			}
			fmt.Fprint(w, `{"lines":[{"timestamp":"2026-10-13T00:00:00Z","stream":"stdout","message":"ready"}]}`)
		default:
			t.Error(r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	rc, _ := accessContext(t, srv, "disabled", "brokersm0")
	rc.Backend = fake.New(fake.Options{})
	rc.Agents = accessAgentDialer{url: srv.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})}
	rc.Cluster.Internal = json.RawMessage(`{"members":[{"name":"brokersm0","index":0,"stage":"ready"},{"name":"brokersm1","index":1,"stage":"ready"},{"name":"brokersm2","index":2,"stage":"planned"}]}`)
	p := New(Options{})
	placement, e := p.Placement(t.Context(), rc)
	if e != nil || len(placement.Groups["same-host"]) != 3 || !strings.Contains(placement.Warning, "co-located") {
		t.Fatal(placement, e)
	}
	for _, process := range []string{"rabbitmq-server", "agent"} {
		logs, e := p.MemberLogs(t.Context(), rc, "brokersm0", process, 10)
		if e != nil || len(logs) != 1 || logs[0].Member != "brokersm0" || logs[0].Process != process || logs[0].Message != "ready" {
			t.Fatal(logs, e)
		}
	}
	for _, tc := range []struct {
		name, process string
		lines         int
		err           error
	}{{"missing", "agent", 10, engine.ErrNotFound}, {"brokersm2", "agent", 10, engine.ErrConflict}, {"brokersm0", "mongod", 10, engine.ErrInvalid}, {"brokersm0", "agent", 0, engine.ErrInvalid}, {"brokersm0", "agent", 10001, engine.ErrInvalid}} {
		_, e := p.MemberLogs(t.Context(), rc, tc.name, tc.process, tc.lines)
		if !errors.Is(e, tc.err) {
			t.Fatal(tc, e)
		}
	}
}

func TestAccessMongoUnsupported(t *testing.T) {
	p := New(Options{})
	ctx := t.Context()
	rc := &engine.Context{}
	calls := []func() error{func() error { _, e := p.ListDatabaseUsers(ctx, rc); return e }, func() error { _, e := p.GetDatabaseUser(ctx, rc, "app", "user"); return e }, func() error { _, e := p.CreateDatabaseUser(ctx, rc, domain.DatabaseUser{}, ""); return e }, func() error { return p.UpdateDatabaseUser(ctx, rc, domain.DatabaseUser{}, "") }, func() error { return p.DeleteDatabaseUser(ctx, rc, "app", "user") }, func() error { _, e := p.ListDatabaseRoles(ctx, rc); return e }, func() error { _, e := p.GetDatabaseRole(ctx, rc, "app", "role"); return e }, func() error { return p.CreateDatabaseRole(ctx, rc, domain.DatabaseRole{}) }, func() error { return p.UpdateDatabaseRole(ctx, rc, domain.DatabaseRole{}) }, func() error { return p.DeleteDatabaseRole(ctx, rc, "app", "role") }, func() error { _, e := p.Balancer(ctx, rc); return e }, func() error { return p.SetBalancer(ctx, rc, true) }, func() error { _, e := p.PITRWindow(ctx, rc); return e }}
	for _, call := range calls {
		if e := call(); !errors.Is(e, engine.ErrNotSupported) {
			t.Fatal(e)
		}
	}
}
