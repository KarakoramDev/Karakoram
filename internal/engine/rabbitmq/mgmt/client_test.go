// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestManagementCRUD(t *testing.T) {
	password := "secret"
	seen := 0
	cases := []struct {
		name, method, path, response string
		call                         func(*Client) error
	}{
		{"listVhosts", "GET", "/api/vhosts", "[]", func(c *Client) error { _, e := c.ListVhosts(context.Background()); return e }},
		{"getVhost", "GET", "/api/vhosts/%2F", "{}", func(c *Client) error { _, e := c.GetVhost(context.Background(), "/"); return e }},
		{"putVhost", "PUT", "/api/vhosts/%2F", "", func(c *Client) error {
			return c.PutVhost(context.Background(), Vhost{Name: "/", Tags: []string{"one", "two"}})
		}},
		{"deleteVhost", "DELETE", "/api/vhosts/%2F", "", func(c *Client) error { return c.DeleteVhost(context.Background(), "/") }},
		{"listUsers", "GET", "/api/users", "[]", func(c *Client) error { _, e := c.ListUsers(context.Background()); return e }},
		{"getUser", "GET", "/api/users/app%2Fa", "{}", func(c *Client) error { _, e := c.GetUser(context.Background(), "app/a"); return e }},
		{"putUser", "PUT", "/api/users/app%2Fa", "", func(c *Client) error {
			return c.PutUser(context.Background(), "app/a", UserWrite{Password: &password, Tags: []string{"management"}})
		}},
		{"deleteUser", "DELETE", "/api/users/app%2Fa", "", func(c *Client) error { return c.DeleteUser(context.Background(), "app/a") }},
		{"listPermissions", "GET", "/api/permissions", "[]", func(c *Client) error { _, e := c.ListPermissions(context.Background()); return e }},
		{"getPermission", "GET", "/api/permissions/%2F/app%2Fa", "{}", func(c *Client) error { _, e := c.GetPermission(context.Background(), "/", "app/a"); return e }},
		{"putPermission", "PUT", "/api/permissions/%2F/app%2Fa", "", func(c *Client) error {
			return c.PutPermission(context.Background(), Permission{Vhost: "/", User: "app/a", Configure: ".*", Read: ".*", Write: ".*"})
		}},
		{"deletePermission", "DELETE", "/api/permissions/%2F/app%2Fa", "", func(c *Client) error { return c.DeletePermission(context.Background(), "/", "app/a") }},
		{"listTopicPermissions", "GET", "/api/topic-permissions", "[]", func(c *Client) error { _, e := c.ListTopicPermissions(context.Background()); return e }},
		{"getTopicPermission", "GET", "/api/topic-permissions/%2F/app%2Fa", "[]", func(c *Client) error { _, e := c.GetTopicPermission(context.Background(), "/", "app/a"); return e }},
		{"putTopicPermission", "PUT", "/api/topic-permissions/%2F/app%2Fa", "", func(c *Client) error {
			return c.PutTopicPermission(context.Background(), TopicPermission{Vhost: "/", User: "app/a", Exchange: "events", Read: ".*", Write: ".*"})
		}},
		{"deleteTopicPermission", "DELETE", "/api/topic-permissions/%2F/app%2Fa", "", func(c *Client) error { return c.DeleteTopicPermission(context.Background(), "/", "app/a") }},
		{"listPolicies", "GET", "/api/policies", "[]", func(c *Client) error { _, e := c.ListPolicies(context.Background()); return e }},
		{"getPolicy", "GET", "/api/policies/%2F/ttl%2Fa", "{}", func(c *Client) error { _, e := c.GetPolicy(context.Background(), "/", "ttl/a"); return e }},
		{"putPolicy", "PUT", "/api/policies/%2F/ttl%2Fa", "", func(c *Client) error {
			return c.PutPolicy(context.Background(), Policy{Vhost: "/", Name: "ttl/a", Pattern: ".*", ApplyTo: "queues", Definition: map[string]any{"message-ttl": 10}})
		}},
		{"deletePolicy", "DELETE", "/api/policies/%2F/ttl%2Fa", "", func(c *Client) error { return c.DeletePolicy(context.Background(), "/", "ttl/a") }},
		{"listOperatorPolicies", "GET", "/api/operator-policies", "[]", func(c *Client) error { _, e := c.ListOperatorPolicies(context.Background()); return e }},
		{"getOperatorPolicy", "GET", "/api/operator-policies/%2F/ttl%2Fa", "{}", func(c *Client) error { _, e := c.GetOperatorPolicy(context.Background(), "/", "ttl/a"); return e }},
		{"putOperatorPolicy", "PUT", "/api/operator-policies/%2F/ttl%2Fa", "", func(c *Client) error {
			return c.PutOperatorPolicy(context.Background(), Policy{Vhost: "/", Name: "ttl/a", Pattern: ".*", ApplyTo: "queues", Definition: map[string]any{"message-ttl": 10}})
		}},
		{"deleteOperatorPolicy", "DELETE", "/api/operator-policies/%2F/ttl%2Fa", "", func(c *Client) error { return c.DeleteOperatorPolicy(context.Background(), "/", "ttl/a") }},
		{"limit", "PUT", "/api/vhost-limits/%2F/max-queues", "", func(c *Client) error { return c.SetVhostLimit(context.Background(), "/", "max-queues", 10) }},
		{"clear-limit", "DELETE", "/api/vhost-limits/%2F/max-queues", "", func(c *Client) error { return c.DeleteVhostLimit(context.Background(), "/", "max-queues") }},
		{"list-limits", "GET", "/api/vhost-limits", "[]", func(c *Client) error { _, e := c.ListVhostLimits(context.Background()); return e }},
		{"get-limits", "GET", "/api/vhost-limits/%2F", "[]", func(c *Client) error { _, e := c.GetVhostLimits(context.Background(), "/"); return e }},
		{"overview", "GET", "/api/overview", "{}", func(c *Client) error { _, e := c.Overview(context.Background()); return e }},
		{"nodes", "GET", "/api/nodes", "[]", func(c *Client) error { _, e := c.Nodes(context.Background()); return e }},
		{"queues", "GET", "/api/queues", "[]", func(c *Client) error { _, e := c.Queues(context.Background()); return e }},
		{"alarms", "GET", "/api/health/checks/alarms", "{}", func(c *Client) error { return c.Alarms(context.Background()) }},
		{"critical", "GET", "/api/health/checks/node-is-quorum-critical", "{}", func(c *Client) error {
			v, e := c.NodeIsQuorumCritical(context.Background())
			if v {
				t.Fatal("healthy node critical")
			}
			return e
		}},
		{"export", "GET", "/api/definitions", `{"users":[]}`, func(c *Client) error {
			v, e := c.ExportDefinitions(context.Background())
			if string(v) != `{"users":[]}` {
				t.Fatalf("export %s", v)
			}
			return e
		}},
		{"import", "POST", "/api/definitions", "", func(c *Client) error { return c.ImportDefinitions(context.Background(), []byte(`{"users":[]}`)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen++
				if r.Method != tc.method || r.URL.EscapedPath() != tc.path {
					t.Errorf("request %s %s, want %s %s", r.Method, r.URL.EscapedPath(), tc.method, tc.path)
				}
				u, p, ok := r.BasicAuth()
				if !ok || u != "karakoram" || p != password {
					t.Error("basic auth")
				}
				if tc.method == "PUT" || tc.method == "POST" {
					var body map[string]any
					if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
						t.Error(e)
					}
					if tc.name == "putVhost" && body["tags"] != "one,two" {
						t.Errorf("vhost tags: %+v", body)
					}
					if r.Header.Get("Content-Type") != "application/json" {
						t.Error("missing content type")
					}
				}
				if tc.response == "" {
					w.WriteHeader(204)
				} else {
					fmt.Fprint(w, tc.response)
				}
			}))
			defer srv.Close()
			c, e := New(srv.URL, "karakoram", password, srv.Client())
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			if e = tc.call(c); e != nil {
				t.Fatal(e)
			}
		})
	}
	if seen != len(cases) {
		t.Fatal(seen)
	}
}
func TestErrorsAndContext(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{{400, engine.ErrInvalid}, {404, engine.ErrNotFound}, {409, engine.ErrConflict}, {412, engine.ErrConflict}, {422, engine.ErrInvalid}, {500, nil}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, "secret password")
			}))
			defer srv.Close()
			c, _ := New(srv.URL, "admin", "secret", srv.Client())
			_, e := c.GetUser(t.Context(), "app")
			if e == nil || strings.Contains(e.Error(), "secret") {
				t.Fatal(e)
			}
			if tc.want != nil && !errors.Is(e, tc.want) {
				t.Fatal(e)
			}
			var status *HTTPError
			if !errors.As(e, &status) || status.StatusCode != tc.status {
				t.Fatal(e)
			}
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	c, _ := New(srv.URL, "admin", "secret", srv.Client())
	critical, e := c.NodeIsQuorumCritical(t.Context())
	if e != nil || !critical {
		t.Fatal(critical, e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e = c.Overview(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e = c.ImportDefinitions(t.Context(), []byte(`[]`)); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
}
func TestInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "not JSON") }))
	defer srv.Close()
	c, _ := New(srv.URL, "admin", "secret", srv.Client())
	if _, e := c.Nodes(t.Context()); e == nil {
		t.Fatal("accepted malformed JSON")
	}
	for _, u := range []string{"", "://bad", "ftp://example.com", "http://u:p@example.com", "http://example.com?q=1"} {
		if _, e := New(u, "admin", "secret", nil); e == nil {
			t.Fatal(u)
		}
	}
}

func TestQueuePlacementMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/queues" {
			t.Error(r.URL.Path)
		}
		fmt.Fprint(w, `[{"name":"durable-classic","vhost":"/","type":"classic","durable":true,"node":"rabbit@leaving"},{"name":"replicated","vhost":"/","type":"quorum","durable":true,"leader":"rabbit@survivor","members":["rabbit@survivor","rabbit@leaving"]}]`)
	}))
	defer srv.Close()
	c, err := New(srv.URL, "karakoram", "secret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	queues, err := c.Queues(t.Context())
	if err != nil || len(queues) != 2 {
		t.Fatal(queues, err)
	}
	if !queues[0].Durable || queues[0].Node != "rabbit@leaving" || queues[0].Type != "classic" {
		t.Fatal(queues[0])
	}
	if queues[1].Leader != "rabbit@survivor" || len(queues[1].Members) != 2 {
		t.Fatal(queues[1])
	}
}

// RabbitMQ's vhost-limits resource has no representation and rejects a strict
// JSON Accept header with 406, which once failed every vhost limit write.
func TestWritesWithoutResponseAcceptAnyRepresentation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/vhost-limits/") && r.Method != http.MethodGet && r.Header.Get("Accept") == "application/json" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		if r.Method == http.MethodGet {
			if r.Header.Get("Accept") != "application/json" {
				t.Errorf("decoded %s request did not ask for JSON", r.URL.Path)
			}
			fmt.Fprint(w, `[{"vhost":"app","value":{"max-queues":5}}]`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	c, err := New(server.URL, "karakoram", "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.SetVhostLimit(ctx, "app", "max-queues", 5); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteVhostLimit(ctx, "app", "max-queues"); err != nil {
		t.Fatal(err)
	}
	if limits, err := c.GetVhostLimits(ctx, "app"); err != nil || len(limits) != 1 || limits[0].Value["max-queues"] != 5 {
		t.Fatalf("limits=%+v err=%v", limits, err)
	}
}
