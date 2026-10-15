// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/pkg/client"
)

type pathRabbitMQAccess struct {
	fakeRabbitMQAccess
	vhost string
}

func (f *pathRabbitMQAccess) GetVhost(_ context.Context, _ *engine.Context, name string) (*rabbitmq.Vhost, error) {
	f.vhost = name
	return &rabbitmq.Vhost{Name: name, Tags: []string{}}, nil
}

func TestRabbitMQEscapedVhostFullHandler(t *testing.T) {
	s, _, token := testServer(t)
	f := &pathRabbitMQAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	handler := testHandler(s)
	for _, name := range []string{"/", "%2F", "a/b", "%"} {
		for _, resource := range []string{"vhosts", "permissions", "topic-permissions", "policies", "operator-policies"} {
			for _, method := range []string{"GET", "PUT", "DELETE"} {
				t.Run(name+"/"+resource+"/"+method, func(t *testing.T) {
					path := "/api/v1/clusters/prod/rabbitmq/" + resource + "/" + url.PathEscape(name)
					body := ""
					switch resource {
					case "vhosts":
						body = fmt.Sprintf(`{"name":%q,"tags":[]}`, name)
					case "permissions":
						path += "/app"
						body = fmt.Sprintf(`{"vhost":%q,"user":"app","configure":"","read":"","write":""}`, name)
					case "topic-permissions":
						path += "/app"
						body = fmt.Sprintf(`{"vhost":%q,"user":"app","exchange":"events","read":"","write":""}`, name)
					default:
						path += "/app"
						body = fmt.Sprintf(`{"vhost":%q,"name":"app","pattern":".*","applyTo":"queues","priority":0,"definition":{}}`, name)
					}
					if method != "PUT" {
						body = ""
					}
					req := httptest.NewRequest(method, path, strings.NewReader(body))
					if req.URL.EscapedPath() != path {
						t.Fatalf("wire path %q, want %q", req.URL.EscapedPath(), path)
					}
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+token)
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, req)
					want := http.StatusOK
					if method == "DELETE" {
						want = http.StatusNoContent
					}
					if w.Code != want {
						t.Fatalf("%s (RawPath=%q): %d %s Location=%s", path, req.URL.RawPath, w.Code, w.Body.String(), w.Header().Get("Location"))
					}
					if resource == "vhosts" && method == "GET" && f.vhost != name {
						t.Fatalf("handler received %q, want %q", f.vhost, name)
					}
				})
			}
		}
	}
}

func TestRabbitMQGeneratedClientVhostPaths(t *testing.T) {
	s, _, token := testServer(t)
	f := &pathRabbitMQAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	seen := make(chan string, 1)
	handler := testHandler(s)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.EscapedPath()
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	for _, name := range []string{"/", "%2F"} {
		req, err := client.NewGetRabbitMQVhostRequest(server.URL+"/api/v1/", "prod", name, nil)
		if err != nil {
			t.Fatal(err)
		}
		expected := "/api/v1/clusters/prod/rabbitmq/vhosts/" + url.PathEscape(name)
		if req.URL.EscapedPath() != expected {
			t.Fatalf("wire=%q RawPath=%q want=%q", req.URL.EscapedPath(), req.URL.RawPath, expected)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var vhost rabbitmq.Vhost
		err = json.NewDecoder(resp.Body).Decode(&vhost)
		resp.Body.Close()
		if wire := <-seen; resp.StatusCode != 200 || err != nil || vhost.Name != name || wire != expected {
			t.Fatal(resp.StatusCode, err, vhost.Name, wire)
		}
	}
}
