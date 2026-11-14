// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"text/template"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRenderResolutionTombstones(t *testing.T) {
	type tuple struct{ K, V string }
	type service struct{ Name string }
	type registration struct{ Address string }
	tuples := []tuple{{"VISIBLE", "value"}, {"karakoram_tombstone_retired", "retired"}, {"karakoram_tombstone_bad", "bad.name"}, {"karakoram_tombstone_mismatch", "other"}}
	funcs := template.FuncMap{
		"regexMatch":     regexp.MatchString,
		"contains":       func(value string, list []string) bool { return slices.Contains(list, value) },
		"split":          func(sep, value string) []string { return strings.Split(value, sep) },
		"env":            func(string) string { return "worker-a" },
		"nomadVarExists": func(path string) bool { return path == "nomad/jobs/survivor" },
		"nomadVar":       func(string) any { return struct{ Tuples []tuple }{tuples} },
		"nomadServices":  func() []service { return []service{{"survivor"}, {"retired"}} },
		"nomadService": func(name string) []registration {
			if name == "retired" {
				t.Fatal("retired service queried")
			}
			return []registration{{"192.0.2.1"}}
		},
		"replaceAll": func(old, replacement, value string) string { return strings.ReplaceAll(value, old, replacement) },
	}
	for _, tc := range []struct{ source, want string }{
		{renderTombstoneHostsTemplate("survivor"), "# karakoram resolution tombstones: enabled\n239.192.0.1 retired\nff18::4b4b:1 retired\n127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n192.0.2.1 survivor\n127.0.1.1 worker-a\n"},
		{renderEnvTemplate("survivor"), "export VISIBLE='value'\n"},
	} {
		parsed, err := template.New("test").Funcs(funcs).Parse(tc.source)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := parsed.Execute(&out, nil); err != nil {
			t.Fatal(err)
		}
		if out.String() != tc.want {
			t.Fatalf("got %q want %q", out.String(), tc.want)
		}
	}
	tuples = append(tuples, tuple{"karakoram_tombstone_worker-a", "worker-a"})
	parsed, err := template.New("uts-collision").Funcs(funcs).Parse(renderTombstoneHostsTemplate("survivor"))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := parsed.Execute(&out, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "127.0.1.1 worker-a") || !strings.Contains(out.String(), "239.192.0.1 worker-a") {
		t.Fatal("local alias duplicated a retired-name mapping")
	}
}
func TestDeployResolutionWriter(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			d := testDeployment(t, "volume")
			d.Runtime.Env = map[string]string{"KARAKORAM_ENGINE": engine}
			op, err := b.Deploy(t.Context(), "runtime", d)
			if err != nil {
				t.Fatal(err)
			}
			// Reload through a new backend, as after a control-plane restart.
			// Render-only fixtures with Runtime.Env still set miss its removal
			// by Deploy before the deployment is persisted.
			b = fakeBackend(t, f, nil)
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			if r.Deployment.Runtime.Env != nil {
				t.Fatal("deployment environment leaked into the service record")
			}
			awaitFake(t, b, roundTripOp(t, op))
			var job map[string]any
			if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil {
				t.Fatal(err)
			}
			group := job["TaskGroups"].([]any)[0].(map[string]any)
			task := group["Tasks"].([]any)[0].(map[string]any)
			env, _ := task["Env"].(map[string]any)
			hosts := task["Templates"].([]any)[2].(map[string]any)
			if engine == "mysql" {
				if env["KARAKORAM_RESOLVER_TOMBSTONES"] != "nomad-variable:" {
					t.Fatal("writer environment missing")
				}
				if hosts["ChangeMode"] != "noop" || hosts["EmbeddedTmpl"] != renderTombstoneHostsTemplate("runtime") {
					t.Fatal("tombstone hosts template missing")
				}
			} else if env["KARAKORAM_RESOLVER_TOMBSTONES"] != nil || hosts["EmbeddedTmpl"] != renderHostsTemplate() {
				t.Fatal("non-MySQL deployment received resolver tombstone wiring")
			}
		})
	}
}
func TestProjectResolutionTombstones(t *testing.T) {
	items := map[string]string{"VISIBLE": "value", "PASSWORD": "fixture", "karakoram_tombstone_old": "old"}
	index := uint64(7)
	writes := 0
	conflict := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/var/karakoram/services/survivor":
			fmt.Fprint(w, `{"Path":"karakoram/services/survivor","ModifyIndex":1,"Items":{"record":"{\"ID\":\"runtime-id\",\"Hostname\":\"survivor\",\"Kind\":\"runtime\"}"}}`)
		case "/v1/var/nomad/jobs/survivor":
			if r.Method == http.MethodPut {
				writes++
				if r.URL.Query().Get("cas") != fmt.Sprint(index) {
					t.Error("CAS index missing")
				}
				if conflict {
					w.WriteHeader(http.StatusConflict)
					fmt.Fprint(w, `{}`)
					return
				}
				var request variable
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				items = request.Items
				index++
			}
			json.NewEncoder(w).Encode(variable{Path: "nomad/jobs/survivor", Items: items, ModifyIndex: index})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	backend, err := NewFactory(WithAllowedAddrs(server.URL)).New(infra.Target{Kind: "nomad", ID: "example", Token: "fixture", Config: map[string]string{"address": server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	b := backend.(*Backend)
	if err := b.ProjectResolutionTombstones(context.Background(), "survivor", []string{"retired"}); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(items, map[string]string{"VISIBLE": "value", "PASSWORD": "fixture", "karakoram_tombstone_retired": "retired"}) {
		t.Fatal(items)
	}
	if err := b.ProjectResolutionTombstones(context.Background(), "survivor", []string{"retired"}); err != nil || writes != 1 {
		t.Fatalf("idempotent writes=%d err=%v", writes, err)
	}
	conflict = true
	if err := b.ProjectResolutionTombstones(context.Background(), "survivor", nil); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	conflict = false
	if err := b.ProjectResolutionTombstones(context.Background(), "survivor", nil); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatal(items)
	}
	for _, hosts := range [][]string{{"survivor"}, {"bad.name"}, make([]string, 65)} {
		if b.ProjectResolutionTombstones(context.Background(), "survivor", hosts) == nil {
			t.Fatal("invalid projection accepted")
		}
	}
	if validateEnvironment(map[string]string{"karakoram_tombstone_injected": "injected"}) == nil {
		t.Fatal("reserved key accepted")
	}
}
