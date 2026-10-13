// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestManagementPluginRequiresBasePlugin(t *testing.T) {
	for _, base := range []string{"rabbitmq_stream", "rabbitmq_shovel", "rabbitmq_federation"} {
		t.Run(base, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.TLS.Mode = "requireTLS"
			rc.Cluster.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"plugins":[%q]}`, base+"_management"))
			if err := p.Validate(t.Context(), nil, rc.Cluster); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "requires") || !strings.Contains(err.Error(), base) {
				t.Fatalf("missing dependency accepted or unclear error: %v", err)
			}
			// Input ordering does not matter when the dependency is explicit.
			rc.Cluster.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"plugins":[%q,%q]}`, base+"_management", base))
			if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEmptyPermissionPatternsPassThrough(t *testing.T) {
	exists := map[string]bool{}
	puts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := "permission"
		if strings.HasPrefix(r.URL.Path, "/api/topic-permissions/") {
			kind = "topic"
		}
		switch {
		case r.URL.Path == "/api/overview":
			fmt.Fprint(w, `{}`)
		case r.Method == "GET":
			if !exists[kind] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if kind == "topic" {
				fmt.Fprint(w, `[{"vhost":"/","user":"app","exchange":"events","write":"","read":""}]`)
			} else {
				fmt.Fprint(w, `{"vhost":"/","user":"app","configure":"","write":"","read":""}`)
			}
		case r.Method == "PUT":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			keys := []string{"write", "read"}
			if kind == "permission" {
				keys = append(keys, "configure")
			}
			for _, key := range keys {
				if body[key] != "" {
					t.Errorf("deny-all %s was omitted or changed: %#v", key, body)
				}
			}
			exists[kind] = true
			puts++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	rc, _ := accessContext(t, srv, "disabled", "broker")
	p := New(Options{})
	permission := Permission{Vhost: "/", User: "app"}
	topic := TopicPermission{Vhost: "/", User: "app", Exchange: "events"}
	for _, apply := range []func() error{
		func() error { return p.CreatePermission(t.Context(), rc, permission) },
		func() error { return p.UpdatePermission(t.Context(), rc, permission) },
		func() error { return p.CreateTopicPermission(t.Context(), rc, topic) },
		func() error { return p.UpdateTopicPermission(t.Context(), rc, topic) },
	} {
		if err := apply(); err != nil {
			t.Fatal(err)
		}
	}
	if puts != 4 {
		t.Fatalf("expected four deny-all writes, got %d", puts)
	}
	policy := Policy{Vhost: "/", Name: "deny", ApplyTo: "queues", Definition: map[string]any{}}
	if err := p.CreatePolicy(t.Context(), rc, policy); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("empty policy pattern accepted: %v", err)
	}
	if err := p.CreateOperatorPolicy(t.Context(), rc, policy); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("empty operator policy pattern accepted: %v", err)
	}
}

func TestDefinitionsBackupExcludesReservedUsersAndGrants(t *testing.T) {
	f := newDefinitionsFixture(t)
	finishDefinitions(t, f, definitionsOperation("backup", "daily"), f.p.backupOp)
	reader, err := gzip.NewReader(bytes.NewReader(f.s3.objects["/test-bucket/"+f.store.backup.Location]))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"users", "permissions", "topic_permissions"} {
		var entries []map[string]any
		if err := json.Unmarshal(object[key], &entries); err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s contains reserved entries: %s", key, object[key])
		}
		field := "user"
		if key == "users" {
			field = "name"
		}
		if entries[0][field] != "customer" {
			t.Fatalf("%s retained wrong identity: %s", key, object[key])
		}
	}
	for _, hash := range []string{"source-secret", "admin-secret", "monitor-secret", "guest-secret"} {
		if bytes.Contains(data, []byte(hash)) {
			t.Fatal("reserved password hash uploaded")
		}
	}
	if !bytes.Contains(data, []byte("customer-secret")) || !bytes.Contains(object["queues"], []byte("orders")) {
		t.Fatal("application definitions were lost")
	}
}

func TestExposureReplacesObsoleteTLSRoute(t *testing.T) {
	for _, mode := range []string{"disabled", "preferTLS"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			rc.Cluster.Spec.Exposure.Public = true
			rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/64"}
			ready(t, p, rc)
			s := state(t, rc)
			m := s.runningMembers()[0]
			port := m.PublicPort
			oldTarget, newTarget := 5671, 5672
			if mode == "preferTLS" {
				oldTarget, newTarget = 5672, 5671
			}
			for i := range f.publicPorts {
				if f.publicPorts[i].Hostname == m.Name {
					f.publicPorts[i].TargetPort = oldTarget
				}
			}
			foreign := infra.PublicPortRoute{Hostname: "foreign", PublicPort: infra.PublicPort{Port: 60000, TargetPort: oldTarget, IPType: "IPV6"}}
			f.publicPorts = append(f.publicPorts, foreign)
			m.TLSMode = mode
			m.PublicHash = "obsolete"
			rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8:1::/64"}
			start := len(f.log.snapshot())
			changed, err := p.syncExposure(t.Context(), rc, s)
			if err != nil || !changed {
				t.Fatalf("route change: %v %v", changed, err)
			}
			log := f.log.snapshot()[start:]
			deleted := slices.Index(log, "infra:"+m.Name+":unexpose")
			created := slices.Index(log, fmt.Sprintf("infra:%s:expose:%d", m.Name, port))
			if deleted < 0 || created <= deleted || m.PublicPort != port {
				t.Fatalf("old route not removed before port reuse: %v", log)
			}
			count := 0
			for _, route := range f.publicPorts {
				if route.Hostname == m.Name {
					count++
					if route.Port != port || route.TargetPort != newTarget || !slices.Equal(route.Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
						t.Fatalf("obsolete route survived: %+v", route)
					}
				}
			}
			if count != 1 || !slices.ContainsFunc(f.publicPorts, func(route infra.PublicPortRoute) bool {
				return route.Hostname == "foreign" && route.Port == foreign.Port && route.TargetPort == foreign.TargetPort
			}) {
				t.Fatal("route replacement changed unrelated routes or left duplicates")
			}
		})
	}
}
