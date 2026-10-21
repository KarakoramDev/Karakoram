// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

type rabbitMQAccessNamespaces struct {
	engine.StaticNamespaces
	address string
}

func (n rabbitMQAccessNamespaces) Dialer(string) engine.ContextDialer { return n }
func (n rabbitMQAccessNamespaces) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, n.address)
}

// Exercise the real API, auth, policy, encrypted store, provider and Management
// client together. The local Management endpoint records every outbound write.
func TestRabbitMQUserTagAuthorizationIntegration(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := t.Context()
	var mu sync.Mutex
	users := map[string]json.RawMessage{"app": json.RawMessage(`{"name":"app","tags":["management"],"password_hash":"private-hash","hashing_algorithm":"rabbit_password_hashing_sha256"}`)}
	writes := 0
	management := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		user, password, ok := r.BasicAuth()
		if !ok || user != "karakoram" || password != "integration-control" {
			t.Error("Management authentication mismatch")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/api/overview" {
			fmt.Fprint(w, `{}`)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/api/users/")
		switch r.Method {
		case http.MethodGet:
			if data, exists := users[name]; exists {
				_, _ = w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{}`)
			}
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body["name"] = name
			if _, exists := body["password_hash"]; !exists {
				body["password_hash"] = "private-hash"
			}
			users[name], _ = json.Marshal(body)
			writes++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected Management method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(management.Close)
	s.namespaces = rabbitMQAccessNamespaces{StaticNamespaces: engine.StaticNamespaces{DefaultBackend: fakeBackend{}}, address: management.Listener.Addr().String()}
	var err error
	s.Registry, err = engine.NewRegistry(rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	cluster := domain.Cluster{Name: "brokers", Spec: domain.ClusterSpec{Provider: "rabbitmq", Version: "4.3.6", TLS: domain.TLSConfig{Mode: "disabled"}}, Internal: json.RawMessage(`{"members":[{"name":"brokersm0","stage":"ready","tlsMode":"disabled"}]}`)}
	if err := s.Store.CreateCluster(ctx, &cluster); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(ctx, cluster.Ref(), "users/karakoram", []byte("integration-control")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"user-manager", "cluster-manager"} {
		if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: name}, "integration-account-password"); err != nil {
			t.Fatal(err)
		}
	}
	basePolicy := s.RBAC.Policy() + "\np, user:user-manager, database-users, create, default/brokers, allow\np, user:user-manager, database-users, update, default/brokers, allow\np, user:cluster-manager, clusters, update, default/brokers, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, basePolicy); err != nil {
		t.Fatal(err)
	}
	delegated, _, err := s.Auth.CreateToken(ctx, "user-manager", "integration", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	highOnly, _, err := s.Auth.CreateToken(ctx, "cluster-manager", "integration", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return writes
	}
	request := func(method, name, tags, token string, status int) {
		t.Helper()
		path := "/api/v1/clusters/brokers/rabbitmq/users"
		if method == http.MethodPut {
			path += "/" + name
		}
		before := writeCount()
		w := call(s, method, path, fmt.Sprintf(`{"name":%q,"tags":%s}`, name, tags), token)
		if w.Code != status {
			t.Fatalf("%s %s tags=%s: got %d, want %d", method, name, tags, w.Code, status)
		}
		wantWrites := before
		if status == http.StatusCreated || status == http.StatusOK {
			wantWrites++
			if w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "private-hash") {
				t.Fatal("user response leaks private authentication data or is cacheable")
			}
		}
		if writeCount() != wantWrites {
			t.Fatal("unexpected Management write count")
		}
	}
	for _, tags := range []string{`[]`, `["management"]`} {
		request(http.MethodPost, fmt.Sprintf("new-%d", writeCount()), tags, delegated, http.StatusCreated)
		request(http.MethodPut, "app", tags, delegated, http.StatusOK)
	}
	privileged := []string{"administrator", "policymaker", "monitoring", "impersonator"}
	for _, tag := range privileged {
		for _, tags := range []string{fmt.Sprintf(`[%q]`, tag), fmt.Sprintf(`["management",%q]`, tag)} {
			request(http.MethodPost, "denied", tags, delegated, http.StatusForbidden)
			request(http.MethodPut, "app", tags, delegated, http.StatusForbidden)
		}
	}
	// Higher permission is additive, not a replacement for the primary grant.
	request(http.MethodPost, "denied", `["administrator"]`, highOnly, http.StatusForbidden)
	request(http.MethodPut, "app", `["administrator"]`, highOnly, http.StatusForbidden)
	if err := s.RBAC.ReplacePolicy(ctx, basePolicy+"p, user:user-manager, clusters, update, other/brokers, allow\np, user:user-manager, clusters, update, default/other, allow\n"); err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "denied", `["administrator"]`, delegated, http.StatusForbidden)
	request(http.MethodPut, "app", `["administrator"]`, delegated, http.StatusForbidden)
	if err := s.RBAC.ReplacePolicy(ctx, basePolicy+"p, user:user-manager, clusters, update, default/brokers, allow\n"); err != nil {
		t.Fatal(err)
	}
	for _, tag := range privileged {
		tags := fmt.Sprintf(`["management",%q]`, tag)
		request(http.MethodPost, "privileged-"+tag, tags, delegated, http.StatusCreated)
		request(http.MethodPut, "app", tags, delegated, http.StatusOK)
		// Replaying an already-present privileged tag still needs the grant.
		if err := s.RBAC.ReplacePolicy(ctx, basePolicy); err != nil {
			t.Fatal(err)
		}
		request(http.MethodPut, "app", tags, delegated, http.StatusForbidden)
		if err := s.RBAC.ReplacePolicy(ctx, basePolicy+"p, user:user-manager, clusters, update, default/brokers, allow\n"); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"admin", "karakoram", "monitor", "guest"} {
		for _, token := range []string{delegated, admin} {
			request(http.MethodPost, name, `["management"]`, token, http.StatusConflict)
			request(http.MethodPut, name, `["management"]`, token, http.StatusConflict)
			request(http.MethodPost, name, `["administrator"]`, token, http.StatusConflict)
			request(http.MethodPut, name, `["administrator"]`, token, http.StatusConflict)
		}
	}
	if err := s.RBAC.ReplacePolicy(ctx, basePolicy+"p, user:user-manager, clusters, update, default/**, allow\np, user:user-manager, clusters, update, default/brokers, deny\n"); err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "denied", `["administrator"]`, delegated, http.StatusForbidden)
	request(http.MethodPut, "app", `["administrator"]`, delegated, http.StatusForbidden)
}

func TestRabbitMQPrivilegedTagMetadataFailsClosed(t *testing.T) {
	s, _, admin := testServer(t)
	f := &fakeRabbitMQAccess{}
	var err error
	s.Registry, err = engine.NewRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct{ id, method, suffix string }{
		{"createRabbitMQUser", http.MethodPost, ""},
		{"replaceRabbitMQUser", http.MethodPut, "/app"},
	} {
		rule, exists := api.OperationAdditionalRBAC[operation.id]
		if !exists || rule.Resource != "clusters" || rule.Action != "update" || rule.ObjectParam != "cluster" || rule.Public {
			t.Fatalf("incorrect privileged tag authorization metadata for %s", operation.id)
		}
		func() {
			delete(api.OperationAdditionalRBAC, operation.id)
			defer func() { api.OperationAdditionalRBAC[operation.id] = rule }()
			f.last = ""
			w := call(s, operation.method, "/api/v1/clusters/prod/rabbitmq/users"+operation.suffix, `{"name":"app","tags":["administrator"]}`, admin)
			if w.Code != http.StatusForbidden || f.last != "" {
				t.Fatal("missing secondary metadata did not fail closed")
			}
		}()
	}
}
