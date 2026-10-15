// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

// The inventory uses the native provider's decoding and API mapping, with only
// its node transport redirected to an in-process Elasticsearch security API.
type inventoryElasticProvider struct {
	*elastic.Provider
	address string
}

func (p *inventoryElasticProvider) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, p.address)
}
func (p *inventoryElasticProvider) ListUsers(ctx context.Context, rc *engine.Context) ([]elastic.User, error) {
	rc.Dialer = p
	return p.Provider.ListUsers(ctx, rc)
}
func (p *inventoryElasticProvider) GetUser(ctx context.Context, rc *engine.Context, name string) (*elastic.User, error) {
	rc.Dialer = p
	return p.Provider.GetUser(ctx, rc, name)
}
func (p *inventoryElasticProvider) ListRoles(ctx context.Context, rc *engine.Context) ([]elastic.AccessRole, error) {
	rc.Dialer = p
	return p.Provider.ListRoles(ctx, rc)
}
func (p *inventoryElasticProvider) GetRole(ctx context.Context, rc *engine.Context, name string) (*elastic.AccessRole, error) {
	rc.Dialer = p
	return p.Provider.GetRole(ctx, rc, name)
}
func (p *inventoryElasticProvider) ListAPIKeys(ctx context.Context, rc *engine.Context) ([]elastic.APIKey, error) {
	rc.Dialer = p
	return p.Provider.ListAPIKeys(ctx, rc)
}

func newInventoryElasticProvider(t *testing.T, s *Server) *inventoryElasticProvider {
	t.Helper()
	query := `{"term":{"tenant":"sample"}}`
	native := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "karakoram" || password != "inventory-password" {
			t.Error("native inventory request lacks administrative authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		var body any
		switch {
		case strings.HasPrefix(r.URL.Path, "/_security/user"):
			body = map[string]any{"app": map[string]any{"username": "app", "roles": []string{"custom"}, "full_name": "Search reader", "email": "reader@example.com", "enabled": true}}
		case strings.HasPrefix(r.URL.Path, "/_security/role"):
			body = map[string]any{"custom": map[string]any{
				"cluster":      []string{"monitor"},
				"indices":      []any{map[string]any{"names": []string{"logs-*"}, "privileges": []string{"read"}, "allow_restricted_indices": false, "query": query, "field_security": map[string]any{"grant": []string{"public.*"}, "except": []string{"secret.*"}}}},
				"applications": []any{map[string]any{"application": "kibana-.kibana", "privileges": []string{"read"}, "resources": []string{"space:default"}}},
			}}
		case r.URL.Path == "/_security/api_key":
			body = map[string]any{"api_keys": []any{map[string]any{"id": "native-key", "name": "reader", "creation": 123, "expiration": 456, "invalidated": false, "username": "karakoram", "metadata": map[string]any{"karakoram": true}}}}
		default:
			t.Errorf("unexpected native inventory request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(native.Close)
	p := &inventoryElasticProvider{Provider: elastic.New(elastic.ProviderOptions{Store: s.Store}), address: native.Listener.Addr().String()}
	cluster := domain.Cluster{Name: "search", Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
	p.ApplyDefaults(&cluster.Spec)
	cluster.Internal = json.RawMessage(`{"elasticsearch":{"phase":"ready","clusterUUID":"inventory","members":[{"name":"example.com","kind":"node","stage":"joined"}]}}`)
	if err := s.Store.CreateCluster(t.Context(), &cluster); err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: native.Certificate().Raw})
	for name, value := range map[string][]byte{"tls/ca.crt": ca, "users/karakoram": []byte("inventory-password")} {
		if err := s.Store.SetClusterSecret(t.Context(), cluster.Ref(), name, value); err != nil {
			t.Fatal(err)
		}
	}
	completed := time.Now().UTC()
	backup := domain.Backup{Name: "snapshot", Cluster: cluster.Name, Provider: "elasticsearch", Type: "snapshot", Trigger: "manual", Storage: "archive", Phase: domain.PhaseSucceeded, Location: "s3://backups/search?repository=archive#snapshot", SourceVersion: cluster.Spec.Version, CompletedAt: &completed}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	return p
}
