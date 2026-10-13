// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
)

type namespaceTestBackend struct {
	fakeBackend
	project, token string
}

func (b *namespaceTestBackend) SetToken(token string) { b.token = token }
func (b *namespaceTestBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: b.project, Name: "Test project"}, nil
}
func namespaceTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	s, _, token := testServer(t)
	r, err := namespaces.New(context.Background(), namespaces.Options{Store: s.Store, DefaultBackend: &namespaceTestBackend{project: "proj-default"}, Zerops: zerops.Options{ProjectID: "proj-default", Token: "env-token"}, PublicURL: "https://example.com", AdvertiseURL: "https://control:9443", RelayHostname: "controlrelay", ExecutableHash: "test-hash", Logger: s.Logger, Validate: func(_ context.Context, z zerops.Options) (namespaces.Backend, infra.Project, error) {
		if z.Token == "bad-token" {
			return nil, infra.Project{}, errors.New("invalid project token")
		}
		return &namespaceTestBackend{project: z.ProjectID, token: z.Token}, infra.Project{ID: z.ProjectID, Name: "Test project"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	s.namespaces = r
	s.Namespaces = r
	s.RelayHandler = r.Hub.Handler()
	return s, token
}
func TestNamespaceAPIStatusAndSecretBoundaries(t *testing.T) {
	s, token := namespaceTestServer(t)
	w := call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"testing","projectId":"proj-test","token":"synthetic-token","description":"Test namespace"}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, forbidden := range []string{"synthetic-token", "clientSerial", "ClientSerial", "ownershipMarker", "clientCert", "relay-key", "zerops-token"} {
		if strings.Contains(w.Body.String(), forbidden) {
			t.Fatal("namespace response exposes enrollment data")
		}
	}
	var n api.Namespace
	if err := json.Unmarshal(w.Body.Bytes(), &n); err != nil {
		t.Fatal(err)
	}
	if n.ClusterCount == nil || *n.ClusterCount != 0 || n.Name != "testing" {
		t.Fatal("namespace response", n)
	}
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{{"GET", "/api/v1/namespaces/missing", "", 404}, {"PUT", "/api/v1/namespaces/default/token", `{"token":"replacement-token"}`, 409}, {"DELETE", "/api/v1/namespaces/default", "", 409}, {"POST", "/api/v1/namespaces", `{"name":"another","projectId":"proj-test","token":"synthetic-token"}`, 422}, {"POST", "/api/v1/namespaces", `{"name":"another","projectId":"proj-other","token":"bad-token"}`, 422}, {"PATCH", "/api/v1/namespaces/testing", `{"description":"Updated"}`, 200}, {"POST", "/api/v1/namespaces/testing/validate", "", 200}, {"PUT", "/api/v1/namespaces/testing/token", `{"token":"replacement-token"}`, 200}, {"DELETE", "/api/v1/namespaces/testing", "", 202}} {
		w = call(s, tc.method, tc.path, tc.body, token)
		if w.Code != tc.code {
			t.Fatalf("%s %s: %d want %d: %s", tc.method, tc.path, w.Code, tc.code, w.Body.String())
		}
	}
}
func TestNamespaceListPerItemFiltering(t *testing.T) {
	s, admin := namespaceTestServer(t)
	ctx := context.Background()
	w := call(s, "POST", "/api/v1/namespaces", `{"name":"testing","projectId":"proj-test","token":"synthetic-token"}`, admin)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited", Roles: []string{"role:limited"}}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, role:limited, namespaces, read, testing, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "limited", "testing", nil)
	if err != nil {
		t.Fatal(err)
	}
	w = call(s, "GET", "/api/v1/namespaces", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var list api.NamespaceList
	if err = json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "testing" {
		t.Fatal("namespace filter", w.Body.String())
	}
	w = call(s, "GET", "/api/v1/namespaces/default", "", token)
	if w.Code != 403 {
		t.Fatal("namespace detail bypass", w.Code, w.Body.String())
	}
}
func TestRelayRouteIsPublicButNotUnauthenticatedTunnel(t *testing.T) {
	s, _ := namespaceTestServer(t)
	w := call(s, "GET", "/relay/v1/connect", "", "")
	if w.Code == 401 || w.Code == 403 || w.Code == 404 {
		t.Fatal("relay endpoint passes through API authentication", w.Code)
	}
}
func TestInternalTLSAddsRelaySANWithoutChangingCA(t *testing.T) {
	s, sealer, _ := testServer(t)
	ctx := context.Background()
	_, originalCA, err := internalTLS(ctx, s.Store, sealer, "control")
	if err != nil {
		t.Fatal(err)
	}
	config, ca, err := internalTLS(ctx, s.Store, sealer, "control", "controlrelay")
	if err != nil {
		t.Fatal(err)
	}
	if ca != originalCA {
		t.Fatal("internal CA replaced for SAN addition")
	}
	cert, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"control", "controlrelay", "karakoram", "localhost"} {
		if err = cert.Leaf.VerifyHostname(name); err != nil {
			t.Fatalf("missing SAN %s: %v", name, err)
		}
	}
}
