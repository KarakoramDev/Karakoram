// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
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
	backends, err := infra.NewRegistry(namespaceTestFactory{})
	if err != nil {
		t.Fatal(err)
	}
	s.Backends = backends
	r, err := namespaces.New(context.Background(), namespaces.Options{Store: s.Store, DefaultBackend: &namespaceTestBackend{project: "proj-default"}, Backends: backends, DefaultTarget: infra.Target{Kind: "zerops", ID: "proj-default", Token: "env-token"}, PublicURL: "https://example.com", AdvertiseURL: "https://control:9443", RelayHostname: "controlrelay", ExecutableHash: "test-hash", Logger: s.Logger})
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

func TestNamespaceValidationOutageResponses(t *testing.T) {
	s, token := namespaceTestServer(t)
	if w := call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"testing","projectId":"proj-test","token":"synthetic-token"}`, token); w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/v1/namespaces", `{"name":"another","projectId":"proj-other","token":"unavailable-token"}`},
		{http.MethodPut, "/api/v1/namespaces/testing/token", `{"token":"unavailable-token"}`},
	} {
		w := call(s, tc.method, tc.path, tc.body, token)
		var body struct {
			Code, Message string
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusInternalServerError || body.Code != "internal_error" || !strings.HasPrefix(body.Message, "Zerops API temporarily unavailable: ") || !strings.Contains(body.Message, "platformUnavailable") || !strings.HasSuffix(body.Message, "; retrying") {
			t.Fatalf("outage lost safe diagnostic: %d %s", w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "unavailable-token") {
			t.Fatal("validation token leaked")
		}
	}
	stored, err := s.Store.GetNamespaceTokenForUpdate(t.Context(), "testing")
	if err != nil || stored != "synthetic-token" {
		t.Fatal("failed rotation replaced stored token", err)
	}
	before := len(auditEvents(t, s, "NamespaceValidationFailed"))
	n, err := s.Store.GetNamespaceForUpdate(t.Context(), "testing")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.UpdateNamespaceCredentials(t.Context(), n, "unavailable-token", nil); err != nil {
		t.Fatal(err)
	}
	w := call(s, http.MethodPost, "/api/v1/namespaces/testing/validate", "", token)
	var validated api.Namespace
	if err = json.Unmarshal(w.Body.Bytes(), &validated); err != nil || w.Code != http.StatusOK || !validated.Token.Valid || validated.Token.LastError == nil || !strings.Contains(*validated.Token.LastError, "temporarily unavailable") {
		t.Fatalf("outage changed known validity: %d %s (%v)", w.Code, w.Body.String(), err)
	}
	if len(auditEvents(t, s, "NamespaceValidationFailed")) != before {
		t.Fatal("platform outage audited as invalid-token rejection")
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

type namespaceTestFactory struct{}

func (namespaceTestFactory) Info() infra.BackendInfo {
	return infra.BackendInfo{Kind: "zerops", DisplayName: "Zerops", Relay: true, Capabilities: fakeBackend{}.Capabilities()}
}
func (namespaceTestFactory) TargetFromEnv(func(string) string) (infra.Target, error) {
	return infra.Target{}, nil
}
func (namespaceTestFactory) New(target infra.Target) (infra.Backend, error) {
	return &namespaceTestBackend{project: target.ID, token: target.Token}, nil
}
func (f namespaceTestFactory) Validate(_ context.Context, target infra.Target) (infra.Backend, infra.Project, error) {
	if target.Token == "bad-token" {
		return nil, infra.Project{}, infra.ErrUnauthorized
	}
	if target.Token == "unavailable-token" {
		return nil, infra.Project{}, namespaceUnavailableError{}
	}
	if target.Token == "rejected-token" {
		return nil, infra.Project{}, &infra.RejectedError{Code: "invalidTarget", Message: "backend target is invalid"}
	}
	if target.Token == "unsupported-token" {
		return nil, infra.Project{}, infra.ErrUnsupported
	}
	backend, err := f.New(target)
	return backend, infra.Project{ID: target.ID, Name: "Test project"}, err
}

type namespaceUnavailableError struct{}

func (namespaceUnavailableError) Error() string        { return "platformUnavailable" }
func (namespaceUnavailableError) Is(target error) bool { return target == infra.ErrUnavailable }
func (namespaceUnavailableError) SafeDetail() string   { return "platformUnavailable" }

func TestBackendDiscoveryAndNamespaceConfig(t *testing.T) {
	s, token := namespaceTestServer(t)
	w := call(s, http.MethodGet, "/api/v1/backends", "", token)
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	var list api.BackendList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Kind != "zerops" || list.Items[0].DisplayName != "Zerops" || !list.Items[0].Relay || len(list.Items[0].Capabilities) == 0 || len(list.Items[0].ConfigKeys) != 0 {
		t.Fatalf("backend discovery: %+v", list)
	}
	if w := call(s, http.MethodGet, "/api/v1/backends", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatal("unauthenticated discovery", w.Code)
	}
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "unprivileged"}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	unprivileged, _, err := s.Auth.CreateToken(t.Context(), "unprivileged", "testing", nil)
	if err != nil {
		t.Fatal(err)
	}
	if w := call(s, http.MethodGet, "/api/v1/backends", "", unprivileged); w.Code != http.StatusForbidden {
		t.Fatal("unprivileged discovery", w.Code, w.Body.String())
	}
	for _, body := range []string{
		`{"name":"testing","backend":"unknown","projectId":"target","token":"credential"}`,
		`{"name":"testing","backend":"zerops","config":{"unknown":"value"},"projectId":"target","token":"credential"}`,
		`{"name":"testing","backend":"zerops","config":{"apiUrl":"https://untrusted.example.com"},"projectId":"target","token":"credential"}`,
	} {
		if w := call(s, http.MethodPost, "/api/v1/namespaces", body, token); w.Code != http.StatusBadRequest {
			t.Fatal("invalid backend configuration", w.Code, w.Body.String())
		}
	}
	w = call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"testing","backend":"zerops","projectId":"target","token":"credential"}`, token)
	if w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	var n api.Namespace
	if err := json.Unmarshal(w.Body.Bytes(), &n); err != nil {
		t.Fatal(err)
	}
	if n.Backend != "zerops" || len(n.Capabilities) == 0 || (n.Config != nil && len(*n.Config) != 0) {
		t.Fatalf("namespace backend metadata: %+v", n)
	}
}

func TestNamespaceRejectedAndUnsupportedCredentials(t *testing.T) {
	s, token := namespaceTestServer(t)
	if w := call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"testing","projectId":"proj-test","token":"synthetic-token"}`, token); w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, credential := range []string{"rejected-token", "unsupported-token"} {
		for _, request := range []struct{ method, path, body string }{
			{http.MethodPost, "/api/v1/namespaces", `{"name":"another","projectId":"proj-other","token":"` + credential + `"}`},
			{http.MethodPut, "/api/v1/namespaces/testing/token", `{"token":"` + credential + `"}`},
		} {
			w := call(s, request.method, request.path, request.body, token)
			var body api.Error
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusBadRequest || body.Code != "invalid_request" {
				t.Fatalf("%s %s: %d %s (%v)", request.method, request.path, w.Code, w.Body.String(), err)
			}
		}
	}
	stored, err := s.Store.GetNamespaceTokenForUpdate(t.Context(), "testing")
	if err != nil || stored != "synthetic-token" {
		t.Fatal("failed rotation replaced stored token", err)
	}
}

func TestManualNamespaceValidationReturnsStoredTokenReadError(t *testing.T) {
	s, token := namespaceTestServer(t)
	if w := call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"testing","projectId":"proj-test","token":"synthetic-token"}`, token); w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	n, err := s.Store.GetNamespaceForUpdate(t.Context(), "testing")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteNamespaceToken(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	w := call(s, http.MethodPost, "/api/v1/namespaces/testing/validate", "", token)
	if w.Code != http.StatusNotFound {
		t.Fatal("stored token read error hidden", w.Code, w.Body.String())
	}
	after, err := s.Store.GetNamespaceForUpdate(t.Context(), n.Name)
	if err != nil || after.Token != n.Token || after.Phase != n.Phase {
		t.Fatalf("failed read changed token health: %+v %v", after, err)
	}
}

func TestClusterAdmissionChecksNamespaceBackendCapabilities(t *testing.T) {
	s, token := namespaceTestServer(t)
	if err := s.Backends.Register(fake.NewFactory(fake.Options{Name: "fake", Capabilities: infra.Capabilities{Logs: true}})); err != nil {
		t.Fatal(err)
	}
	if w := call(s, http.MethodPost, "/api/v1/namespaces", `{"name":"limited","backend":"fake","projectId":"fake-project","token":"credential"}`, token); w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	path := "/api/v1/clusters?namespace=limited"
	publicSpec := `{"provider":"fake","exposure":{"public":true,"allowlist":["2001:db8::/32"]}}`
	assertUnsupported := func(method, path, body string) {
		t.Helper()
		w := call(s, method, path, body, token)
		var result api.Error
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != http.StatusBadRequest || result.Code != "invalid_request" || !strings.Contains(result.Message, "backend fake") || !strings.Contains(result.Message, "publicTcp") {
			t.Fatalf("%s %s: %d %s (%v)", method, path, w.Code, w.Body.String(), err)
		}
	}
	assertUnsupported(http.MethodPost, path, `{"name":"public","spec":`+publicSpec+`}`)
	if w := call(s, http.MethodPost, path, `{"name":"private","spec":{"provider":"fake"}}`, token); w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	path = "/api/v1/clusters/private?namespace=limited"
	assertUnsupported(http.MethodPut, path, `{"spec":`+publicSpec+`}`)
	assertUnsupported(http.MethodPatch, path, `{"spec":{"exposure":{"public":true,"allowlist":["2001:db8::/32"]}}}`)
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Namespace: "limited", Name: "private"})
	if err != nil || c.Spec.Exposure.Public || c.Generation != 1 {
		t.Fatalf("rejected update changed cluster: %+v %v", c, err)
	}
}
