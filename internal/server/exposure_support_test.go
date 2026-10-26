// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type exposureBackend struct {
	fakeBackend
	supported bool
	err       error
}

func (b *exposureBackend) SupportsPublicIP(context.Context, string) (bool, error) {
	return b.supported, b.err
}

func TestExposureAdmissionCreateUpdate(t *testing.T) {
	s, _, token := testServer(t)
	b := &exposureBackend{}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	exposure := `{"public":true,"ipType":"IPV4","allowlist":["192.0.2.1/32"]}`
	assertRejected := func(method, path, body string) {
		t.Helper()
		w := call(s, method, path, body, token)
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "spec.exposure.ipType IPV4 is unavailable") {
			t.Fatalf("%s: %d %s", method, w.Code, w.Body.String())
		}
	}
	assertRejected(http.MethodPost, "/api/v1/clusters", `{"name":"public","spec":{"provider":"fake","exposure":`+exposure+`}}`)
	if _, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "public"}); err == nil {
		t.Fatal("rejected create persisted")
	}
	w := call(s, http.MethodPost, "/api/v1/clusters", `{"name":"private","spec":{"provider":"fake"}}`, token)
	if w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body.String())
	}
	assertRejected(http.MethodPut, "/api/v1/clusters/private", `{"spec":{"provider":"fake","exposure":`+exposure+`}}`)
	assertRejected(http.MethodPatch, "/api/v1/clusters/private", `{"spec":{"exposure":`+exposure+`}}`)
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "private"})
	if err != nil || c.Generation != 1 {
		t.Fatalf("rejected update changed state: %+v %v", c, err)
	}
	b.err = errors.New("project lookup unavailable")
	w = call(s, http.MethodPost, "/api/v1/clusters", `{"name":"unknown","spec":{"provider":"fake","exposure":`+exposure+`}}`, token)
	if w.Code != http.StatusCreated {
		t.Fatal("unknown rejected", w.Code, w.Body.String())
	}
	b.err = nil
	b.supported = true
	w = call(s, http.MethodPost, "/api/v1/clusters", `{"name":"supported","spec":{"provider":"fake","exposure":`+exposure+`}}`, token)
	if w.Code != http.StatusCreated {
		t.Fatal("supported rejected", w.Code, w.Body.String())
	}
	b.supported = false
	w = call(s, http.MethodPatch, "/api/v1/clusters/supported", `{"spec":{"monitoring":{"enabled":true}}}`, token)
	if w.Code != http.StatusOK {
		t.Fatal("unrelated legacy PATCH rejected", w.Code, w.Body.String())
	}
	assertRejected(http.MethodPatch, "/api/v1/clusters/supported", `{"spec":{"exposure":{"allowlist":["192.0.2.2/32"]}}}`)
}

type exposureNamespaces struct {
	engine.StaticNamespaces
	backends map[string]infra.Backend
}

func (n exposureNamespaces) Backend(namespace string) (infra.Backend, error) {
	return n.backends[namespace], nil
}

func TestExposureAdmissionNamespaceIsolation(t *testing.T) {
	s, _, token := testServer(t)
	s.namespaces = exposureNamespaces{backends: map[string]infra.Backend{
		"default":   &exposureBackend{supported: true},
		"limited":   &exposureBackend{},
		"available": &exposureBackend{supported: true},
	}}
	for _, namespace := range []string{"limited", "available"} {
		if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: namespace, Backend: "zerops", Phase: "Ready"}); err != nil {
			t.Fatal(err)
		}
	}
	body := `{"name":"public","spec":{"provider":"fake","exposure":{"public":true,"ipType":"IPV4","allowlist":["192.0.2.1/32"]}}}`
	for _, tc := range []struct {
		namespace string
		status    int
	}{
		{"limited", http.StatusBadRequest}, {"available", http.StatusCreated}, {"default", http.StatusCreated},
	} {
		w := call(s, http.MethodPost, "/api/v1/clusters?namespace="+tc.namespace, body, token)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.namespace, w.Code, w.Body.String())
		}
	}
}
