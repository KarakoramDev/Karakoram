// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeTemporalAccess struct {
	fakeProvider
	err                 error
	cn, secret, deleted string
	ttl                 time.Duration
}

func (*fakeTemporalAccess) Name() string { return "temporal" }
func (f *fakeTemporalAccess) Describe() domain.ProviderInfo {
	info := f.fakeProvider.Describe()
	info.Name = "temporal"
	return info
}
func temporalTestCertificate() temporal.ClientCertificate {
	return temporal.ClientCertificate{Serial: "1", CommonName: "worker", CurrentCA: true, FingerprintSHA256: "fingerprint", CAFingerprintSHA256: "ca-fingerprint", IssuedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)}
}
func (f *fakeTemporalAccess) IssueClientCertificate(_ context.Context, _ *engine.Context, cn string, ttl time.Duration) (temporal.IssuedClientCertificate, error) {
	f.cn, f.ttl = cn, ttl
	return temporal.IssuedClientCertificate{ClientCertificate: temporalTestCertificate(), CertificatePEM: "cert-pem", PrivateKeyPEM: "private-key-pem", CAPEM: "ca-pem", FrontendAddress: "frontend:7233", ServerName: "temporal"}, f.err
}
func (f *fakeTemporalAccess) ListClientCertificates(context.Context, *engine.Context) ([]temporal.ClientCertificate, error) {
	old := temporalTestCertificate()
	old.Serial, old.CurrentCA = "old", false
	return []temporal.ClientCertificate{temporalTestCertificate(), old}, f.err
}
func (f *fakeTemporalAccess) ListNamespaces(context.Context, *engine.Context) ([]temporal.Namespace, error) {
	return []temporal.Namespace{{Name: "orders", Retention: "72h", State: "REGISTERED", Managed: false}}, f.err
}
func (f *fakeTemporalAccess) DeleteNamespace(_ context.Context, _ *engine.Context, name string) error {
	f.deleted = name
	return f.err
}
func (f *fakeTemporalAccess) SetUIOIDCClientSecret(_ context.Context, _ *engine.Context, secret string) error {
	f.secret = secret
	return f.err
}
func temporalTestServer(t *testing.T) (*Server, *fakeTemporalAccess, string) {
	t.Helper()
	s, _, token := testServer(t)
	f := &fakeTemporalAccess{}
	registry, err := engine.NewRegistry(f, &fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "temporal"
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	return s, f, token
}
func TestTemporalHandlersAndAudit(t *testing.T) {
	s, f, token := temporalTestServer(t)
	for _, tc := range []struct {
		method, path, body string
		status             int
		secret             bool
	}{
		{"POST", "client-certificates", `{"commonName":"worker"}`, 201, true},
		{"GET", "client-certificates", "", 200, false},
		{"GET", "namespaces", "", 200, false},
		{"DELETE", "namespaces/orders", "", 202, false},
		{"PUT", "ui/oidc-client-secret", `{"clientSecret":"private-oidc-secret"}`, 204, true},
	} {
		w := call(s, tc.method, "/api/v1/clusters/prod/temporal/"+tc.path, tc.body, token)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
		if tc.secret && w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("secret response cached")
		}
		if tc.method == "GET" && strings.Contains(w.Body.String(), "privateKeyPem") {
			t.Fatal("list leaked key")
		}
		if tc.path == "client-certificates" {
			if !strings.Contains(w.Body.String(), `"currentCa":true`) {
				t.Fatal("missing current CA flag", w.Body.String())
			}
			if tc.method == "GET" && !strings.Contains(w.Body.String(), `"currentCa":false`) {
				t.Fatal("superseded certificate must retain false", w.Body.String())
			}
		}
	}
	if f.cn != "worker" || f.ttl != 720*time.Hour || f.secret != "private-oidc-secret" || f.deleted != "orders" {
		t.Fatal("access arguments not forwarded")
	}
	if calls := s.Trigger.(*triggerSpy).names; len(calls) != 1 {
		t.Fatalf("OIDC secret update must trigger reconciliation once: %+v", calls)
	}
	events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Cluster: "prod", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-key-pem", "private-oidc-secret"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("secret in audit")
		}
	}
	for _, reason := range []string{"TemporalClientCertificateIssued", "TemporalUIOIDCClientSecretUpdated", "DeleteTemporalNamespaceSucceeded"} {
		if !strings.Contains(string(data), reason) {
			t.Fatal("missing audit", reason)
		}
	}
}
func TestTemporalValidationAndErrors(t *testing.T) {
	s, f, token := temporalTestServer(t)
	for _, body := range []string{`{}`, `{"commonName":""}`, `{"commonName":"worker","ttlHours":0}`, `{"commonName":"worker","ttlHours":9529}`} {
		w := call(s, "POST", "/api/v1/clusters/prod/temporal/client-certificates", body, token)
		if w.Code != 422 {
			t.Fatalf("%s: %d %s", body, w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		err    error
		status int
	}{{engine.ErrInvalid, 422}, {engine.ErrConflict, 409}, {engine.ErrNotFound, 404}, {engine.ErrUnavailable, 503}} {
		f.err = tc.err
		w := call(s, "DELETE", "/api/v1/clusters/prod/temporal/namespaces/orders", "", token)
		if w.Code != tc.status {
			t.Fatalf("%v: %d %s", tc.err, w.Code, w.Body.String())
		}
	}
	f.err = nil
	for _, cluster := range []string{"prod", "missing"} {
		if cluster == "prod" {
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			c.Spec.Provider = "fake"
			if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
				t.Fatal(err)
			}
		}
		w := call(s, "GET", "/api/v1/clusters/"+cluster+"/temporal/namespaces", "", token)
		if w.Code != 404 {
			t.Fatalf("%s: %d %s", cluster, w.Code, w.Body.String())
		}
	}
}
func TestTemporalRBACInventory(t *testing.T) {
	for _, id := range []string{"issueTemporalClientCertificate", "listTemporalClientCertificates", "listTemporalNamespaces", "deleteTemporalNamespace", "setTemporalUIOIDCClientSecret"} {
		rule, ok := api.OperationRBAC[id]
		if !ok || rule.Public || rule.ObjectParam != "cluster" {
			t.Fatal("missing cluster RBAC", id)
		}
		if id == "issueTemporalClientCertificate" && (rule.Resource != "cluster-credentials" || rule.Action != "read") {
			t.Fatal("issuance must require credential reveal permission")
		}
	}
}

func TestTemporalRestoreAcknowledgementPayload(t *testing.T) {
	for _, ack := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "acknowledged"}[ack], func(t *testing.T) {
			s, _, token := temporalTestServer(t)
			backup := domain.Backup{Name: "saved", Cluster: "prod", Provider: "temporal", Type: "snapshot", Phase: domain.PhaseSucceeded}
			if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
				t.Fatal(err)
			}
			body := `{"name":"restore","source":{"sourceCluster":"prod","backup":"saved"}`
			if ack {
				body += `,"acknowledgeSharedSource":true`
			}
			body += `}`
			w := call(s, "POST", "/api/v1/clusters/prod/restores", body, token)
			if w.Code != 202 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			if len(ops) != 1 {
				t.Fatalf("operations: %d", len(ops))
			}
			var payload struct {
				Restore                 string `json:"restore"`
				AcknowledgeSharedSource bool   `json:"acknowledgeSharedSource"`
			}
			if err := json.Unmarshal(ops[0].Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Restore != "restore" || payload.AcknowledgeSharedSource != ack {
				t.Fatalf("payload: %s", ops[0].Payload)
			}
		})
	}
}

// Admission never calls binding operations; the service only needs to be present.
type temporalAdmissionBindings struct{ pgbinding.Service }

func TestTemporalPrivateIPv4Admission(t *testing.T) {
	s, _, token := testServer(t)
	p := temporal.New(temporal.Options{Bindings: temporalAdmissionBindings{}})
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	spec := `{"provider":"temporal","engineOptions":{"persistence":{"postgres":{"cluster":"appdb"}}}}`
	body := `{"name":"workflows","spec":` + spec + `}`
	setFamily := func(family string) {
		s.namespaces = engine.StaticNamespaces{DefaultBackend: infrafake.New(infrafake.Options{
			Layout: infra.Layout{PrivateAddressFamily: family},
		})}
	}
	assertRejected := func(method, path, body string) {
		t.Helper()
		w := call(s, method, path, body, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_request") ||
			!strings.Contains(w.Body.String(), "Temporal requires a backend with private IPv4 networking; Zerops is not supported yet") {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
	}
	setFamily(infra.AddressFamilyIPv6)
	for _, path := range []string{"/api/v1/clusters", "/api/v1/dry-run/clusters"} {
		assertRejected("POST", path, body)
	}
	if _, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "workflows"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected create persisted: %v", err)
	}
	setFamily(infra.AddressFamilyIPv4)
	previewBody(t, call(s, "POST", "/api/v1/dry-run/clusters", body, token))
	if w := call(s, "POST", "/api/v1/clusters", body, token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	setFamily(infra.AddressFamilyIPv6)
	for _, prefix := range []string{"/api/v1/clusters/", "/api/v1/dry-run/clusters/"} {
		assertRejected("PUT", prefix+"workflows", `{"spec":`+spec+`}`)
		assertRejected("PATCH", prefix+"workflows", `{"spec":{"monitoring":{"enabled":true}}}`)
	}
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "workflows"})
	if err != nil || c.Generation != 1 {
		t.Fatalf("rejected update changed state: %+v %v", c, err)
	}
}
