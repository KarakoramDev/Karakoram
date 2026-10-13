// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/coder/websocket"
)

func startTestRelay(t *testing.T, r *Registry, n *domain.Namespace) {
	t.Helper()
	key, err := r.o.Store.GetNamespaceRelayKey(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(r.Hub.Handler())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- relay.Run(ctx, relay.Options{URL: server.URL, Namespace: n.Name, Hostname: n.Relay.Hostname, CertPEM: n.ClientCert, KeyPEM: key, CAPEM: r.pki.CACert, ListenAgent: "127.0.0.1:0", HTTPClient: server.Client(), Logger: r.o.Logger, BackoffMin: time.Second, BackoffMax: time.Second})
	}()
	t.Cleanup(func() {
		cancel()
		r.Close()
		server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	waitNamespace(t, func() bool { return r.Hub.Status(n.Name).Connected })
}

func waitNamespace(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("namespace condition did not become true")
}

func TestDeletionRevokesSessionAndTokenRepairPreservesIntent(t *testing.T) {
	r, backend, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	startTestRelay(t, r, n)
	cert, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Delete(t.Context(), n.Name); err != nil {
		t.Fatal(err)
	}
	if r.Hub.Status(n.Name).Connected {
		t.Fatal("deletion kept active relay")
	}
	if r.Authorize(t.Context(), n.Name, cert) == nil {
		t.Fatal("deleting enrollment authorized")
	}
	if _, err = r.RotateToken(t.Context(), n.Name, "bad-token"); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid deletion token accepted", err)
	}
	repaired, err := r.RotateToken(t.Context(), n.Name, "replacement-token")
	if err != nil {
		t.Fatal(err)
	}
	token, err := r.o.Store.GetNamespaceToken(t.Context(), n.Name)
	if err != nil || token != "replacement-token" || backend.token != token || repaired.Phase != "Deleting" {
		t.Fatal("token repair lost deletion intent", err, repaired.Phase)
	}
	r.step(t.Context(), n.Name)
	if _, err = r.Get(t.Context(), n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("repaired deletion did not finish", err)
	}
}

func TestSessionStatusRejectsReusedNamespaceEnrollment(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	startTestRelay(t, r, n)
	replacement := *n
	replacement.ClientSerial = "new-enrollment"
	replacement.PreviousClientSerial = ""
	replacement.Relay.Connected = false
	// A status snapshot must be checked against the record being synchronized,
	// even if a namespace name was registered again for a different project.
	r.syncSession(t.Context(), &replacement)
	if replacement.Relay.Connected || replacement.Relay.Version != "" || !replacement.Relay.ConnectedSince.IsZero() || r.Hub.Status(n.Name).Connected {
		t.Fatal("old session status trusted for new enrollment")
	}
}

func TestRelayHostnameBoundsAndDeferredValidation(t *testing.T) {
	for _, host := range []string{"control", strings.Repeat("a", 20), strings.Repeat("a", 21), strings.Repeat("a", 25)} {
		name := RelayHostname(host)
		if len(name) > 25 || !relayHostnamePattern.MatchString(name) || !strings.HasSuffix(name, "relay") || name != RelayHostname(host) {
			t.Fatal("invalid derived name", name)
		}
	}
	if RelayHostname(strings.Repeat("a", 24)+"b") == RelayHostname(strings.Repeat("a", 24)+"c") {
		t.Fatal("truncation lost hostname identity")
	}
	r, _, _ := setupRegistry(t)
	options := r.o
	options.RelayHostname = strings.Repeat("a", 26)
	restored, err := New(t.Context(), options)
	if err != nil {
		t.Fatal("default-only startup rejected relay configuration", err)
	}
	defer restored.Close()
	if _, err = restored.Create(t.Context(), "testing", "proj-test", "valid-token", ""); !errors.Is(err, ErrInvalid) {
		t.Fatal("non-default namespace accepted invalid relay hostname", err)
	}
}

func TestPersistedRelayHostnameSurvivesConfigurationChange(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	options := r.o
	options.RelayHostname = "replacementrelay"
	restored, err := New(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.AgentURL(n.Name) != "https://"+n.Relay.Hostname+":9443" {
		t.Fatal("member URL ignored persisted relay hostname")
	}
	restored.step(t.Context(), n.Name)
	backend := restored.backend(n.Name).(*projectBackend)
	if backend.service == nil || backend.service.Hostname != n.Relay.Hostname {
		t.Fatal("reconciliation ignored persisted relay hostname")
	}
}

func hubPeerCertificate(t *testing.T, r *Registry, server *httptest.Server, n *domain.Namespace) *x509.Certificate {
	t.Helper()
	ws, _, err := websocket.Dial(t.Context(), "wss"+strings.TrimPrefix(server.URL, "https")+"/relay/v1/connect", &websocket.DialOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	key, err := r.o.Store.GetNamespaceRelayKey(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(n.ClientCert, key)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(r.pki.CACert)
	raw := websocket.NetConn(t.Context(), ws, websocket.MessageBinary)
	ws.SetReadLimit(relay.MaxWebSocketMessageSize)
	conn := tls.Client(raw, &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: "karakoram-relay-hub", MinVersion: tls.VersionTLS13})
	if err = conn.HandshakeContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	return conn.ConnectionState().PeerCertificates[0]
}

func TestPeriodicHubCertificateRenewalPublishesAndPersists(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	cert, key, err := relay.IssueHubCertificate(r.pki.CACert, r.pki.CAKey, time.Now().Add(-340*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	r.pki.HubCert, r.pki.HubKey = cert, key
	if err = r.o.Store.PutRelayPKI(t.Context(), r.pki); err != nil {
		t.Fatal(err)
	}
	r.Hub.SetCertificate(pair)
	server := httptest.NewTLSServer(r.Hub.Handler())
	defer server.Close()
	before := hubPeerCertificate(t, r, server, n)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitNamespace(t, func() bool {
		persisted, e := r.o.Store.GetRelayPKI(t.Context())
		return e == nil && string(persisted.HubCert) != string(cert)
	})
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	after := hubPeerCertificate(t, r, server, n)
	if before.SerialNumber.Cmp(after.SerialNumber) == 0 || relay.NeedsRenewal(after, time.Now()) {
		t.Fatal("live hub leaf was not renewed")
	}
	persisted, err := r.o.Store.GetRelayPKI(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := relay.ParseCertificatePEM(persisted.HubCert)
	if err != nil || leaf.SerialNumber.Cmp(after.SerialNumber) != 0 {
		t.Fatal("served leaf was not persisted", err)
	}
	restored, err := New(t.Context(), r.o)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if string(restored.pki.HubCert) != string(persisted.HubCert) {
		t.Fatal("restart replaced renewed persisted leaf")
	}
}

func TestDeletionIntentBlocksPreviouslyAdmittedResourceInsert(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	// Model a request that resolved its namespace before deletion committed.
	if _, err := r.Backend(n.Name); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(t.Context(), n.Name); err != nil {
		t.Fatal(err)
	}
	cluster := &domain.Cluster{Namespace: n.Name, Name: "orders", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := r.o.Store.CreateCluster(t.Context(), cluster); !errors.Is(err, ErrConflict) {
		t.Fatal("stale cluster request bypassed deletion intent", err)
	}
	storage := &domain.BackupStorage{Namespace: n.Name, Name: "archive"}
	if err := r.o.Store.CreateBackupStorage(t.Context(), storage); !errors.Is(err, ErrConflict) {
		t.Fatal("stale storage request bypassed deletion intent", err)
	}
}

func TestReregisteredNamespaceRejectsDeletedEnrollment(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	startTestRelay(t, r, n)
	oldCert, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Delete(t.Context(), n.Name); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), n.Name)
	replacement, err := r.Create(t.Context(), n.Name, "proj-replacement", "replacement-token", "")
	if err != nil {
		t.Fatal(err)
	}
	r.syncSession(t.Context(), replacement)
	if replacement.Relay.Connected || r.Authorize(t.Context(), n.Name, oldCert) == nil {
		t.Fatal("reused name trusted deleted enrollment")
	}
}

func TestRegistrationWaitsForNamespaceLifecycleLock(t *testing.T) {
	r, _, _ := setupRegistry(t)
	m := r.lock("testing")
	m.Lock()
	done := make(chan error, 1)
	go func() { _, err := r.Create(t.Context(), "testing", "proj-test", "valid-token", ""); done <- err }()
	select {
	case err := <-done:
		m.Unlock()
		t.Fatal("registration bypassed namespace lifecycle lock", err)
	case <-time.After(20 * time.Millisecond):
	}
	m.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
