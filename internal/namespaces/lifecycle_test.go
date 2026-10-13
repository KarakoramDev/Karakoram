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
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
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
	waitNamespace(t, func() bool {
		s := r.Hub.Status(n.Name)
		return s.Connected && s.CertificateSerial == n.ClientSerial
	})
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

func TestSessionSnapshotCannotRevokePublishedEnrollment(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	startTestRelay(t, r, n)
	oldCert, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	key, err := r.issue(n)
	if err != nil {
		t.Fatal(err)
	}
	token, err := r.o.Store.GetNamespaceToken(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.o.Store.UpdateNamespaceCredentials(t.Context(), n, token, key); err != nil {
		t.Fatal(err)
	}
	if err = r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	snapshot := r.Hub.Status(n.Name)
	if snapshot.CertificateSerial != oldCert.SerialNumber.String() || !snapshot.Connected {
		t.Fatal("renewal did not retain the previous session")
	}
	// Pause synchronization after its snapshot, then publish the current leaf.
	startTestRelay(t, r, n)
	published := r.Hub.Status(n.Name)
	if r.Hub.AcceptsSerial(n.Name, oldCert.SerialNumber.String()) {
		t.Fatal("current publication did not revoke the previous serial")
	}
	r.syncSessionSnapshot(t.Context(), n, snapshot)
	if !n.Relay.Connected || n.Relay.ConnectedSince != published.ConnectedSince || n.PreviousClientSerial != "" || !n.PreviousCertNotAfter.IsZero() {
		t.Fatal("stale snapshot lost current session or retained previous enrollment")
	}
	if err = r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	status := r.Hub.Status(n.Name)
	if !status.Connected || status.CertificateSerial != n.ClientSerial || status.ConnectedSince != published.ConnectedSince {
		t.Fatal("synchronization revoked the newly published session")
	}
	if r.Authorize(t.Context(), n.Name, oldCert) == nil {
		t.Fatal("save restored the revoked serial")
	}
	persisted, err := r.Get(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.PreviousClientSerial != "" || !persisted.PreviousCertNotAfter.IsZero() {
		t.Fatal("previous enrollment revocation was not persisted")
	}
	restored, err := New(t.Context(), r.o)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.Authorize(t.Context(), n.Name, oldCert) == nil || restored.Hub.AcceptsSerial(n.Name, oldCert.SerialNumber.String()) {
		t.Fatal("reload restored the revoked serial")
	}
	currentCert, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.Authorize(t.Context(), n.Name, currentCert); err != nil {
		t.Fatal("reload rejected current enrollment", err)
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
	r, _, log := setupRegistry(t)
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
	found := false
	for _, e := range log.items {
		if e.Reason == "RelayHubCertificateRenewed" {
			found = true
			if e.Namespace != "" || e.Cluster != "" || e.Actor != "system" || e.Target != "relay-hub" || e.Resource != "namespaces" {
				t.Fatal("hub renewal audit has incorrect scope or actor", e)
			}
		}
	}
	if !found {
		t.Fatal("hub certificate renewal was not audited")
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

func TestNamespaceDeleteAndRecreatePersistsFreshEnrollment(t *testing.T) {
	r, backend, _ := setupRegistry(t)
	ctx := t.Context()
	old, err := r.Create(ctx, "production", "proj-test", "valid-token", "Original description")
	if err != nil {
		t.Fatal(err)
	}
	oldKey, err := r.o.Store.GetNamespaceRelayKey(ctx, old.Name)
	if err != nil {
		t.Fatal(err)
	}
	step := func() {
		m := r.lock(old.Name)
		m.Lock()
		defer m.Unlock()
		r.step(ctx, old.Name)
	}
	for range 4 {
		step()
	}
	if backend.imports != 1 || backend.deploys != 1 {
		t.Fatal("original relay did not deploy")
	}
	startTestRelay(t, r, old)
	oldCert, err := relay.ParseCertificatePEM(old.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Delete(ctx, old.Name); err != nil {
		t.Fatal(err)
	}
	step()
	deletingOptions := r.o
	deletingOptions.NewBackend = func(_ zerops.Options) (Backend, error) { return backend, nil }
	deletingReload, err := New(ctx, deletingOptions)
	if err != nil {
		t.Fatal(err)
	}
	deleting, err := deletingReload.Get(ctx, old.Name)
	if err != nil || deleting.Phase != "Deleting" || deleting.PendingOpKind != "delete" || deleting.PendingOpID == "" {
		t.Fatal("registry reload lost persisted deletion intent", err)
	}
	if deletingReload.Authorize(ctx, old.Name, oldCert) == nil {
		t.Fatal("registry reload authorized a deleting enrollment")
	}
	if err = deletingReload.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		step()
	}
	if backend.deletes != 1 || backend.service != nil || r.backend(old.Name) != nil {
		t.Fatal("relay deletion did not retire the original backend")
	}
	for _, get := range []func() error{
		func() error { _, err := r.Get(ctx, old.Name); return err },
		func() error { _, err := r.o.Store.GetNamespaceToken(ctx, old.Name); return err },
		func() error { _, err := r.o.Store.GetNamespaceRelayKey(ctx, old.Name); return err },
	} {
		if err = get(); !errors.Is(err, store.ErrNotFound) {
			t.Fatal("deleted namespace or credential survives", err)
		}
	}
	lateSave := func() error {
		result := make(chan error, 1)
		go func() {
			m := r.lock(old.Name)
			m.Lock()
			defer m.Unlock()
			snapshot := *old
			result <- r.save(ctx, &snapshot)
		}()
		return <-result
	}
	if err = lateSave(); !errors.Is(err, store.ErrStale) {
		t.Fatal("delayed original snapshot resurrected the deleted namespace")
	}
	if _, err = r.Get(ctx, old.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("delayed snapshot restored the deleted row", err)
	}
	fresh, err := r.Create(ctx, old.Name, old.ProjectID, "replacement-token", "Replacement description")
	if err != nil {
		t.Fatal(err)
	}
	if err = lateSave(); !errors.Is(err, store.ErrStale) {
		t.Fatal("delayed original snapshot overwrote the replacement")
	}
	if !fresh.CreatedAt.After(old.CreatedAt) || fresh.ClientSerial == old.ClientSerial || fresh.OwnershipMarker == old.OwnershipMarker {
		t.Fatal("re-registration reused the original identity")
	}
	if fresh.Phase != "Pending" || fresh.Relay.Phase != "Pending" || fresh.Relay.ServiceID != "" || fresh.PendingOpID != "" || fresh.DeployedExecutableHash != "" || fresh.PreviousClientSerial != "" {
		t.Fatal("re-registration retained relay checkpoints")
	}
	check := func(registry *Registry) {
		t.Helper()
		got, err := registry.Get(ctx, fresh.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !got.CreatedAt.Equal(fresh.CreatedAt) || got.Description != fresh.Description || got.ClientSerial != fresh.ClientSerial || got.OwnershipMarker != fresh.OwnershipMarker || got.Phase == "Deleting" {
			t.Fatal("original namespace snapshot replaced the new registration")
		}
		if registry.Authorize(ctx, fresh.Name, oldCert) == nil || registry.Hub.AcceptsSerial(fresh.Name, old.ClientSerial) {
			t.Fatal("original relay enrollment was restored")
		}
		token, err := registry.o.Store.GetNamespaceToken(ctx, fresh.Name)
		if err != nil || token != "replacement-token" {
			t.Fatal("original token restored", err)
		}
		key, err := registry.o.Store.GetNamespaceRelayKey(ctx, fresh.Name)
		if err != nil || string(key) == string(oldKey) {
			t.Fatal("original relay key restored", err)
		}
	}
	check(r)
	// Deliver queued session notices from the deleted enrollment, then run
	// periodic reconciliation with its original import/deploy/delete history.
	for {
		select {
		case notice := <-r.notices:
			r.sessionEvent(ctx, notice)
		default:
			goto drained
		}
	}
drained:
	check(r)
	step()
	got, err := r.Get(ctx, fresh.Name)
	if err != nil || got.Relay.Phase != "Deploying" || got.PendingOpKind != "import" {
		t.Fatal("replacement did not transition Pending to Deploying", err)
	}
	for range 3 {
		step()
		check(r)
	}
	runTicks := func(registry *Registry) {
		t.Helper()
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- registry.Run(runCtx) }()
		defer func() {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal("registry loop shutdown", err)
			}
		}()
		for range 3 {
			before, err := registry.Get(ctx, fresh.Name)
			if err != nil {
				t.Fatal(err)
			}
			registry.trigger()
			waitNamespace(t, func() bool {
				registry.trigger()
				after, err := registry.Get(ctx, fresh.Name)
				return err == nil && after.UpdatedAt.After(before.UpdatedAt)
			})
			check(registry)
		}
	}
	runTicks(r)
	if backend.imports != 2 || backend.deploys != 2 || backend.deletes != 1 {
		t.Fatal("replacement repeated the original relay deletion")
	}
	o := r.o
	o.NewBackend = func(_ zerops.Options) (Backend, error) { return backend, nil }
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	check(reloaded)
	for range 3 {
		reloaded.step(ctx, fresh.Name)
		check(reloaded)
	}
	runTicks(reloaded)
	if backend.deletes != 1 || backend.imports != 2 || backend.deploys != 2 {
		t.Fatal("registry reload resumed an obsolete relay operation")
	}
}
