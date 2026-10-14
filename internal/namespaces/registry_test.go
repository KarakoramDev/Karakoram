// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

type projectBackend struct {
	infra.Backend
	project                             string
	token                               string
	service                             *infra.Service
	imports, deploys, restarts, deletes int
	env                                 map[string]string
	opDone                              bool
}

func (b *projectBackend) SetToken(s string) { b.token = s }
func (b *projectBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: b.project, Name: "Test project"}, nil
}
func (b *projectBackend) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, nil
}
func (b *projectBackend) ImportServices(_ context.Context, s []infra.ServiceSpec) (infra.Op, error) {
	b.imports++
	b.env = s[0].Env
	b.service = &infra.Service{ID: "service-test", Hostname: s[0].Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": s[0].Env["KARAKORAM_RELAY_OWNER"]}}
	return infra.Op{ID: "op-import", Kind: "import", ServiceID: b.service.ID}, nil
}
func (b *projectBackend) SetEnvSecrets(_ context.Context, _ string, env map[string]string) error {
	b.env = env
	return nil
}
func (b *projectBackend) Restart(context.Context, string) (infra.Op, error) {
	b.restarts++
	return infra.Op{ID: "op-restart"}, nil
}
func (b *projectBackend) Deploy(_ context.Context, _ string, d infra.Deployment) (infra.Op, error) {
	b.deploys++
	_, err := io.Copy(io.Discard, d.Artifact)
	return infra.Op{ID: "op-deploy"}, err
}
func (b *projectBackend) DeleteServiceByID(ctx context.Context, hostname, id string) (infra.Op, error) {
	if b.service == nil || b.service.ID != id {
		return infra.Op{}, errors.New("service identity changed")
	}
	return b.DeleteService(ctx, hostname)
}
func (b *projectBackend) DeleteService(context.Context, string) (infra.Op, error) {
	b.deletes++
	b.service = nil
	return infra.Op{ID: "op-delete"}, nil
}
func (b *projectBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: b.opDone}, nil
}

type testArtifact struct{}

func (testArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type eventLog struct {
	mu      sync.Mutex
	reasons []string
	items   []domain.Event
}

func (l *eventLog) Record(_ context.Context, e domain.Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reasons = append(l.reasons, e.Reason)
	l.items = append(l.items, e)
	return nil
}
func setupRegistry(t *testing.T) (*Registry, *projectBackend, *eventLog) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	defaultBackend := &projectBackend{project: "proj-default", token: "env-token", opDone: true}
	remote := &projectBackend{project: "proj-test", token: "valid-token", opDone: true}
	log := &eventLog{}
	o := Options{Store: st, DefaultBackend: defaultBackend, Zerops: zerops.Options{ProjectID: "proj-default", Token: "env-token"}, RelayHostname: "controlrelay", PublicURL: "https://example.com", AdvertiseURL: "https://control:9443", Events: log, Artifact: testArtifact{}, ExecutableHash: "test-hash", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	o.Validate = func(_ context.Context, z zerops.Options) (Backend, infra.Project, error) {
		if z.Token == "bad-token" {
			return nil, infra.Project{}, &zerops.APIError{StatusCode: 403, Code: "accessDenied"}
		}
		if z.Token == "wrong-project-token" {
			return nil, infra.Project{}, &zerops.APIError{Code: "accessDenied", Message: "Token project does not match requested project"}
		}
		b := remote
		if z.ProjectID == "proj-default" {
			b = defaultBackend
		}
		return b, infra.Project{ID: z.ProjectID, Name: "Test project"}, nil
	}
	o.NewBackend = func(z zerops.Options) (Backend, error) {
		return &projectBackend{project: z.ProjectID, token: z.Token, opDone: true}, nil
	}
	r, err := New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, remote, log
}
func createTestNamespace(t *testing.T, r *Registry) *domain.Namespace {
	t.Helper()
	n, err := r.Create(context.Background(), "testing", "proj-test", "valid-token", "")
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func TestCreateValidationAndRouting(t *testing.T) {
	for _, tc := range []struct{ name, project, token string }{{"default", "proj-test", "valid-token"}, {"x", "proj-test", "valid-token"}, {"testing", "proj-default", "valid-token"}, {"testing", "proj-test", "bad-token"}, {"testing", "proj-test", "wrong-project-token"}} {
		t.Run(tc.name+tc.token+tc.project, func(t *testing.T) {
			r, _, _ := setupRegistry(t)
			if _, err := r.Create(context.Background(), tc.name, tc.project, tc.token, ""); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
	r, b, _ := setupRegistry(t)
	b.service = &infra.Service{ID: "someone-elses-service", Hostname: "controlrelay", Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": "other-owner"}}
	if _, err := r.Create(context.Background(), "testing", "proj-test", "valid-token", ""); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign relay accepted", err)
	}
	b.service = nil
	n := createTestNamespace(t, r)
	if _, err := r.Create(context.Background(), "another", "proj-test", "valid-token", ""); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate project", err)
	}
	if r.Dialer("default") != nil || r.Dialer("") != nil || r.Dialer(n.Name) == nil {
		t.Fatal("namespace routing")
	}
	if r.AgentURL("default") != "https://control:9443" || r.AgentURL(n.Name) != "https://controlrelay:9443" {
		t.Fatal("agent URLs")
	}
	if _, err := r.Backend("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
func TestAuthorizeEnrollment(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	cert, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Authorize(context.Background(), n.Name, cert); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"default", "unknown"} {
		if r.Authorize(context.Background(), name, cert) == nil {
			t.Fatal("unenrolled namespace accepted")
		}
	}
	revoked := *cert
	revoked.SerialNumber = big.NewInt(1)
	if r.Authorize(context.Background(), n.Name, &revoked) == nil {
		t.Fatal("revoked serial accepted")
	}
	n.PreviousClientSerial = "1"
	n.PreviousCertNotAfter = time.Now().Add(time.Hour)
	if err = r.save(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if err = r.Authorize(context.Background(), n.Name, &revoked); err != nil {
		t.Fatal("overlap rejected", err)
	}
	n.PreviousCertNotAfter = time.Now().Add(-time.Hour)
	if err = r.save(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if r.Authorize(context.Background(), n.Name, &revoked) == nil {
		t.Fatal("expired overlap accepted")
	}
	n.Phase = "Deleting"
	if err = r.save(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if r.Authorize(context.Background(), n.Name, cert) == nil {
		t.Fatal("deleting namespace accepted")
	}
	if r.Authorize(context.Background(), "unknown", (*x509.Certificate)(nil)) == nil {
		t.Fatal("nil certificate accepted")
	}
}
func TestRotationPreservesLiveRelaySession(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	key, err := r.o.Store.GetNamespaceRelayKey(context.Background(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(r.Hub.Handler())
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- relay.Run(ctx, relay.Options{URL: srv.URL, Namespace: n.Name, Hostname: n.Relay.Hostname, CertPEM: n.ClientCert, KeyPEM: key, CAPEM: r.pki.CACert, ListenAgent: "127.0.0.1:0", HTTPClient: srv.Client(), Version: "test", ExecutableSHA256: "test-hash", Logger: r.o.Logger, BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond})
	}()
	defer func() { cancel(); r.Close(); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for !r.Hub.Status(n.Name).Connected {
		if time.Now().After(deadline) {
			t.Fatal("relay failed to connect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before := r.Hub.Status(n.Name)
	r.syncSession(context.Background(), n)
	n.DeployedExecutableHash = "test-hash"
	n.SecretsPending = false
	b.service = &infra.Service{ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
	if err = r.save(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if _, err = r.RotateToken(context.Background(), n.Name, "rotated-token"); err != nil {
		t.Fatal(err)
	}
	r.step(context.Background(), n.Name)
	after := r.Hub.Status(n.Name)
	if !after.Connected || !after.ConnectedSince.Equal(before.ConnectedSince) || after.Reconnects != before.Reconnects {
		t.Fatal("rotation touched relay session")
	}
	if b.token != "rotated-token" || b.restarts != 0 || b.deploys != 0 {
		t.Fatal("rotation restarted or redeployed relay")
	}
	stored, err := r.o.Store.GetNamespaceToken(context.Background(), n.Name)
	if err != nil || stored != "rotated-token" {
		t.Fatal("rotation not durable", err)
	}
	if _, err = r.RotateToken(context.Background(), "default", "rotated-token"); !errors.Is(err, ErrConflict) {
		t.Fatal("default rotation allowed", err)
	}
}
func TestDeleteGuardsAndOwnedRelay(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	ref := domain.ClusterRef{Namespace: n.Name, Name: "orders"}
	c := &domain.Cluster{Namespace: n.Name, Name: ref.Name, Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := r.o.Store.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, n.Name); !errors.Is(err, ErrConflict) {
		t.Fatal("cluster guard", err)
	}
	if err := r.o.Store.MarkClusterDeleting(ctx, ref, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, n.Name); !errors.Is(err, ErrConflict) {
		t.Fatal("deleting cluster guard", err)
	}
	if err := r.o.Store.DeleteCluster(ctx, ref); err != nil {
		t.Fatal(err)
	}
	storage := &domain.BackupStorage{Namespace: n.Name, Name: "archive"}
	if err := r.o.Store.CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, n.Name); !errors.Is(err, ErrConflict) {
		t.Fatal("storage guard", err)
	}
	if err := r.o.Store.DeleteBackupStorage(ctx, n.Name, storage.Name); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Namespace: n.Name, Cluster: ref.Name, Name: "retained"}
	if err := r.o.Store.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, n.Name); !errors.Is(err, ErrConflict) {
		t.Fatal("retained backup guard", err)
	}
	if err := r.o.Store.DeleteBackup(ctx, ref, backup.Name); err != nil {
		t.Fatal(err)
	}
	orphan := &domain.OrphanedVolume{Namespace: n.Name, Hostname: "ordervolume"}
	if err := r.o.Store.PutOrphanedVolume(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, n.Name); !errors.Is(err, ErrConflict) {
		t.Fatal("orphan guard", err)
	}
	if err := r.o.Store.DeleteOrphanedVolume(ctx, n.Name, orphan.Hostname); err != nil {
		t.Fatal(err)
	}
	b.service = &infra.Service{ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
	if err := r.Delete(ctx, n.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Backend(n.Name); err == nil {
		t.Fatal("deleting namespace backend remains usable")
	}
	for range 3 {
		r.step(ctx, n.Name)
	}
	if b.deletes != 1 {
		t.Fatal("owned relay was not deleted once")
	}
	if _, err := r.Get(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("namespace survives", err)
	}
	if _, err := r.o.Store.GetNamespaceToken(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("token survives", err)
	}
	if err := r.Delete(ctx, "default"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}
func TestForeignRelayNeverDeleted(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	b.service = &infra.Service{ID: "foreign-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": "foreign"}}
	if err := r.Delete(context.Background(), n.Name); err != nil {
		t.Fatal(err)
	}
	r.step(context.Background(), n.Name)
	if b.deletes != 0 {
		t.Fatal("foreign service deleted")
	}
	got, err := r.Get(context.Background(), n.Name)
	if err != nil || got.Phase != "Deleting" || got.Relay.LastError == "" {
		t.Fatal("ownership failure not retained", err)
	}
}
func TestRelayReconcileDurableOperationsAndSecretFreeYAML(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	b.opDone = false
	r.step(ctx, n.Name)
	got, err := r.Get(ctx, n.Name)
	if err != nil || got.PendingOpID != "op-import" || b.imports != 1 {
		t.Fatal("import checkpoint", err)
	}
	r.step(ctx, n.Name)
	if b.imports != 1 || b.deploys != 0 {
		t.Fatal("pending operation repeated")
	}
	b.opDone = true
	r.step(ctx, n.Name)
	r.step(ctx, n.Name)
	if b.deploys != 1 {
		t.Fatal("deploy missing")
	}
	r.step(ctx, n.Name)
	got, err = r.Get(ctx, n.Name)
	if err != nil || got.DeployedExecutableHash != "test-hash" {
		t.Fatal("deploy checkpoint", err)
	}
	if b.env["KARAKORAM_RELAY_KEY"] == "" || b.env["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker {
		t.Fatal("enrollment env secrets missing")
	}
	text, err := RelayYAML("controlrelay", "https://example.com", "testing")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Zerops []struct {
			Run struct {
				Env   map[string]string `yaml:"envVariables"`
				Start string            `yaml:"start"`
			} `yaml:"run"`
		} `yaml:"zerops"`
	}
	if err = yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatal(err)
	}
	env := doc.Zerops[0].Run.Env
	if len(env) != 2 || env["KARAKORAM_RELAY_URL"] != "https://example.com" || env["KARAKORAM_RELAY_NAMESPACE"] != "testing" {
		t.Fatal("relay public configuration", env)
	}
	for _, secret := range []string{"KARAKORAM_RELAY_CERT", "KARAKORAM_RELAY_KEY", "KARAKORAM_RELAY_CA", "KARAKORAM_RELAY_OWNER"} {
		if strings.Contains(text, secret) {
			t.Fatal("enrollment secret in pipeline")
		}
	}
}

func TestClientRenewalUsesSecretsAndRestartWithoutRedeploy(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	oldSerial := n.ClientSerial
	oldKey, err := r.o.Store.GetNamespaceRelayKey(ctx, n.Name)
	if err != nil {
		t.Fatal(err)
	}
	n.Relay.CertNotAfter = time.Now().Add(24 * time.Hour)
	n.DeployedExecutableHash = "test-hash"
	n.SecretsPending = false
	b.service = &infra.Service{ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
	if err = r.save(ctx, n); err != nil {
		t.Fatal(err)
	}
	r.step(ctx, n.Name)
	renewed, err := r.Get(ctx, n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.ClientSerial == oldSerial || renewed.PreviousClientSerial != oldSerial || !renewed.SecretsPending {
		t.Fatal("certificate renewal checkpoint")
	}
	newKey, err := r.o.Store.GetNamespaceRelayKey(ctx, n.Name)
	if err != nil || string(newKey) == string(oldKey) {
		t.Fatal("renewed key not sealed", err)
	}
	r.step(ctx, n.Name)
	if b.restarts != 1 || b.deploys != 0 || b.env["KARAKORAM_RELAY_CERT"] != string(renewed.ClientCert) {
		t.Fatal("renewal must update secrets and restart only")
	}
	r.step(ctx, n.Name)
	r.step(ctx, n.Name)
	if b.restarts != 1 || b.deploys != 0 {
		t.Fatal("renewal repeated")
	}
}

func TestTokenHealthEventsOnlyOnTransitions(t *testing.T) {
	r, _, log := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	if err := r.o.Store.UpdateNamespaceCredentials(ctx, n, "bad-token", nil); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		current, err := r.o.Store.GetNamespaceForUpdate(ctx, n.Name)
		if err != nil {
			t.Fatal(err)
		}
		current.Token.LastValidatedAt = time.Now().Add(-10 * time.Minute)
		if err = r.o.Store.UpdateNamespace(ctx, current); err != nil {
			t.Fatal(err)
		}
		r.step(ctx, n.Name)
		got, err := r.Get(ctx, n.Name)
		if err != nil || got.Token.Valid || got.Phase != "Error" {
			t.Fatal("failed validation state", err)
		}
	}
	failures := 0
	for _, reason := range log.reasons {
		if reason == "NamespaceValidationFailed" {
			failures++
		}
	}
	if failures != 1 {
		t.Fatal("validation event spam", log.reasons)
	}
	if _, err := r.Backend(n.Name); err == nil {
		t.Fatal("invalid token backend usable")
	}
	if _, err := r.Backend("default"); err != nil {
		t.Fatal("broken namespace affects default", err)
	}
	if _, err := r.RotateToken(ctx, n.Name, "replacement-token"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Backend(n.Name); err != nil {
		t.Fatal("rotation did not recover token", err)
	}
	for range 3 {
		r.sessionEvent(ctx, sessionNotice{n.Name, relay.SessionEvent{Type: "refused", Target: "demom0:27017", Reason: "target hostname could not be resolved"}})
	}
	var refusals []string
	for _, event := range log.items {
		if event.Reason == "RelayTargetRefused" {
			refusals = append(refusals, event.Message)
		}
	}
	if len(refusals) != 1 || refusals[0] != "Relay could not open demom0:27017: target hostname could not be resolved" {
		t.Fatal("target refusal events", refusals)
	}
	// Relay-supplied text outside the fixed reasons never reaches events.
	r.refused[n.Name] = time.Time{}
	r.sessionEvent(ctx, sessionNotice{n.Name, relay.SessionEvent{Type: "refused", Target: "demom1:27017", Reason: "forged\nreason"}})
	if last := log.items[len(log.items)-1]; last.Reason != "RelayTargetRefused" || last.Message != "Relay could not open demom1:27017: target refused" {
		t.Fatal("unsanitized target refusal", last.Message)
	}
}

func TestStartupIsolatesUnavailableStoredToken(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	if err := r.o.Store.DeleteNamespaceToken(ctx, n); err != nil {
		t.Fatal(err)
	}
	restored, err := New(ctx, r.o)
	if err != nil {
		t.Fatal("broken namespace blocks startup", err)
	}
	defer restored.Close()
	got, err := restored.Get(ctx, n.Name)
	if err != nil || got.Token.Valid || got.Token.LastError == "" {
		t.Fatal("missing token health", err)
	}
	if _, err = restored.Backend("default"); err != nil {
		t.Fatal("default unavailable", err)
	}
	if _, err = restored.RotateToken(ctx, n.Name, "replacement-token"); err != nil {
		t.Fatal("missing backend recovery", err)
	}
	if _, err = restored.Backend(n.Name); err != nil {
		t.Fatal("recovered backend unavailable", err)
	}
}

func TestRegistryLifecycleEventsUseSystemActor(t *testing.T) {
	r, b, log := setupRegistry(t)
	ctx := store.WithActor(t.Context(), "alice")
	n, err := r.Create(ctx, "testing", "proj-test", "valid-token", "")
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		r.step(ctx, n.Name)
	}
	n, err = r.o.Store.GetNamespaceForUpdate(ctx, n.Name)
	if err != nil {
		t.Fatal(err)
	}
	r.syncSessionSnapshot(ctx, n, relay.SessionStatus{Connected: true})
	r.syncSessionSnapshot(ctx, n, relay.SessionStatus{})
	n.Relay.CertNotAfter = time.Now().Add(time.Hour)
	if err = r.save(ctx, n); err != nil {
		t.Fatal(err)
	}
	r.step(ctx, n.Name)
	if err = r.Delete(ctx, n.Name); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		r.step(ctx, n.Name)
	}
	if b.deletes != 1 {
		t.Fatal("relay deletion did not complete", b.deletes)
	}
	seen := map[string]bool{}
	for _, e := range log.items {
		if e.Actor != "system" || e.Namespace != n.Name || e.Cluster != "" || e.Resource != "namespaces" || e.Target != n.Name {
			t.Fatal("registry lifecycle event lost system attribution", e)
		}
		seen[e.Reason] = true
	}
	for _, reason := range []string{"RelayEnrolled", "RelayDeploying", "RelayConnected", "RelayDisconnected", "RelayCertificateRenewed", "NamespaceDeleted"} {
		if !seen[reason] {
			t.Error("missing lifecycle event", reason)
		}
	}
}
