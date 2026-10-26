// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
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
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type projectBackend struct {
	infra.Backend
	project                             string
	token                               string
	service                             *infra.Service
	imports, deploys, restarts, deletes int
	env                                 map[string]string
	vars                                map[string]string
	envVarWrites                        int
	envVarError                         error
	opDone                              bool
	opExpired                           bool
}

func (b *projectBackend) SetToken(s string) { b.token = s }
func (b *projectBackend) Name() string      { return "zerops" }
func (b *projectBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{PublicTCP: true, Logs: true}
}
func (b *projectBackend) Layout() infra.Layout {
	return infra.Layout{AppDir: "/var/www", VolumeRoot: "/mnt", User: "zerops"}
}
func (b *projectBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: b.project, Name: "Test project"}, nil
}
func (b *projectBackend) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, nil
}
func (b *projectBackend) ImportServices(_ context.Context, s []infra.ServiceSpec) (infra.Op, error) {
	b.imports++
	b.env = s[0].Env
	b.vars = s[0].Vars
	ownership := make(map[string]string, len(s[0].Vars))
	for key, value := range s[0].Vars {
		ownership[key] = value
	}
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "service-test", Hostname: s[0].Hostname, Ownership: ownership}
	return infra.Op{ID: "op-import", Kind: "import", Services: map[string]string{s[0].Hostname: b.service.ID}}, nil
}
func (b *projectBackend) SetEnvSecrets(_ context.Context, _ string, env map[string]string) error {
	b.env = env
	return nil
}
func (b *projectBackend) SetEnvVars(_ context.Context, _ string, vars map[string]string) error {
	b.envVarWrites++
	if b.envVarError != nil {
		return b.envVarError
	}
	b.vars = vars
	if b.service != nil {
		if b.service.Ownership == nil {
			b.service.Ownership = make(map[string]string)
		}
		for key, value := range vars {
			b.service.Ownership[key] = value
		}
	}
	return nil
}
func (b *projectBackend) Restart(context.Context, string) (infra.Op, error) {
	b.restarts++
	return infra.Op{ID: "op-restart"}, nil
}
func (b *projectBackend) Deploy(_ context.Context, _ string, d infra.Deployment) (infra.Op, error) {
	b.deploys++
	_, err := io.Copy(io.Discard, d.Artifact.Reader)
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

// OpStatus models the Zerops backend: an expired import finishes so callers
// observe services; any other expired action fails so it is re-issued.
func (b *projectBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: b.opDone || b.opExpired, Expired: b.opExpired, Failed: b.opExpired && op.Kind != "import"}, nil
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
	backends, err := infra.NewRegistry(&testFactory{})
	if err != nil {
		t.Fatal(err)
	}
	o := Options{Store: st, DefaultBackend: defaultBackend, Backends: backends, DefaultTarget: infra.Target{Kind: "zerops", ID: "proj-default", Token: "env-token"}, RelayHostname: "controlrelay", PublicURL: "https://example.com", AdvertiseURL: "https://control:9443", Events: log, Artifact: testArtifact{}, ExecutableHash: "test-hash", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	registryFactory(o.Backends).validate = func(_ context.Context, z infra.Target) (Backend, infra.Project, error) {
		if z.Token == "bad-token" {
			return nil, infra.Project{}, infra.ErrUnauthorized
		}
		if z.Token == "wrong-project-token" {
			return nil, infra.Project{}, &credentialError{message: "Token project does not match requested project"}
		}
		b := remote
		if z.ID == "proj-default" {
			b = defaultBackend
		}
		return b, infra.Project{ID: z.ID, Name: "Test project"}, nil
	}
	registryFactory(o.Backends).newBackend = func(z infra.Target) (Backend, error) {
		return &projectBackend{project: z.ID, token: z.Token, opDone: true}, nil
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
	n, err := r.Create(context.Background(), "testing", "proj-test", "valid-token", "", "zerops", nil)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func TestCreateValidationAndRouting(t *testing.T) {
	for _, tc := range []struct{ name, project, token string }{{"default", "proj-test", "valid-token"}, {"x", "proj-test", "valid-token"}, {"testing", "proj-default", "valid-token"}, {"testing", "proj-test", "bad-token"}, {"testing", "proj-test", "wrong-project-token"}} {
		t.Run(tc.name+tc.token+tc.project, func(t *testing.T) {
			r, _, _ := setupRegistry(t)
			want := ErrInvalid
			if tc.name == "default" {
				want = ErrConflict
			}
			if _, err := r.Create(context.Background(), tc.name, tc.project, tc.token, "", "zerops", nil); !errors.Is(err, want) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
	r, b, _ := setupRegistry(t)
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "someone-elses-service", Hostname: "controlrelay", Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": "other-owner"}}
	if _, err := r.Create(context.Background(), "testing", "proj-test", "valid-token", "", "zerops", nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign relay accepted", err)
	}
	b.service = nil
	n := createTestNamespace(t, r)
	if _, err := r.Create(context.Background(), "another", "proj-test", "valid-token", "", "zerops", nil); !errors.Is(err, ErrInvalid) {
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
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
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
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
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
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "foreign-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": "foreign"}}
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
	if b.env["KARAKORAM_RELAY_KEY"] == "" || b.vars["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker {
		t.Fatal("enrollment secrets or ownership marker missing")
	}
	if _, sensitive := b.env["KARAKORAM_RELAY_OWNER"]; sensitive {
		t.Fatal("ownership marker imported as a sensitive variable")
	}
	spec := RelayRuntime("https://example.com", "testing")
	text := fmt.Sprintf("%+v", spec)
	env := spec.Env
	if len(env) != 2 || env["KARAKORAM_RELAY_URL"] != "https://example.com" || env["KARAKORAM_RELAY_NAMESPACE"] != "testing" {
		t.Fatal("relay public configuration", env)
	}
	for _, key := range []string{"KARAKORAM_RELAY_CERT", "KARAKORAM_RELAY_KEY", "KARAKORAM_RELAY_CA", "KARAKORAM_RELAY_OWNER"} {
		if strings.Contains(text, key) {
			t.Fatal("enrollment configuration in pipeline")
		}
	}
}

// Renewal admission uses the signed leaf, not mutable display metadata.
func expiringClientIdentity(t *testing.T, r *Registry, n *domain.Namespace) {
	t.Helper()
	cert, key, err := relay.IssueClientCertificate(r.pki.CACert, r.pki.CAKey, n.Name, time.Now().Add(-364*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := relay.ParseCertificatePEM(cert)
	if err != nil {
		t.Fatal(err)
	}
	n.ClientCert, n.ClientSerial, n.Relay.CertNotAfter = cert, leaf.SerialNumber.String(), leaf.NotAfter
	token, err := r.o.Store.GetNamespaceTokenForUpdate(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.o.Store.UpdateNamespaceCredentials(t.Context(), n, token, key); err != nil {
		t.Fatal(err)
	}
	r.enroll(n)
}

func TestClientRenewalUsesSecretsAndRestartWithoutRedeploy(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	ctx := context.Background()
	expiringClientIdentity(t, r, n)
	oldSerial := n.ClientSerial
	oldKey, err := r.o.Store.GetNamespaceRelayKey(ctx, n.Name)
	if err != nil {
		t.Fatal(err)
	}
	n.DeployedExecutableHash = "test-hash"
	n.SecretsPending = false
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: "owned-relay", Hostname: n.Relay.Hostname, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
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
	if len(b.env) != 3 || b.env["KARAKORAM_RELAY_KEY"] != string(newKey) || b.env["KARAKORAM_RELAY_CA"] != string(r.pki.CACert) {
		t.Fatal("renewal must write only the enrollment certificate, key, and CA as secrets")
	}
	if _, sensitive := b.env["KARAKORAM_RELAY_OWNER"]; sensitive {
		t.Fatal("renewal wrote the ownership marker as a sensitive variable")
	}
	if b.envVarWrites != 1 || b.vars["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker {
		t.Fatal("renewal did not write the non-sensitive ownership marker")
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
	n, err := r.Create(ctx, "testing", "proj-test", "valid-token", "", "zerops", nil)
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
	expiringClientIdentity(t, r, n)
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

func TestRelayImportUsesPublicOwnershipAndRecordsServiceID(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	if err := r.reconcile(t.Context(), n, b); err != nil {
		t.Fatal(err)
	}
	if b.imports != 1 || b.vars["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker || len(b.vars) != 1 {
		t.Fatal("relay import did not use a non-sensitive ownership marker")
	}
	if _, sensitive := b.env["KARAKORAM_RELAY_OWNER"]; sensitive {
		t.Fatal("relay owner imported as a sensitive variable")
	}
	if len(b.env) != 3 || b.env["KARAKORAM_RELAY_CERT"] != string(n.ClientCert) || b.env["KARAKORAM_RELAY_KEY"] == "" || b.env["KARAKORAM_RELAY_CA"] != string(r.pki.CACert) {
		t.Fatal("relay enrollment secrets missing")
	}
	stored, err := r.Get(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Relay.ServiceID != b.service.ID || stored.Relay.ServiceID == "" || stored.PendingOpKind != "import" {
		t.Fatal("aggregate import did not persist the relay service identity")
	}
}

func TestMaskedRelayRecordedIdentityBackfillsOwnership(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.Relay.ServiceID = "owned-relay"
	n.DeployedExecutableHash = r.o.ExecutableHash
	n.SecretsPending = false
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname, Masked: true}
	if !owned(n, b.service) {
		t.Fatal("recorded service identity did not prove ownership")
	}
	if err := r.reconcile(t.Context(), n, b); err != nil {
		t.Fatal(err)
	}
	if b.envVarWrites != 1 || b.vars["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker || b.service.Ownership["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker {
		t.Fatal("legacy masked ownership marker was not made non-sensitive")
	}
	if b.env != nil || b.restarts != 0 || b.deploys != 0 {
		t.Fatal("ownership migration changed relay enrollment or deployment")
	}
	if err := r.reconcile(t.Context(), n, b); err != nil {
		t.Fatal(err)
	}
	if b.envVarWrites != 1 {
		t.Fatal("ownership marker migration repeated")
	}
}

func TestMaskedRelayWithoutIdentityRequiresOwnershipRecovery(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting=%t", deleting), func(t *testing.T) {
			r, b, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			if deleting {
				n.Phase = "Deleting"
			}
			b.service = &infra.Service{Kind: infra.KindRuntime, ID: "unverified-relay", Hostname: n.Relay.Hostname, Masked: true}
			err := r.reconcile(t.Context(), n, b)
			if err == nil {
				t.Fatal("masked hostname alone proved ownership")
			}
			for _, text := range []string{"earlier version", "namespace backend masks sensitive variables", "delete the relay service in the namespace backend", "credentials that can read sensitive variables", "sudo-mode token on Zerops"} {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("ownership recovery diagnostic missing %q: %v", text, err)
				}
			}
			if b.deletes != 0 || b.envVarWrites != 0 || b.restarts != 0 || b.deploys != 0 || b.imports != 0 {
				t.Fatal("unverified relay was modified")
			}
		})
	}
}

func TestRelayOwnershipRejectsForeignServices(t *testing.T) {
	for _, tc := range []struct {
		name       string
		recordedID string
		marker     string
		masked     bool
	}{
		{name: "foreign marker", marker: "foreign"},
		{name: "foreign marker with masked secrets", marker: "foreign", masked: true},
		{name: "replaced recorded service", recordedID: "previous-relay", masked: true},
		{name: "hostname only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, b, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			n.Relay.ServiceID = tc.recordedID
			b.service = &infra.Service{Kind: infra.KindRuntime, ID: "foreign-relay", Hostname: n.Relay.Hostname, Masked: tc.masked, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": tc.marker}}
			if owned(n, b.service) {
				t.Fatal("foreign relay accepted")
			}
			err := r.reconcile(t.Context(), n, b)
			if err == nil || err.Error() != "Relay hostname belongs to a service this namespace does not own" {
				t.Fatal("foreign relay ownership error changed", err)
			}
			if b.deletes != 0 || b.envVarWrites != 0 || b.restarts != 0 || b.deploys != 0 {
				t.Fatal("foreign relay was modified")
			}
		})
	}
}

func TestMaskedRelayDeletionUsesRecordedIdentity(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.Relay.ServiceID = "owned-relay"
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname, Masked: true}
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(t.Context(), n.Name); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		r.step(t.Context(), n.Name)
	}
	if b.deletes != 1 || b.service != nil {
		t.Fatal("masked owned relay was not deleted")
	}
	if _, err := r.Get(t.Context(), n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("namespace deletion did not finish", err)
	}
}

func TestRelayOwnershipMigrationFailureIsRetried(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.Relay.ServiceID = "owned-relay"
	n.DeployedExecutableHash = r.o.ExecutableHash
	n.SecretsPending = false
	b.service = &infra.Service{Kind: infra.KindRuntime, ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname, Masked: true}
	b.envVarError = errors.New("write failed")
	if err := r.reconcile(t.Context(), n, b); err == nil || err.Error() != "Cannot update relay ownership marker" {
		t.Fatal("ownership migration failure was ignored", err)
	}
	b.envVarError = nil
	if err := r.reconcile(t.Context(), n, b); err != nil {
		t.Fatal(err)
	}
	if b.envVarWrites != 2 || b.vars["KARAKORAM_RELAY_OWNER"] != n.OwnershipMarker {
		t.Fatal("ownership migration was not retried")
	}
}

func TestExpiredRelayOperationsResumeFromObservedState(t *testing.T) {
	for _, kind := range []string{"import", "deploy", "restart", "delete"} {
		t.Run(kind, func(t *testing.T) {
			r, b, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			n.PendingOpID = "expired-operation"
			n.PendingOpKind = kind
			n.Relay.ServiceID = "owned-relay"
			n.SecretsPending = false
			n.DeployedExecutableHash = "previous-hash"
			if kind == "deploy" {
				n.PendingExecutableHash = r.o.ExecutableHash
			}
			if kind == "delete" {
				n.Phase = "Deleting"
			} else {
				b.service = &infra.Service{Kind: infra.KindRuntime, ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname, Masked: true}
			}
			b.opExpired = true
			err := r.reconcile(t.Context(), n, b)
			if kind == "import" && err != nil {
				t.Fatal(err)
			}
			if kind != "import" && err == nil {
				t.Fatal("expired action was not reported for retry")
			}
			if n.PendingOpID != "" || n.PendingOpKind != "" || n.PendingExecutableHash != "" {
				t.Fatal("expired operation retained its pending checkpoint")
			}
			if n.DeployedExecutableHash != "previous-hash" {
				t.Fatal("expired operation was mistaken for a successful deployment")
			}
			b.opExpired = false
			if err := r.reconcile(t.Context(), n, b); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "delete":
				if _, err := r.Get(t.Context(), n.Name); !errors.Is(err, ErrNotFound) {
					t.Fatal("expired deletion did not observe the absent relay", err)
				}
			case "restart":
				if n.PendingOpKind != "restart" || b.envVarWrites != 1 {
					t.Fatal("expired restart was not re-issued", n.PendingOpKind, b.envVarWrites)
				}
			default:
				if b.deploys != 1 || b.envVarWrites != 1 {
					t.Fatal("expired operation did not resume from the observed relay")
				}
			}
		})
	}
}

type testFactory struct {
	validate   func(context.Context, infra.Target) (Backend, infra.Project, error)
	newBackend func(infra.Target) (Backend, error)
}

func (*testFactory) Info() infra.BackendInfo {
	return infra.BackendInfo{Kind: "zerops", DisplayName: "Zerops", Relay: true}
}
func (*testFactory) TargetFromEnv(func(string) string) (infra.Target, error) {
	return infra.Target{}, nil
}
func (f *testFactory) New(target infra.Target) (Backend, error) { return f.newBackend(target) }
func (f *testFactory) Validate(ctx context.Context, target infra.Target) (Backend, infra.Project, error) {
	return f.validate(ctx, target)
}
func registryFactory(registry *infra.Registry) *testFactory {
	f, _ := registry.Get("zerops")
	return f.(*testFactory)
}

type alternateFactory struct {
	testFactory
	info infra.BackendInfo
}

func (f *alternateFactory) Info() infra.BackendInfo { return f.info }

type immutableCredentialBackend struct{ infra.Backend }

func TestBackendTargetIsolationAndOptionalRelay(t *testing.T) {
	r, _, _ := setupRegistry(t)
	r.o.DefaultTarget.Config = map[string]string{"apiUrl": "https://api.example.com"}
	var observed infra.Target
	original := registryFactory(r.o.Backends).validate
	registryFactory(r.o.Backends).validate = func(ctx context.Context, target infra.Target) (Backend, infra.Project, error) {
		observed = target
		return original(ctx, target)
	}
	first, err := r.Create(t.Context(), "testing", "proj-test", "credential", "", "zerops", nil)
	if err != nil || len(first.Config) != 0 || len(observed.Config) != 0 {
		t.Fatalf("inherited operator config: %+v %+v %v", first, observed, err)
	}
	if _, err := r.Create(t.Context(), "unknown", "target", "credential", "", "unknown", nil); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal("unknown backend", err)
	}
	if _, err := r.Create(t.Context(), "unknown", "target", "credential", "", "zerops", map[string]string{"secret": "value"}); !errors.Is(err, infra.ErrRejected) {
		t.Fatal("unknown config", err)
	}
	if _, err := r.Create(t.Context(), "unknown", "target", "credential", "", "zerops", map[string]string{"apiUrl": "https://untrusted.example.com"}); !errors.Is(err, infra.ErrRejected) {
		t.Fatal("namespace API URL override accepted", err)
	}
	var clients []Backend
	newBackend := func(target infra.Target) (Backend, error) {
		observed = target
		b := immutableCredentialBackend{&projectBackend{project: target.ID, token: target.Token}}
		clients = append(clients, b)
		return b, nil
	}
	factory := &alternateFactory{info: infra.BackendInfo{Kind: "nomad", DisplayName: "Nomad", ConfigKeys: []string{"region"}}}
	factory.newBackend = newBackend
	factory.validate = func(ctx context.Context, target infra.Target) (Backend, infra.Project, error) {
		b, err := newBackend(target)
		return b, infra.Project{ID: target.ID, Name: "Target"}, err
	}
	if err := r.o.Backends.Register(factory); err != nil {
		t.Fatal(err)
	}
	config := map[string]string{"region": "example"}
	n, err := r.Create(t.Context(), "nomad", first.ProjectID, "credential", "", "nomad", config)
	if err != nil {
		t.Fatal("same target ID across backend kinds", err)
	}
	config["region"] = "changed"
	if n.Config["region"] != "example" || observed.Config["region"] != "example" {
		t.Fatal("caller config was not isolated", n.Config, observed.Config)
	}
	if n.Phase != "Ready" || n.Relay.Phase != "NotRequired" || n.ClientSerial != "" || n.Relay.Hostname != "" {
		t.Fatalf("unexpected relay state: %+v", n)
	}
	if _, inherited := observed.Config["apiUrl"]; inherited {
		t.Fatal("configuration leaked across backend kinds")
	}
	if r.Dialer(n.Name) != nil || r.AgentURL(n.Name) != r.o.AdvertiseURL {
		t.Fatal("direct namespace uses relay networking")
	}
	before := r.backend(n.Name)
	if _, err := r.RotateToken(t.Context(), n.Name, "replacement"); err != nil {
		t.Fatal(err)
	}
	if r.backend(n.Name) == before || len(clients) != 2 {
		t.Fatal("immutable credential client was not rebuilt")
	}
	if _, err := r.Create(t.Context(), "duplicate", n.ProjectID, "credential", "", "nomad", nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate backend target accepted", err)
	}
	r.step(t.Context(), n.Name)
	if err := r.Delete(t.Context(), n.Name); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), n.Name)
	if _, err := r.Get(t.Context(), n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("direct namespace not deleted", err)
	}
}

func TestRelayResumesIncompleteImportBeforeRenewingIdentity(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	expiringClientIdentity(t, r, n)
	serial := n.ClientSerial
	n.SecretsPending = true
	b.service = &infra.Service{
		ID: "service-test", Hostname: n.Relay.Hostname, Kind: infra.KindRuntime,
		Status: infra.StatusCreating, Importing: true,
		Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker},
	}
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), n.Name)
	got, err := r.Get(t.Context(), n.Name)
	if err != nil || got.PendingOpKind != "import" || got.ClientSerial != serial || b.imports != 1 || b.deploys != 0 || b.restarts != 0 || b.envVarWrites != 0 {
		t.Fatalf("incomplete import did not retain its original enrollment: %+v %v", got, err)
	}
	if b.env["KARAKORAM_RELAY_CERT"] != string(n.ClientCert) || b.service.Importing {
		t.Fatal("original import was not resumed with its original secrets")
	}
	r.step(t.Context(), n.Name) // consume completed import
	r.step(t.Context(), n.Name) // renewal is now admitted
	got, err = r.Get(t.Context(), n.Name)
	if err != nil || got.ClientSerial == serial || !got.SecretsPending {
		t.Fatalf("completed import did not release certificate renewal: %+v %v", got, err)
	}
}
