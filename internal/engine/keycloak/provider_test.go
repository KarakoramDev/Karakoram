// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRequiredCapabilities(t *testing.T) {
	p := New(Options{})
	for _, public := range []bool{false, true} {
		t.Run(map[bool]string{false: "private", true: "public"}[public], func(t *testing.T) {
			options, err := json.Marshal(EngineOptions{
				Database: DatabaseOptions{Cluster: "appdb"}, PublicHTTPS: public,
				Hostname: "https://login.example.com", ProxyHeaders: "xforwarded",
			})
			if err != nil {
				t.Fatal(err)
			}
			spec := domain.ClusterSpec{EngineOptions: options}
			err = engine.CheckCapabilities(p, spec, "limited", infra.Capabilities{})
			if public {
				if !errors.Is(err, engine.ErrNotSupported) || !strings.Contains(err.Error(), "publicHttps") {
					t.Fatalf("missing public HTTPS requirement: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := engine.CheckCapabilities(p, spec, "capable", infra.Capabilities{PublicHTTPS: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type bindingFake struct {
	conn       pgbinding.Connection
	password   string
	requests   []pgbinding.Request
	released   bool
	releasedID string
	keep       bool
	backend    *backendFake
	fail       error
}

func (b *bindingFake) Ensure(_ context.Context, r pgbinding.Request) (pgbinding.Connection, error) {
	b.requests = append(b.requests, r)
	return b.conn, b.fail
}
func (b *bindingFake) Resolve(context.Context, string) (pgbinding.Connection, error) {
	return b.conn, nil
}
func (b *bindingFake) Password(context.Context, string) (string, error) { return b.password, nil }
func (b *bindingFake) Release(_ context.Context, id string, keep bool) error {
	if b.backend.service != nil {
		return errors.New("released before runtime deletion")
	}
	b.released = true
	b.releasedID = id
	b.keep = keep
	return nil
}

type memorySecrets struct {
	engine.SecretStore
	values map[string][]byte
	token  *domain.AgentToken
}

func (m *memorySecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, key string) ([]byte, error) {
	b, ok := m.values[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return b, nil
}
func (m *memorySecrets) SetClusterSecret(_ context.Context, _ domain.ClusterRef, key string, b []byte) error {
	m.values[key] = append([]byte(nil), b...)
	return nil
}
func (m *memorySecrets) DeleteClusterSecret(_ context.Context, _ domain.ClusterRef, key string) error {
	delete(m.values, key)
	return nil
}
func (m *memorySecrets) GetAgentToken(context.Context, domain.ClusterRef, string) (*domain.AgentToken, error) {
	return m.token, nil
}
func (m *memorySecrets) SetAgentToken(_ context.Context, t *domain.AgentToken) error {
	m.token = t
	return nil
}
func (m *memorySecrets) DeleteAgentToken(context.Context, domain.ClusterRef, string) error {
	m.token = nil
	return nil
}

type backendFake struct {
	infra.Backend
	service *infra.Service
	deploys int
	resizes int
	public  bool
	pending bool
	fail    bool
	deploy  infra.RuntimeSpec
}

func (*backendFake) Name() string                     { return "fake" }
func (*backendFake) Layout() infra.Layout             { return infra.Layout{AppDir: "/app", User: "app"} }
func (*backendFake) Capabilities() infra.Capabilities { return infra.Capabilities{PublicHTTPS: true} }
func (b *backendFake) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, nil
}
func (*backendFake) Containers(context.Context, string) ([]infra.Container, error) {
	return nil, nil
}
func (b *backendFake) SetEnvVars(_ context.Context, _ string, vars map[string]string) error {
	for k, v := range vars {
		b.service.Ownership[k] = v
	}
	return nil
}
func (b *backendFake) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	s := specs[0]
	b.service = &infra.Service{ID: "runtime-id", Hostname: s.Hostname, Kind: infra.KindRuntime, Ownership: s.Vars, Resources: s.Resources}
	return infra.Op{Services: map[string]string{s.Hostname: "runtime-id"}}, nil
}
func (b *backendFake) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: !b.pending, Failed: b.fail, Status: "FAILED"}, nil
}
func (b *backendFake) Deploy(_ context.Context, _ string, d infra.Deployment) (infra.Op, error) {
	b.deploys++
	b.deploy = d.Runtime
	return infra.Op{}, nil
}
func (b *backendFake) DeleteServiceByID(_ context.Context, _ string, id string) (infra.Op, error) {
	if b.service.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	b.service = nil
	return infra.Op{}, nil
}
func (b *backendFake) SetResources(_ context.Context, _ string, r domain.Resources) (infra.Op, error) {
	b.resizes++
	b.service.Resources = r
	return infra.Op{}, nil
}
func (b *backendFake) SetSubdomainAccess(_ context.Context, _ string, on bool) (infra.Op, error) {
	b.public = on
	return infra.Op{}, nil
}

type artifactFake struct{}

func (artifactFake) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("archive")), nil
}
func setup(t *testing.T) (*Provider, *engine.Context, *bindingFake, *memorySecrets) {
	t.Helper()
	b := &backendFake{}
	bindings := &bindingFake{conn: pgbinding.Connection{BindingID: "binding", Host: "writer", Port: 5432, Databases: []string{"keycloak"}, Username: "consumer_role", PasswordSecretRef: "sealed-ref", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: []byte("test-ca")}, CredentialGeneration: 1}, password: "consumer-password", backend: b}
	secrets := &memorySecrets{values: map[string][]byte{}}
	p := New(Options{Bindings: bindings})
	c := &domain.Cluster{Name: "login", Spec: domain.ClusterSpec{EngineOptions: json.RawMessage(`{"database":{"cluster":"pg"}}`)}}
	p.ApplyDefaults(&c.Spec)
	rc := &engine.Context{Cluster: c, Backend: b, Secrets: secrets, Artifact: artifactFake{}}
	return p, rc, bindings, secrets
}
func converge(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	for range 9 {
		if _, e := p.Sync(t.Context(), rc); e != nil {
			t.Fatal(e)
		}
	}
}
func TestOptionsStrictAndDefaults(t *testing.T) {
	o, e := ParseOptions(json.RawMessage(`{"database":{"cluster":"pg"}}`))
	if e != nil || o.Database.Name != "keycloak" {
		t.Fatal(o, e)
	}
	for _, raw := range []string{`null`, `{}`, `{"database":{"cluster":"pg","password":"leak"}}`, `{"database":{"cluster":"pg"},"unknown":true}`, `{"database":{"cluster":"pg"}} {}`, `{"database":{"cluster":"pg"},"proxyHeaders":"bad"}`, `{"database":{"cluster":"pg"},"publicHttps":true}`} {
		if _, e = ParseOptions(json.RawMessage(raw)); e == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}
func TestValidate(t *testing.T) {
	p, rc, _, _ := setup(t)
	if e := p.Validate(t.Context(), nil, rc.Cluster); e != nil {
		t.Fatal(e)
	}
	old := *rc.Cluster
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"database":{"cluster":"other"}}`)
	if e := p.Validate(t.Context(), &old, rc.Cluster); e == nil {
		t.Fatal("allowed rebinding")
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 2
	if e := p.Validate(t.Context(), nil, rc.Cluster); e == nil {
		t.Fatal("allowed unqualified clustering")
	}
}
func TestBindingBeforeRuntimeAndSealedProjection(t *testing.T) {
	p, rc, b, secrets := setup(t)
	b.fail = errors.New("source unavailable")
	if _, e := p.Sync(t.Context(), rc); e == nil {
		t.Fatal("missing binding failure")
	}
	if b.backend.service != nil {
		t.Fatal("runtime created before binding")
	}
	b.fail = nil
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	if !s.Deployed || b.backend.deploys != 1 {
		t.Fatal(s, b.backend.deploys)
	}
	if len(b.requests) == 0 || b.requests[0].ConsumerKind != "keycloak" || b.requests[0].MaxMajor != 18 {
		t.Fatal(b.requests)
	}
	config, e := p.MemberConfig(t.Context(), rc, s.Member)
	if e != nil {
		t.Fatal(e)
	}
	var spec kcspec.Spec
	if e = json.Unmarshal(config.EngineSpec, &spec); e != nil {
		t.Fatal(e)
	}
	if spec.Password != b.password || spec.Username != "consumer_role" || spec.AdminPassword == "" || spec.CredentialGeneration != 1 {
		t.Fatal("wrong projected credentials")
	}
	if string(secrets.values["keycloak/admin"]) != spec.AdminPassword || config.AgentTLS.KeyPEM == "" {
		t.Fatal("missing sealed bootstrap/transport credentials")
	}
	if strings.Contains(string(rc.Cluster.Internal), b.password) || strings.Contains(string(rc.Cluster.Internal), spec.AdminPassword) {
		t.Fatal("secret in nonsecret state")
	}
	runtime, _ := json.Marshal(b.backend.deploy)
	if strings.Contains(string(runtime), b.password) || strings.Contains(string(runtime), spec.AdminPassword) {
		t.Fatal("secret in runtime arguments or environment")
	}
	if _, e = p.MemberConfig(t.Context(), rc, "other"); !errors.Is(e, engine.ErrNotFound) {
		t.Fatal("member isolation", e)
	}
	prior := s.ConfigVersion
	b.conn.CredentialGeneration++
	b.password = "rotated-password"
	if _, e = p.Sync(t.Context(), rc); e != nil {
		t.Fatal(e)
	}
	s, _ = load(rc.Cluster)
	if s.ConfigVersion != prior+1 {
		t.Fatal("rotation did not republish")
	}
	config, _ = p.MemberConfig(t.Context(), rc, s.Member)
	if !strings.Contains(string(config.EngineSpec), "rotated-password") {
		t.Fatal("rotation projection stale")
	}
}
func TestCleanupDeletesRuntimeBeforeRelease(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "drop", true: "retain"}[keep], func(t *testing.T) {
			p, rc, b, _ := setup(t)
			converge(t, p, rc)
			done := false
			for range 8 {
				var e error
				done, e = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
				if e != nil {
					t.Fatal(e)
				}
				if done {
					break
				}
			}
			if !done || !b.released || b.keep != keep || b.backend.service != nil {
				t.Fatal("cleanup order or retention broken")
			}
		})
	}
}

func TestCleanupReleasesBindingAfterInitialEnsureFailure(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "drop", true: "retain"}[keep], func(t *testing.T) {
			p, rc, b, _ := setup(t)
			b.fail = errors.New("binding partially provisioned")
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, b.fail) {
				t.Fatalf("initial Ensure failure: %v", err)
			}
			if len(b.requests) != 1 {
				t.Fatalf("Ensure requests: %v", b.requests)
			}
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
			if err != nil || !done {
				t.Fatalf("cleanup: done=%v err=%v", done, err)
			}
			if !b.released || b.releasedID != pgbinding.BindingID(b.requests[0]) || b.keep != keep {
				t.Fatalf("release: called=%v id=%q keep=%v", b.released, b.releasedID, b.keep)
			}
		})
	}
}
func TestCleanupRefusesForeignRuntime(t *testing.T) {
	p, rc, b, _ := setup(t)
	converge(t, p, rc)
	b.backend.service.ID = "replacement"
	b.backend.service.Ownership = map[string]string{}
	if _, e := p.Cleanup(t.Context(), rc, domain.DeleteOptions{}); e == nil {
		t.Fatal("deleted foreign runtime")
	}
	if b.released {
		t.Fatal("released foreign runtime's binding")
	}
}
func TestNoRedeployWhilePending(t *testing.T) {
	p, rc, b, _ := setup(t)
	for range 5 {
		if _, e := p.Sync(t.Context(), rc); e != nil {
			t.Fatal(e)
		}
	}
	b.backend.pending = true
	before := b.backend.deploys
	for range 3 {
		if _, e := p.Sync(t.Context(), rc); e != nil {
			t.Fatal(e)
		}
	}
	if b.backend.deploys != before {
		t.Fatal("duplicate deployment")
	}
}
func TestTLSBindingRequired(t *testing.T) {
	p, rc, b, _ := setup(t)
	b.conn.TLS.Mode = pgbinding.TLSVerifyCA
	var e error
	for range 8 {
		_, e = p.Sync(t.Context(), rc)
		if e != nil {
			break
		}
	}
	if e == nil || b.backend.deploys != 0 {
		t.Fatal("accepted unverifiable binding")
	}
}

func (b *backendFake) Start(context.Context, string) (infra.Op, error) {
	b.service.Status = infra.StatusRunning
	return infra.Op{Kind: "start"}, nil
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

func TestRecoverStoppedAndMissingRuntime(t *testing.T) {
	p, rc, b, _ := setup(t)
	converge(t, p, rc)
	b.backend.service.Status = infra.StatusStopped
	converge(t, p, rc)
	if b.backend.service.Status != infra.StatusRunning {
		t.Fatal("stopped runtime not recovered")
	}
	b.backend.service = nil
	// A second infrastructure loss must honor the shared recovery backoff.
	prior, _ := load(rc.Cluster)
	rc.Clock = fixedClock{at: prior.Recovery.RetryAt.Add(time.Second)}
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	if !s.Deployed || b.backend.deploys != 2 {
		t.Fatal("missing runtime not recreated and deployed")
	}
}
