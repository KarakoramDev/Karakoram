// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

type fakeBindings struct {
	conn          pgbinding.Connection
	request       pgbinding.Request
	unavailable   bool
	failure       error
	released      bool
	keep          bool
	passwordCalls int
}

func (b *fakeBindings) Ensure(_ context.Context, r pgbinding.Request) (pgbinding.Connection, error) {
	b.request = r
	if b.failure != nil {
		return pgbinding.Connection{}, b.failure
	}
	if b.unavailable {
		return b.conn, pgbinding.ErrSourceUnavailable
	}
	return b.conn, nil
}

func (b *fakeBindings) Resolve(context.Context, string) (pgbinding.Connection, error) {
	if b.failure != nil {
		return pgbinding.Connection{}, b.failure
	}
	if b.unavailable {
		return b.conn, pgbinding.ErrSourceUnavailable
	}
	return b.conn, nil
}

func (b *fakeBindings) Password(context.Context, string) (string, error) {
	b.passwordCalls++
	return "bound-password", nil
}

func (b *fakeBindings) Release(_ context.Context, _ string, keep bool) error {
	b.released = true
	b.keep = keep
	return nil
}

type memorySecrets struct {
	values map[string][]byte
	tokens map[string]*domain.AgentToken
}

func (s *memorySecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, k string) ([]byte, error) {
	v, ok := s.values[k]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
}

func (s *memorySecrets) SetClusterSecret(_ context.Context, _ domain.ClusterRef, k string, v []byte) error {
	s.values[k] = append([]byte(nil), v...)
	return nil
}

func (s *memorySecrets) DeleteClusterSecret(_ context.Context, _ domain.ClusterRef, k string) error {
	delete(s.values, k)
	return nil
}

func (s *memorySecrets) GetAgentToken(_ context.Context, _ domain.ClusterRef, k string) (*domain.AgentToken, error) {
	return s.tokens[k], nil
}

func (s *memorySecrets) SetAgentToken(_ context.Context, v *domain.AgentToken) error {
	s.tokens[v.Member] = v
	return nil
}

func (s *memorySecrets) DeleteAgentToken(_ context.Context, _ domain.ClusterRef, k string) error {
	delete(s.tokens, k)
	return nil
}

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

type fakeArtifact struct{}

func (fakeArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("test-artifact")), nil
}

type fakeAgents struct {
	server  *httptest.Server
	rc      *engine.Context
	unready string
}

func (a *fakeAgents) Dial(_ context.Context, _ domain.ClusterRef, member string) (*agentapi.Client, error) {
	u, _ := url.Parse(a.server.URL)
	return agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: a.server.URL + "/" + member, Token: "agent-token",
		CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.server.Certificate().Raw}),
		DialInstanceContext: func(ctx context.Context, _, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
		},
	})
}

type testBackend struct{ *infrafake.Backend }

func (b *testBackend) Containers(ctx context.Context, member string) ([]infra.Container, error) {
	containers, err := b.Backend.Containers(ctx, member)
	for i := range containers {
		containers[i].Hostname = fmt.Sprintf("%s-instance-%d", member, i)
	}
	return containers, err
}

func fixture(t *testing.T, topology string) (*Provider, *engine.Context, *fakeBindings, *fakeAgents) {
	t.Helper()
	bindings := &fakeBindings{conn: pgbinding.Connection{
		BindingID: "binding-1", Host: "postgres.example", Port: 5432,
		Username: "temporal_owner", PasswordSecretRef: "sealed-password",
		TLS:            pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: []byte("database-ca")},
		MaxConnections: 200, CredentialGeneration: 1, ConsumerCount: 2, Shared: true,
	}}
	p := New(Options{Bindings: bindings, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control.example"})
	cluster := &domain.Cluster{
		Name: "workflows", Namespace: "default", Generation: 1,
		Spec: domain.ClusterSpec{
			Version: "1.31.4", Topology: domain.Topology{Type: topology},
			EngineOptions: json.RawMessage(`{"persistence":{"postgres":{"cluster":"pg1"}}}`),
		},
	}
	p.ApplyDefaults(&cluster.Spec)
	rc := &engine.Context{
		Cluster: cluster, Backend: &testBackend{infrafake.New(infrafake.Options{})},
		Secrets:  &memorySecrets{values: map[string][]byte{}, tokens: map[string]*domain.AgentToken{}},
		Artifact: fakeArtifact{}, Clock: &testClock{now: time.Now().UTC()},
	}
	agents := &fakeAgents{rc: rc}
	agents.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 2 {
			http.NotFound(w, r)
			return
		}
		member := parts[0]
		config, e := p.MemberConfig(r.Context(), rc, member)
		if e != nil {
			http.Error(w, "not configured", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/v1/status") {
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: config.ConfigVersion})
			return
		}
		var sp tspec.Spec
		json.Unmarshal(config.EngineSpec, &sp)
		switch parts[len(parts)-1] {
		case tspec.ActionStatus:
			json.NewEncoder(w).Encode(tspec.Status{
				Component: sp.Component, Version: sp.Version, RestartToken: sp.RestartToken,
				Ready: sp.Component != agents.unready, SchemaVersion: "1.0", VisibilitySchemaVersion: "1.1",
			})
		case tspec.ActionEnsureNamespaces:
			var desired []tspec.Namespace
			json.NewDecoder(r.Body).Decode(&desired)
			out := []tspec.NamespaceStatus{}
			for _, n := range desired {
				out = append(out, tspec.NamespaceStatus{Namespace: n, State: "Registered"})
			}
			json.NewEncoder(w).Encode(out)
		default:
			json.NewEncoder(w).Encode(struct{}{})
		}
	}))
	t.Cleanup(agents.server.Close)
	rc.Agents = agents
	return p, rc, bindings, agents
}

func converge(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	for range 150 {
		if _, e := p.Sync(context.Background(), rc); e != nil {
			t.Fatal(e)
		}
		status, e := p.Status(context.Background(), rc)
		if e != nil {
			t.Fatal(e)
		}
		if status.Phase == domain.PhaseReady {
			return
		}
		rc.Clock.(*testClock).now = rc.Clock.Now().Add(time.Second)
	}
	t.Fatal("did not converge")
}

func TestDefaultsValidationAndCatalog(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	if e := p.Validate(context.Background(), nil, rc.Cluster); e != nil {
		t.Fatal(e)
	}
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		t.Fatal(e)
	}
	if o.HistoryShards != 512 || !o.Frontend.RequireClientAuth ||
		o.Persistence.Postgres.Namespace != "default" || o.Persistence.Postgres.Database != "temporal_workflows" ||
		o.Persistence.Postgres.VisibilityDatabase != "temporal_workflows_visibility" {
		t.Fatal("incorrect option defaults")
	}
	info := p.Describe()
	if info.Name != "temporal" || len(info.Versions) != 2 || !info.Versions[0].Default || len(info.UpgradePaths) != 1 || len(info.Presets) != 2 {
		t.Fatal("incorrect catalog")
	}
	for _, preset := range info.Presets {
		c := *rc.Cluster
		c.Spec.Components = preset.Components
		c.Spec.Topology = preset.Topology
		if e := p.Validate(context.Background(), nil, &c); e != nil {
			t.Fatalf("preset %s: %v", preset.Name, e)
		}
	}
	for _, raw := range []string{
		`{"persistence":{"postgres":{}}}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},"historyShards":0}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},"historyShards":16385}`,
		`{"persistence":{"postgres":{"cluster":"pg1","database":"bad-name"}}}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},
		  "ui":{"oidc":{"issuer":"https://id.example","clientId":"id","clientSecret":"secret"}}}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},"namespaces":[{"name":"orders","retention":"1h"}]}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},
		  "namespaces":[{"name":"orders","retention":"72h"},{"name":"orders","retention":"72h"}]}`,
		`{"persistence":{"postgres":{"cluster":"pg1"}},"ui":{"publicHttps":true}}`,
	} {
		c := *rc.Cluster
		c.Spec.EngineOptions = json.RawMessage(raw)
		if e := p.Validate(context.Background(), nil, &c); e == nil {
			t.Errorf("admitted %s", raw)
		}
	}
	if e := New(Options{}).Validate(context.Background(), nil, rc.Cluster); e == nil || !strings.Contains(e.Error(), "PostgreSQL bindings are unavailable") {
		t.Fatal("nil bindings admitted")
	}
	old := *rc.Cluster
	next := old
	next.Spec.EngineOptions = json.RawMessage(`{"persistence":{"postgres":{"cluster":"other"}}}`)
	if e := p.Validate(context.Background(), &old, &next); e == nil {
		t.Fatal("mutable persistence")
	}
	next = old
	next.Spec.EngineOptions = json.RawMessage(`{"persistence":{"postgres":{"cluster":"pg1"}},"historyShards":256}`)
	if e := p.Validate(context.Background(), &old, &next); e == nil {
		t.Fatal("mutable historyShards")
	}
	next = old
	next.Spec.Topology.Type = "split"
	if e := p.Validate(context.Background(), &old, &next); e == nil {
		t.Fatal("mutable topology")
	}
	next = old
	next.Spec.Version = "1.32.9"
	if e := p.Validate(context.Background(), nil, &next); e == nil {
		t.Fatal("unlocked patch admitted")
	}
}

func TestUpgradePaths(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		want     bool
	}{
		{"1.31.4", "1.32.1", true},
		{"1.32.1", "1.32.2", true},
		{"1.32.1", "1.31.4", false},
		{"1.31.4", "1.33.0", false},
		{"1.32.1", "1.32.0", false},
		{"1.32.1", "2.0.0", false},
		{"bad", "1.32.1", false},
	} {
		if upgradeAllowed(tc.from, tc.to) != tc.want {
			t.Errorf("%s to %s", tc.from, tc.to)
		}
	}
}

func TestConfigProjectionAndBinding(t *testing.T) {
	p, rc, b, _ := fixture(t, "combined")
	converge(t, p, rc)
	if b.request.MinMajor != 12 || b.request.ConsumerKind != "temporal" ||
		!reflect.DeepEqual(b.request.Databases, []string{"temporal_workflows", "temporal_workflows_visibility"}) {
		t.Fatal("bad binding request")
	}
	state, e := loadState(rc.Cluster)
	if e != nil {
		t.Fatal(e)
	}
	if state.BindingID != "binding-1" || !state.Binding.Shared || state.Binding.ConsumerCount != 2 || len(state.Namespaces) != 1 {
		t.Fatal("binding or namespaces not observed")
	}
	for _, name := range []string{tspec.Server, tspec.UI} {
		m := state.Members[name]
		config, e := p.MemberConfig(context.Background(), rc, m.Name)
		if e != nil {
			t.Fatal(e)
		}
		var sp tspec.Spec
		if e = json.Unmarshal(config.EngineSpec, &sp); e != nil {
			t.Fatal(e)
		}
		if e = sp.Validate(); e != nil {
			t.Fatal(e)
		}
		if name == tspec.UI && sp.Persistence != nil {
			t.Fatal("UI received database credentials")
		}
		if name == tspec.Server && (sp.Persistence == nil || sp.Persistence.Password != "bound-password") {
			t.Fatal("server missing credentials")
		}
		if config.AgentTLS.KeyPEM == "" {
			t.Fatal("agent missing TLS")
		}
	}
	if strings.Contains(string(rc.Cluster.Internal), "bound-password") || strings.Contains(string(rc.Cluster.Internal), "PRIVATE KEY") {
		t.Fatal("secrets persisted outside sealed store")
	}
	status, e := p.Status(context.Background(), rc)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(status)
	if strings.Contains(string(raw), "bound-password") || strings.Contains(string(raw), "PRIVATE KEY") {
		t.Fatal("secrets leaked in status")
	}
}

func TestUIWaitsForFrontendAndCredentialRoll(t *testing.T) {
	p, rc, b, a := fixture(t, "combined")
	a.unready = tspec.Server
	for range 18 {
		if _, e := p.Sync(context.Background(), rc); e != nil {
			t.Fatal(e)
		}
	}
	s, _ := loadState(rc.Cluster)
	if s.Members[tspec.UI] != nil {
		t.Fatal("UI created before frontend Ready")
	}
	a.unready = ""
	converge(t, p, rc)
	s, _ = loadState(rc.Cluster)
	oldServer := s.Members[tspec.Server].ConfigHash
	oldUI := s.Members[tspec.UI].ConfigHash
	b.conn.CredentialGeneration++
	for range 4 {
		if _, e := p.Sync(context.Background(), rc); e != nil {
			t.Fatal(e)
		}
	}
	s, _ = loadState(rc.Cluster)
	if s.Members[tspec.Server].ConfigHash == oldServer {
		t.Fatal("credential generation did not reconfigure server")
	}
	converge(t, p, rc)
	s, _ = loadState(rc.Cluster)
	if s.Members[tspec.UI].ConfigHash == oldUI || s.CredentialGeneration != b.conn.CredentialGeneration {
		t.Fatal("credential roll did not complete")
	}
}

func TestUpgradeOrderCheckpointAndSettle(t *testing.T) {
	p, rc, _, _ := fixture(t, "split")
	converge(t, p, rc)
	rc.Cluster.Spec.Version = "1.32.1"
	rc.Cluster.Generation++
	order := []string{}
	previous := map[string]string{}
	s, _ := loadState(rc.Cluster)
	for name, m := range s.Members {
		previous[name] = m.ConfigHash
	}
	for range 200 {
		if _, e := p.Sync(context.Background(), rc); e != nil {
			t.Fatal(e)
		}
		s, _ = loadState(rc.Cluster)
		for _, name := range tspec.RollOrder {
			if m := s.Members[name]; m != nil && m.ConfigHash != previous[name] {
				order = append(order, name)
				previous[name] = m.ConfigHash
			}
		}
		if s.Upgrade != nil && !s.Upgrade.SettleUntil.IsZero() {
			if s.Members[tspec.Matching].Version != "1.31.4" {
				t.Fatal("matching upgraded during history settle")
			}
			rc.Clock.(*testClock).now = s.Upgrade.SettleUntil
		}
		// Reconstruct the provider and deserialize state every pass, as after a restart.
		p = New(p.options)
		raw := append([]byte(nil), rc.Cluster.Internal...)
		rc.Cluster.Internal = raw
		if s.Upgrade == nil && s.Version == "1.32.1" {
			break
		}
	}
	if !reflect.DeepEqual(order, []string{tspec.History, tspec.Matching, tspec.Frontend, tspec.Worker, tspec.UI}) {
		t.Fatalf("upgrade order %v", order)
	}
	s, _ = loadState(rc.Cluster)
	if s.Version != "1.32.1" || s.Upgrade != nil {
		t.Fatal("upgrade did not finish")
	}
}

func TestScaleIsolation(t *testing.T) {
	p, rc, _, _ := fixture(t, "split")
	converge(t, p, rc)
	before, _ := loadState(rc.Cluster)
	hashes := map[string]string{}
	for name, m := range before.Members {
		hashes[name] = m.ConfigHash
	}
	d := rc.Cluster.Spec.Components.Extra[tspec.History]
	d.Replicas = 2
	rc.Cluster.Spec.Components.Extra[tspec.History] = d
	rc.Cluster.Generation++
	converge(t, p, rc)
	after, _ := loadState(rc.Cluster)
	for name, m := range after.Members {
		if name != tspec.History && hashes[name] != m.ConfigHash {
			t.Fatalf("scaling history restarted %s", name)
		}
	}
}

func TestCleanupReverseOrderAndRetention(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "retain"}[keep], func(t *testing.T) {
			p, rc, b, _ := fixture(t, "split")
			converge(t, p, rc)
			removed := []string{}
			previous, _ := loadState(rc.Cluster)
			done := false
			for range 100 {
				var e error
				done, e = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: keep})
				if e != nil {
					t.Fatal(e)
				}
				next, _ := loadState(rc.Cluster)
				for i := len(tspec.RollOrder) - 1; i >= 0; i-- {
					name := tspec.RollOrder[i]
					if previous.Members[name] != nil && next.Members[name] == nil {
						removed = append(removed, name)
					}
				}
				if len(next.Members) > 0 && b.released {
					t.Fatal("binding released before components deleted")
				}
				previous = next
				if done {
					break
				}
			}
			if !done || !b.released || b.keep != keep {
				t.Fatal("cleanup or retention incomplete")
			}
			if !reflect.DeepEqual(removed, []string{tspec.UI, tspec.Worker, tspec.Frontend, tspec.Matching, tspec.History}) {
				t.Fatalf("delete order %v", removed)
			}
			secrets := rc.Secrets.(*memorySecrets)
			_, exists := secrets.values["tls/ca.key"]
			if exists != keep {
				t.Fatal("CA retention mismatch")
			}
		})
	}
}

func TestUnavailableSourceWaits(t *testing.T) {
	p, rc, b, _ := fixture(t, "combined")
	b.unavailable = true
	if _, e := p.Sync(context.Background(), rc); e != nil {
		t.Fatal(e)
	}
	s, _ := loadState(rc.Cluster)
	if !s.WaitingSource || len(s.Members) > 0 {
		t.Fatal("unavailable source did not gate provisioning")
	}
	status, e := p.Status(context.Background(), rc)
	if e != nil || status.Phase == domain.PhaseReady {
		t.Fatal("unavailable source reported Ready")
	}
}

func TestBindingConditionRequiresResolvedSource(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(fmt.Sprintf("saved-identity-%t", saved), func(t *testing.T) {
			p, rc, bindings, _ := fixture(t, "combined")
			if saved {
				s, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				s.BindingID = bindings.conn.BindingID
				if err := saveState(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			assertPending := func() {
				t.Helper()
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				for _, condition := range status.Conditions {
					if condition.Type == "PostgreSQLBinding" {
						if condition.Status != "False" || condition.Reason != "SourceUnavailable" {
							t.Fatalf("unresolved binding condition: %+v", condition)
						}
						return
					}
				}
				t.Fatal("missing binding condition")
			}
			if !saved {
				assertPending()
			}
			bindings.failure = engine.ErrUnavailable
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			assertPending()
		})
	}
}

func TestCapabilities(t *testing.T) {
	p, rc, _, _ := fixture(t, "split")
	d := rc.Cluster.Spec.Components.Extra[tspec.Frontend]
	d.Replicas = 2
	rc.Cluster.Spec.Components.Extra[tspec.Frontend] = d
	if e := engine.CheckCapabilities(p, rc.Cluster.Spec, "limited", infra.Capabilities{}); !errors.Is(e, engine.ErrNotSupported) {
		t.Fatal("horizontal scaling not required")
	}
}
