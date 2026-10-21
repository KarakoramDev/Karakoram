package companion

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type provider struct {
	engine.Provider
	password string
	kind     engine.DataMcpKind
}

func (*provider) Name() string { return "test" }
func (p *provider) DataMcp(*domain.Cluster) (engine.DataMcpBackend, error) {
	return engine.DataMcpBackend{Kind: p.kind, Upstream: companionspec.UpstreamMongoDB}, nil
}
func (p *provider) DataAccessCredentials(_ context.Context, _ *engine.Context, l engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	return engine.DataAccessCredentials{Username: engine.DataAccessUser(l), Password: p.password, URI: "mongodb://db:27017"}, nil
}

type artifact struct{}

func (artifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type agents struct{ client *agentapi.Client }

func (a agents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return a.client, nil
}

type fixture struct {
	controller *Controller
	rc         *engine.Context
	backend    *fake.Backend
	provider   *provider
	ca         *pki.CA
	unhealthy  atomic.Bool
	reloads    atomic.Int64
}

func setup(t *testing.T) *fixture {
	t.Helper()
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.invalid")
	key, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := &domain.Cluster{Name: "demo", Namespace: domain.DefaultNamespace, Spec: domain.ClusterSpec{Provider: "test", DataMcp: &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}}, Internal: json.RawMessage(`{"provider":"unchanged"}`)}
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.NewCA("cluster", 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string][]byte{"tls/ca.crt": ca.CertPEM, "tls/ca.key": ca.KeyPEM} {
		if err = st.SetClusterSecret(t.Context(), c.Ref(), name, value); err != nil {
			t.Fatal(err)
		}
	}
	b := fake.New(fake.Options{Polls: 1})
	p := &provider{password: "first", kind: engine.DataMcpCompanion}
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{controller: &Controller{Registry: registry, InternalCAPEM: "control-plane-ca"}, backend: b, provider: p, ca: ca, rc: &engine.Context{Cluster: c, Backend: b, Secrets: st, Store: st, Artifact: artifact{}, Clock: engine.RealClock{}, AgentURL: "https://control-plane.invalid"}}
	crt, private, err := ca.Issue(pki.CertRequest{CommonName: Member(c.Name), DNSNames: []string{Member(c.Name)}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(crt, private)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/v1/reload" {
			f.reloads.Add(1)
			w.WriteHeader(200)
			return
		}
		cfg, err := MemberConfig(r.Context(), f.rc, Member(c.Name))
		if err != nil {
			w.WriteHeader(503)
			return
		}
		v := map[string]any{}
		for _, name := range cfg.DesiredProcesses {
			v[name] = map[string]bool{"healthy": !f.unhealthy.Load()}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"configVersion": cfg.ConfigVersion, "processes": v})
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + Member(c.Name) + ":9443", Token: "test-token", CAPEM: ca.CertPEM, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	f.rc.Agents = agents{client}
	return f
}
func (f *fixture) converge(t *testing.T) {
	t.Helper()
	for range 30 {
		result, err := f.controller.Sync(t.Context(), f.rc)
		if err != nil {
			t.Fatal(err)
		}
		if result.RequeueAfter == 0 {
			return
		}
	}
	t.Fatal("companion did not converge")
}
func TestLifecycleRotationDisable(t *testing.T) {
	f := setup(t)
	original := string(f.rc.Cluster.Internal)
	result, err := f.controller.Sync(t.Context(), f.rc)
	if err != nil || result.RequeueAfter == 0 {
		t.Fatal(result, err)
	}
	if slices.Contains(f.backend.Calls(), "ImportServices") {
		t.Fatal("import submitted before frozen checkpoint")
	}
	f.converge(t)
	if string(f.rc.Cluster.Internal) != original {
		t.Fatal("provider state clobbered")
	}
	cfg, err := MemberConfig(t.Context(), f.rc, Member("demo"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "datamcp" || len(cfg.DesiredProcesses) != 2 || cfg.ConfigVersion != 1 {
		t.Fatal(cfg)
	}
	var spec companionspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Processes) != 2 || spec.Processes[0].MaxDocuments != 100 || spec.Processes[1].MaxTimeMS != 30000 {
		t.Fatal(spec)
	}
	status := domain.ClusterStatus{}
	if err = f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp.Phase != "Ready" || len(status.Members) != 0 {
		t.Fatal(status, err)
	}
	if !slices.Contains(f.backend.Calls(), "Deploy") {
		t.Fatal("no guarded deployment")
	}
	before, err := Fingerprint(t.Context(), f.rc, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	f.provider.password = "rotated"
	f.converge(t)
	next, err := MemberConfig(t.Context(), f.rc, Member("demo"))
	if err != nil || next.ConfigVersion != 2 || f.reloads.Load() < 2 {
		t.Fatal(next, err, f.reloads.Load())
	}
	after, err := Fingerprint(t.Context(), f.rc, engine.DataAccessRead)
	if err != nil || before == after {
		t.Fatal("rotation fingerprint unchanged", err)
	}
	transport, err := Transport(t.Context(), f.rc, engine.DataAccessReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	if transport.(*mcp.StreamableClientTransport).Endpoint != "https://demomcp:9443/v1/datamcp/rw" {
		t.Fatal(transport)
	}
	f.unhealthy.Store(true)
	if err = f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp.Phase != "Degraded" {
		t.Fatal(status, err)
	}
	f.rc.Cluster.Spec.DataMcp.Enabled = false
	f.converge(t)
	if !slices.Contains(f.backend.Calls(), "DeleteServiceByID") {
		t.Fatal("delete did not use identity")
	}
	service, err := f.backend.GetService(t.Context(), "demomcp")
	if err != nil || service != nil {
		t.Fatal(service, err)
	}
	if err = f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp != nil {
		t.Fatal(status, err)
	}
}
func TestCleanupWaitsAndKeepsProviderState(t *testing.T) {
	f := setup(t)
	f.converge(t)
	f.rc.Cluster.DeleteOptions.KeepVolumes = true
	done, err := f.controller.Cleanup(t.Context(), f.rc)
	if done || err != nil {
		t.Fatal(done, err)
	}
	for range 20 {
		done, err = f.controller.Cleanup(t.Context(), f.rc)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("cleanup did not finish")
	}
	if string(f.rc.Cluster.Internal) != `{"provider":"unchanged"}` {
		t.Fatal("provider state changed")
	}
}
func TestStatusNativeProvisioningAndRelay(t *testing.T) {
	f := setup(t)
	var status domain.ClusterStatus
	if err := f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp.Phase != "Provisioning" {
		t.Fatal(status, err)
	}
	if !slices.Equal(f.controller.RelayTargets(f.rc.Cluster), []string{"demomcp:9443"}) {
		t.Fatal("missing relay target")
	}
	f.provider.kind = engine.DataMcpNative
	if err := f.controller.Observe(t.Context(), f.rc, &status); err != nil || status.DataMcp.Phase != "Ready" {
		t.Fatal(status, err)
	}
	if len(f.controller.RelayTargets(f.rc.Cluster)) != 0 {
		t.Fatal("native companion relay")
	}
}
func TestAdmissionAndCertificateRenewal(t *testing.T) {
	f := setup(t)
	b, _ := f.provider.DataMcp(f.rc.Cluster)
	if err := Validate(f.rc.Cluster, b); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Name = "invalid-name"
	if err := Validate(f.rc.Cluster, b); err == nil {
		t.Fatal("invalid member accepted")
	}
	f.rc.Cluster.Name = "demo"
	f.converge(t)
	cfg, _ := MemberConfig(t.Context(), f.rc, "demomcp")
	short, key, err := f.ca.Issue(pki.CertRequest{CommonName: "demomcp", DNSNames: []string{"demomcp"}, Server: true, Validity: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cfg.AgentTLS = agentapi.Certificate{CertPEM: string(short), KeyPEM: string(key)}
	raw, _ := json.Marshal(cfg)
	if err = f.rc.Secrets.SetClusterSecret(t.Context(), f.rc.Cluster.Ref(), configKey, raw); err != nil {
		t.Fatal(err)
	}
	f.converge(t)
	next, _ := MemberConfig(t.Context(), f.rc, "demomcp")
	if next.ConfigVersion != cfg.ConfigVersion+1 || next.AgentTLS.CertPEM == cfg.AgentTLS.CertPEM {
		t.Fatal("certificate not renewed")
	}
}
