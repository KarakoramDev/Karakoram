// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package keycloak reconciles an independent PostgreSQL binding consumer.
package keycloak

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type DatabaseOptions struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
}
type EngineOptions struct {
	Database     DatabaseOptions `json:"database"`
	Hostname     string          `json:"hostname,omitempty"`
	PublicHTTPS  bool            `json:"publicHttps,omitempty"`
	ProxyHeaders string          `json:"proxyHeaders,omitempty"`
}
type Options struct {
	Bindings                       pgbinding.Service
	ControlPlaneURL, InternalCAPEM string
}
type Provider struct{ options Options }

var _ engine.Provider = (*Provider)(nil)
var _ engine.AgentConfigSource = (*Provider)(nil)

func New(o Options) *Provider  { return &Provider{options: o} }
func (*Provider) Name() string { return "keycloak" }

func ParseOptions(raw json.RawMessage) (EngineOptions, error) {
	var o EngineOptions
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&o); err != nil {
		return o, engine.Invalid("invalid Keycloak engine options")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return o, engine.Invalid("Keycloak engine options must be one JSON object")
	}
	if o.Database.Name == "" {
		o.Database.Name = "keycloak"
	}
	if o.Database.Cluster == "" {
		return o, engine.Invalid("database.cluster is required")
	}
	if o.ProxyHeaders != "" && o.ProxyHeaders != "forwarded" && o.ProxyHeaders != "xforwarded" {
		return o, engine.Invalid("proxyHeaders must be forwarded or xforwarded")
	}
	if o.Hostname != "" {
		u, err := url.Parse(o.Hostname)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
			return o, engine.Invalid("hostname must be an absolute HTTP(S) origin")
		}
	}
	if o.PublicHTTPS && (o.Hostname == "" || o.ProxyHeaders == "") {
		return o, engine.Invalid("publicHttps requires an explicit HTTPS hostname and proxyHeaders")
	}
	if o.PublicHTTPS {
		u, _ := url.Parse(o.Hostname)
		if u.Scheme != "https" {
			return o, engine.Invalid("publicHttps requires an HTTPS hostname")
		}
	}
	return o, nil
}
func request(c *domain.Cluster, o EngineOptions) pgbinding.Request {
	ns := o.Database.Namespace
	if ns == "" {
		ns = c.Ref().Namespace
	}
	return pgbinding.Request{ConsumerKind: "keycloak", ConsumerNamespace: c.Ref().Namespace, ConsumerName: c.Name, SourceNamespace: ns, SourceCluster: o.Database.Cluster, Databases: []string{o.Database.Name}, MinMajor: 14, MaxMajor: 18}
}
func defaults() domain.ComponentSpec {
	return domain.ComponentSpec{Replicas: 1, Resources: domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 1.5, MaxRAM: 2, MinDisk: 1, MaxDisk: 3}}
}
func (*Provider) Describe() domain.ProviderInfo {
	c := defaults()
	return domain.ProviderInfo{
		Name: "keycloak", DisplayName: "Keycloak",
		Versions:     []domain.VersionInfo{{Version: kcspec.Version, Major: "26", Default: true, Status: "supported"}},
		Topologies:   []domain.TopologyInfo{{Type: "single", Components: map[string]domain.ComponentInfo{"engine": {Defaults: c, MinReplicas: 1, MaxReplicas: 1}}}},
		Presets:      []domain.Preset{{Name: "dev", Description: "Single Keycloak runtime with a PostgreSQL binding; maintenance interrupts access", Topology: domain.Topology{Type: "single"}, Components: domain.Components{Engine: c}}},
		Capabilities: []string{"monitoring"}, Operations: []string{}, Processes: []string{"keycloak"},
		EngineOptions: map[string]any{"database": map[string]any{"required": true}, "proxyHeaders": map[string]any{"enum": []string{"forwarded", "xforwarded"}}},
	}
}
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "keycloak"
	}
	if s.Version == "" {
		s.Version = kcspec.Version
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "single"
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 1
	}
	if s.Components.Engine.Resources == (domain.Resources{}) {
		s.Components.Engine.Resources = defaults().Resources
	}
	if s.TLS.Mode == "" {
		s.TLS.Mode = "requireTLS"
	}
	if s.TLS.CA == "" {
		s.TLS.CA = "selfSigned"
	}
	s.Monitoring.Enabled = true
}

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)

func (p *Provider) Validate(_ context.Context, old, c *domain.Cluster) error {
	if c == nil {
		return engine.Invalid("cluster is required")
	}
	s := c.Spec
	if !clusterName.MatchString(c.Name) || s.Provider != "keycloak" || s.Version != kcspec.Version || s.Topology.Type != "single" || s.Topology.Shards != 0 || s.Components.Engine.Replicas != 1 {
		return engine.Invalid("Keycloak requires a valid cluster name, pinned version, single topology and exactly one replica")
	}
	if s.Components.ConfigServer.Replicas != 0 || s.Components.Router.Replicas != 0 || len(s.Components.Extra) > 0 || s.EngineConfig != "" || s.DataSource != nil || s.Exposure.Public || s.Backup.Storage != "" || len(s.Backup.Schedules) > 0 || s.Backup.PITR.Enabled {
		return engine.Invalid("unsupported Keycloak component, raw configuration, exposure, data source or backup; manage backups on the bound PostgreSQL cluster")
	}
	if s.TLS.Mode != "requireTLS" || s.TLS.CA != "selfSigned" {
		return engine.Invalid("Keycloak agent TLS requires selfSigned requireTLS")
	}
	o, err := ParseOptions(s.EngineOptions)
	if err != nil {
		return err
	}
	if err = request(c, o).Validate(); err != nil {
		return engine.Invalid("invalid Keycloak database binding: %s", err)
	}
	r := s.Components.Engine.Resources
	if r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinRAM < 1.5 || r.MaxRAM < r.MinRAM || r.MinDisk < 1 || r.MaxDisk < r.MinDisk {
		return engine.Invalid("Keycloak requires at least one CPU, 1.5 GiB RAM and 1 GiB runtime disk with ordered bounds")
	}
	if old != nil {
		prior, e := ParseOptions(old.Spec.EngineOptions)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(request(old, prior), request(c, o)) {
			return engine.Invalid("database binding is immutable; create a new consumer to change it")
		}
	}
	return nil
}

type state struct {
	Member        string                 `json:"member"`
	ServiceID     string                 `json:"serviceId,omitempty"`
	Import        *engine.ImportPlan     `json:"import,omitempty"`
	Pending       *pending               `json:"pending,omitempty"`
	Deployed      bool                   `json:"deployed,omitempty"`
	Public        bool                   `json:"public,omitempty"`
	Resources     domain.Resources       `json:"resources"`
	ConfigVersion int64                  `json:"configVersion"`
	ConfigHash    string                 `json:"configHash,omitempty"`
	Recovery      engine.RuntimeRecovery `json:"recovery,omitempty"`
}
type pending struct {
	Op        infra.Op         `json:"op"`
	Action    string           `json:"action"`
	Public    bool             `json:"public,omitempty"`
	Resources domain.Resources `json:"resources"`
}

func (s *state) runtimeMember(rc *engine.Context, token string) engine.RuntimeMember {
	return engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: s.Member, Token: token, Resources: s.Resources, Containers: 1}, ServiceID: &s.ServiceID, Recovery: &s.Recovery}
}

func load(c *domain.Cluster) (*state, error) {
	s := &state{Member: c.Name + "kc"}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, s); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func save(c *domain.Cluster, s *state) error { b, e := json.Marshal(s); c.Internal = b; return e }
func missing(e error) bool {
	return errors.Is(e, store.ErrNotFound) || errors.Is(e, engine.ErrNotFound)
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	b, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
	if e == nil && len(b) > 0 {
		return b, nil
	}
	if e != nil && !missing(e) {
		return nil, e
	}
	value, e := secrets.Token()
	if e != nil {
		return nil, e
	}
	b = []byte(value)
	return b, rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, b)
}
func token(ctx context.Context, rc *engine.Context, s *state) (*domain.AgentToken, error) {
	t, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), s.Member)
	if e != nil && !missing(e) {
		return nil, e
	}
	if t != nil {
		return t, nil
	}
	v, e := secrets.Token()
	if e != nil {
		return nil, e
	}
	t = &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: s.Member, Token: v, Hash: secrets.Hash(v)}
	return t, rc.Secrets.SetAgentToken(ctx, t)
}
func verify(ctx context.Context, rc *engine.Context, s *state) (*infra.Service, error) {
	v, e := rc.Backend.GetService(ctx, s.Member)
	if errors.Is(e, infra.ErrNotFound) {
		return nil, nil
	}
	if e != nil || v == nil {
		return v, e
	}
	t, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), s.Member)
	if e != nil {
		return nil, e
	}
	if t == nil {
		return nil, engine.UnverifiedOwnership(v)
	}
	if marker := v.Ownership[engine.OwnerVariable]; marker != "" && marker != engine.OwnerMarker(t.Token, rc.Cluster.Name, s.Member, v.Hostname) {
		return nil, engine.UnverifiedOwnership(v)
	}
	if v.ID != s.ServiceID && !engine.HasOwnership(v, t.Token, rc.Cluster.Name, s.Member) {
		return nil, engine.UnverifiedOwnership(v)
	}
	if e = engine.EnsureOwnerMarker(ctx, rc.Backend, v, t.Token, rc.Cluster.Name, s.Member); e != nil {
		return nil, e
	}
	s.ServiceID = v.ID
	return v, nil
}
func observe(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	status, e := rc.Backend.OpStatus(ctx, q.Op)
	if e != nil {
		return true, e
	}
	if !status.Done && !status.Failed {
		return true, nil
	}
	if status.Failed {
		return true, fmt.Errorf("keycloak %s failed (%s): %s", q.Action, status.Status, status.Message)
	}
	switch q.Action {
	case "import":
		v, e := rc.Backend.GetService(ctx, s.Member)
		if e != nil {
			return true, e
		}
		if id := engine.ImportedServiceID(q.Op, status, v); id != "" {
			s.ServiceID = id
		}
		v, e = verify(ctx, rc, s)
		if e != nil {
			return true, e
		}
		if v == nil || v.Importing {
			return true, engine.ErrUnavailable
		}
		s.Import = nil
	case "deploy":
		s.Deployed = true
	case "public":
		s.Public = q.Public
	case "resize":
		s.Resources = q.Resources
	case "delete":
		v, e := verify(ctx, rc, s)
		if e != nil {
			return true, e
		}
		if v != nil {
			return true, nil
		}
		s.ServiceID = ""
		s.Deployed = false
	}
	s.Pending = nil
	return true, nil
}
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 10 * time.Second
	if p.options.Bindings == nil {
		return result, engine.ErrUnavailable
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return result, err
	}
	conn, err := p.options.Bindings.Ensure(ctx, request(rc.Cluster, o))
	if err != nil {
		return result, err
	}
	if s.Import != nil {
		t, e := token(ctx, rc, s)
		if e != nil {
			return result, e
		}
		op, e := engine.ResumeImportPlan(ctx, rc, s.Import, s.Member, t.Token)
		if e != nil {
			return result, e
		}
		if op != nil {
			s.Pending = &pending{Op: *op, Action: "import"}
			return result, nil
		}
	}
	if busy, e := observe(ctx, rc, s); busy || e != nil {
		return result, e
	}
	t, err := token(ctx, rc, s)
	if err != nil {
		return result, err
	}
	v, err := verify(ctx, rc, s)
	if err != nil {
		return result, err
	}
	if s.Deployed {
		recovery, e := engine.ReconcileRuntime(ctx, rc, s.runtimeMember(rc, t.Token))
		if recovery.Imported {
			s.Deployed = false
		}
		if e != nil || recovery.Busy {
			return result, e
		}
	}
	if v == nil {
		s.Deployed = false
		if s.Import == nil {
			s.Import = engine.NewImportPlan([]infra.ServiceSpec{engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: s.Member, Token: t.Token, Containers: 1, Resources: rc.Cluster.Spec.Components.Engine.Resources})})
			s.Resources = rc.Cluster.Spec.Components.Engine.Resources
			return result, nil
		}
		op, e := s.Import.Submit(ctx, rc, s.Member, t.Token)
		if e != nil {
			return result, e
		}
		s.Pending = &pending{Op: op, Action: "import"}
		return result, nil
	}
	if v.Importing {
		return result, engine.Invalid("Keycloak runtime has incomplete import without an original import plan")
	}
	if s.Import != nil {
		op, e := s.Import.Submit(ctx, rc, s.Member, t.Token)
		if e != nil {
			return result, e
		}
		s.Pending = &pending{Op: op, Action: "import"}
		return result, nil
	}
	if err = p.publish(ctx, rc, s, conn, o); err != nil {
		return result, err
	}
	if !s.Deployed {
		runtime, e := p.runtime(rc, s)
		if e != nil {
			return result, e
		}
		op, e := engine.Deploy(ctx, rc, s.Member, runtime)
		if e != nil {
			return result, e
		}
		s.Pending = &pending{Op: op, Action: "deploy"}
		return result, nil
	}
	if rc.Agents != nil {
		_, _, err = engine.VisitLiveInstancesConcurrent(ctx, rc, s.Member, func(client *agentapi.Client) (bool, error) {
			applied, e := client.Status(ctx)
			if e != nil {
				return false, e
			}
			if applied.ConfigVersion < s.ConfigVersion {
				return false, client.Reload(ctx)
			}
			return true, nil
		})
		if err != nil {
			return result, err
		}
	}
	if s.Resources != rc.Cluster.Spec.Components.Engine.Resources {
		r := rc.Cluster.Spec.Components.Engine.Resources
		op, e := rc.Backend.SetResources(ctx, s.Member, r)
		if e != nil {
			return result, e
		}
		s.Pending = &pending{Op: op, Action: "resize", Resources: r}
		return result, nil
	}
	if s.Public != o.PublicHTTPS {
		access, e := engine.PublicHTTPS(rc.Backend)
		if e != nil {
			return result, e
		}
		op, e := access.SetSubdomainAccess(ctx, s.Member, o.PublicHTTPS)
		if e != nil {
			return result, e
		}
		s.Pending = &pending{Op: op, Action: "public", Public: o.PublicHTTPS}
		return result, nil
	}
	return result, nil
}
func (p *Provider) runtime(rc *engine.Context, s *state) (infra.RuntimeSpec, error) {
	a, e := nativeartifacts.Get("keycloak")
	if e != nil {
		return infra.RuntimeSpec{}, e
	}
	u, e := nativeartifacts.ResolveURL(a)
	if e != nil {
		return infra.RuntimeSpec{}, e
	}
	prepare := []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl python3 openjdk-21-jre-headless"}
	prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram/keycloak-"+a.Version, rc.Backend.Layout().User)...)
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	return (engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: s.Member, Engine: "keycloak", AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9443, Protocol: "tcp"}}, Prepare: prepare}).Spec(rc.Backend.Layout()), nil
}

func (p *Provider) publish(ctx context.Context, rc *engine.Context, s *state, c pgbinding.Connection, o EngineOptions) error {
	if c.BindingID == "" || c.TLS.Mode != pgbinding.TLSVerifyFull || len(c.TLS.CAPEM) == 0 {
		return engine.Invalid("Keycloak requires a verify-full PostgreSQL binding")
	}
	password, e := p.options.Bindings.Password(ctx, c.PasswordSecretRef)
	if e != nil {
		return e
	}
	admin, e := secret(ctx, rc, "keycloak/admin")
	if e != nil {
		return e
	}
	spec := kcspec.Spec{Version: kcspec.Version, Host: c.Host, Port: c.Port, Database: o.Database.Name, Username: c.Username, Password: password, CAPEM: string(c.TLS.CAPEM), Hostname: o.Hostname, ProxyHeaders: o.ProxyHeaders, AdminPassword: string(admin), CredentialGeneration: c.CredentialGeneration}
	if e = spec.Validate(); e != nil {
		return e
	}
	payload, e := json.Marshal(spec)
	if e != nil {
		return e
	}
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	previous, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "keycloak/config/"+s.Member)
	var config agentapi.MemberConfig
	if e != nil && !missing(e) {
		return e
	}
	if e == nil {
		if e = json.Unmarshal(previous, &config); e != nil {
			return e
		}
	}
	renew := config.AgentTLS.CertPEM == ""
	if !renew {
		renew, e = pki.NeedsRenewal([]byte(config.AgentTLS.CertPEM), 30*24*time.Hour, time.Now())
		if e != nil {
			return e
		}
	}
	if s.ConfigHash == hash && !renew {
		return nil
	}
	cert := config.AgentTLS
	if renew {
		cert, e = agentCertificate(ctx, rc, s.Member)
		if e != nil {
			return e
		}
	}
	config = agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: s.Member, Engine: "keycloak", EngineSpec: payload, Role: "keycloak", Port: 8080, EngineVersion: kcspec.Version, DesiredProcesses: []string{"keycloak"}, ConfigVersion: s.ConfigVersion + 1, AgentTLS: cert}
	raw, e := json.Marshal(config)
	if e != nil {
		return e
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "keycloak/config/"+s.Member, raw); e != nil {
		return e
	}
	s.ConfigVersion = config.ConfigVersion
	s.ConfigHash = hash
	// Reload retries are driven by observed applied versions, not a one-shot RPC.
	return nil
}
func agentCertificate(ctx context.Context, rc *engine.Context, member string) (agentapi.Certificate, error) {
	var out agentapi.Certificate
	bundle, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "keycloak/agent-ca")
	if e != nil && !missing(e) {
		return out, e
	}
	var pair agentapi.Certificate
	var ca *pki.CA
	if len(bundle) == 0 {
		ca, e = pki.NewCA(rc.Cluster.Name+" agents", 0)
		if e != nil {
			return out, e
		}
		pair = agentapi.Certificate{CertPEM: string(ca.CertPEM), KeyPEM: string(ca.KeyPEM)}
		bundle, e = json.Marshal(pair)
		if e != nil {
			return out, e
		}
		// Persist the pair atomically before publishing public trust material.
		if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "keycloak/agent-ca", bundle); e != nil {
			return out, e
		}
	} else {
		if e = json.Unmarshal(bundle, &pair); e != nil {
			return out, e
		}
		ca, e = pki.LoadCA([]byte(pair.CertPEM), []byte(pair.KeyPEM))
		if e != nil {
			return out, e
		}
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt", ca.CertPEM); e != nil {
		return out, e
	}
	crt, k, e := ca.Issue(pki.CertRequest{CommonName: member, DNSNames: []string{member, "localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Server: true})
	if e != nil {
		return out, e
	}
	return agentapi.Certificate{CertPEM: string(crt), KeyPEM: string(k)}, nil
}
func (*Provider) MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	var out agentapi.MemberConfig
	s, e := load(rc.Cluster)
	if e != nil {
		return out, e
	}
	if member != s.Member || s.ConfigVersion == 0 {
		return out, engine.ErrNotFound
	}
	b, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "keycloak/config/"+member)
	if e != nil {
		return out, e
	}
	e = json.Unmarshal(b, &out)
	return out, e
}
func (*Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.ClusterStatus{}, e
	}
	out := domain.ClusterStatus{Phase: domain.PhaseInitializing, Version: kcspec.Version, Message: "Waiting for Keycloak readiness", DeploymentFailures: rc.Cluster.Status.DeploymentFailures}
	if !s.Deployed || s.Pending != nil {
		return out, nil
	}
	ready, count, err := engine.VisitLiveInstancesConcurrent(ctx, rc, s.Member, func(client *agentapi.Client) (bool, error) {
		applied, e := client.Status(ctx)
		if e != nil {
			return false, e
		}
		if applied.ConfigVersion < s.ConfigVersion {
			return false, nil
		}
		var health struct {
			Ready bool `json:"ready"`
		}
		if e = client.Engine(ctx, "keycloak", "status", struct{}{}, &health); e != nil {
			return false, e
		}
		return health.Ready && applied.Error == "", nil
	})
	ready = ready && count == 1
	out.Members = []domain.MemberStatus{{Name: s.Member, Set: "keycloak", Role: "keycloak", State: "running", Health: ready, Version: kcspec.Version, Agent: domain.AgentStatus{Healthy: err == nil && count > 0}}}
	if ready {
		out.Phase = domain.Phase("Ready")
		out.Message = "Keycloak is ready"
	} else if err != nil {
		out.Message = "Keycloak agent or management health unavailable"
	}
	return out, nil
}
func (*Provider) RunOperation(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error) {
	return engine.OpResult{}, engine.ErrNotSupported
}
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.Recovery.Op != nil || s.Recovery.ImportPlan != nil {
		t, e := token(ctx, rc, s)
		if e != nil {
			return false, e
		}
		recovery, e := engine.ObserveRuntimeRecovery(ctx, rc, s.runtimeMember(rc, t.Token))
		if e != nil || recovery.Busy {
			return false, e
		}
	}
	if s.Import != nil {
		t, e := token(ctx, rc, s)
		if e != nil {
			return false, e
		}
		op, e := engine.ResumeImportPlan(ctx, rc, s.Import, s.Member, t.Token)
		if e != nil {
			return false, e
		}
		if op != nil {
			s.Pending = &pending{Op: *op, Action: "import"}
			return false, nil
		}
		if s.Pending == nil {
			op, e := s.Import.Submit(ctx, rc, s.Member, t.Token)
			if e != nil {
				return false, e
			}
			s.Pending = &pending{Op: op, Action: "import"}
			return false, nil
		}
	}
	if s.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op) {
		s.Pending = nil
	}
	if busy, e := observe(ctx, rc, s); busy || e != nil {
		return false, e
	}
	v, err := verify(ctx, rc, s)
	if err != nil {
		return false, err
	}
	if v != nil {
		if s.Public {
			access, e := engine.PublicHTTPS(rc.Backend)
			if e != nil {
				return false, e
			}
			op, e := access.SetSubdomainAccess(ctx, s.Member, false)
			if e != nil {
				return false, e
			}
			s.Pending = &pending{Op: op, Action: "public"}
			return false, nil
		}
		op, e := rc.Backend.DeleteServiceByID(ctx, s.Member, s.ServiceID)
		if e != nil {
			return false, e
		}
		s.Pending = &pending{Op: op, Action: "delete"}
		return false, nil
	}
	if p.options.Bindings == nil {
		return false, engine.ErrUnavailable
	}
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return false, err
	}
	if err = p.options.Bindings.Release(ctx, pgbinding.BindingID(request(rc.Cluster, o)), opts.KeepVolumes); err != nil {
		return false, err
	}
	for _, key := range []string{"keycloak/config/" + s.Member, "keycloak/admin", "keycloak/agent-ca", "tls/ca.crt"} {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return false, err
		}
	}
	if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), s.Member); err != nil && !missing(err) {
		return false, err
	}
	return true, nil
}

func (*Provider) Metrics() []domain.MetricDescriptor {
	return []domain.MetricDescriptor{{Exposition: "karakoram_keycloak_ready", Name: "keycloakReady", Scale: 1}}
}
