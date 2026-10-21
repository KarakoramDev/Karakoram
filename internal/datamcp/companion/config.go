package companion

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativees"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativermq"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Capture the reviewed manifest versions once, without rebuilding tool maps on
// every status observation.
var (
	nativeESVersion  = nativees.Manifest().Version
	nativeRMQVersion = nativermq.Manifest().Version
)

func publish(ctx context.Context, rc *engine.Context, p engine.DataMcpProvider, b engine.DataMcpBackend, s *state) (bool, error) {
	a, err := nativeartifacts.Get(b.Upstream)
	if err != nil {
		return false, err
	}
	old, err := MemberConfig(ctx, rc, Member(rc.Cluster.Name))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	cert := old.AgentTLS
	now := time.Now()
	if rc.Clock != nil {
		now = rc.Clock.Now()
	}
	renew := cert.CertPEM == ""
	if !renew {
		renew, err = pki.NeedsRenewal([]byte(cert.CertPEM), 30*24*time.Hour, now)
		if err != nil {
			return false, err
		}
	}
	caCert, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt")
	if err != nil {
		return false, err
	}
	// Verify the published certificate still chains to the current cluster CA.
	if !renew {
		renew = !certificateTrusted(cert, caCert, Member(rc.Cluster.Name), now)
	}
	if renew {
		key, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key")
		if err != nil {
			return false, err
		}
		ca, err := pki.LoadCA(caCert, key)
		if err != nil {
			return false, err
		}
		m := Member(rc.Cluster.Name)
		crt, k, err := ca.Issue(pki.CertRequest{CommonName: m, DNSNames: []string{m, "localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Server: true})
		if err != nil {
			return false, err
		}
		cert = agentapi.Certificate{CertPEM: string(crt), KeyPEM: string(k)}
	}
	sp := companionspec.Spec{Upstream: b.Upstream, Version: a.Version, InstallDir: installDir(a)}
	levels := []engine.DataAccessLevel{engine.DataAccessRead}
	_, rw := engine.DataAccessLevels(rc.Cluster)
	if rw {
		levels = append(levels, engine.DataAccessReadWrite)
	}
	names := []string{companionspec.ProcessRead}
	if rw {
		names = append(names, companionspec.ProcessReadWrite)
	}
	for i, l := range levels {
		v, err := p.DataAccessCredentials(ctx, rc, l)
		if err != nil {
			return false, err
		}
		sp.Processes = append(sp.Processes, companionspec.Process{Level: string(l), Port: 18080 + i, Username: v.Username, Password: v.Password, Hosts: v.Hosts, Database: v.Database, URI: v.URI, TLS: v.TLS, CAPEM: string(v.CAPEM), MaxDocuments: 100, MaxTimeMS: 30000})
	}
	payload, err := json.Marshal(sp)
	if err != nil {
		return false, err
	}
	cfg := agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: Member(rc.Cluster.Name), Engine: companionspec.Engine, Role: companionspec.Engine, EngineSpec: payload, EngineVersion: a.Version, AgentTLS: cert, DesiredProcesses: names, ConfigVersion: old.ConfigVersion}
	desiredSpec, err := json.Marshal(rc.Cluster.Spec.DataMcp)
	if err != nil {
		return false, err
	}
	specHash := secrets.Hash(string(desiredSpec))
	if reflect.DeepEqual(cfg, old) && specHash == s.SpecHash {
		return false, nil
	}
	cfg.ConfigVersion++
	raw, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}
	// Persist the reload intent first: a crash after publishing must not strand
	// the agent on its previous credentials or certificate.
	s.Reload = true
	if err = save(ctx, rc, s); err != nil {
		return false, err
	}
	err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), configKey, raw)
	if err == nil {
		s.SpecHash = specHash
	}
	return err == nil, err
}
func installDir(a nativeartifacts.Artifact) string {
	return "/opt/karakoram/" + a.Name + "-" + a.Version
}
func (c *Controller) runtime(rc *engine.Context, b engine.DataMcpBackend) (infra.RuntimeSpec, error) {
	a, err := nativeartifacts.Get(b.Upstream)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	u, err := nativeartifacts.ResolveURL(a)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	deps := append([]string{"ca-certificates", "curl", "xz-utils", "python3"}, a.RuntimeDeps...)
	prepare := []string{"sudo apt-get update && sudo apt-get install -y " + strings.Join(deps, " ")}
	prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, installDir(a), rc.Backend.Layout().User)...)
	return (engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: Member(rc.Cluster.Name), Engine: companionspec.Engine, AgentURL: rc.AgentURL, InternalCAPEM: c.InternalCAPEM, Ports: []infra.Port{{Port: 9443, Protocol: "tcp"}}, Prepare: prepare}).Spec(rc.Backend.Layout()), nil
}
func ready(ctx context.Context, rc *engine.Context) error {
	cfg, err := MemberConfig(ctx, rc, Member(rc.Cluster.Name))
	if err != nil {
		return err
	}
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), Member(rc.Cluster.Name))
	if err != nil {
		return err
	}
	var v struct {
		ConfigVersion int64 `json:"configVersion"`
		Processes     map[string]struct {
			Healthy bool `json:"healthy"`
		} `json:"processes"`
	}
	if err = client.Engine(ctx, companionspec.Engine, "readiness", struct{}{}, &v); err != nil {
		return err
	}
	if v.ConfigVersion != cfg.ConfigVersion {
		return errors.New("data MCP configuration is not applied")
	}
	for _, name := range cfg.DesiredProcesses {
		if !v.Processes[name].Healthy {
			return errors.New("data MCP upstream is not ready")
		}
	}
	return nil
}
func (c *Controller) Observe(ctx context.Context, rc *engine.Context, status *domain.ClusterStatus) error {
	status.DataMcp = nil
	if !rc.Cluster.Spec.DataMcp.DataMcpEnabled() {
		return nil
	}
	_, b, err := c.backend(rc)
	if err != nil {
		return err
	}
	if b.Kind == "" {
		return nil
	}
	v := &domain.DataMcpStatus{Phase: domain.DataMcpPhaseReady, Kind: string(b.Kind), Upstream: b.Upstream, Access: domain.DataMcpAccessRead}
	if rc.Cluster.Spec.DataMcp.AllowsWrite() {
		v.Access = domain.DataMcpAccessReadWrite
	}
	status.DataMcp = v
	if b.Kind != engine.DataMcpCompanion {
		switch b.Upstream {
		case "native-elasticsearch":
			v.UpstreamVersion = nativeESVersion
		case "native-rabbitmq":
			v.UpstreamVersion = nativeRMQVersion
		}
		return nil
	}
	v.Phase = domain.DataMcpPhaseProvisioning
	a, err := nativeartifacts.Get(b.Upstream)
	if err != nil {
		v.Phase = domain.DataMcpPhaseDegraded
		v.Message = "Companion artifact unavailable"
		return err
	}
	v.UpstreamVersion = a.Version
	s, err := load(ctx, rc)
	if err != nil {
		return err
	}
	if s == nil || s.Stage != "ready" {
		v.Phase = domain.DataMcpPhaseProvisioning
		return nil
	}
	if err = ready(ctx, rc); err != nil {
		v.Phase = domain.DataMcpPhaseDegraded
		v.Message = "Companion readiness unavailable"
	} else {
		v.Phase = domain.DataMcpPhaseReady
	}
	return nil
}
func (c *Controller) RelayTargets(cluster *domain.Cluster) []string {
	if !cluster.Spec.DataMcp.DataMcpEnabled() {
		return nil
	}
	p, err := c.Registry.Get(cluster.Spec.Provider)
	if err != nil {
		return nil
	}
	d, ok := p.(engine.DataMcpProvider)
	if !ok {
		return nil
	}
	b, err := d.DataMcp(cluster)
	if err != nil || b.Kind != engine.DataMcpCompanion {
		return nil
	}
	return []string{Member(cluster.Name) + ":9443"}
}
func Transport(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (mcp.Transport, error) {
	if !level.Valid() {
		return nil, engine.ErrInvalid
	}
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	if !ro || (level == engine.DataAccessReadWrite && !rw) {
		return nil, engine.ErrNotSupported
	}
	if _, err := MemberConfig(ctx, rc, Member(rc.Cluster.Name)); err != nil {
		return nil, err
	}
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), Member(rc.Cluster.Name))
	if err != nil {
		return nil, err
	}
	route := companionspec.RouteRead
	if level == engine.DataAccessReadWrite {
		route = companionspec.RouteReadWrite
	}
	return &mcp.StreamableClientTransport{Endpoint: "https://" + Member(rc.Cluster.Name) + ":9443/v1/datamcp/" + route, HTTPClient: client.StreamingClient()}, nil
}
func Fingerprint(ctx context.Context, rc *engine.Context, level engine.DataAccessLevel) (string, error) {
	if !level.Valid() {
		return "", engine.ErrInvalid
	}
	cfg, err := MemberConfig(ctx, rc, Member(rc.Cluster.Name))
	if err != nil {
		return "", err
	}
	t, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), Member(rc.Cluster.Name))
	if err != nil {
		return "", err
	}
	ca, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt")
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal([]any{level, t.Token, string(ca), cfg.AgentTLS, cfg.ConfigVersion})
	return secrets.Hash(string(raw)), nil
}
