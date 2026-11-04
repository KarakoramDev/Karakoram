// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/s3endpoint"
)

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (p *Provider) apiURL(ctx context.Context, rc *engine.Context) (string, error) {
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return "", err
	}
	if rc.Cluster.Spec.Components.Extra["gateway"].Replicas == 0 {
		return "", nil
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return "", err
	}
	public := o.Gateway.PublicHTTPS
	if s.GatewayAdmission.Admitted != nil {
		public = s.GatewayAdmission.Admitted.Public
	}
	if public {
		endpoint, err := gatewayPublicURL(ctx, rc)
		if err != nil {
			return "", err
		}
		if endpoint != "" {
			return endpoint, nil
		}
		if s.GatewayURL != "" {
			return s.GatewayURL, nil
		}
		return "", engine.ErrUnavailable
	}
	return "http://" + rc.Cluster.Name + "gw:8000", nil
}

func gatewayPublicURL(ctx context.Context, rc *engine.Context) (string, error) {
	access, err := engine.PublicHTTPS(rc.Backend)
	if err != nil {
		return "", err
	}
	urls, err := access.SubdomainURLs(ctx, rc.Cluster.Name+"gw")
	if err != nil {
		return "", err
	}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err == nil && u.Scheme == "https" && u.Host != "" {
			return strings.TrimRight(raw, "/"), nil
		}
	}
	return "", nil
}

// Retain the applied canonical URL before legacy recovery or suspension removes
// the runtime/config receipt. Reusing it avoids a public-to-private bootstrap
// config transition that would otherwise need a window to reverse after healing.
func retainGatewayURL(ctx context.Context, rc *engine.Context, s *componentState) error {
	m := s.Members["gateway"]
	if s.GatewayURL != "" || m == nil || s.GatewayAdmission.Admitted == nil || !s.GatewayAdmission.Admitted.Public {
		return nil
	}
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name)
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var config agentapi.MemberConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return err
	}
	var spec sbspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		return err
	}
	u, err := url.Parse(spec.APIExternalURL)
	if err == nil && u.Scheme == "https" && u.Host != "" {
		s.GatewayURL = strings.TrimRight(spec.APIExternalURL, "/")
	}
	return nil
}
func (p *Provider) desiredSpec(ctx context.Context, rc *engine.Context, m *componentMember) (sbspec.Spec, error) {
	var out sbspec.Spec
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return out, err
	}
	c, err := loadCredentials(ctx, rc)
	if err != nil {
		return out, err
	}
	if c.RealtimeAdminSecret == "" {
		if err = ensureCredentials(ctx, rc); err != nil {
			return out, err
		}
		c, err = loadCredentials(ctx, rc)
		if err != nil {
			return out, err
		}
	}
	a, err := nativeartifacts.Get(componentArtifact(m.Component))
	if err != nil {
		return out, err
	}
	private := "http://" + rc.Cluster.Name + "gw:8000"
	api := private
	// A gateway must first deploy its HTTP port before the backend can publish
	// HTTPS routing. Other components are provisioned only after that transition.
	state, err := loadState(rc.Cluster)
	if err != nil {
		return out, err
	}
	if m.Component != "gateway" || m.Public || state.GatewayURL != "" {
		api, err = p.apiURL(ctx, rc)
		if err != nil {
			return out, err
		}
	}
	site := o.SiteURL
	if site == "" {
		site = api
		if site == "" {
			site = private
		}
	}
	peers := p.Core.DataMembers(rc.Cluster)
	if len(peers) == 0 {
		return out, engine.ErrUnavailable
	}
	data, err := p.Core.MemberConfig(ctx, rc, peers[0])
	if err != nil {
		return out, err
	}
	var pg pgspec.Spec
	if err = json.Unmarshal(data.EngineSpec, &pg); err != nil {
		return out, err
	}
	roles := make(map[string]string, len(c.Roles)+1)
	for name, password := range c.Roles {
		roles[name] = password
	}
	roles["postgres"] = pg.Superuser.Password
	out = sbspec.Spec{
		Component: m.Component, Version: a.Version,
		InstallDir: "/opt/karakoram/" + a.Name + "-" + a.Version,
		Topology:   rc.Cluster.Spec.Topology.Type, DataPeers: peers,
		CAPEM: pg.CAPEM, PatroniUsername: pg.PatroniREST.Username, PatroniPassword: pg.PatroniREST.Password,
		Database: "postgres", DatabaseTLS: rc.Cluster.Spec.TLS.Mode != "disabled", DBRoles: roles,
		DatabaseIPv6: rc.Backend.Layout().PrivateAddressFamily != infra.AddressFamilyIPv4,
		SiteURL:      site, APIExternalURL: api, GatewayURL: private,
		JWTSecret: c.Keys.JWTSecret, JWKS: string(c.Keys.JWKS),
		AnonKey: c.Keys.AnonKey, ServiceRoleKey: c.Keys.ServiceRoleKey,
		PublishableKey: c.Keys.PublishableKey, SecretKey: c.Keys.SecretKey,
		AnonAsymmetricJWT: c.Keys.AnonJWT, ServiceAsymmetricJWT: c.Keys.ServiceRoleJWT,
		Upstreams: map[string]string{}, REST: o.REST,
		CORSOrigins:   o.Gateway.CORSOrigins,
		SecretKeyBase: c.Keys.SecretKeyBase, RealtimeDBEncKey: c.Keys.RealtimeDBEncKey,
		RealtimeAdminSecret: c.RealtimeAdminSecret,
		VaultEncKey:         c.Keys.VaultEncKey, LogflarePublicToken: c.Keys.LogflarePublicToken,
		LogflarePrivateToken: c.Keys.LogflarePrivateToken, VectorSinkToken: c.VectorToken,
		VerifyJWT: true,
	}
	if m.Component == "auth" {
		out.JWTKeys = string(c.Keys.JWTKeys)
	}
	if out.APIExternalURL == "" {
		out.APIExternalURL = private
	}
	for _, name := range stackComponents.Enabled(rc.Cluster.Spec.Components) {
		out.Upstreams[name] = net.JoinHostPort(rc.Cluster.Name+codes[name], strconv.Itoa(ports[name]))
	}
	for _, extension := range o.Extensions {
		if extension == "pg_graphql" {
			out.GraphQL = true
		}
	}
	out.Auth = sbspec.Auth{
		AdditionalRedirectURLs: o.AdditionalRedirectURLs, DisableSignup: o.Auth.DisableSignup,
		Autoconfirm: o.Auth.Autoconfirm, JWTExpiry: o.Auth.JWTExpiry,
		SMTP: sbspec.SMTP{
			Host: o.Auth.SMTP.Host, Port: o.Auth.SMTP.Port, User: o.Auth.SMTP.User,
			AdminEmail: o.Auth.SMTP.AdminEmail, SenderName: o.Auth.SMTP.SenderName,
		},
	}
	password, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/smtp-password")
	if err != nil && !missing(err) {
		return out, err
	}
	out.Auth.SMTP.Password = string(password)
	out.Studio = sbspec.Studio{
		Organization: o.Studio.Organization, Project: o.Studio.Project,
		DashboardUsername: "supabase", DashboardPassword: c.Keys.DashboardPassword,
		MetaCryptoKey: c.Keys.PGMetaCryptoKey,
	}
	out.Pooler = sbspec.Pooler{
		TenantID:        rc.Cluster.Ref().Namespace + "-" + rc.Cluster.Name,
		DefaultPoolSize: 20, MaxClientConnections: 100, DBPoolSize: 5,
	}
	if o.Storage.BackendStorage != "" {
		storage, err := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, o.Storage.BackendStorage)
		if err != nil {
			return out, err
		}
		bucket := storage.Bucket
		if o.Storage.Bucket != "" {
			bucket = o.Storage.Bucket
		}
		endpoint, err := s3endpoint.Parse(storage.EndpointURL)
		if err != nil {
			return out, err
		}
		out.Storage = sbspec.Storage{
			Endpoint: storage.EndpointURL, Bucket: bucket, Region: storage.Region,
			PathStyle: s3endpoint.PathStyle(endpoint, bucket, storage.ForcePathStyle), TLS: endpoint.Scheme == "https",
			AccessKey: storage.Credentials.AccessKeyID, SecretKey: storage.Credentials.SecretAccessKey,
			TenantID:      rc.Cluster.Ref().Namespace + "-" + rc.Cluster.Name,
			FileSizeLimit: o.Storage.FileSizeLimit, ImageTransformation: o.Storage.ImageTransformation,
			ProtocolAccessKey: c.Keys.StorageS3AccessKeyID, ProtocolSecretKey: c.Keys.StorageS3SecretAccessKey,
		}
	}
	if o.Functions.VerifyJWT != nil {
		out.VerifyJWT = *o.Functions.VerifyJWT
	}
	if m.Component == "functions" {
		out.Functions, err = loadFunctions(ctx, rc)
		if err != nil {
			return out, err
		}
	}
	if rc.Cluster.Spec.Components.Extra["vector"].Replicas > 0 && m.Component != "vector" {
		out.VectorSinkURL = "http://" + rc.Cluster.Name + "vc:9002"
	}
	out = out.ProjectSecrets()
	if m.Component == "gateway" {
		// Upgrade existing gateways to the loopback admin/DNS bootstrap even
		// when their component settings and native Envoy version are unchanged.
		out.RestartToken = configDigest(struct {
			Spec                     sbspec.Spec
			GatewayBootstrapRevision int
		}{out, 1})
	} else {
		out.RestartToken = configDigest(out)
	}
	return out, out.Validate()
}
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	for _, m := range s.Members {
		if m.Name != name {
			continue
		}
		raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+name)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		var out agentapi.MemberConfig
		err = json.Unmarshal(raw, &out)
		return out, err
	}
	return p.Core.MemberConfig(ctx, rc, name)
}

func componentCertificateNeedsRenewal(ctx context.Context, rc *engine.Context, m *componentMember) (bool, error) {
	// Inspect the published pair, not just the separately stored certificate:
	// failed publication must retry even if issuance already succeeded.
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name)
	if missing(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var config agentapi.MemberConfig
	if err = json.Unmarshal(raw, &config); err != nil {
		return false, err
	}
	return componentCertificateExpiry(rc, config.AgentTLS)
}

func componentCertificateExpiry(rc *engine.Context, cert agentapi.Certificate) (bool, error) {
	return pki.NeedsRenewal([]byte(cert.CertPEM), 30*24*time.Hour, clockNow(rc))
}

func componentAgentCertificate(ctx context.Context, rc *engine.Context, m *componentMember) (agentapi.Certificate, error) {
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/agent-cert/"+m.Name)
	renew := missing(err)
	var cert agentapi.Certificate
	if err != nil && !renew {
		return cert, err
	}
	if !renew {
		if err = json.Unmarshal(raw, &cert); err != nil {
			return cert, err
		}
		renew, err = componentCertificateExpiry(rc, cert)
		if err != nil {
			return cert, err
		}
	}
	if renew {
		caCert, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt")
		if err != nil {
			return cert, err
		}
		key, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key")
		if err != nil {
			return cert, err
		}
		ca, err := pki.LoadCA(caCert, key)
		if err != nil {
			return cert, err
		}
		crt, k, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Server: true})
		if err != nil {
			return cert, err
		}
		cert = agentapi.Certificate{CertPEM: string(crt), KeyPEM: string(k)}
		raw, err = json.Marshal(cert)
		if err != nil {
			return cert, err
		}
		if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/agent-cert/"+m.Name, raw); err != nil {
			return cert, err
		}
	}
	return cert, nil
}

// renewPublishedCertificates changes transport only, even when core health or
// component lifecycle gates are blocked. Preserve the already-published process,
// maintenance and application settings rather than rendering new desired state.
func (p *Provider) renewPublishedCertificates(ctx context.Context, rc *engine.Context) error {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return err
	}
	for _, m := range s.Members {
		raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name)
		if missing(err) {
			continue
		}
		if err != nil {
			return err
		}
		var config agentapi.MemberConfig
		if err := json.Unmarshal(raw, &config); err != nil {
			return err
		}
		renew, err := componentCertificateExpiry(rc, config.AgentTLS)
		if err != nil {
			return err
		}
		if !renew {
			continue
		}
		cert, err := componentAgentCertificate(ctx, rc, m)
		if err != nil {
			return err
		}
		config.AgentTLS = cert
		config.ConfigVersion = max(config.ConfigVersion, m.ConfigVersion) + 1
		raw, err = json.Marshal(config)
		if err != nil {
			return err
		}
		if err := rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name, raw); err != nil {
			return err
		}
		m.ConfigVersion = config.ConfigVersion
		if err := saveState(rc.Cluster, s); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) publishConfig(ctx context.Context, rc *engine.Context, m *componentMember, sp sbspec.Spec) error {
	cert, err := componentAgentCertificate(ctx, rc, m)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(sp)
	if err != nil {
		return err
	}
	version := m.ConfigVersion + 1
	config := agentapi.MemberConfig{
		Cluster: rc.Cluster.Name, Member: m.Name, Engine: "supabase", EngineSpec: payload,
		Role: m.Component, Port: ports[m.Component], EngineVersion: sp.Version,
		ConfigVersion: version, DesiredProcesses: []string{m.Component}, AgentTLS: cert,
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name, raw); err != nil {
		return err
	}
	m.ConfigVersion = version
	m.ConfigHash = sp.RestartToken
	m.Version = sp.Version
	return nil
}
func (p *Provider) runtimeSpec(rc *engine.Context, m *componentMember) (infra.RuntimeSpec, error) {
	a, err := nativeartifacts.Get(componentArtifact(m.Component))
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	u, err := nativeartifacts.ResolveURL(a)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	deps := "ca-certificates curl xz-utils python3"
	switch m.Component {
	case "storage", "meta", "studio":
		deps += " libstdc++6"
	case "realtime", "pooler", "analytics":
		deps += " libstdc++6 libssl3t64 libncurses6 locales"
	case "functions":
		deps += " libssl3t64 libblas3 liblapack3 libopenblas0"
	case "imgproxy":
		deps += " libvips42t64 fonts-dejavu-core"
	}
	prepare := []string{"sudo apt-get update && sudo apt-get install -y " + deps}
	prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram/"+a.Name+"-"+a.Version, rc.Backend.Layout().User)...)
	exposed := []infra.Port{{Port: ports[m.Component], Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}}
	if m.Component == "gateway" {
		exposed[0].HTTP = true
	}
	if m.Component == "pooler" {
		exposed = append(exposed, infra.Port{Port: 6543, Protocol: "tcp"})
	}
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	runtime := engine.AgentRuntime{
		Cluster: rc.Cluster.Name, Member: m.Name, Engine: "supabase",
		AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM,
		Ports: exposed, Prepare: prepare,
		ReadinessArgs: engine.StatelessReadiness(rc, m.Name, &m.NativeReadiness, "--engine=supabase", "--data-dir="+rc.Backend.Layout().DataDir(""), m.Component),
	}
	return runtime.Spec(rc.Backend.Layout()), nil
}
