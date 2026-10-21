// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func configDigest(v any) string {
	raw, _ := json.Marshal(v)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func ensureCA(ctx context.Context, rc *engine.Context) (*pki.CA, error) {
	cert, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt")
	if e != nil && !missing(e) {
		return nil, e
	}
	key, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key")
	if e != nil && !missing(e) {
		return nil, e
	}
	if len(cert) > 0 && len(key) > 0 {
		return pki.LoadCA(cert, key)
	}
	ca, e := pki.NewCA(rc.Cluster.Name+" Temporal CA", 0)
	if e != nil {
		return nil, e
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key", ca.KeyPEM); e != nil {
		return nil, e
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt", ca.CertPEM); e != nil {
		return nil, e
	}
	return ca, nil
}

func componentCertificate(ctx context.Context, rc *engine.Context, m *componentMember, kind string, ca *pki.CA) (agentapi.Certificate, error) {
	key := "temporal/" + kind + "-cert/" + m.Name
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
	if e != nil && !missing(e) {
		return agentapi.Certificate{}, e
	}
	var pair agentapi.Certificate
	renew := len(raw) == 0
	if !renew {
		if e = json.Unmarshal(raw, &pair); e != nil {
			return pair, e
		}
		renew, e = pki.NeedsRenewal([]byte(pair.CertPEM), 30*24*time.Hour, clockNow(rc))
		if e != nil {
			return pair, e
		}
		block, _ := pem.Decode([]byte(pair.CertPEM))
		if block == nil {
			renew = true
		} else {
			leaf, err := x509.ParseCertificate(block.Bytes)
			renew = renew || err != nil
			if err == nil && leaf.CheckSignatureFrom(ca.Cert) != nil {
				renew = true
			}
		}
	}
	names := []string{m.Name, "localhost"}
	server, client := true, false
	switch kind {
	case "internode":
		names = append(names, InternodeServerName)
		client = true
	case "frontend":
		names = append(names, FrontendServerName, memberName(rc.Cluster, frontendComponent(rc.Cluster)))
	}
	if renew {
		crt, k, err := ca.Issue(pki.CertRequest{
			CommonName: m.Name + "-" + kind,
			DNSNames:   names,
			IPs:        []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
			Server:     server,
			Client:     client,
		})
		if err != nil {
			return pair, err
		}
		pair = agentapi.Certificate{CertPEM: string(crt), KeyPEM: string(k)}
		raw, e = json.Marshal(pair)
		if e != nil {
			return pair, e
		}
		e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, raw)
	}
	return pair, e
}

func (p *Provider) desiredSpec(ctx context.Context, rc *engine.Context, m *componentMember, conn pgbinding.Connection, ca *pki.CA) (tspec.Spec, error) {
	var out tspec.Spec
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		return out, e
	}
	a, e := serverArtifact(rc.Cluster.Spec.Version)
	if e != nil {
		return out, e
	}
	if m.Component == tspec.UI {
		a, e = nativeartifacts.Get("temporal-ui-server")
		if e != nil {
			return out, e
		}
	}
	out = tspec.Spec{
		Component: m.Component, Version: a.Version,
		InstallDir:    "/opt/karakoram/" + a.Name + "-" + a.Version,
		ClusterName:   rc.Cluster.Name,
		HistoryShards: o.HistoryShards,
		FrontendAddress: net.JoinHostPort(
			memberName(rc.Cluster, frontendComponent(rc.Cluster)),
			strconv.Itoa(tspec.Ports[tspec.Frontend].GRPC),
		),
	}
	internode, e := componentCertificate(ctx, rc, m, "internode", ca)
	if e != nil {
		return out, e
	}
	out.TLS = tspec.TLS{
		CAPEM:               string(ca.CertPEM),
		InternodeCertPEM:    internode.CertPEM,
		InternodeKeyPEM:     internode.KeyPEM,
		InternodeServerName: InternodeServerName,
		FrontendServerName:  FrontendServerName,
		RequireClientAuth:   o.Frontend.RequireClientAuth,
	}
	trust, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), rotationTrustKey)
	if err != nil && !missing(err) {
		return out, err
	}
	if len(trust) > 0 {
		out.TLS.CAPEM = string(trust)
	}
	if m.Component == tspec.Server || m.Component == tspec.Frontend {
		pair, e := componentCertificate(ctx, rc, m, "frontend", ca)
		if e != nil {
			return out, e
		}
		out.TLS.FrontendCertPEM = pair.CertPEM
		out.TLS.FrontendKeyPEM = pair.KeyPEM
	}
	if m.Component != tspec.UI {
		if conn.TLS.Mode != pgbinding.TLSVerifyFull {
			return out, engine.Invalid("Temporal requires PostgreSQL binding TLS verify-full")
		}
		password, e := p.options.Bindings.Password(ctx, conn.PasswordSecretRef)
		if e != nil {
			return out, engine.ErrUnavailable
		}
		// Reserve an equal budget per server component. Scaling one role must
		// not change other roles' configuration or restart their processes.
		roles := 1
		if rc.Cluster.Spec.Topology.Type == "split" {
			roles = 4
		}
		replicas := rc.Cluster.Spec.Components.Extra[m.Component].Replicas * roles
		pool := 20
		if conn.MaxConnections > 0 {
			pool = max(1, conn.MaxConnections/(max(1, replicas)*2))
		}
		pg := o.Persistence.Postgres
		out.Persistence = &tspec.Database{
			Host: conn.Host, Port: conn.Port,
			Database: pg.Database, VisibilityDatabase: pg.VisibilityDatabase,
			Username: conn.Username, Password: password,
			CAPEM:    string(conn.TLS.CAPEM),
			MaxConns: pool, VisibilityMaxConns: pool,
		}
	} else {
		out.UI = &tspec.UISettings{}
		if o.UI.PublicHTTPS && m.Public {
			access, e := engine.PublicHTTPS(rc.Backend)
			if e != nil {
				return out, e
			}
			urls, e := access.SubdomainURLs(ctx, m.Name)
			if e != nil {
				return out, e
			}
			if len(urls) == 0 {
				return out, engine.Invalid("waiting for UI HTTPS origin")
			}
			out.UI.PublicURL = urls[0]
		}
		if o.UI.OIDC != nil && m.Public {
			secret, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), UIOIDCClientSecretKey)
			if e != nil && !missing(e) {
				return out, e
			}
			if len(secret) > 0 {
				if out.UI.PublicURL == "" {
					return out, engine.Invalid("UI OIDC requires a public HTTPS callback origin")
				}
				out.UI.OIDC = &tspec.OIDC{
					Issuer: o.UI.OIDC.Issuer, ClientID: o.UI.OIDC.ClientID,
					ClientSecret: string(secret),
					CallbackURL:  out.UI.PublicURL + "/auth/sso/callback",
				}
			} else if o.UI.PublicHTTPS {
				return out, engine.Invalid("ui.publicHttps requires a sealed OIDC client secret")
			}
		}
	}
	// Include the binding generation even if an administrator reuses a password.
	out.RestartToken = configDigest(struct {
		Spec          tspec.Spec
		Generation    int64
		ServerVersion string
		RestartNonce  string
	}{out, conn.CredentialGeneration, rc.Cluster.Spec.Version, m.RestartNonce})
	return out, out.Validate()
}

// MemberConfig returns a sealed, secret-bearing envelope for a known member.
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	var out agentapi.MemberConfig
	s, e := loadState(rc.Cluster)
	if e != nil {
		return out, e
	}
	found := false
	for _, m := range s.Members {
		if m.Name == name {
			found = true
			break
		}
	}
	if !found {
		return out, engine.ErrNotFound
	}
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), ConfigSecretPrefix+name)
	if e != nil {
		return out, e
	}
	e = json.Unmarshal(raw, &out)
	return out, e
}

func (p *Provider) publishConfig(ctx context.Context, rc *engine.Context, m *componentMember, sp tspec.Spec, ca *pki.CA) error {
	if m.Component == tspec.UI && m.Public && (sp.UI == nil || sp.UI.OIDC == nil) {
		return engine.Invalid("remove the public UI route before publishing unauthenticated configuration")
	}
	if e := sp.Validate(); e != nil {
		return e
	}
	cert, e := componentCertificate(ctx, rc, m, "agent", ca)
	if e != nil {
		return e
	}
	payload, e := json.Marshal(sp)
	if e != nil {
		return e
	}
	process := "temporal-server"
	port := tspec.Ports[tspec.Frontend].GRPC
	if m.Component == tspec.UI {
		process = "ui-server"
		port = tspec.UIPort
	} else if m.Component != tspec.Server {
		port = tspec.Ports[m.Component].GRPC
	}
	version := m.ConfigVersion + 1
	config := agentapi.MemberConfig{
		Cluster: rc.Cluster.Name, Member: m.Name,
		Engine: tspec.Engine, EngineSpec: payload,
		Role: m.Component, Port: port,
		EngineVersion: sp.Version, ConfigVersion: version,
		DesiredProcesses: []string{process},
		AgentTLS:         cert,
	}
	raw, e := json.Marshal(config)
	if e != nil {
		return e
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), ConfigSecretPrefix+m.Name, raw); e != nil {
		return e
	}
	m.ConfigVersion = version
	m.ConfigHash = sp.RestartToken
	m.Version = sp.Version
	return nil
}

func (p *Provider) runtimeSpec(rc *engine.Context, m *componentMember) (infra.RuntimeSpec, error) {
	a, e := serverArtifact(rc.Cluster.Spec.Version)
	if e != nil {
		return infra.RuntimeSpec{}, e
	}
	if m.Component == tspec.UI {
		a, e = nativeartifacts.Get("temporal-ui-server")
		if e != nil {
			return infra.RuntimeSpec{}, e
		}
	}
	u, e := nativeartifacts.ResolveURL(a)
	if e != nil {
		return infra.RuntimeSpec{}, e
	}
	prepare := []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl python3"}
	prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram/"+a.Name+"-"+a.Version, rc.Backend.Layout().User)...)
	ports := []infra.Port{{Port: 9443, Protocol: "tcp"}}
	for _, role := range tspec.Roles(m.Component) {
		pair := tspec.Ports[role]
		ports = append(ports, infra.Port{Port: pair.GRPC, Protocol: "tcp"}, infra.Port{Port: pair.Membership, Protocol: "tcp"})
		if role == tspec.Frontend {
			ports = append(ports, infra.Port{Port: tspec.FrontendHTTPPort, Protocol: "tcp"})
		}
	}
	if m.Component == tspec.UI {
		ports = append(ports, infra.Port{Port: tspec.UIPort, Protocol: "tcp", HTTP: true})
	}
	url := rc.AgentURL
	if url == "" {
		url = p.options.ControlPlaneURL
	}
	runtime := engine.AgentRuntime{
		Cluster: rc.Cluster.Name, Member: m.Name,
		Engine: tspec.Engine, AgentURL: url,
		InternalCAPEM: p.options.InternalCAPEM,
		Ports:         ports, Prepare: prepare,
		ReadinessArgs: engine.StatelessReadiness(
			rc, m.Name, &m.NativeReadiness,
			"--engine=temporal", "--data-dir="+rc.Backend.Layout().DataDir(""), m.Component,
		),
	}
	return runtime.Spec(rc.Backend.Layout()), nil
}
