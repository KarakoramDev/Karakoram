// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base32"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

func (p *Provider) zeropsYAML(rc *engine.Context, m *member, version string) (string, error) {
	if !slices.ContainsFunc(catalog.Versions, func(v domain.VersionInfo) bool { return v.Version == version }) {
		return "", invalid("unsupported engine version %q", version)
	}
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	prepare := []string{"sudo apt-get update", "sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg", "curl -fsSL https://keys.openpgp.org/vks/v1/by-fingerprint/0A9AF2115F4687BD29803A206B73A36E6026DFCA | sudo gpg --dearmor -o /usr/share/keyrings/com.rabbitmq.team.gpg"}
	sources := []string{}
	for _, mirror := range []string{"deb1", "deb2"} {
		for _, repo := range []string{"rabbitmq-erlang", "rabbitmq-server"} {
			sources = append(sources, "deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://"+mirror+".rabbitmq.com/"+repo+"/ubuntu/noble noble main")
		}
	}
	prepare = append(prepare, "printf '%s\\n' '"+strings.Join(sources, "' '")+"' | sudo tee /etc/apt/sources.list.d/rabbitmq.list", "printf '#!/bin/sh\\nexit 101\\n' | sudo tee /usr/sbin/policy-rc.d && sudo chmod 755 /usr/sbin/policy-rc.d", "sudo apt-get update")
	packages := []string{"rabbitmq-server=" + version + "-1"}
	for _, pkg := range []string{"base", "asn1", "crypto", "eldap", "inets", "mnesia", "os-mon", "parsetools", "public-key", "runtime-tools", "ssl", "syntax-tools", "tools", "xmerl"} {
		packages = append(packages, "erlang-"+pkg+"="+catalog.ErlangVersion)
	}
	prepare = append(prepare, "sudo apt-get install -y --no-install-recommends "+strings.Join(packages, " "))
	options, err := parseOptions(m.EngineOptions)
	if err != nil {
		return "", err
	}
	ports := []map[string]any{}
	for _, port := range []int{5672, 5671, 15672, 15671, 15692, 4369, 25672} {
		v := map[string]any{"port": port, "protocol": "tcp"}
		if port == 15672 && options.ManagementUI.Public && (m.TLSMode == "disabled" || m.TLSMode == "preferTLS") {
			v["httpSupport"] = true
		}
		ports = append(ports, v)
	}
	if slices.Contains(options.Plugins, "rabbitmq_stream") {
		for _, port := range []int{5551, 5552} {
			ports = append(ports, map[string]any{"port": port, "protocol": "tcp"})
		}
	}
	run := map[string]any{"base": "ubuntu@24.04", "ports": ports, "prepareCommands": prepare, "envVariables": map[string]string{"KARAKORAM_URL": agentURL, "KARAKORAM_CLUSTER": rc.Cluster.Name, "KARAKORAM_MEMBER": m.Name, "KARAKORAM_INTERNAL_CA": p.options.InternalCAPEM, "KARAKORAM_DATA_DIR": "/mnt/" + m.Volume, "KARAKORAM_ENGINE": "rabbitmq"}, "start": "./karakoram agent", "volume": map[string]string{"hostname": m.Volume}}
	b, err := yaml.Marshal(map[string]any{"zerops": []map[string]any{{"setup": m.Name, "deploy": map[string]any{"temporaryShutdown": true, "readinessCheck": map[string]any{"exec": map[string]string{"command": "/var/www/karakoram agent readiness --engine=rabbitmq --data-dir=/mnt/" + m.Volume}, "failureTimeout": "300s", "retryPeriod": "10s"}}, "run": run}}})
	return string(b), err
}
func runtimeResources(r domain.Resources) domain.Resources { r.MinDisk = 1; r.MaxDisk = 3; return r }
func ownershipEnv(cluster, member, token string) map[string]string {
	return map[string]string{"KARAKORAM_CLUSTER": cluster, "KARAKORAM_MEMBER": member, "KARAKORAM_AGENT_TOKEN": token}
}
func serviceSpec(c *domain.Cluster, m *member, token string) infra.ServiceSpec {
	env := ownershipEnv(c.Name, m.Name, token)
	env["KARAKORAM_DATA_DIR"] = "/mnt/" + m.Volume
	return infra.ServiceSpec{Hostname: m.Name, Type: "ubuntu@24.04", Resources: runtimeResources(c.Spec.Components.Engine.Resources), MinContainers: 1, MaxContainers: 1, Env: env, Volume: m.Volume}
}
func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
}
func putSecret(ctx context.Context, rc *engine.Context, key string, v []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, v)
}
func ensureSecret(ctx context.Context, rc *engine.Context, key string, generate func() (string, error)) error {
	_, err := secret(ctx, rc, key)
	if err == nil {
		return nil
	}
	if !missing(err) {
		return err
	}
	v, err := generate()
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, key, []byte(v))
}
func cookie() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b), nil
}
func (p *Provider) ensureSecrets(ctx context.Context, rc *engine.Context, s *internalState) error {
	crt, err := secret(ctx, rc, "tls/ca.crt")
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return invalid("userProvided requires tls/ca.crt and tls/ca.key")
		}
		ca, e := pki.NewCA(rc.Cluster.Name, 10*365*24*time.Hour)
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/ca.key", ca.KeyPEM); e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/ca.crt", ca.CertPEM); e != nil {
			return e
		}
		crt = ca.CertPEM
	} else if err != nil {
		return err
	}
	key, err := secret(ctx, rc, "tls/ca.key")
	if err != nil {
		return err
	}
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		return err
	}
	if err = ensureSecret(ctx, rc, "erlang-cookie", cookie); err != nil {
		return err
	}
	for _, u := range []string{"karakoram", "admin", "monitor"} {
		if err = ensureSecret(ctx, rc, "users/"+u, secrets.Password); err != nil {
			return err
		}
	}
	ips, err := memberIPs(ctx, rc)
	if err != nil {
		return err
	}
	for _, m := range s.Members {
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if missing(e) || e == nil && tok == nil {
			v, e := secrets.Token()
			if e != nil {
				return e
			}
			if e = rc.Secrets.SetAgentToken(ctx, &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: v, Hash: secrets.Hash(v)}); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		for _, kind := range []string{"members", "agents"} {
			if kind == "members" && rc.Cluster.Spec.TLS.CA == "userProvided" {
				_, e := secret(ctx, rc, "tls/user.crt")
				if e == nil {
					if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/members/"+m.Name+".pending.pem"); e != nil && !missing(e) {
						return e
					}
					continue
				}
				if !missing(e) {
					return e
				}
			}
			base := "tls/" + kind + "/" + m.Name
			existing, e := secret(ctx, rc, base+".pem")
			if e == nil {
				renew, e := pki.NeedsRenewal(existing, 30*24*time.Hour, now(rc))
				if e != nil {
					return e
				}
				if !renew {
					continue
				}
				base += ".pending"
				pending, e := secret(ctx, rc, base+".pem")
				if e == nil {
					renew, e = pki.NeedsRenewal(pending, 30*24*time.Hour, now(rc))
					if e != nil {
						return e
					}
					if !renew {
						continue
					}
				} else if !missing(e) {
					return e
				}
			} else if !missing(e) {
				return e
			}
			cert, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Validity: 365 * 24 * time.Hour, Server: true, Client: kind == "members"})
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".pem", append(cert, key...)); e != nil {
				return e
			}
		}
		if m.ConfigVersion == 0 {
			activateConfig(rc.Cluster, s, m)
		}
	}
	return nil
}
func activateConfig(c *domain.Cluster, s *internalState, m *member) {
	m.ConfigVersion = s.ConfigVersion
	m.TLSMode = c.Spec.TLS.Mode
	m.EngineConfig = c.Spec.EngineConfig
	m.EngineOptions = append(json.RawMessage(nil), c.Spec.EngineOptions...)
	m.DesiredVersion = c.Spec.Version
	m.ConfigResources = runtimeResources(c.Spec.Components.Engine.Resources)
	m.ConfigDisk = c.Spec.Components.Engine.Resources
}
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	v := agentapi.MemberConfig{Engine: "rabbitmq", Cluster: rc.Cluster.Name, Member: m.Name, Role: "rabbitmq", EngineVersion: m.DesiredVersion, EngineConfig: m.EngineConfig, ConfigVersion: m.ConfigVersion, TLS: agentapi.TLSConfig{Mode: m.TLSMode}, SystemUsers: map[string]agentapi.Credentials{}, DesiredProcesses: []string{"rabbitmq-server"}}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return v, err
	}
	v.TLS.CAPEM = string(ca)
	cert, err := memberCertificate(ctx, rc, "tls/members/"+name+".pem")
	if err != nil {
		return v, err
	}
	v.TLS.CertPEM = cert.CertPEM
	v.TLS.KeyPEM = cert.KeyPEM
	v.AgentTLS, err = memberCertificate(ctx, rc, "tls/agents/"+name+".pem")
	if err != nil {
		return v, err
	}
	for _, u := range []string{"karakoram", "admin", "monitor"} {
		key := "users/" + u
		if m.ChangeAction == "password" && m.TargetConfig == u {
			key = m.TargetVersion
		}
		b, e := secret(ctx, rc, key)
		if e != nil {
			return v, e
		}
		v.SystemUsers[u] = agentapi.Credentials{Username: u, Password: string(b)}
	}
	options, err := parseOptions(m.EngineOptions)
	if err != nil {
		return v, err
	}
	cookie, err := secret(ctx, rc, "erlang-cookie")
	if err != nil {
		return v, err
	}
	peers := []string{}
	for _, other := range ordered(s) {
		// Fresh planned peers have no runtime yet. Keep retained identities in
		// discovery so a resumed or replaced broker still finds the old cluster.
		if (!other.Removing || other.Name == name) && (other.Name == name || other.Stage != "planned" || other.ServiceID != "" || other.Resume) {
			peers = append(peers, "rabbit@"+other.Name)
		}
	}
	r := m.ConfigResources
	if r.MinRAM == 0 {
		r = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	}
	spec := rmqspec.MemberSpec{NodeName: "rabbit@" + name, Peers: peers, Cookie: string(cookie), Version: m.DesiredVersion, Plugins: slices.Concat(alwaysEnabledPlugins, options.Plugins), PrometheusPort: 15692, ConfigOverrides: m.EngineConfig, TotalMemoryBytes: int64(r.MinRAM * 1e9), MemoryHighWatermarkBytes: int64(r.MinRAM * 1e9 * 0.6), DiskFreeLimitBytes: int64(max(0.5, r.MinRAM) * 1e9), DefaultQueueType: "quorum", InterNodeTLS: s.InterNodeTLS}
	switch m.TLSMode {
	case "disabled":
		spec.AMQPPort = 5672
		spec.ManagementPort = 15672
	case "preferTLS":
		spec.AMQPPort = 5672
		spec.AMQPSPort = 5671
		spec.ManagementPort = 15672
		spec.ManagementTLSPort = 15671
	default:
		spec.AMQPSPort = 5671
		spec.ManagementTLSPort = 15671
	}
	if slices.Contains(options.Plugins, "rabbitmq_stream") {
		switch m.TLSMode {
		case "disabled":
			spec.StreamPort = 5552
		case "preferTLS":
			spec.StreamPort = 5552
			spec.StreamTLSPort = 5551
		default:
			spec.StreamTLSPort = 5551
		}
	}
	if spec.AMQPSPort > 0 {
		v.Port = spec.AMQPSPort
	} else {
		v.Port = spec.AMQPPort
	}
	v.EngineSpec, err = json.Marshal(spec)
	return v, err
}
func memberCertificate(ctx context.Context, rc *engine.Context, key string) (agentapi.Certificate, error) {
	bundle, err := secret(ctx, rc, key)
	if err != nil {
		return agentapi.Certificate{}, err
	}
	var cert, priv []byte
	for len(bundle) > 0 {
		b, rest := pem.Decode(bundle)
		if b == nil {
			return agentapi.Certificate{}, invalid("invalid certificate bundle")
		}
		if b.Type == "CERTIFICATE" {
			cert = append(cert, pem.EncodeToMemory(b)...)
		} else if strings.HasSuffix(b.Type, "PRIVATE KEY") {
			priv = pem.EncodeToMemory(b)
		}
		bundle = rest
	}
	if _, err = tls.X509KeyPair(cert, priv); err != nil {
		return agentapi.Certificate{}, err
	}
	return agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(priv)}, nil
}
func memberIPs(ctx context.Context, rc *engine.Context) ([]net.IP, error) {
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return nil, err
	}
	ips := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	for _, v := range []string{project.PublicIPv6, project.PublicIPv4} {
		if ip := net.ParseIP(v); ip != nil {
			ips = append(ips, ip)
		}
	}
	return ips, nil
}
func hasPendingCertificate(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	for _, kind := range []string{"members", "agents"} {
		_, err := secret(ctx, rc, "tls/"+kind+"/"+m.Name+".pending.pem")
		if err == nil {
			return true, nil
		}
		if !missing(err) {
			return false, err
		}
	}
	return false, nil
}
func publishRenewedCertificates(ctx context.Context, rc *engine.Context, m *member) error {
	for _, kind := range []string{"members", "agents"} {
		base := "tls/" + kind + "/" + m.Name
		b, err := secret(ctx, rc, base+".pending.pem")
		if missing(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err = putSecret(ctx, rc, base+".pem", b); err != nil {
			return err
		}
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+".pending.pem"); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}
func rotateCertificate(ctx context.Context, rc *engine.Context, m *member) error {
	crt, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return err
	}
	key, err := secret(ctx, rc, "tls/ca.key")
	if err != nil {
		return err
	}
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		return err
	}
	ips, err := memberIPs(ctx, rc)
	if err != nil {
		return err
	}
	for _, kind := range []string{"members", "agents"} {
		cert, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Validity: 365 * 24 * time.Hour, Server: true, Client: kind == "members"})
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/"+kind+"/"+m.Name+".pending.pem", append(cert, key...)); e != nil {
			return e
		}
	}
	return nil
}
func applyProvidedCertificate(ctx context.Context, rc *engine.Context, m *member) error {
	if rc.Cluster.Spec.TLS.CA != "userProvided" {
		return nil
	}
	crt, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	key, err := secret(ctx, rc, "tls/user.key")
	if err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(crt, key)
	if err != nil {
		return invalid("invalid provided certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return invalid("invalid CA")
	}
	intermediates := x509.NewCertPool()
	for _, raw := range pair.Certificate[1:] {
		c, e := x509.ParseCertificate(raw)
		if e != nil {
			return e
		}
		intermediates.AddCert(c)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: m.Name, CurrentTime: now(rc), KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return invalid("provided certificate for %s: %v", m.Name, err)
		}
	}
	if rc.Cluster.Spec.Exposure.Public {
		project, err := rc.Backend.ProjectInfo(ctx)
		if err != nil {
			return err
		}
		ip := project.PublicIPv6
		if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
			ip = project.PublicIPv4
		}
		if leaf.VerifyHostname(ip) != nil {
			return invalid("provided certificate must cover public IP")
		}
	}
	bundle := append(crt, key...)
	if err = putSecret(ctx, rc, "tls/members/"+m.Name+".pem", bundle); err != nil {
		return err
	}
	m.ProvidedCertificateHash = hash(bundle)
	return nil
}

func providedCertificateChanged(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if rc.Cluster.Spec.TLS.CA != "userProvided" {
		return false, nil
	}
	crt, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	key, err := secret(ctx, rc, "tls/user.key")
	if err != nil {
		return false, err
	}
	return hash(append(crt, key...)) != m.ProvidedCertificateHash, nil
}

func providedCertificateCondition(ctx context.Context, rc *engine.Context) (*domain.Condition, error) {
	if rc.Cluster.Spec.TLS.CA != "userProvided" {
		return nil, nil
	}
	crt, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	renew, err := pki.NeedsRenewal(crt, 30*24*time.Hour, now(rc))
	if err != nil {
		return nil, err
	}
	c := &domain.Condition{Type: "TLSCertificate", Status: "True", Reason: "Valid", LastTransitionTime: now(rc)}
	if renew {
		c.Status = "False"
		c.Reason = "ExpiringSoon"
		c.Message = "Replace the supplied certificate and private key before expiry."
	}
	expired, err := pki.NeedsRenewal(crt, 0, now(rc))
	if err != nil {
		return nil, err
	}
	if expired {
		c.Status = "False"
		c.Reason = "Expired"
		c.Message = "The supplied certificate has expired; replace it and its private key."
	}
	return c, nil
}

var _ engine.AgentConfigSource = (*Provider)(nil)
