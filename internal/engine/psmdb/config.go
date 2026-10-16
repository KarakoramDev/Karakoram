// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const pbmVersion = "2.16.0-1.noble"

// PrepareCommands returns the installation commands used by member deployments
// for the requested Percona Server for MongoDB version.
func PrepareCommands(version string) []string {
	series := strings.ReplaceAll(major(version), ".", "")
	shell := "2.10.0.noble"
	if series == "60" {
		shell = "2.8.3.noble"
	}
	return []string{"sudo apt-get update", "sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release", "curl -fsSL https://repo.percona.com/apt/percona-release_latest.generic_all.deb -o /tmp/percona-release.deb", "sudo dpkg -i /tmp/percona-release.deb", "sudo percona-release enable psmdb-" + series + " release --scheme https", "sudo percona-release enable pbm release --scheme https", "sudo apt-get update", fmt.Sprintf("sudo PERCONA_TELEMETRY_DISABLE=1 apt-get install -y --no-install-recommends percona-server-mongodb-server=%[1]s.noble percona-server-mongodb-mongos=%[1]s.noble percona-server-mongodb-tools=%[1]s.noble percona-mongodb-mongosh=%[2]s percona-backup-mongodb=%[3]s", version, shell, pbmVersion)}
}

func (p *Provider) runtimeSpec(rc *engine.Context, m *member, version string) infra.RuntimeSpec {
	c := rc.Cluster
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	prepare := PrepareCommands(version)
	return (engine.AgentRuntime{
		Cluster: c.Name, Member: m.Name, Volume: m.Volume,
		AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM,
		Ports: []infra.Port{{Port: 27017, Protocol: "tcp"}}, Prepare: prepare,
		Env: map[string]string{"PERCONA_TELEMETRY_DISABLE": "1"},
	}).Spec(rc.Backend.Layout())
}
func runtimeResources(r domain.Resources) domain.Resources { r.MinDisk = 1; r.MaxDisk = 3; return r }
func serviceSpec(layout infra.Layout, c *domain.Cluster, m *member, token string) infra.ServiceSpec {
	comp := component(c.Spec, m)
	containers := 1
	if m.Role == agentapi.RoleRouter {
		containers = comp.Replicas
	}
	return engine.RuntimeServiceSpec(layout, engine.MemberService{
		Cluster: c.Name, Member: m.Name, Token: token, Volume: m.Volume,
		Resources: runtimeResources(comp.Resources), Containers: containers,
	})
}

func requireRouterScaling(backend infra.Backend, containers int) error {
	if containers > 1 && !backend.Capabilities().HorizontalScaling {
		return fmt.Errorf("%w: backend %s cannot scale mongos beyond one container", engine.ErrNotSupported, backend.Name())
	}
	return nil
}

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
}
func putSecret(ctx context.Context, rc *engine.Context, key string, value []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, value)
}
func ensureSecret(ctx context.Context, rc *engine.Context, key string, generate func() (string, error)) error {
	_, e := secret(ctx, rc, key)
	if e == nil {
		return nil
	}
	if !missing(e) {
		return e
	}
	v, e := generate()
	if e != nil {
		return e
	}
	return putSecret(ctx, rc, key, []byte(v))
}
func (p *Provider) ensureSecrets(ctx context.Context, rc *engine.Context, s *internalState) error {
	crt, e := secret(ctx, rc, "tls/ca.crt")
	if missing(e) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return invalid("userProvided requires tls/ca.crt and tls/ca.key secrets")
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
	} else if e != nil {
		return e
	}
	key, e := secret(ctx, rc, "tls/ca.key")
	if e != nil {
		return e
	}
	ca, e := pki.LoadCA(crt, key)
	if e != nil {
		return e
	}
	if e = ensureSecret(ctx, rc, "keyfile", secrets.Keyfile); e != nil {
		return e
	}
	ips, err := memberIPs(ctx, rc)
	if err != nil {
		return err
	}
	for _, u := range users.SystemUserDefs() {
		if e = ensureSecret(ctx, rc, "users/"+u.Name, secrets.Password); e != nil {
			return e
		}
	}
	providedLeaf := false
	if rc.Cluster.Spec.TLS.CA == "userProvided" {
		_, err := secret(ctx, rc, "tls/user.crt")
		if err != nil && !missing(err) {
			return err
		}
		providedLeaf = err == nil
	}
	for _, m := range s.Members {
		newConfig := m.ConfigVersion == 0
		for _, user := range []string{"backup", "clusterMonitor"} {
			if err := ensureSecret(ctx, rc, "members/"+m.Name+"/users/"+user, func() (string, error) {
				pw, err := secret(ctx, rc, "users/"+user)
				return string(pw), err
			}); err != nil {
				return err
			}
		}
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if missing(e) || e == nil && tok == nil {
			v, e := secrets.Token()
			if e != nil {
				return e
			}
			if e = rc.Secrets.SetAgentToken(ctx, &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hash: secrets.Hash(v), Token: v}); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		for _, kind := range []string{"members", "agents"} {
			base := "tls/" + kind + "/" + m.Name
			if kind == "members" && providedLeaf {
				// Supplied leaves belong to the operator, not the auto-renewer.
				if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+".pending.pem"); err != nil && !missing(err) {
					return err
				}
				continue
			}
			existing, e := secret(ctx, rc, base+".pem")
			renewing := e == nil
			if e == nil {
				renew, err := pki.NeedsRenewal(existing, 30*24*time.Hour, now(rc))
				if err != nil {
					return err
				}
				if !renew {
					continue
				}
				// Agents read active secrets independently of ConfigVersion. Keep
				// renewals private until this member passes the disruption gate.
				pending, err := secret(ctx, rc, base+".pending.pem")
				if err == nil {
					renew, err = pki.NeedsRenewal(pending, 30*24*time.Hour, now(rc))
					if err != nil {
						return err
					}
					if !renew {
						continue
					}
				} else if !missing(err) {
					return err
				}
				base += ".pending"
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
			if renewing {
				s.ConfigVersion++
			}
		}
		if newConfig {
			if err := applyProvidedCertificate(ctx, rc, m); err != nil {
				return err
			}
			activateConfig(rc.Cluster, s, m)
		}
	}
	return nil
}
func passwords(ctx context.Context, rc *engine.Context) (map[string]string, error) {
	out := map[string]string{}
	for _, u := range users.SystemUserDefs() {
		b, e := secret(ctx, rc, "users/"+u.Name)
		if e != nil {
			return nil, e
		}
		out[u.Name] = string(b)
	}
	return out, nil
}

// MemberConfig renders the precise per-member secret-bearing desired agent state.
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	get := func(key string) (string, error) { b, e := secret(ctx, rc, key); return string(b), e }
	v := agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: name, Role: m.Role, ReplSetName: m.Set, Port: 27017, EngineVersion: m.DesiredVersion, EngineConfig: m.EngineConfig, ConfigVersion: m.ConfigVersion, SystemUsers: map[string]agentapi.Credentials{}, TLS: agentapi.TLSConfig{Mode: m.TLSMode}}
	if v.EngineVersion == "" {
		v.EngineVersion = rc.Cluster.Spec.Version
	}
	if v.Keyfile, e = get("keyfile"); e != nil {
		return v, e
	}
	if v.TLS.CAPEM, e = get("tls/ca.crt"); e != nil {
		return v, e
	}
	cert, e := memberCertificate(ctx, rc, "tls/members/"+name+".pem")
	if e != nil {
		return v, e
	}
	v.TLS.CertPEM, v.TLS.KeyPEM = cert.CertPEM, cert.KeyPEM
	if v.AgentTLS, e = memberCertificate(ctx, rc, "tls/agents/"+name+".pem"); e != nil {
		return v, e
	}
	for _, u := range []string{"backup", "clusterMonitor"} {
		pw, e := get("members/" + name + "/users/" + u)
		if e != nil {
			return v, e
		}
		v.SystemUsers[u] = agentapi.Credentials{Username: u, Password: pw, Database: "admin"}
	}
	v.PBMEnabled = m.Role != agentapi.RoleRouter && s.Sets[m.Set] != nil && s.Sets[m.Set].Stage == "ready"
	process := "mongod"
	if m.Role == agentapi.RoleRouter {
		process = "mongos"
		v.ReplSetName = ""
		v.ConfigDB = m.ConfigDB
	}
	v.DesiredProcesses = []string{process}
	if v.PBMEnabled {
		v.DesiredProcesses = append(v.DesiredProcesses, "pbm-agent")
	}
	v.ShardServer = m.Role == agentapi.RoleReplicaSet && rc.Cluster.Spec.Topology.Type == "sharded"
	v.WiredTigerCacheSizeGB = m.CacheGB
	var restore struct {
		Maintenance map[string]bool     `json:"pbmMaintenance"`
		Stopped     map[string][]string `json:"pbmStopped"`
	}
	if e = decodeJSON(rc.Cluster.Internal, &restore); e != nil {
		return v, e
	}
	v.Maintenance = restore.Maintenance[name]
	processes := make([]string, 0, len(v.DesiredProcesses))
	for _, process := range v.DesiredProcesses {
		held := false
		for _, stopped := range restore.Stopped[name] {
			held = held || stopped == process
		}
		if !held {
			processes = append(processes, process)
		}
	}
	v.DesiredProcesses = processes
	v.SlowOpThresholdMS = 100
	return v, nil
}
func hosts(ms []*member) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Name+":27017")
	}
	return out
}

func activateConfig(c *domain.Cluster, s *internalState, m *member) {
	m.ConfigVersion = s.ConfigVersion
	m.TLSMode = s.TLSMode
	m.EngineConfig = c.Spec.EngineConfig
	m.CacheGB = max(0.25, 0.5*(component(c.Spec, m).Resources.MaxRAM-0.5))
	if m.Role == agentapi.RoleRouter {
		m.ConfigDB = "cfg/" + strings.Join(hosts(s.setMembers("cfg")), ",")
	}
}

// publishRenewedCertificates is called only inside the gated rolling reload.
func publishRenewedCertificates(ctx context.Context, rc *engine.Context, m *member) error {
	for _, kind := range []string{"members", "agents"} {
		base := "tls/" + kind + "/" + m.Name
		bundle, err := secret(ctx, rc, base+".pending.pem")
		if missing(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err = putSecret(ctx, rc, base+".pem", bundle); err != nil {
			return err
		}
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+".pending.pem"); err != nil && !missing(err) {
			return err
		}
	}
	return nil
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

func providedCertificateCondition(ctx context.Context, rc *engine.Context) (*domain.Condition, error) {
	if rc.Cluster.Spec.TLS.CA != "userProvided" {
		return nil, nil
	}
	cert, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	expiring, err := pki.NeedsRenewal(cert, 30*24*time.Hour, now(rc))
	if err != nil {
		return nil, err
	}
	condition := &domain.Condition{Type: "TLSCertificate", Status: "True", Reason: "Valid", LastTransitionTime: now(rc)}
	if expiring {
		condition.Status = "False"
		condition.Reason = "ExpiringSoon"
		condition.Message = "Provided member certificate expires within 30 days; replace the supplied certificate and private key."
		expired, err := pki.NeedsRenewal(cert, 0, now(rc))
		if err != nil {
			return nil, err
		}
		if expired {
			condition.Reason = "Expired"
			condition.Message = "Provided member certificate has expired; replace the supplied certificate and private key."
		}
	}
	return condition, nil
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
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Validity: 365 * 24 * time.Hour, Server: true, Client: true})
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, "tls/members/"+m.Name+".pem", append(cert, key...))
}

func memberCertificate(ctx context.Context, rc *engine.Context, key string) (agentapi.Certificate, error) {
	bundle, err := secret(ctx, rc, key)
	if err != nil {
		return agentapi.Certificate{}, err
	}
	var certificate, privateKey []byte
	for len(bundle) > 0 {
		block, rest := pem.Decode(bundle)
		if block == nil {
			return agentapi.Certificate{}, invalid("invalid certificate bundle")
		}
		if block.Type == "CERTIFICATE" {
			certificate = append(certificate, pem.EncodeToMemory(block)...)
		} else if strings.HasSuffix(block.Type, "PRIVATE KEY") {
			privateKey = pem.EncodeToMemory(block)
		}
		bundle = rest
	}
	if _, err = tls.X509KeyPair(certificate, privateKey); err != nil {
		return agentapi.Certificate{}, err
	}
	return agentapi.Certificate{CertPEM: string(certificate), KeyPEM: string(privateKey)}, nil
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
		return invalid("invalid provided server certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || leaf.VerifyHostname(m.Name) != nil {
		return invalid("provided certificate does not cover member %s", m.Name)
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return invalid("invalid provided CA")
	}
	intermediates := x509.NewCertPool()
	for _, raw := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return invalid("invalid provided intermediate certificate")
		}
		intermediates.AddCert(cert)
	}
	if err = leaf.VerifyHostname("127.0.0.1"); err != nil {
		return invalid("provided certificate must cover localhost IP 127.0.0.1 for PBM")
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
		if err = leaf.VerifyHostname(ip); err != nil {
			return invalid("provided certificate must cover public IP %s", ip)
		}
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: m.Name, KeyUsages: []x509.ExtKeyUsage{usage}, CurrentTime: now(rc)}); err != nil {
			return invalid("provided member certificate verification: %v", err)
		}
	}
	return putSecret(ctx, rc, "tls/members/"+m.Name+".pem", append(crt, key...))
}

func memberIPs(ctx context.Context, rc *engine.Context) ([]net.IP, error) {
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return nil, err
	}
	ips := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	for _, address := range []string{project.PublicIPv6, project.PublicIPv4} {
		if ip := net.ParseIP(address); ip != nil {
			ips = append(ips, ip)
		}
	}
	return ips, nil
}
