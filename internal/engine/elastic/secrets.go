// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"crypto/subtle"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const (
	secretCA                 = "tls/ca.crt"
	secretCAKey              = "tls/ca.key"
	secretAdmin              = "users/karakoram"
	secretMonitor            = "users/karakoram-monitor"
	secretElastic            = "users/elastic"
	secretLogstash           = "users/karakoram_logstash"
	secretAPM                = "users/karakoram_apm"
	secretKibanaToken        = "kibana/token"
	secretKibanaTokenName    = "kibana/token-name"
	secretKibanaSecurity     = "kibana/keys/security"
	secretKibanaSavedObjects = "kibana/keys/savedObjects"
	secretKibanaReporting    = "kibana/keys/reporting"
	secretAPMToken           = "apm/secret-token"
	secretFileRealm          = "filerealm/users"
)

var clusterSecretKeys = []string{secretCA, secretCAKey, secretAdmin, secretMonitor, secretElastic, secretLogstash, secretAPM, secretKibanaToken, secretKibanaTokenName, secretKibanaSecurity, secretKibanaSavedObjects, secretKibanaReporting, secretAPMToken, secretFileRealm}

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func now(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
}
func putSecret(ctx context.Context, rc *engine.Context, key string, b []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, b)
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
func (p *Provider) ensureSecrets(ctx context.Context, rc *engine.Context, s *state) error {
	crt, err := secret(ctx, rc, secretCA)
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return invalid("userProvided CA requires a certificate and signing key")
		}
		ca, e := NewCA(rc.Cluster.Name, 10*365*24*time.Hour)
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, secretCAKey, ca.KeyPEM); e != nil {
			return e
		}
		if e = putSecret(ctx, rc, secretCA, ca.CertPEM); e != nil {
			return e
		}
		crt = ca.CertPEM
	} else if err != nil {
		return err
	}
	key, err := secret(ctx, rc, secretCAKey)
	if err != nil {
		return err
	}
	ca, err := LoadCA(crt, key)
	if err != nil {
		return err
	}
	for _, key := range []string{secretAdmin, secretMonitor, secretElastic, secretLogstash, secretAPM, secretAPMToken} {
		if err := ensureSecret(ctx, rc, key, secrets.Password); err != nil {
			return err
		}
	}
	for _, key := range []string{secretKibanaSecurity, secretKibanaSavedObjects, secretKibanaReporting} {
		if err := ensureSecret(ctx, rc, key, func() (string, error) { return secrets.RandomString(64) }); err != nil {
			return err
		}
	}
	for _, m := range s.Members {
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if missing(err) || err == nil && tok == nil {
			v, e := secrets.Token()
			if e != nil {
				return e
			}
			if e = rc.Secrets.SetAgentToken(ctx, &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: v, Hash: secrets.Hash(v)}); e != nil {
				return e
			}
		} else if err != nil {
			return err
		}
		base := "tls/agents/" + m.Name
		crt, e := secret(ctx, rc, base+".crt")
		renew := missing(e)
		if e != nil && !renew {
			return e
		}
		if !renew {
			renew, e = NeedsRenewal(crt, 30*24*time.Hour, now(rc))
			if e != nil {
				return e
			}
		}
		if renew {
			crt, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Validity: 365 * 24 * time.Hour, Server: true})
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".key", key); e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".crt", crt); e != nil {
				return e
			}
			p.bumpConfig(s)
		}
		if isES(m) {
			if err := p.issueLeafs(ctx, rc, s, m, false); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *Provider) issueLeafs(ctx context.Context, rc *engine.Context, s *state, m *member, force bool) error {
	crt, err := secret(ctx, rc, secretCA)
	if err != nil {
		return err
	}
	key, err := secret(ctx, rc, secretCAKey)
	if err != nil {
		return err
	}
	ca, err := LoadCA(crt, key)
	if err != nil {
		return err
	}
	ips := []net.IP{}
	if rc.Cluster.Spec.Exposure.Public {
		project, e := rc.Backend.ProjectInfo(ctx)
		if e != nil {
			return e
		}
		for _, address := range []string{project.PublicIPv4, project.PublicIPv6} {
			if ip := net.ParseIP(address); ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	for _, kind := range []string{"transport", "http"} {
		base := "tls/nodes/" + m.Name + "/" + kind
		old, e := secret(ctx, rc, base+".crt")
		renew := missing(e) || force
		if e != nil && !missing(e) {
			return e
		}
		if !renew {
			renew, e = NeedsRenewal(old, 30*24*time.Hour, now(rc))
			if e != nil {
				return e
			}
			if kind == "http" {
				block, _ := pem.Decode(old)
				if block == nil {
					return errors.New("invalid node certificate")
				}
				leaf, e := x509.ParseCertificate(block.Bytes)
				if e != nil {
					return e
				}
				for _, ip := range ips {
					renew = renew || leaf.VerifyHostname(ip.String()) != nil
				}
			}
		}
		if !renew {
			continue
		}
		var crt, key []byte
		if kind == "transport" {
			crt, key, e = IssueTransport(ca, m.Name, nil)
		} else {
			crt, key, e = IssueHTTP(ca, m.Name, nil, ips)
		}
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, base+".key", key); e != nil {
			return e
		}
		if e = putSecret(ctx, rc, base+".crt", crt); e != nil {
			return e
		}
		p.bumpConfig(s)
	}
	return nil
}
func ownershipEnv(cluster, member, token string) map[string]string {
	return map[string]string{"KARAKORAM_CLUSTER": cluster, "KARAKORAM_MEMBER": member, "KARAKORAM_AGENT_TOKEN": token}
}
func verifyMemberServices(ctx context.Context, rc *engine.Context, m *member) error {
	var runtimeOwnership map[string]string
	for _, volume := range []bool{false, true} {
		name := m.Name
		recorded := m.ServiceID
		if volume {
			name = m.Volume
			recorded = m.VolumeID
			if m.VolumeOrphaned {
				continue
			}
		}
		if name == "" {
			continue
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		if service == nil {
			continue
		}
		verified := recorded != "" && recorded == service.ID
		if !verified && volume && rc.Store != nil {
			retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
			if e != nil {
				return e
			}
			for _, v := range retained {
				verified = verified || v.Member == m.Name && v.Hostname == name && v.ServiceID != "" && v.ServiceID == service.ID
			}
		}
		if !verified {
			tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return e
			}
			env := service.Ownership
			verified = tok != nil && tok.Token != "" && env["KARAKORAM_CLUSTER"] == rc.Cluster.Name && env["KARAKORAM_MEMBER"] == m.Name && subtle.ConstantTimeCompare([]byte(env["KARAKORAM_AGENT_TOKEN"]), []byte(tok.Token)) == 1
			// Local-storage services may have no environment after a partial
			// import. Only the token-verified runtime's exact mount is evidence.
			if !verified && volume && runtimeOwnership["KARAKORAM_DATA_DIR"] == "/mnt/"+name {
				env = runtimeOwnership
				verified = tok != nil && tok.Token != "" && env["KARAKORAM_CLUSTER"] == rc.Cluster.Name && env["KARAKORAM_MEMBER"] == m.Name && subtle.ConstantTimeCompare([]byte(env["KARAKORAM_AGENT_TOKEN"]), []byte(tok.Token)) == 1
			}
		}
		if !verified {
			return fmt.Errorf("%w: service %s has no verified cluster ownership", engine.ErrConflict, name)
		}
		if volume {
			m.VolumeID = service.ID
		} else {
			m.ServiceID = service.ID
			runtimeOwnership = service.Ownership
		}
	}
	return nil
}
func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return err
	}
	keys := []string{"tls/agents/" + name + ".crt", "tls/agents/" + name + ".key", "members/" + name + "/restart-spec"}
	for _, kind := range []string{"transport", "http"} {
		for _, ext := range []string{"crt", "key"} {
			keys = append(keys, "tls/nodes/"+name+"/"+kind+"."+ext)
		}
	}
	for _, key := range keys {
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}
