// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
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

var clusterSecretKeys = []string{secretCA, secretCAKey, secretAdmin, secretMonitor, secretElastic, secretLogstash, secretAPM, secretKibanaToken, secretKibanaTokenName, secretKibanaSecurity, secretKibanaSavedObjects, secretKibanaReporting, secretAPMToken, secretFileRealm, secretDataAccessRead, secretDataAccessReadWrite}

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
		if !force && len(old) > 0 {
			urgent, err := NeedsRenewal(old, 7*24*time.Hour, now(rc))
			if err != nil {
				return err
			}
			if !urgent {
				if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
					continue
				}
			}
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

// verifyMemberServices records proven identities, then backfills non-sensitive
// markers only after both runtime and storage have passed ownership checks.
func verifyMemberServices(ctx context.Context, rc *engine.Context, m *member) error {
	var runtime *infra.Service
	var verifiedServices [2]*infra.Service
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
			if !m.RuntimeRecovery.VolumeLost || !engine.ConfirmedVolumeLoss(err, m.Volume, m.VolumeID) {
				return err
			}
			if service != nil && (recorded == "" || service.ID != recorded) {
				return engine.UnverifiedOwnership(service)
			}
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
			verified = tok != nil && engine.HasOwnership(service, tok.Token, rc.Cluster.Name, m.Name)
			// Local Storage may have no environment after a partial import.
			// A recorded runtime ID alone cannot authorize a new volume;
			// its marker or readable legacy secret and exact mount must match.
			if !verified && volume && tok != nil && engine.HasOwnership(runtime, tok.Token, rc.Cluster.Name, m.Name) && engine.MountsVolume(rc.Backend, runtime, name) {
				verified = true
			}
		}
		if !verified {
			return engine.UnverifiedOwnership(service)
		}
		if volume {
			m.VolumeID = service.ID
			verifiedServices[1] = service
		} else {
			m.ServiceID = service.ID
			runtime = service
			verifiedServices[0] = service
		}
	}
	// Writing the runtime marker before checking storage would let a recorded
	// runtime ID become new evidence for an otherwise unverifiable volume.
	// Services of a cluster being deleted are left alone, and a member without
	// a stored agent token has nothing to derive a marker from.
	if (verifiedServices[0] == nil && verifiedServices[1] == nil) || rc.Cluster.Deleting || m.RuntimeRecovery.VolumeLost {
		return nil
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if missing(err) {
		return nil
	}
	if err != nil || tok == nil {
		return err
	}
	for _, service := range verifiedServices {
		if service == nil {
			continue
		}
		if err := engine.EnsureOwnerMarker(ctx, rc.Backend, service, tok.Token, rc.Cluster.Name, m.Name); err != nil {
			return err
		}
	}
	return nil
}

// adoptImported records identities proven by this member's own import and
// reports whether all its services exist, including component runtimes.
func adoptImported(ctx context.Context, rc *engine.Context, m *member, op infra.Op, st infra.OpState) (bool, error) {
	complete := true
	for _, volume := range []bool{false, true} {
		name, recorded := m.Name, &m.ServiceID
		if volume {
			if m.Volume == "" || m.VolumeOrphaned {
				continue
			}
			name, recorded = m.Volume, &m.VolumeID
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return false, err
		}
		if service == nil || service.Importing {
			complete = false
			continue
		}
		if *recorded == "" {
			*recorded = engine.ImportedServiceID(op, st, service)
		}
	}
	return complete, nil
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
