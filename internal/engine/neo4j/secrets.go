// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net"
	"time"
)

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func now(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
func secret(ctx context.Context, rc *engine.Context, k string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), k)
}
func putSecret(ctx context.Context, rc *engine.Context, k string, b []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), k, b)
}
func ensureSecrets(ctx context.Context, rc *engine.Context, s *state) error {
	if _, err := secret(ctx, rc, "tls/ca.crt"); missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return engine.Invalid("userProvided CA requires a certificate and signing key")
		}
		ca, e := pki.NewCA(rc.Cluster.Name, 0)
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/ca.key", ca.KeyPEM); e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/ca.crt", ca.CertPEM); e != nil {
			return e
		}
	} else if err != nil {
		return err
	}
	if _, err := secret(ctx, rc, "users/neo4j"); missing(err) {
		pw, e := secrets.Password()
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "users/neo4j", []byte(pw)); e != nil {
			return e
		}
	} else if err != nil {
		return err
	}
	m := s.Member
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
	return ensureCertificates(ctx, rc, s, false)
}
func ensureCertificates(ctx context.Context, rc *engine.Context, s *state, force bool) error {
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
	dns := []string{s.Member.Name, "localhost"}
	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	if rc.Cluster.Spec.Exposure.Public {
		project, e := rc.Backend.ProjectInfo(ctx)
		if e != nil {
			return e
		}
		for _, h := range []string{project.PublicIPv4, project.PublicIPv6} {
			if h == "" {
				continue
			}
			if ip := net.ParseIP(h); ip != nil {
				ips = append(ips, ip)
			} else {
				dns = append(dns, h)
			}
		}
	}
	for _, kind := range []string{"servers", "agents"} {
		base := "tls/" + kind + "/" + s.Member.Name
		active, e := secret(ctx, rc, base+".crt")
		renew := missing(e) || force
		if e != nil && !missing(e) {
			return e
		}
		if !renew {
			renew, e = pki.NeedsRenewal(active, 30*24*time.Hour, now(rc))
			if e != nil {
				return e
			}
			block, _ := pem.Decode(active)
			if block == nil {
				return engine.Invalid("invalid member certificate")
			}
			cert, e := x509.ParseCertificate(block.Bytes)
			if e != nil {
				return e
			}
			for _, h := range dns {
				renew = renew || cert.VerifyHostname(h) != nil
			}
			for _, ip := range ips {
				renew = renew || cert.VerifyHostname(ip.String()) != nil
			}
		}
		if !renew {
			continue
		}
		if kind == "servers" && !force && len(active) > 0 {
			urgent, e := pki.NeedsRenewal(active, 7*24*time.Hour, now(rc))
			if e != nil {
				return e
			}
			if !urgent && rc.WaitForWindow().RequeueAfter > 0 {
				continue
			}
		}
		c, k, e := ca.Issue(pki.CertRequest{CommonName: s.Member.Name, DNSNames: dns, IPs: ips, Server: true})
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, base+".key", k); e != nil {
			return e
		}
		if e = putSecret(ctx, rc, base+".crt", c); e != nil {
			return e
		}
		s.ConfigVersion++
	}
	return nil
}
