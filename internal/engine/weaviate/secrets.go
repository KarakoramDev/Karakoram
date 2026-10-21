// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net"
	"time"
)

const (
	AdminUser             = "karakoram-admin"
	ClusterUser           = "karakoram-cluster"
	secretCA              = "tls/ca.crt"
	secretCAKey           = "tls/ca.key"
	secretAdmin           = "users/karakoram-admin"
	secretClusterPassword = "users/karakoram-cluster"
)

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
}
func putSecret(ctx context.Context, rc *engine.Context, key string, b []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, b)
}
func (p *Provider) ensureSecrets(ctx context.Context, rc *engine.Context, s *state) error {
	crt, err := secret(ctx, rc, secretCA)
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return invalid("userProvided CA requires tls/ca.crt and tls/ca.key")
		}
		key, e := secret(ctx, rc, secretCAKey)
		if e == nil && len(key) > 0 {
			return fmt.Errorf("cluster CA signing key exists without its certificate; restore the matching CA certificate")
		}
		if e != nil && !missing(e) {
			return e
		}
		ca, e := pki.NewCA(rc.Cluster.Name, 10*365*24*time.Hour)
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
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		return err
	}
	for _, k := range []string{secretAdmin, secretClusterPassword} {
		_, e := secret(ctx, rc, k)
		if missing(e) {
			v, e := secrets.Password()
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, k, []byte(v)); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
	}
	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	if rc.Cluster.Spec.Exposure.Public {
		project, e := rc.Backend.ProjectInfo(ctx)
		if e != nil {
			return e
		}
		for _, v := range []string{project.PublicIPv4, project.PublicIPv6} {
			if ip := net.ParseIP(v); ip != nil {
				ips = append(ips, ip)
			}
		}
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
		for _, kind := range []string{"agents", "members"} {
			base := "tls/" + kind + "/" + m.Name
			old, e := secret(ctx, rc, base+".crt")
			renew := missing(e)
			if e != nil && !renew {
				return e
			}
			if !renew {
				renew, e = pki.NeedsRenewal(old, 30*24*time.Hour, now(rc))
				if e != nil {
					return e
				}
				block, _ := pem.Decode(old)
				if block == nil {
					return fmt.Errorf("invalid member certificate")
				}
				leaf, e := x509.ParseCertificate(block.Bytes)
				if e != nil {
					return e
				}
				for _, ip := range ips {
					renew = renew || leaf.VerifyHostname(ip.String()) != nil
				}
			}
			if !renew {
				continue
			}
			crt, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Server: true})
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".key", key); e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".crt", crt); e != nil {
				return e
			}
		}
	}
	return nil
}
func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	for _, key := range []string{"config/" + name, "target/" + name, "tls/agents/" + name + ".crt", "tls/agents/" + name + ".key", "tls/members/" + name + ".crt", "tls/members/" + name + ".key"} {
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return err
		}
	}
	return rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name)
}
