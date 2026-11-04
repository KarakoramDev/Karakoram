// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net"
	"time"
)

var systemNames = []string{"root", "admin", "kk_agent", "kk_repl", "kk_monitor", "kk_backup", "kk_proxy_admin"}

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
func (*Core) ensureSecrets(ctx context.Context, rc *engine.Context, s *state) error {
	crt, err := secret(ctx, rc, "tls/ca.crt")
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return engine.Invalid("userProvided CA requires certificate and signing key")
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
	if rc.Cluster.Spec.Provider == "pxc" {
		if err := ensureGaleraTrafficCertificate(ctx, rc, s); err != nil {
			return err
		}
	}
	for _, name := range systemNames {
		if _, e := secret(ctx, rc, "users/"+name); missing(e) {
			// START GROUP_REPLICATION accepts at most 32 recovery-password
			// characters (ER_GROUP_REPLICATION_PASSWORD_LENGTH, MySQL 3972).
			// Generate the account credential at that length; never truncate
			// an existing credential independently of its native SQL account.
			length := 40
			if rc.Cluster.Spec.Provider == "mysql" && name == "kk_repl" {
				length = 32
			}
			value, e := secrets.RandomString(length)
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, "users/"+name, []byte(value)); e != nil {
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
		token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if missing(e) || e == nil && token == nil {
			value, e := secrets.Token()
			if e != nil {
				return e
			}
			if e = rc.Secrets.SetAgentToken(ctx, &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: value, Hash: secrets.Hash(value)}); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		for _, kind := range []string{"agents", "servers"} {
			base := "tls/" + kind + "/" + m.Name
			active, e := secret(ctx, rc, base+".crt")
			renew := missing(e)
			if e != nil && !renew {
				return e
			}
			if !renew {
				renew, e = pki.NeedsRenewal(active, 30*24*time.Hour, now(rc))
				if e != nil {
					return e
				}
			}
			if !renew {
				continue
			}
			if kind == "servers" && len(active) > 0 {
				urgent, e := pki.NeedsRenewal(active, 7*24*time.Hour, now(rc))
				if e != nil {
					return e
				}
				if !urgent && rc.WaitForWindow().RequeueAfter > 0 {
					continue
				}
			}
			cert, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Server: true, Client: kind == "servers"})
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".key", key); e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base+".crt", cert); e != nil {
				return e
			}
			publishRevision(s)
		}
	}
	return nil
}
