// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var systemNames = []string{"postgres", "kk_replicator", "kk_rewind", "kk_monitor", "kk_admin", "kk_proxy_admin"}

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

func (p *Core) ensureSecrets(ctx context.Context, rc *engine.Context, s *state) error {
	_, err := secret(ctx, rc, "tls/ca.crt")
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return engine.Invalid("userProvided CA requires certificate and signing key")
		}
		ca, err := pki.NewCA(rc.Cluster.Name, 0)
		if err != nil {
			return err
		}
		if err := putSecret(ctx, rc, "tls/ca.key", ca.KeyPEM); err != nil {
			return err
		}
		if err := putSecret(ctx, rc, "tls/ca.crt", ca.CertPEM); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	for _, name := range systemNames {
		if _, err := secret(ctx, rc, "users/"+name); missing(err) {
			v, err := secrets.Password()
			if err != nil {
				return err
			}
			if err := putSecret(ctx, rc, "users/"+name, []byte(v)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	for _, m := range s.Members {
		token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if missing(err) || err == nil && token == nil {
			value, err := secrets.Token()
			if err != nil {
				return err
			}
			if err := rc.Secrets.SetAgentToken(ctx, &domain.AgentToken{
				Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
				Member: m.Name, Token: value, Hash: secrets.Hash(value),
			}); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return ensureCertificates(ctx, rc, s, false)
}

// ensureCertificates publishes hot-swappable agent identities independently of
// member health. Database leaves remain staged until their rolling reload.
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
	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	if rc.Cluster.Spec.Exposure.Public {
		project, err := rc.Backend.ProjectInfo(ctx)
		if err != nil {
			return err
		}
		for _, address := range []string{project.PublicIPv4, project.PublicIPv6} {
			if ip := net.ParseIP(address); ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	for _, m := range s.Members {
		for _, kind := range []string{"agents", "servers"} {
			base := "tls/" + kind + "/" + m.Name
			active, err := secret(ctx, rc, base+".crt")
			initial := missing(err)
			if err != nil && !initial {
				return err
			}
			if kind == "agents" {
				// Older releases staged this transport behind a live health gate.
				// Discard that journal: no database process uses this identity.
				for _, suffix := range []string{".pending.crt", ".pending.key"} {
					if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+suffix); err != nil && !missing(err) {
						return err
					}
				}
				renew := initial || force
				if !renew {
					renew, err = pki.NeedsRenewal(active, 30*24*time.Hour, now(rc))
					if err != nil {
						return err
					}
				}
				if !renew {
					continue
				}
				certPEM, keyPEM, err := ca.Issue(pki.CertRequest{
					CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Server: true,
					Now: now(rc),
				})
				if err != nil {
					return err
				}
				if err := putSecret(ctx, rc, base+".key", keyPEM); err != nil {
					return err
				}
				if err := putSecret(ctx, rc, base+".crt", certPEM); err != nil {
					return err
				}
				continue
			}
			pending, err := secret(ctx, rc, base+".pending.crt")
			if err != nil && !missing(err) {
				return err
			}
			if len(pending) > 0 {
				unusable, err := pki.NeedsRenewal(pending, 0, now(rc))
				if err != nil {
					return err
				}
				if unusable {
					for _, suffix := range []string{".pending.crt", ".pending.key"} {
						if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+suffix); err != nil && !missing(err) {
							return err
						}
					}
					pending = nil
				}
			}
			if len(pending) > 0 && !m.TLSPending {
				// Secret writes may have committed before the cluster state.
				// Recover the activation journal and publish a rolling revision
				// instead of treating an existing pending pair as completed work.
				pendingKey, err := secret(ctx, rc, base+".pending.key")
				if err != nil {
					return err
				}
				if _, err := tls.X509KeyPair(pending, pendingKey); err != nil {
					return err
				}
				m.TLSPending = true
				s.ConfigVersion++
			}
			var certPEM, keyPEM []byte
			if kind == "servers" && rc.Cluster.Spec.TLS.CA == "userProvided" {
				certPEM, keyPEM, err = suppliedCertificate(ctx, rc, m, ips)
				if err != nil {
					return err
				}
			}
			if len(certPEM) > 0 {
				if bytes.Equal(certPEM, active) || bytes.Equal(certPEM, pending) {
					continue
				}
			} else {
				if len(pending) > 0 {
					continue
				}
				renew := initial || force
				if !renew {
					renew, err = pki.NeedsRenewal(active, 30*24*time.Hour, now(rc))
					if err != nil {
						return err
					}
					activeKey, err := secret(ctx, rc, base+".key")
					if err != nil {
						return err
					}
					pair, err := tls.X509KeyPair(active, activeKey)
					if err != nil {
						return err
					}
					leaf, err := x509.ParseCertificate(pair.Certificate[0])
					if err != nil {
						return err
					}
					if kind == "servers" {
						for _, ip := range ips {
							renew = renew || leaf.VerifyHostname(ip.String()) != nil
						}
					}
				}
				if !renew {
					continue
				}
				certPEM, keyPEM, err = ca.Issue(pki.CertRequest{
					CommonName: m.Name, DNSNames: []string{m.Name, "localhost"},
					IPs: ips, Server: true, Client: kind == "servers",
					Now: now(rc),
				})
				if err != nil {
					return err
				}
			}
			suffix := ""
			if !initial {
				suffix = ".pending"
			}
			if err := putSecret(ctx, rc, base+suffix+".key", keyPEM); err != nil {
				return err
			}
			if err := putSecret(ctx, rc, base+suffix+".crt", certPEM); err != nil {
				return err
			}
			if !initial {
				m.TLSPending = true
			}
			s.ConfigVersion++
		}
	}
	return nil
}

func suppliedCertificate(ctx context.Context, rc *engine.Context, m *member, ips []net.IP) ([]byte, []byte, error) {
	crt, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	key, err := secret(ctx, rc, "tls/user.key")
	if err != nil {
		return nil, nil, err
	}
	pair, err := tls.X509KeyPair(crt, key)
	if err != nil {
		return nil, nil, engine.Invalid("invalid provided server certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, engine.Invalid("invalid provided server certificate")
	}
	rootsPEM, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return nil, nil, err
	}
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return nil, nil, engine.Invalid("invalid provided CA")
	}
	for _, raw := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, nil, engine.Invalid("invalid provided intermediate certificate")
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates, DNSName: m.Name,
			KeyUsages: []x509.ExtKeyUsage{usage}, CurrentTime: now(rc),
		}); err != nil {
			return nil, nil, engine.Invalid("provided certificate must cover member %s with server and client usages", m.Name)
		}
	}
	for _, ip := range ips {
		if err := leaf.VerifyHostname(ip.String()); err != nil {
			return nil, nil, engine.Invalid("provided certificate must cover loopback and exposed public addresses")
		}
	}
	return crt, key, nil
}

func activateCertificates(ctx context.Context, rc *engine.Context, m *member) error {
	if !m.TLSPending {
		return nil
	}
	for _, kind := range []string{"servers"} {
		base := "tls/" + kind + "/" + m.Name
		crt, err := secret(ctx, rc, base+".pending.crt")
		if missing(err) {
			continue
		}
		if err != nil {
			return err
		}
		key, err := secret(ctx, rc, base+".pending.key")
		if err != nil {
			return err
		}
		if err := putSecret(ctx, rc, base+".key", key); err != nil {
			return err
		}
		if err := putSecret(ctx, rc, base+".crt", crt); err != nil {
			return err
		}
		for _, suffix := range []string{".pending.crt", ".pending.key"} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), base+suffix); err != nil && !missing(err) {
				return err
			}
		}
	}
	m.TLSPending = false
	m.TLSReloadBlocked = ""
	return nil
}

func providedCertificateCondition(ctx context.Context, rc *engine.Context) (*domain.Condition, error) {
	if rc.Cluster.Spec.TLS.CA != "userProvided" {
		return nil, nil
	}
	certificate, err := secret(ctx, rc, "tls/user.crt")
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	renew, err := pki.NeedsRenewal(certificate, 30*24*time.Hour, now(rc))
	if err != nil {
		return nil, err
	}
	condition := &domain.Condition{
		Type: "TLSCertificate", Status: "True", Reason: "Valid",
		LastTransitionTime: now(rc),
	}
	if renew {
		condition.Status, condition.Reason = "False", "ExpiringSoon"
		condition.Message = "The supplied server certificate expires within 30 days; rotate its certificate and private key."
		expired, err := pki.NeedsRenewal(certificate, 0, now(rc))
		if err != nil {
			return nil, err
		}
		if expired {
			condition.Reason = "Expired"
			condition.Message = "The supplied server certificate has expired; rotate its certificate and private key."
		}
	}
	return condition, nil
}
