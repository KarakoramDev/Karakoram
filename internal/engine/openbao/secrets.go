// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net"
	"strings"
	"time"
)

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func secret(ctx context.Context, rc *engine.Context, key string) ([]byte, error) {
	return rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
}
func putSecret(ctx context.Context, rc *engine.Context, key string, v []byte) error {
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, v)
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
			return agentapi.Certificate{}, engine.Invalid("invalid certificate bundle")
		}
		if b.Type == "CERTIFICATE" {
			cert = append(cert, pem.EncodeToMemory(b)...)
		} else if strings.HasSuffix(b.Type, "PRIVATE KEY") {
			priv = pem.EncodeToMemory(b)
		}
		bundle = rest
	}
	if _, err = tls.X509KeyPair(cert, priv); err != nil {
		return agentapi.Certificate{}, engine.Invalid("invalid certificate key pair")
	}
	return agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(priv)}, nil
}
func memberIPs(ctx context.Context, rc *engine.Context) ([]net.IP, error) {
	ips := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	if !rc.Cluster.Spec.Exposure.Public {
		return ips, nil
	}
	p, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range []string{p.PublicIPv4, p.PublicIPv6} {
		if ip := net.ParseIP(v); ip != nil {
			ips = append(ips, ip)
		}
	}
	return ips, nil
}
func certificateCurrent(bundle []byte, ca *x509.Certificate, name string, ips []net.IP, at time.Time) (bool, error) {
	renew, err := pki.NeedsRenewal(bundle, 30*24*time.Hour, at)
	if err != nil || renew {
		return false, err
	}
	block, _ := pem.Decode(bundle)
	if block == nil {
		return false, engine.Invalid("invalid certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false, err
	}
	if cert.CheckSignatureFrom(ca) != nil {
		return false, nil
	}
	for _, host := range []string{name, "localhost"} {
		if cert.VerifyHostname(host) != nil {
			return false, nil
		}
	}
	for _, ip := range ips {
		if cert.VerifyHostname(ip.String()) != nil {
			return false, nil
		}
	}
	return true, nil
}

const (
	secretRecoveryCA     = "tls/recovery/ca.pem"
	secretRecoveryClient = "tls/recovery/client.pem"
)

func (p *Provider) ensureRecoveryPKI(ctx context.Context, rc *engine.Context) error {
	_, err := secret(ctx, rc, secretRecoveryCA)
	var ca *pki.CA
	if missing(err) {
		if _, clientErr := secret(ctx, rc, secretRecoveryClient); !missing(clientErr) {
			if clientErr != nil {
				return clientErr
			}
			return engine.Invalid("OpenBao recovery CA missing while recovery client exists")
		}
		ca, err = pki.NewCA(rc.Cluster.Name+"-recovery", 10*365*24*time.Hour)
		if err != nil {
			return err
		}
		if err = putSecret(ctx, rc, secretRecoveryCA, append(ca.CertPEM, ca.KeyPEM...)); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		// Parse the existing atomic CA bundle; never replace an invalid CA.
		cert, err := memberCertificate(ctx, rc, secretRecoveryCA)
		if err != nil {
			return err
		}
		ca, err = pki.LoadCA([]byte(cert.CertPEM), []byte(cert.KeyPEM))
		if err != nil {
			return err
		}
	}
	client, err := secret(ctx, rc, secretRecoveryClient)
	if err == nil {
		renew, err := pki.NeedsRenewal(client, 30*24*time.Hour, now(rc))
		if err != nil {
			return err
		}
		if !renew {
			cert, err := memberCertificate(ctx, rc, secretRecoveryClient)
			if err != nil {
				return err
			}
			pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
			if err != nil {
				return err
			}
			leaf, err := x509.ParseCertificate(pair.Certificate[0])
			if err != nil {
				return err
			}
			roots := x509.NewCertPool()
			roots.AddCert(ca.Cert)
			_, err = leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now(rc), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
			return err
		}
	} else if !missing(err) {
		return err
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "karakoram-openbao-recovery", Client: true, Validity: 365 * 24 * time.Hour})
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, secretRecoveryClient, append(cert, key...))
}

func (p *Provider) ensureSecrets(ctx context.Context, rc *engine.Context, s *internalState) error {
	if err := p.ensureRecoveryPKI(ctx, rc); err != nil {
		return err
	}
	crt, err := secret(ctx, rc, "tls/ca.crt")
	if missing(err) {
		if rc.Cluster.Spec.TLS.CA == "userProvided" {
			return engine.Invalid("userProvided requires tls/ca.crt and tls/ca.key")
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
		changed := false
		for _, kind := range []string{"members", "agents"} {
			base := "tls/" + kind + "/" + m.Name + ".pem"
			existing, e := secret(ctx, rc, base)
			if e == nil {
				ok, e := certificateCurrent(existing, ca.Cert, m.Name, ips, now(rc))
				if e != nil {
					return e
				}
				if ok {
					continue
				}
			} else if !missing(e) {
				return e
			}
			if kind == "members" && len(existing) > 0 {
				urgent, e := pki.NeedsRenewal(existing, 7*24*time.Hour, now(rc))
				if e != nil {
					return e
				}
				if !urgent && rc.WaitForWindow().RequeueAfter > 0 {
					continue
				}
			}
			cert, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Validity: 365 * 24 * time.Hour, Server: true, Client: true})
			if e != nil {
				return e
			}
			if e = putSecret(ctx, rc, base, append(cert, key...)); e != nil {
				return e
			}
			changed = true
		}
		if changed && m.ConfigVersion > 0 {
			s.ConfigVersion++
			m.ConfigVersion = s.ConfigVersion
		}
		if m.ConfigVersion == 0 {
			activateConfig(rc.Cluster, s, m)
		}
	}
	return nil
}

// rotateCertificate publishes listener certificates without changing the restart epoch.
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
		cert, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: ips, Validity: 365 * 24 * time.Hour, Server: true, Client: true})
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "tls/"+kind+"/"+m.Name+".pem", append(cert, key...)); e != nil {
			return e
		}
	}
	return nil
}

func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return err
	}
	for _, kind := range []string{"members", "agents"} {
		for _, suffix := range []string{".pem", ".pending.pem"} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+name+suffix); err != nil && !missing(err) {
				return err
			}
		}
	}
	return nil
}
