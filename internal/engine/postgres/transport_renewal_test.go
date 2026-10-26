// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"testing"
	"time"
)

func TestAgentCertificateRecoveryWithoutLiveGate(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, future := range []bool{false, true} {
			name := topology + "/expired"
			if future {
				name = topology + "/future-not-before"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, _ := setup(t, topology)
				s := converge(t, p, rc)

				caCert, err := secret(t.Context(), rc, "tls/ca.crt")
				if err != nil {
					t.Fatal(err)
				}
				caKey, err := secret(t.Context(), rc, "tls/ca.key")
				if err != nil {
					t.Fatal(err)
				}
				ca, err := pki.LoadCA(caCert, caKey)
				if err != nil {
					t.Fatal(err)
				}

				m := s.Members[0]
				base := "tls/agents/" + m.Name
				old, err := secret(t.Context(), rc, base+".crt")
				if err != nil {
					t.Fatal(err)
				}
				bad := invalidTransportLeaf(t, ca, old, future)
				if err := putSecret(t.Context(), rc, base+".crt", bad); err != nil {
					t.Fatal(err)
				}
				// Old releases may have left a staged agent leaf; it must never overwrite recovery.
				if err := putSecret(t.Context(), rc, base+".pending.crt", bad); err != nil {
					t.Fatal(err)
				}
				activeKey, err := secret(t.Context(), rc, base+".key")
				if err != nil {
					t.Fatal(err)
				}
				if err := putSecret(t.Context(), rc, base+".pending.key", activeKey); err != nil {
					t.Fatal(err)
				}
				// Publication must not require contacting the member at all.
				rc.Agents = nil
				if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
					t.Fatal(err)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				got, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Equal([]byte(got.AgentTLS.CertPEM), bad) {
					t.Fatal("invalid agent leaf still published")
				}
				renew, err := pki.NeedsRenewal([]byte(got.AgentTLS.CertPEM), 30*24*time.Hour, time.Now())
				if err != nil || renew {
					t.Fatalf("agent pull still receives unusable leaf: renew=%v err=%v", renew, err)
				}
				roots := x509.NewCertPool()
				roots.AddCert(ca.Cert)
				block, _ := pem.Decode([]byte(got.AgentTLS.CertPEM))
				leaf, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: m.Name}); err != nil {
					t.Fatal(err)
				}
				if _, err := secret(t.Context(), rc, base+".pending.crt"); !missing(err) {
					t.Fatal("staged agent leaf can overwrite recovered transport")
				}
			})
		}
	}
}

func invalidTransportLeaf(t *testing.T, ca *pki.CA, certPEM []byte, future bool) []byte {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("missing certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	leaf.NotBefore, leaf.NotAfter = now.Add(-2*time.Hour), now.Add(-time.Hour)
	if future {
		leaf.NotBefore, leaf.NotAfter = now.Add(24*time.Hour), now.Add(365*24*time.Hour)
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.Cert, leaf.PublicKey, ca.Key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestUnusableStagedServerCertificateIsReissued(t *testing.T) {
	for _, future := range []bool{false, true} {
		name := "expired"
		if future {
			name = "future-not-before"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setup(t, "ha")
			s := converge(t, p, rc)
			caCert, err := secret(t.Context(), rc, "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			caKey, err := secret(t.Context(), rc, "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caCert, caKey)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			base := "tls/servers/" + m.Name
			crt, err := secret(t.Context(), rc, base+".crt")
			if err != nil {
				t.Fatal(err)
			}
			key, err := secret(t.Context(), rc, base+".key")
			if err != nil {
				t.Fatal(err)
			}
			bad := invalidTransportLeaf(t, ca, crt, future)
			for suffix, value := range map[string][]byte{".crt": bad, ".pending.crt": bad, ".pending.key": key} {
				if err := putSecret(t.Context(), rc, base+suffix, value); err != nil {
					t.Fatal(err)
				}
			}
			m.TLSPending = true
			if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
				t.Fatal(err)
			}
			pending, err := secret(t.Context(), rc, base+".pending.crt")
			if err != nil {
				t.Fatal(err)
			}
			if renew, err := pki.NeedsRenewal(pending, 0, time.Now()); err != nil || renew {
				t.Fatal("unusable staged server identity retained", err)
			}
			active, err := secret(t.Context(), rc, base+".crt")
			if err != nil || !bytes.Equal(active, bad) {
				t.Fatal("server identity bypassed rolling activation", err)
			}
		})
	}
}
