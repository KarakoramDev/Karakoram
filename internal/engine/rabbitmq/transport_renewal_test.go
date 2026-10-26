// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

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
	for _, replicas := range []int{1, 3} {
		for _, future := range []bool{false, true} {
			name := "single/expired"
			if replicas == 3 {
				name = "ha/expired"
			}
			if future {
				name += "-future-not-before"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, _, _ := setup(t)
				rc.Cluster.Spec.Components.Engine.Replicas = replicas
				ready(t, p, rc)
				s := state(t, rc)
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
				old, err := secret(t.Context(), rc, base+".pem")
				if err != nil {
					t.Fatal(err)
				}
				bad := invalidTransportLeaf(t, ca, old, future)
				_, key := pem.Decode(old)
				bad = append(bad, key...)
				if err := putSecret(t.Context(), rc, base+".pem", bad); err != nil {
					t.Fatal(err)
				}
				// Old releases may have left a staged agent leaf; it must never overwrite recovery.
				if err := putSecret(t.Context(), rc, base+".pending.pem", bad); err != nil {
					t.Fatal(err)
				}
				// Publication must not require contacting the member at all.
				rc.Agents = nil
				if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if err := s.save(rc.Cluster); err != nil {
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
				if _, err := secret(t.Context(), rc, base+".pending.pem"); !missing(err) {
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
