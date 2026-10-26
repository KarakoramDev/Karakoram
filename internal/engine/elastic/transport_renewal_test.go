// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

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
	for _, future := range []bool{false, true} {
		name := "expired"
		if future {
			name = "future-not-before"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, backend)

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
			if err != nil {
				t.Fatal(err)
			}
			// Publication must not require contacting the member at all.
			rc.Agents = nil
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
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
		})
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
