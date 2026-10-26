// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
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
			p, rc, _ := setupProvider(t)
			convergeProvider(t, p, rc)

			caCert, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			caKey, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caCert, caKey)
			if err != nil {
				t.Fatal(err)
			}

			memberName := "demop0"
			base := "tls/agents/" + memberName
			old, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".crt")
			if err != nil {
				t.Fatal(err)
			}
			bad := invalidTransportLeaf(t, ca, old, future)
			if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".crt", bad); err != nil {
				t.Fatal(err)
			}
			// Old releases may have left a staged agent leaf; it must never overwrite recovery.
			if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".pending.crt", bad); err != nil {
				t.Fatal(err)
			}
			activeKey, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".key")
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".pending.key", activeKey); err != nil {
				t.Fatal(err)
			}
			// Publication must not require contacting the member at all.
			rc.Agents = expiredTransportDialer{}
			_, _ = p.Sync(t.Context(), rc) // The unavailable live observation may fail after publication.
			got, err := p.MemberConfig(t.Context(), rc, memberName)
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
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: memberName}); err != nil {
				t.Fatal(err)
			}
			if _, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), base+".pending.crt"); !missing(err) {
				t.Fatal("staged agent leaf can overwrite recovered transport")
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

type expiredTransportDialer struct{}

func (expiredTransportDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return nil, errors.New("x509: certificate has expired or is not yet valid")
}

func TestComponentCertificateRecoveryWhileCoreUnavailable(t *testing.T) {
	for _, future := range []bool{false, true} {
		name := "expired"
		if future {
			name = "future-not-before"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["gateway"]
			before, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			caCert, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			caKey, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caCert, caKey)
			if err != nil {
				t.Fatal(err)
			}
			before.AgentTLS.CertPEM = string(invalidTransportLeaf(t, ca, []byte(before.AgentTLS.CertPEM), future))
			before.Maintenance = true
			before.DesiredProcesses = []string{}
			for path, value := range map[string]any{
				"supabase/config/" + m.Name:     before,
				"supabase/agent-cert/" + m.Name: before.AgentTLS,
			} {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), path, raw); err != nil {
					t.Fatal(err)
				}
			}
			rc.Agents = expiredTransportDialer{}
			_, _ = p.Sync(t.Context(), rc)
			after, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			if renew, err := componentCertificateExpiry(rc, after.AgentTLS); err != nil || renew {
				t.Fatal("unavailable core blocked optional agent transport renewal", err)
			}
			if after.ConfigVersion != before.ConfigVersion+1 || !bytes.Equal(after.EngineSpec, before.EngineSpec) || !after.Maintenance || after.DesiredProcesses == nil || len(after.DesiredProcesses) != 0 {
				t.Fatal("transport renewal changed existing application or stop/maintenance intent")
			}
		})
	}
}
