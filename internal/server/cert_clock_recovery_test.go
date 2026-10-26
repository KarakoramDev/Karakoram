// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestInternalTLSRepairsFutureLeafDuringOperation(t *testing.T) {
	s, sealer, _ := testServer(t)
	config, originalCA, err := internalTLS(t.Context(), s.Store, sealer, "control")
	if err != nil {
		t.Fatal(err)
	}
	first, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(first.Certificate[0])
	first.Leaf.NotBefore = time.Now().Add(24 * time.Hour)
	next, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(original, next.Certificate[0]) || time.Now().Before(next.Leaf.NotBefore) {
		t.Fatal("future leaf survived clock correction")
	}
	restarted, ca, err := internalTLS(t.Context(), s.Store, sealer, "control")
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || ca != originalCA || !bytes.Equal(persisted.Certificate[0], next.Certificate[0]) {
		t.Fatal("repaired leaf or unchanged CA not persisted", err)
	}
}

func TestInternalTLSRepairsPersistedFutureLeaf(t *testing.T) {
	s, sealer, _ := testServer(t)
	_, originalCA, err := internalTLS(t.Context(), s.Store, sealer, "control")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.Store.GetSetting(t.Context(), "server/internal-pki")
	if err != nil {
		t.Fatal(err)
	}
	var ciphertext string
	if err := json.Unmarshal(raw, &ciphertext); err != nil {
		t.Fatal(err)
	}
	plain, err := sealer.OpenFor("server/internal-pki", ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	var identity internalIdentity
	if err := json.Unmarshal(plain, &identity); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(identity.CACert, identity.CAKey)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(identity.Cert)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	leaf.NotBefore = time.Now().Add(24 * time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.Cert, leaf.PublicKey, ca.Key)
	if err != nil {
		t.Fatal(err)
	}
	identity.Cert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := persistInternalIdentity(t.Context(), s.Store, sealer, identity); err != nil {
		t.Fatal(err)
	}
	config, rootsPEM, err := internalTLS(t.Context(), s.Store, sealer, "control")
	if err != nil {
		t.Fatal(err)
	}
	next, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(rootsPEM))
	if _, err := next.Leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "control"}); err != nil {
		t.Fatal(err)
	}
	if rootsPEM != originalCA || bytes.Equal(der, next.Certificate[0]) {
		t.Fatal("future leaf retained or CA replaced")
	}
}

func TestInternalTLSRetainsValidLeafWhenRenewalPersistenceFails(t *testing.T) {
	for _, validity := range []string{"valid", "expired", "future"} {
		t.Run(validity, func(t *testing.T) {
			s, sealer, _ := testServer(t)
			config, _, err := internalTLS(t.Context(), s.Store, sealer, "control")
			if err != nil {
				t.Fatal(err)
			}
			first, err := config.GetCertificate(&tls.ClientHelloInfo{})
			if err != nil {
				t.Fatal(err)
			}
			original := bytes.Clone(first.Certificate[0])
			first.Leaf.NotAfter = time.Now().Add(29 * 24 * time.Hour)
			if validity == "expired" {
				first.Leaf.NotAfter = time.Now().Add(-time.Hour)
			}
			if validity == "future" {
				first.Leaf.NotBefore = time.Now().Add(time.Hour)
			}
			if err := s.Store.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := config.GetCertificate(&tls.ClientHelloInfo{})
			if validity != "valid" {
				if err == nil {
					t.Fatal("unusable leaf accepted after failed renewal persistence")
				}
				return
			}
			if err != nil || !bytes.Equal(next.Certificate[0], original) {
				t.Fatal("still-valid active identity discarded on persistence failure", err)
			}
		})
	}
}
