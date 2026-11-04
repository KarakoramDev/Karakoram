// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestReadinessPinnedExpiredLeaf(t *testing.T) {
	now := time.Now()
	newCA := func(serial int64) (*x509.Certificate, *ecdsa.PrivateKey, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		certificate := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "readiness-test-ca"}, NotBefore: now.Add(-48 * time.Hour), NotAfter: now.Add(48 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return certificate, key, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}
	ca, key, caPEM := newCA(1)
	_, _, otherCAPEM := newCA(2)
	leaf := func(serial int64, hostname string, start, end time.Time) (tls.Certificate, string) {
		certificate := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: hostname}, DNSNames: []string{hostname}, NotBefore: start, NotAfter: end, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, certificate, ca, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		pair, err := tls.X509KeyPair([]byte(certPEM), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
		if err != nil {
			t.Fatal(err)
		}
		return pair, certPEM
	}
	expired, expiredPEM := leaf(3, "node00", now.Add(-24*time.Hour), now.Add(-time.Hour))
	differentExpired, _ := leaf(4, "node00", now.Add(-24*time.Hour), now.Add(-time.Hour))
	valid, validPEM := leaf(5, "node00", now.Add(-time.Hour), now.Add(time.Hour))
	future, futurePEM := leaf(6, "node00", now.Add(time.Hour), now.Add(2*time.Hour))
	wrongHost, wrongHostPEM := leaf(7, "different-node", now.Add(-24*time.Hour), now.Add(-time.Hour))
	for _, test := range []struct {
		name       string
		served     tls.Certificate
		pinned, ca string
		wantReady  bool
	}{
		{"valid pinned", valid, validPEM, caPEM, true},
		{"expired exact pinned", expired, expiredPEM, caPEM, true},
		{"different expired leaf", differentExpired, expiredPEM, caPEM, false},
		{"expired leaf with valid pin", expired, validPEM, caPEM, false},
		{"valid replacement for expired pin", valid, expiredPEM, caPEM, true},
		{"future exact pinned", future, futurePEM, caPEM, false},
		{"expired pinned wrong hostname", wrongHost, wrongHostPEM, caPEM, false},
		{"expired pinned untrusted CA", expired, expiredPEM, otherCAPEM, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := testSpec()
			s.TLS = true
			c := config(s)
			c.TLS = agentapi.TLSConfig{CAPEM: test.ca, CertPEM: test.pinned}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+s.AllowedKeys[0] {
					t.Error("missing native root authorization")
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				if r.URL.Path != "/v1/meta" {
					t.Error("unexpected native route", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"version":"1.40.1"}`)
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.TLS = &tls.Config{Certificates: []tls.Certificate{test.served}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			s.Ports.HTTP = server.Listener.Addr().(*net.TCPAddr).Port
			version, err := nativeVersion(t.Context(), s, c)
			if test.wantReady {
				if err != nil || version != "1.40.1" {
					t.Fatalf("version=%q err=%v", version, err)
				}
			} else if err == nil {
				t.Fatalf("untrusted or non-pinned expired leaf accepted: version=%q", version)
			}
		})
	}
}
