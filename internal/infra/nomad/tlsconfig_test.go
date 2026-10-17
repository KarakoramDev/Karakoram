// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func testCertificate(t *testing.T, serial int64, parent *x509.Certificate, signer *ecdsa.PrivateKey) ([]byte, []byte, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if parent == nil {
		cert.IsCA = true
		cert.BasicConstraintsValid = true
		cert.KeyUsage |= x509.KeyUsageCertSign
		parent = cert
		signer = key
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), parsed, key
}

func TestLoadTLS(t *testing.T) {
	caPEM, _, ca, caKey := testCertificate(t, 1, nil, nil)
	certPEM, keyPEM, _, _ := testCertificate(t, 2, ca, caKey)
	_, wrongKey, _, _ := testCertificate(t, 3, ca, caKey)
	dir := t.TempDir()
	files := map[string][]byte{"ca": caPEM, "cert": certPEM, "key": keyPEM, "wrong": wrongKey, "bad": []byte("PRIVATE PEM CONTENT")}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name string
		env  map[string]string
		fail bool
	}{
		{name: "defaults"},
		{name: "custom CA and name", env: map[string]string{"KARAKORAM_NOMAD_CACERT": filepath.Join(dir, "ca"), "KARAKORAM_NOMAD_TLS_SERVER_NAME": "server.global.nomad"}},
		{name: "mTLS", env: map[string]string{"KARAKORAM_NOMAD_CACERT": filepath.Join(dir, "ca"), "KARAKORAM_NOMAD_CLIENT_CERT": filepath.Join(dir, "cert"), "KARAKORAM_NOMAD_CLIENT_KEY": filepath.Join(dir, "key")}},
		{name: "cert only", env: map[string]string{"KARAKORAM_NOMAD_CLIENT_CERT": filepath.Join(dir, "cert")}, fail: true},
		{name: "key only", env: map[string]string{"KARAKORAM_NOMAD_CLIENT_KEY": filepath.Join(dir, "key")}, fail: true},
		{name: "mismatched pair", env: map[string]string{"KARAKORAM_NOMAD_CLIENT_CERT": filepath.Join(dir, "cert"), "KARAKORAM_NOMAD_CLIENT_KEY": filepath.Join(dir, "wrong")}, fail: true},
		{name: "missing CA", env: map[string]string{"KARAKORAM_NOMAD_CACERT": filepath.Join(dir, "missing")}, fail: true},
		{name: "invalid CA", env: map[string]string{"KARAKORAM_NOMAD_CACERT": filepath.Join(dir, "bad")}, fail: true},
		{name: "invalid cert", env: map[string]string{"KARAKORAM_NOMAD_CLIENT_CERT": filepath.Join(dir, "bad"), "KARAKORAM_NOMAD_CLIENT_KEY": filepath.Join(dir, "key")}, fail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := loadTLS(testEnv(tt.env))
			if tt.fail {
				if err == nil {
					t.Fatal("expected failure")
				}
				if strings.Contains(err.Error(), "PRIVATE PEM CONTENT") || strings.Contains(err.Error(), dir) {
					t.Fatalf("unsafe error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if config.MinVersion != tls.VersionTLS12 || config.ServerName != tt.env["KARAKORAM_NOMAD_TLS_SERVER_NAME"] {
				t.Fatalf("incorrect TLS config: %+v", config)
			}
			if tt.env["KARAKORAM_NOMAD_CACERT"] != "" {
				if config.RootCAs == nil {
					t.Fatal("missing roots")
				}
				// A pool built from just the supplied PEM must equal the configured pool.
				expected := x509.NewCertPool()
				expected.AppendCertsFromPEM(caPEM)
				if !config.RootCAs.Equal(expected) {
					t.Fatal("custom CA did not replace system roots")
				}
			} else if config.RootCAs != nil {
				t.Fatal("system roots should be implicit")
			}
			if tt.env["KARAKORAM_NOMAD_CLIENT_CERT"] != "" && len(config.Certificates) != 1 {
				t.Fatal("missing client identity")
			}
		})
	}
}

func TestFactoryClientCertificateOriginRestriction(t *testing.T) {
	certPEM, keyPEM, _, _ := testCertificate(t, 10, nil, nil)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for name, value := range map[string][]byte{certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(name, value, 0600); err != nil {
			t.Fatal(err)
		}
	}
	f := newFactory(testEnv(map[string]string{
		"KARAKORAM_NOMAD_ADDR":        "https://nomad.example.test",
		"KARAKORAM_NOMAD_CLIENT_CERT": certFile,
		"KARAKORAM_NOMAD_CLIENT_KEY":  keyFile,
	}))
	if _, err := f.New(factoryTarget("https://nomad.example.test")); err != nil {
		t.Fatal("operator mTLS origin rejected:", err)
	}
	if _, err := f.New(factoryTarget("https://other.example.test")); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("mTLS factory accepted implicit public origin: %v", err)
	}
}
