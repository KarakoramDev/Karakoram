// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestCAAndIssuance(t *testing.T) {
	ca, err := NewCA("test CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	if key, ok := ca.Key.(*ecdsa.PrivateKey); !ok || key.Curve != elliptic.P256() {
		t.Fatal("CA is not ECDSA P-256")
	}
	if got := time.Until(ca.Cert.NotAfter); got < defaultCAValidity-time.Minute || got > defaultCAValidity {
		t.Fatalf("default CA lifetime %s", got)
	}
	certPEM, keyPEM, err := ca.Issue(CertRequest{CommonName: "member", DNSNames: []string{"member", "member.example.test"}, IPs: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, Server: true, Client: true})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.SerialNumber.BitLen() != 128 {
		t.Fatalf("serial has %d bits", leaf.SerialNumber.BitLen())
	}
	if leaf.KeyUsage != x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment {
		t.Fatalf("key usage %v", leaf.KeyUsage)
	}
	if len(leaf.ExtKeyUsage) != 2 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth || leaf.ExtKeyUsage[1] != x509.ExtKeyUsageClientAuth {
		t.Fatalf("EKUs %v", leaf.ExtKeyUsage)
	}
	if got := time.Until(leaf.NotAfter); got < defaultCertValidity-time.Minute || got > defaultCertValidity {
		t.Fatalf("default leaf lifetime %s", got)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	for _, name := range []string{"member", "member.example.test", "127.0.0.1", "::1"} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: name}); err != nil {
			t.Fatalf("verify %s: %v", name, err)
		}
	}
	if err := leaf.VerifyHostname("other.example.test"); err == nil {
		t.Fatal("wrong hostname accepted")
	}
	another, _, err := ca.Issue(CertRequest{Server: true})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := parseCertificate(another)
	if parsed.SerialNumber.Cmp(leaf.SerialNumber) == 0 {
		t.Fatal("serial reused")
	}
	for _, test := range []struct {
		now    time.Time
		before time.Duration
		want   bool
	}{
		{leaf.NotAfter.Add(-time.Hour), time.Minute, false}, {leaf.NotAfter.Add(-time.Hour), time.Hour, true}, {leaf.NotAfter.Add(time.Second), 0, true},
	} {
		got, err := NeedsRenewal(certPEM, test.before, test.now)
		if err != nil || got != test.want {
			t.Fatalf("renewal got %v, %v; want %v", got, err, test.want)
		}
	}
}

func TestMutualTLS(t *testing.T) {
	ca, err := NewCA("mutual TLS CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(name string) tls.Certificate {
		t.Helper()
		cert, key, err := ca.Issue(CertRequest{CommonName: name, DNSNames: []string{name}, Server: true, Client: true})
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			t.Fatal(err)
		}
		return pair
	}
	a, b := issue("membera"), issue("memberb")
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	// Each issued certificate serves in both TLS roles, as MongoDB members do.
	for _, test := range []struct {
		server, client   tls.Certificate
		name, clientName string
	}{{a, b, "membera", "memberb"}, {b, a, "memberb", "membera"}} {
		left, right := net.Pipe()
		server := tls.Server(left, &tls.Config{Certificates: []tls.Certificate{test.server}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12})
		client := tls.Client(right, &tls.Config{Certificates: []tls.Certificate{test.client}, RootCAs: roots, ServerName: test.name, MinVersion: tls.VersionTLS12})
		deadline := time.Now().Add(5 * time.Second)
		left.SetDeadline(deadline)
		right.SetDeadline(deadline)
		done := make(chan error, 1)
		go func() { done <- server.Handshake() }()
		clientErr := client.Handshake()
		serverErr := <-done
		// Close the underlying pipe directly to avoid close-notify blocking.
		left.Close()
		right.Close()
		if clientErr != nil || serverErr != nil {
			t.Fatalf("TLS client=%v server=%v", clientErr, serverErr)
		}
		if len(server.ConnectionState().VerifiedChains) == 0 || len(client.ConnectionState().VerifiedChains) == 0 {
			t.Fatal("TLS peer not verified")
		}
		if err := server.ConnectionState().PeerCertificates[0].VerifyHostname(test.clientName); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadCAFormatsAndValidation(t *testing.T) {
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []crypto.Signer{ec, rsaKey} {
		cert := testCertificate(t, key, true, x509.KeyUsageCertSign)
		pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		formats := []pem.Block{{Type: "PRIVATE KEY", Bytes: pkcs8}}
		switch k := key.(type) {
		case *ecdsa.PrivateKey:
			der, err := x509.MarshalECPrivateKey(k)
			if err != nil {
				t.Fatal(err)
			}
			formats = append(formats, pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
		case *rsa.PrivateKey:
			formats = append(formats, pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
		}
		for _, format := range formats {
			ca, err := LoadCA(cert, pem.EncodeToMemory(&format))
			if err != nil {
				t.Fatalf("load %s: %v", format.Type, err)
			}
			if _, _, err := ca.Issue(CertRequest{Server: true}); err != nil {
				t.Fatalf("issue with %s: %v", format.Type, err)
			}
		}
	}
	key, _ := encodeKey(ec)
	for _, cert := range [][]byte{testCertificate(t, rsaKey, true, x509.KeyUsageCertSign), testCertificate(t, ec, false, x509.KeyUsageCertSign), testCertificate(t, ec, true, x509.KeyUsageDigitalSignature), []byte("invalid")} {
		if _, err := LoadCA(cert, key); err == nil {
			t.Fatal("invalid CA accepted")
		}
	}
	cert := testCertificate(t, ec, true, x509.KeyUsageCertSign)
	for _, badKey := range [][]byte{[]byte("invalid"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("invalid")}), pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: []byte("invalid")})} {
		if _, err := LoadCA(cert, badKey); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
}

func TestValidityAndInvalidInput(t *testing.T) {
	ca, err := NewCA("short CA", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, _, err := ca.Issue(CertRequest{Server: true})
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := parseCertificate(cert)
	if !leaf.NotAfter.Equal(ca.Cert.NotAfter) {
		t.Fatal("leaf outlives CA")
	}
	for _, req := range []CertRequest{{}, {Server: true, Validity: -1}, {Server: true, IPs: []net.IP{{1, 2}}}, {Server: true, DNSNames: []string{""}}} {
		if _, _, err := ca.Issue(req); err == nil {
			t.Fatalf("invalid request accepted: %+v", req)
		}
	}
	ca.Cert.NotAfter = time.Now().Add(-time.Hour)
	if _, _, err := ca.Issue(CertRequest{Client: true}); err == nil {
		t.Fatal("expired CA accepted")
	}
	var nilCA *CA
	if _, _, err := nilCA.Issue(CertRequest{Server: true}); err == nil {
		t.Fatal("nil CA accepted")
	}
	if _, err := NewCA("", 0); err == nil {
		t.Fatal("empty common name accepted")
	}
	if _, err := NewCA("CA", -1); err == nil {
		t.Fatal("negative validity accepted")
	}
	if _, err := NeedsRenewal([]byte("invalid"), 0, time.Now()); err == nil {
		t.Fatal("invalid cert accepted")
	}
	if _, err := NeedsRenewal(cert, -1, time.Now()); err == nil {
		t.Fatal("negative renewal window accepted")
	}
}

func testCertificate(t *testing.T, key crypto.Signer, isCA bool, usage x509.KeyUsage) []byte {
	t.Helper()
	now := time.Now()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: isCA, BasicConstraintsValid: true, KeyUsage: usage}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestLoadCARejectsWeakKeys(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []crypto.Signer{rsaKey, ecKey} {
		cert := testCertificate(t, key, true, x509.KeyUsageCertSign)
		encoded, err := encodeKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCA(cert, encoded); err == nil {
			t.Fatalf("accepted weak CA key %T", key)
		}
	}
	for _, curve := range []elliptic.Curve{elliptic.P256(), elliptic.P384(), elliptic.P521()} {
		key, err := ecdsa.GenerateKey(curve, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := testCertificate(t, key, true, x509.KeyUsageCertSign)
		encoded, err := encodeKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCA(cert, encoded); err != nil {
			t.Fatalf("rejected supported curve %s: %v", curve.Params().Name, err)
		}
	}
}
