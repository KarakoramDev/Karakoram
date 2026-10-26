// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package relay provides authenticated, namespace-scoped TCP tunnels and relay credentials.
//
// TCP half-close is not propagated: EOF or error in either proxy direction closes
// both connections. MongoDB wire and HTTPS/TLS callers must use protocol framing,
// not a TCP half-close, to delimit requests. Bytes written before peer close are
// delivered in stream order.
package relay

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"regexp"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/pki"
)

const hubServerName = "karakoram-relay-hub"

var namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)

// GenerateCA creates a ten-year P-256 relay CA and PKCS#8 private key in PEM form.
// Callers must seal the private key before persisting it.
func GenerateCA(now time.Time) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	t := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Karakoram relay CA"}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, t, t, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err = MarshalPrivateKeyPEM(key)
	return MarshalCertificatePEM(der), keyPEM, err
}

// IssueHubCertificate creates a one-year serverAuth certificate with the fixed relay DNS identity.
func IssueHubCertificate(caPEM, keyPEM []byte, now time.Time) ([]byte, []byte, error) {
	return issueCertificate(caPEM, keyPEM, "", now)
}

// IssueClientCertificate creates a one-year clientAuth certificate bound to a non-default namespace.
func IssueClientCertificate(caPEM, keyPEM []byte, namespace string, now time.Time) ([]byte, []byte, error) {
	if !namespacePattern.MatchString(namespace) || namespace == "default" {
		return nil, nil, errors.New("invalid relay namespace")
	}
	return issueCertificate(caPEM, keyPEM, namespace, now)
}
func issueCertificate(caPEM, keyPEM []byte, namespace string, now time.Time) ([]byte, []byte, error) {
	ca, err := ParseCertificatePEM(caPEM)
	if err != nil {
		return nil, nil, err
	}
	caKey, err := ParsePrivateKeyPEM(keyPEM)
	if err != nil {
		return nil, nil, err
	}
	if !ca.IsCA || !caKey.PublicKey.Equal(ca.PublicKey) {
		return nil, nil, errors.New("relay CA key does not match certificate")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	t := &x509.Certificate{SerialNumber: serial, NotBefore: now.Add(-pki.LeafClockSkew), NotAfter: now.AddDate(1, 0, 0), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature}
	if t.NotAfter.After(ca.NotAfter) {
		t.NotAfter = ca.NotAfter
	}
	if namespace == "" {
		t.DNSNames = []string{hubServerName}
		t.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		u, _ := url.Parse("karakoram:relay:" + namespace)
		t.URIs = []*url.URL{u}
		t.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, t, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	outKey, err := MarshalPrivateKeyPEM(key)
	return MarshalCertificatePEM(der), outKey, err
}
func randomSerial() (*big.Int, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	b[0] |= 0x80
	return new(big.Int).SetBytes(b[:]), nil
}

// MarshalCertificatePEM encodes a DER certificate without persisting it.
func MarshalCertificatePEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ParseCertificatePEM parses exactly one PEM certificate.
func ParseCertificatePEM(data []byte) (*x509.Certificate, error) {
	b, rest := pem.Decode(data)
	if b == nil || b.Type != "CERTIFICATE" || len(rest) != 0 {
		return nil, errors.New("invalid certificate PEM")
	}
	return x509.ParseCertificate(b.Bytes)
}

// MarshalPrivateKeyPEM encodes a P-256 key as PKCS#8 PEM; the result is secret.
func MarshalPrivateKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// ParsePrivateKeyPEM parses a PEM-encoded PKCS#8 P-256 key.
func ParsePrivateKeyPEM(data []byte) (*ecdsa.PrivateKey, error) {
	b, rest := pem.Decode(data)
	if b == nil || b.Type != "PRIVATE KEY" || len(rest) != 0 {
		return nil, errors.New("invalid private key PEM")
	}
	v, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := v.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, errors.New("relay key must be P-256")
	}
	return key, nil
}

// NamespaceFromCert extracts a single valid relay URI identity. It does not verify trust or enrollment.
func NamespaceFromCert(cert *x509.Certificate) (string, error) {
	if cert == nil || len(cert.URIs) != 1 {
		return "", errors.New("relay certificate requires one namespace URI")
	}
	u := cert.URIs[0]
	ns := u.Opaque
	const prefix = "relay:"
	if u.Scheme != "karakoram" || len(ns) <= len(prefix) || ns[:len(prefix)] != prefix || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid relay certificate identity")
	}
	ns = ns[len(prefix):]
	if !namespacePattern.MatchString(ns) || ns == "default" {
		return "", errors.New("invalid relay namespace")
	}
	return ns, nil
}

// NeedsRenewal reports whether a certificate is not yet valid or expires in fewer than thirty days.
func NeedsRenewal(cert *x509.Certificate, now time.Time) bool {
	return cert == nil || now.Before(cert.NotBefore) || cert.NotAfter.Sub(now) < 30*24*time.Hour
}
