// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pki issues cluster and agent certificates from a cluster-scoped CA.
package pki

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

const (
	defaultCAValidity   = 10 * 365 * 24 * time.Hour
	defaultCertValidity = 397 * 24 * time.Hour
	clockSkew           = 5 * time.Minute
	// LeafClockSkew tolerates modest clock differences without extending expiry.
	LeafClockSkew = time.Hour
)

// CA holds a certificate authority and its unencrypted signing key. PEM values
// contain secrets and must be stored with the same protections as credentials.
type CA struct {
	Cert            *x509.Certificate
	Key             crypto.Signer
	CertPEM, KeyPEM []byte
}

// NewCA creates a self-signed ECDSA P-256 CA. Zero validity selects ten years.
func NewCA(commonName string, validity time.Duration) (*CA, error) {
	if commonName == "" {
		return nil, errors.New("CA common name is empty")
	}
	if validity == 0 {
		validity = defaultCAValidity
	}
	if validity < 0 {
		return nil, errors.New("CA validity must be positive")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Truncate(time.Second)
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: now.Add(-clockSkew), NotAfter: now.Add(validity), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}
	keyPEM, err := encodeKey(key)
	if err != nil {
		return nil, err
	}
	return LoadCA(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM)
}

// LoadCA accepts a PEM certificate and an EC, RSA PKCS#1, or PKCS#8 private key.
// It requires a signing CA certificate and verifies that the private key matches.
func LoadCA(certPEM, keyPEM []byte) (*CA, error) {
	cert, err := parseCertificate(certPEM)
	if err != nil {
		return nil, err
	}
	if !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, errors.New("certificate is not a certificate-signing CA")
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("private key is not PEM")
	}
	var key any
	switch block.Type {
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	default:
		return nil, fmt.Errorf("unsupported private key PEM type %q", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parse CA private key: %w", err)
	}
	var signer crypto.Signer
	switch k := key.(type) {
	case *ecdsa.PrivateKey:
		signer = k
		switch k.Curve {
		case elliptic.P256(), elliptic.P384(), elliptic.P521():
		default:
			return nil, errors.New("CA ECDSA key must use P-256, P-384 or P-521")
		}
	case *rsa.PrivateKey:
		if k.N.BitLen() < 2048 {
			return nil, errors.New("CA RSA key must be at least 2048 bits")
		}
		if err := k.Validate(); err != nil {
			return nil, fmt.Errorf("invalid RSA private key: %w", err)
		}
		signer = k
	default:
		return nil, errors.New("CA key must be ECDSA or RSA")
	}
	public, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(public, cert.RawSubjectPublicKeyInfo) {
		return nil, errors.New("CA private key does not match certificate")
	}
	return &CA{Cert: cert, Key: signer, CertPEM: bytes.Clone(certPEM), KeyPEM: bytes.Clone(keyPEM)}, nil
}

// CertRequest defines leaf identities and allowed TLS uses. MongoDB member
// certificates must request both Server and Client for intra-cluster TLS.
type CertRequest struct {
	CommonName string
	DNSNames   []string
	IPs        []net.IP
	Validity   time.Duration
	// Now selects the issuer clock; zero uses the local wall clock.
	Now            time.Time
	Server, Client bool
}

// Issue creates an ECDSA P-256 leaf with a random 128-bit serial number. Zero
// validity selects 397 days; the leaf never outlives its issuing CA.
func (ca *CA) Issue(req CertRequest) (certPEM, keyPEM []byte, err error) {
	if ca == nil || ca.Cert == nil || ca.Key == nil {
		return nil, nil, errors.New("CA is not initialized")
	}
	if !ca.Cert.IsCA || ca.Cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, nil, errors.New("certificate is not a signing CA")
	}
	if !req.Server && !req.Client {
		return nil, nil, errors.New("certificate must allow server or client authentication")
	}
	if req.Validity == 0 {
		req.Validity = defaultCertValidity
	}
	if req.Validity < 0 {
		return nil, nil, errors.New("certificate validity must be positive")
	}
	for _, ip := range req.IPs {
		if ip.To16() == nil {
			return nil, nil, errors.New("invalid IP SAN")
		}
	}
	for _, dns := range req.DNSNames {
		if dns == "" {
			return nil, nil, errors.New("empty DNS SAN")
		}
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC().Truncate(time.Second)
	if now.Before(ca.Cert.NotBefore) {
		return nil, nil, errors.New("CA is not yet valid; correct the system clock or restore the valid CA; automatic CA replacement is refused")
	}
	if !now.Before(ca.Cert.NotAfter) {
		return nil, nil, errors.New("CA has expired; check the system clock and arrange coordinated CA migration; leaf renewal cannot extend CA validity")
	}
	notBefore := now.Add(-LeafClockSkew)
	if notBefore.Before(ca.Cert.NotBefore) {
		notBefore = ca.Cert.NotBefore
	}
	notAfter := now.Add(req.Validity)
	if notAfter.After(ca.Cert.NotAfter) {
		notAfter = ca.Cert.NotAfter
	}
	if !notAfter.After(now) {
		return nil, nil, errors.New("certificate validity is less than one second")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate leaf key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: req.CommonName},
		DNSNames: req.DNSNames, IPAddresses: req.IPs, NotBefore: notBefore, NotAfter: notAfter,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	if req.Server {
		tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	if req.Client {
		tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, key.Public(), ca.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("issue certificate: %w", err)
	}
	keyPEM, err = encodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM, nil
}

// NeedsRenewal reports whether the leaf is not yet valid or expires at or before now+before.
func NeedsRenewal(certPEM []byte, before time.Duration, now time.Time) (bool, error) {
	if before < 0 {
		return false, errors.New("renewal window must not be negative")
	}
	cert, err := parseCertificate(certPEM)
	if err != nil {
		return false, err
	}
	return now.Before(cert.NotBefore) || !cert.NotAfter.After(now.Add(before)), nil
}

func parseCertificate(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("certificate is not certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	return cert, nil
}

func encodeKey(key crypto.Signer) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func randomSerial() (*big.Int, error) {
	// Set the high bit so every serial has 128 bits and is non-zero.
	var serial [16]byte
	if _, err := rand.Read(serial[:]); err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	serial[0] |= 0x80
	return new(big.Int).SetBytes(serial[:]), nil
}
