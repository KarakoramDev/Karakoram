// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pki

import (
	"crypto/x509"
	"testing"
	"time"
)

func TestNeedsRenewalNotYetValid(t *testing.T) {
	ca, err := NewCA("clock test CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	crt, _, err := ca.Issue(CertRequest{DNSNames: []string{"member"}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := parseCertificate(crt)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{"before-not-before", leaf.NotBefore.Add(-time.Second), true},
		{"at-not-before", leaf.NotBefore, false},
		{"after-not-before", leaf.NotBefore.Add(time.Second), false},
		{"at-not-after", leaf.NotAfter, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NeedsRenewal(crt, 0, tt.now)
			if err != nil || got != tt.want {
				t.Fatalf("renew=%v err=%v want=%v", got, err, tt.want)
			}
		})
	}
}

func TestIssuanceClockSkewMargin(t *testing.T) {
	ca, err := NewCA("clock test CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	crt, _, err := ca.Issue(CertRequest{DNSNames: []string{"member"}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := parseCertificate(crt)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	now := time.Now().UTC().Truncate(time.Second)
	for _, skew := range []time.Duration{-6 * time.Minute, -4 * time.Minute, 4 * time.Minute, 6 * time.Minute} {
		_, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "member", CurrentTime: now.Add(skew)})
		if (err != nil) != (skew == -6*time.Minute) {
			t.Fatalf("skew=%s verification=%v", skew, err)
		}
	}
	ca.Cert.NotBefore = now.Add(time.Hour)
	if _, _, err := ca.Issue(CertRequest{Server: true}); err == nil {
		t.Fatal("future CA was silently replaced or accepted")
	}
}

func TestIssuerClockBackdatingAndCorrection(t *testing.T) {
	ca, err := NewCA("clock test CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	corrected := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	for _, offset := range []time.Duration{8 * time.Hour, -time.Hour} {
		issuedAt := corrected.Add(offset)
		crt, _, err := ca.Issue(CertRequest{DNSNames: []string{"member"}, Server: true, Now: issuedAt, Validity: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := parseCertificate(crt)
		if !leaf.NotBefore.Equal(issuedAt.Add(-LeafClockSkew)) || !leaf.NotAfter.Equal(issuedAt.Add(time.Hour)) {
			t.Fatal("issuer clock/backdating changed expiry or validity start")
		}
		if renew, err := NeedsRenewal(crt, 0, corrected); err != nil || !renew {
			t.Fatalf("clock correction missed invalid identity: renew=%v err=%v", renew, err)
		}
		replacement, _, err := ca.Issue(CertRequest{DNSNames: []string{"member"}, Server: true, Now: corrected})
		if err != nil {
			t.Fatal(err)
		}
		if renew, err := NeedsRenewal(replacement, 0, corrected); err != nil || renew {
			t.Fatalf("corrected identity needs another renewal: renew=%v err=%v", renew, err)
		}
		roots := x509.NewCertPool()
		roots.AddCert(ca.Cert)
		reissued, _ := parseCertificate(replacement)
		if _, err := reissued.Verify(x509.VerifyOptions{Roots: roots, DNSName: "member", CurrentTime: corrected}); err != nil {
			t.Fatal(err)
		}
	}
}
