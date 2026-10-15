// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestNodeCertificates(t *testing.T) {
	ca, err := NewCA("test cluster CA", 0)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCA(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		client bool
		dns    []string
	}{
		{"transport", true, nil}, {"http", false, []string{"public.example.com", "localhost", "demon0"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP("192.0.2.10")
			var crt, key []byte
			var err error
			if tt.client {
				crt, key, err = IssueTransport(loaded, "demon0", []net.IP{ip, net.IPv4(127, 0, 0, 1)})
			} else {
				crt, key, err = IssueHTTP(loaded, "demon0", tt.dns, []net.IP{ip, net.IPv6loopback})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tls.X509KeyPair(crt, key); err != nil {
				t.Fatal(err)
			}
			block, _ := pem.Decode(crt)
			if block == nil {
				t.Fatal("no certificate PEM")
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if leaf.Subject.CommonName != "demon0" {
				t.Fatal("common name")
			}
			dns := []string{"demon0", "localhost"}
			if !tt.client {
				dns = append(dns, "public.example.com")
			}
			if !reflect.DeepEqual(leaf.DNSNames, dns) {
				t.Fatalf("DNS SANs: %v", leaf.DNSNames)
			}
			if len(leaf.IPAddresses) != 3 {
				t.Fatalf("IP SANs: %v", leaf.IPAddresses)
			}
			for _, host := range append(dns, "127.0.0.1", "::1", "192.0.2.10") {
				if err = leaf.VerifyHostname(host); err != nil {
					t.Fatal(err)
				}
			}
			eku := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			if tt.client {
				eku = append(eku, x509.ExtKeyUsageClientAuth)
			}
			if !reflect.DeepEqual(leaf.ExtKeyUsage, eku) {
				t.Fatalf("EKU: %v", leaf.ExtKeyUsage)
			}
			roots := x509.NewCertPool()
			roots.AddCert(ca.Cert)
			if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "demon0"}); err != nil {
				t.Fatal(err)
			}
			if renew, err := NeedsRenewal(crt, 30*24*time.Hour, time.Now()); err != nil || renew {
				t.Fatalf("unexpected renewal: %v %v", renew, err)
			}
		})
	}
	for _, tt := range []struct {
		name, node string
		ips        []net.IP
		dns        []string
		nilCA      bool
	}{
		{name: "invalid node", node: "bad-host"}, {name: "nil CA", node: "demon0", nilCA: true}, {name: "bad IP", node: "demon0", ips: []net.IP{{1, 2}}}, {name: "empty DNS", node: "demon0", dns: []string{""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			issuer := ca
			if tt.nilCA {
				issuer = nil
			}
			if _, _, err := IssueHTTP(issuer, tt.node, tt.dns, tt.ips); err == nil {
				t.Fatal("expected issuance error")
			}
		})
	}
}
