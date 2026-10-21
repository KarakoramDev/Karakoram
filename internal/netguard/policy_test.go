// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package netguard

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
)

func TestAddressPolicy(t *testing.T) {
	for _, value := range []string{"127.0.0.1", "::1", "0.0.0.0", "::", "169.254.169.254", "fe80::1", "224.0.0.1", "::ffff:127.0.0.1", "100.100.100.200", "fd00:ec2::254", "168.63.129.16", "192.0.0.192"} {
		for _, approved := range []bool{false, true} {
			if AddressAllowed(netip.MustParseAddr(value), approved, false) {
				t.Fatalf("S3 admitted %s", value)
			}
		}
	}
	for _, value := range []string{"10.0.0.1", "192.168.1.1", "172.16.1.1", "fd00::1"} {
		ip := netip.MustParseAddr(value)
		if AddressAllowed(ip, false, false) || !AddressAllowed(ip, true, false) {
			t.Fatalf("private policy %s", value)
		}
	}
	if !AddressAllowed(netip.MustParseAddr("8.8.8.8"), false, false) {
		t.Fatal("public address refused")
	}
}
func TestNotifyPolicy(t *testing.T) {
	p := &Policy{Allowlist: ParseAllowlist(" Example.TEST., 127.0.0.1:8123, [::1]:8123 ")}
	for _, test := range []struct {
		value   string
		allowed bool
	}{
		{"https://public.example/path?token=secret", true}, {"http://public.example", false},
		{"http://example.test", true}, {"http://example.test:8123", false},
		{"http://127.0.0.1:8123", true}, {"http://[::1]:8123", true}, {"https://127.0.0.1", false},
		{"https://169.254.169.254", false}, {"https://100.100.100.200", false},
		{"https://user:secret@public.example", false},
	} {
		u, err := url.Parse(test.value)
		if err != nil {
			t.Fatal(err)
		}
		if got := p.CheckURL(u, true) == nil; got != test.allowed {
			t.Errorf("%s allowed=%v", test.value, got)
		}
	}
	client := p.HTTPClient(0)
	transport := client.Transport.(*http.Transport)
	if transport.Proxy != nil || transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("unsafe HTTP transport configuration")
	}
	if client.CheckRedirect(nil, nil) == nil {
		t.Fatal("redirect accepted")
	}
}

func TestDialTimeResolutionAndMetadataRefusal(t *testing.T) {
	for _, address := range []string{"localhost:443", "127.0.0.1:443", "[::1]:443", "169.254.169.254:443", "100.100.100.200:443"} {
		p := &Policy{}
		if conn, err := p.DialContext(t.Context(), "tcp", address); !errors.Is(err, ErrNotAllowed) {
			if conn != nil {
				conn.Close()
			}
			t.Fatalf("unexpected policy result for %s: %v", address, err)
		}
	}
	for _, address := range []string{"169.254.169.254:443", "100.100.100.200:443", "[fd00:ec2::254]:443", "168.63.129.16:443", "192.0.0.192:443"} {
		p := &Policy{Allowlist: ParseAllowlist(address)}
		if conn, err := p.DialContext(t.Context(), "tcp", address); !errors.Is(err, ErrNotAllowed) {
			if conn != nil {
				conn.Close()
			}
			t.Fatalf("allowlist admitted metadata %s: %v", address, err)
		}
	}
}
