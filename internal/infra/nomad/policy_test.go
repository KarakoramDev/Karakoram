// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func testEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestAddrPolicy(t *testing.T) {
	tests := []struct{ name, raw, allow, base, want string }{
		{name: "public DNS", raw: "https://EXAMPLE.com:443/", want: "https://example.com"},
		{name: "public literal", raw: "https://8.8.8.8", want: "https://8.8.8.8"},
		{name: "public IPv6", raw: "https://[2606:4700::1111]:443", want: "https://[2606:4700::1111]"},
		{name: "nondefault port", raw: "https://example.com:04646", want: "https://example.com:4646"},
		{name: "loopback", raw: "https://127.0.0.1"},
		{name: "private", raw: "https://10.1.2.3"},
		{name: "link local", raw: "https://169.254.1.1"},
		{name: "shared", raw: "https://100.64.1.1"},
		{name: "this network", raw: "https://0.1.2.3"},
		{name: "IPv6 private", raw: "https://[fc00::1]"},
		{name: "IPv6 loopback", raw: "https://[::1]"},
		{name: "mapped", raw: "https://[::ffff:8.8.8.8]"},
		{name: "localhost", raw: "https://localhost"},
		{name: "localhost suffix", raw: "https://test.localhost."},
		{name: "http default denied", raw: "http://example.com"},
		{name: "listed http", raw: "http://EXAMPLE.com:80", allow: "http://example.com", want: "http://example.com"},
		{name: "listed private", raw: "https://10.1.2.3", allow: " https://10.1.2.3 ", want: "https://10.1.2.3"},
		{name: "operator implicitly allowed", raw: "http://127.0.0.1:4646", base: "http://127.0.0.1:4646/", want: "http://127.0.0.1:4646"},
		{name: "operator not restrictive", raw: "https://example.com", base: "http://127.0.0.1:4646", want: "https://example.com"},
		{name: "allowlist restrictive", raw: "https://other.example.com", allow: "https://example.com"},
		{name: "operator plus allowlist", raw: "https://operator.example.com", allow: "https://example.com", base: "https://operator.example.com", want: "https://operator.example.com"},
		{name: "invalid entry fails closed", raw: "https://example.com", allow: "https://example.com,garbage"},
		{name: "empty entry fails closed", raw: "https://example.com", allow: "https://example.com,"},
		{name: "invalid operator fails closed", raw: "https://example.com", base: "not an origin"},
		{name: "userinfo", raw: "https://secret@example.com"},
		{name: "path", raw: "https://example.com/api"},
		{name: "escaped path", raw: "https://example.com/%2f"},
		{name: "query", raw: "https://example.com?token=secret"},
		{name: "empty query", raw: "https://example.com?"},
		{name: "fragment", raw: "https://example.com#"},
		{name: "invalid port", raw: "https://example.com:65536"},
		{name: "empty port", raw: "https://example.com:"},
		{name: "zero port", raw: "https://example.com:0"},
		{name: "zone", raw: "https://[fe80::1%25eth0]"},
		{name: "scheme", raw: "file:///tmp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newAddrPolicy(testEnv(map[string]string{"KARAKORAM_NOMAD_ALLOWED_ADDRS": tt.allow, "KARAKORAM_NOMAD_ADDR": tt.base}))
			got, err := p.check(tt.raw)
			if tt.want != "" {
				if err != nil || got != tt.want {
					t.Fatalf("check = %q, %v; want %q", got, err, tt.want)
				}
				return
			}
			var rejected *infra.RejectedError
			if !errors.As(err, &rejected) || !errors.Is(err, infra.ErrRejected) || got != "" {
				t.Fatalf("check = %q, %v; want rejection", got, err)
			}
			if strings.Contains(err.Error(), tt.raw) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}

func TestAddrPolicyClientIdentityRestrictsOrigins(t *testing.T) {
	for _, material := range []string{"KARAKORAM_NOMAD_CLIENT_CERT", "KARAKORAM_NOMAD_CLIENT_KEY"} {
		t.Run(material, func(t *testing.T) {
			values := map[string]string{
				material:               "/operator/client.pem",
				"KARAKORAM_NOMAD_ADDR": "https://NOMAD.example.test:443/",
			}
			p := newAddrPolicy(testEnv(values))
			if got, err := p.check("https://nomad.example.test"); err != nil || got != "https://nomad.example.test" {
				t.Fatalf("operator origin rejected: %q %v", got, err)
			}
			for _, raw := range []string{"https://other.example.test", "https://nomad.example.test:4646", "http://nomad.example.test"} {
				if _, err := p.check(raw); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("implicit origin accepted with client identity: %q %v", raw, err)
				}
			}
			values["KARAKORAM_NOMAD_ALLOWED_ADDRS"] = "https://other.example.test"
			p = newAddrPolicy(testEnv(values))
			if _, err := p.check("https://other.example.test"); err != nil {
				t.Fatal("explicit second origin rejected:", err)
			}
			delete(values, "KARAKORAM_NOMAD_ADDR")
			delete(values, "KARAKORAM_NOMAD_ALLOWED_ADDRS")
			p = newAddrPolicy(testEnv(values))
			if _, err := p.check("https://other.example.test"); !errors.Is(err, infra.ErrRejected) {
				t.Fatal("client identity without any approved origin did not fail closed:", err)
			}
		})
	}
}
