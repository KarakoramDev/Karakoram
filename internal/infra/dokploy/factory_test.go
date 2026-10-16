// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func factoryEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}
func factoryTarget(origin string) infra.Target {
	return infra.Target{Kind: "dokploy", ID: "environment_1-test", Token: "factory-test-key", Config: map[string]string{"url": origin}}
}

func TestFactoryURLPolicy(t *testing.T) {
	cases := []struct {
		name, origin, base, allow string
		ok                        bool
	}{
		{"https default", "https://example.invalid", "", "", true},
		{"http denied", "http://example.invalid", "", "", false},
		{"http default origin", "http://example.invalid", "http://EXAMPLE.invalid:80/", "", true},
		{"http allowlisted", "http://example.invalid", "", "http://example.invalid", true},
		{"https allowlisted", "https://example.invalid", "", "https://EXAMPLE.invalid:443/", true},
		{"https nonlisted", "https://other.invalid", "", "https://example.invalid", false},
		{"base joins allowlist", "http://example.invalid", "http://example.invalid", "https://other.invalid", true},
		{"invalid allowlist", "https://example.invalid", "", "https://example.invalid,not-an-origin", false},
		{"empty allowlist entry", "https://example.invalid", "", "https://example.invalid,", false},
		{"invalid base fails closed", "https://example.invalid", "invalid", "", false},
		{"userinfo", "https://user:pass@example.invalid", "", "", false},
		{"path", "https://example.invalid/api", "", "", false},
		{"encoded slash", "https://example.invalid/%2f", "", "", false},
		{"query", "https://example.invalid?x=1", "", "", false},
		{"empty query", "https://example.invalid?", "", "", false},
		{"fragment", "https://example.invalid#x", "", "", false},
		{"empty fragment", "https://example.invalid#", "", "", false},
		{"relative", "//example.invalid", "", "", false},
		{"missing", "", "https://example.invalid", "", false},
		{"invalid port", "https://example.invalid:65536", "", "", false},
		{"zero port", "https://example.invalid:0", "", "", false},
		{"empty port", "https://example.invalid:", "", "", false},
		{"scheme", "ftp://example.invalid", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFactory(factoryEnv(map[string]string{"KARAKORAM_DOKPLOY_URL": tc.base, "KARAKORAM_DOKPLOY_ALLOWED_URLS": tc.allow}))
			_, err := f.New(factoryTarget(tc.origin))
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected policy result: %v", err)
			}
			if !tc.ok {
				var rejected *infra.RejectedError
				if !errors.As(err, &rejected) || rejected.Code != "dokployUrlNotAllowed" || !strings.Contains(rejected.Message, "KARAKORAM_DOKPLOY_ALLOWED_URLS") {
					t.Fatalf("bad URL rejection: %v", err)
				}
				// Validate must reject before attempting any backend API requests.
				if _, _, err := f.Validate(context.Background(), factoryTarget(tc.origin)); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("Validate bypassed URL policy: %v", err)
				}
			}
		})
	}
}

func TestFactoryURLPrivateOriginsRequireOperatorApproval(t *testing.T) {
	origins := []string{
		"https://localhost", "https://LOCALHOST:443/", "https://localhost.",
		"https://service.localhost", "https://service.localhost.",
		"https://127.0.0.1", "https://127.255.255.254", "https://0.0.0.0",
		"https://0.0.0.1", "https://0.255.255.255",
		"https://100.64.0.0", "https://100.64.0.1", "https://100.127.255.255",
		"https://10.0.0.1", "https://172.16.0.1", "https://192.168.0.1",
		"https://169.254.169.254", "https://[::1]", "https://[::]",
		"https://[fc00::1]", "https://[fd00::1]", "https://[fe80::1]",
		"https://[ff02::1]", "https://[::ffff:127.0.0.1]", "https://[::ffff:192.0.2.1]",
	}
	for _, origin := range origins {
		t.Run(origin, func(t *testing.T) {
			deny := newFactory(factoryEnv(nil))
			if _, err := deny.New(factoryTarget(origin)); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("private origin accepted without operator approval: %v", err)
			}
			if _, _, err := deny.Validate(context.Background(), factoryTarget(origin)); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("Validate accepted a private origin before network I/O: %v", err)
			}
			for _, key := range []string{"KARAKORAM_DOKPLOY_URL", "KARAKORAM_DOKPLOY_ALLOWED_URLS"} {
				allow := newFactory(factoryEnv(map[string]string{key: origin}))
				if _, err := allow.New(factoryTarget(origin)); err != nil {
					t.Fatalf("operator-approved private origin rejected: %v", err)
				}
			}
		})
	}
	for _, origin := range []string{
		"https://192.0.2.1", "https://[2001:db8::1]", "https://example.invalid",
		"https://1.0.0.0", "https://100.63.255.255", "https://100.128.0.0",
	} {
		if _, err := newFactory(factoryEnv(nil)).New(factoryTarget(origin)); err != nil {
			t.Fatalf("non-private HTTPS origin rejected: %v", err)
		}
	}
}

func TestFactoryDefaultNetworkIsolation(t *testing.T) {
	f := newFactory(factoryEnv(nil))
	for _, tc := range []struct{ id, want string }{
		{"abc", "karakoram-ba7816bf"},
		{"test", "karakoram-9f86d081"},
	} {
		target := factoryTarget("https://example.invalid")
		target.ID = tc.id
		backend, err := f.New(target)
		if err != nil {
			t.Fatal(err)
		}
		if got := backend.(*Backend).target.Config["network"]; got != tc.want {
			t.Fatalf("environment-specific default network = %q, want %q", got, tc.want)
		}
		if _, exists := target.Config["network"]; exists {
			t.Fatal("factory modified caller's target config")
		}
		again, err := f.New(target)
		if err != nil || again.(*Backend).target.Config["network"] != tc.want {
			t.Fatal("default network is not deterministic")
		}
		target.Config["network"] = "operator-overlay"
		explicit, err := f.New(target)
		if err != nil || explicit.(*Backend).target.Config["network"] != "operator-overlay" {
			t.Fatal("factory overwrote operator network")
		}
	}
}

func TestURLNormalization(t *testing.T) {
	for input, want := range map[string]string{
		"https://EXAMPLE.invalid:443/":  "https://example.invalid",
		"http://EXAMPLE.invalid:80/":    "http://example.invalid",
		"https://EXAMPLE.invalid:8443/": "https://example.invalid:8443",
		"https://[2001:DB8::1]:443/":    "https://[2001:db8::1]",
		"https://[2001:DB8::1]:8443/":   "https://[2001:db8::1]:8443",
	} {
		got, err := normalizeOrigin(input)
		if err != nil || got != want {
			t.Errorf("normalizing origin: got %q, %v; want %q", got, err, want)
		}
	}
}

func TestFactoryConfigValidation(t *testing.T) {
	f := newFactory(factoryEnv(nil))
	valid := map[string]string{"url": "https://example.invalid", "network": "app-overlay_1.test", "registryId": "registry_1-test", "publicIPv4": "192.0.2.1", "publicIPv6": "2001:db8::1", "domainSuffix": "apps.example.invalid", "certificateType": "letsencrypt"}
	target := factoryTarget(valid["url"])
	target.Config = valid
	backend, err := f.New(target)
	if err != nil {
		t.Fatal(err)
	}
	if backend.(*Backend).c.origin != valid["url"] {
		t.Fatal("incorrect client origin")
	}
	invalid := map[string][]string{
		"network":         {"", "-network", "network/name", strings.Repeat("n", 65)},
		"registryId":      {"", "two words", "registry/name", "registry\\name", "registry\tname"},
		"publicIPv4":      {"", "2001:db8::1", "192.0.2.1/24", "host.invalid"},
		"publicIPv6":      {"", "192.0.2.1", "fe80::1%eth0", "::ffff:192.0.2.1"},
		"domainSuffix":    {"", "UPPER.invalid", "-bad.invalid", "bad-.invalid", "a..invalid", "trailing.invalid.", strings.Repeat("a", 64) + ".invalid", strings.Repeat("a.", 127) + "a"},
		"certificateType": {"", "automatic", "LetsEncrypt"},
		"unknown":         {"setting"},
	}
	for key, values := range invalid {
		for _, value := range values {
			t.Run(key+"/"+value, func(t *testing.T) {
				target := factoryTarget("https://example.invalid")
				target.Config[key] = value
				if _, err := f.New(target); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("accepted invalid config: %v", err)
				}
			})
		}
	}
	for _, id := range []string{"", "env/id", "env id", "env.id"} {
		target := factoryTarget("https://example.invalid")
		target.ID = id
		if _, err := f.New(target); !errors.Is(err, infra.ErrRejected) {
			t.Fatal("accepted invalid environment ID")
		}
	}
	for _, token := range []string{"", "  "} {
		target := factoryTarget("https://example.invalid")
		target.Token = token
		if _, err := f.New(target); !errors.Is(err, infra.ErrRejected) {
			t.Fatal("accepted empty token")
		}
	}
	for _, certificate := range []string{"none", "custom"} {
		target := factoryTarget("https://example.invalid")
		target.Config["certificateType"] = certificate
		if _, err := f.New(target); err != nil {
			t.Fatal(err)
		}
	}
	original := factoryTarget("https://EXAMPLE.invalid:443/")
	b, err := f.New(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Config["url"] = "https://changed.invalid"
	if b.(*Backend).target.Config["url"] != "https://example.invalid" {
		t.Fatal("target config aliases caller map")
	}
}

func TestFactoryEnvironmentAndInfo(t *testing.T) {
	env := map[string]string{"KARAKORAM_DOKPLOY_ENVIRONMENT_ID": "env", "KARAKORAM_DOKPLOY_API_KEY": "env-test-key"}
	wantConfig := make(map[string]string)
	for _, entry := range configEnv {
		env[entry.env] = "value-" + entry.key
		wantConfig[entry.key] = "value-" + entry.key
	}
	f := newFactory(factoryEnv(nil))
	target, err := f.TargetFromEnv(factoryEnv(env))
	if err != nil || target.Kind != "dokploy" || target.ID != "env" || target.Token != "env-test-key" || !reflect.DeepEqual(target.Config, wantConfig) {
		t.Fatal("incorrect environment mapping")
	}
	empty, err := f.TargetFromEnv(factoryEnv(nil))
	if err != nil || len(empty.Config) != 0 {
		t.Fatal("empty environment values must be omitted")
	}
	info := f.Info()
	wantKeys := []string{"url", "network", "registryId", "publicIPv4", "publicIPv6", "domainSuffix", "certificateType"}
	if info.Kind != "dokploy" || info.DisplayName != "Dokploy (Docker Swarm)" || !info.Experimental || !info.Relay || !reflect.DeepEqual(info.ConfigKeys, wantKeys) || info.Capabilities != dokployCapabilities {
		t.Fatalf("incorrect factory info: %+v", info)
	}
	info.ConfigKeys[0] = "changed"
	if f.Info().ConfigKeys[0] != "url" {
		t.Fatal("Info returned mutable shared keys")
	}
}

func TestFactoryCapturesProcessPolicy(t *testing.T) {
	t.Setenv("KARAKORAM_DOKPLOY_ALLOWED_URLS", "https://example.invalid")
	t.Setenv("KARAKORAM_DOKPLOY_URL", "")
	f := NewFactory()
	t.Setenv("KARAKORAM_DOKPLOY_ALLOWED_URLS", "https://other.invalid")
	if _, err := f.New(factoryTarget("https://example.invalid")); err != nil {
		t.Fatal("factory did not capture initial policy")
	}
	if _, err := f.New(factoryTarget("https://other.invalid")); !errors.Is(err, infra.ErrRejected) {
		t.Fatal("policy changed after construction")
	}
}

func TestFactoryRegistryIntegration(t *testing.T) {
	t.Setenv("KARAKORAM_DOKPLOY_ALLOWED_URLS", "")
	t.Setenv("KARAKORAM_DOKPLOY_URL", "")
	registry, err := infra.NewRegistry(NewFactory())
	if err != nil {
		t.Fatal(err)
	}
	target := factoryTarget("https://example.invalid")
	if _, err := registry.New(target); err != nil {
		t.Fatal(err)
	}
	target.Config["unknown"] = "value"
	if _, err := registry.New(target); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("unknown config not rejected: %v", err)
	}
}
