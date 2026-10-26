// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func factoryTarget(address string) infra.Target {
	return infra.Target{Kind: "nomad", ID: "default", Token: fakeNomadToken, Config: map[string]string{"address": address}}
}

func TestFactoryAddressPolicyWiring(t *testing.T) {
	for _, tc := range []struct {
		name, address, operator, allowed string
		ok                               bool
	}{
		{"public HTTPS", "https://nomad.example.test", "", "", true},
		{"private denied", "https://127.0.0.1:4646", "", "", false},
		{"private allowed", "https://127.0.0.1:4646", "", "https://127.0.0.1:4646", true},
		{"operator private", "http://127.0.0.1:4646", "http://127.0.0.1:4646", "", true},
		{"HTTP denied", "http://nomad.example.test:4646", "", "", false},
		{"operator allowed", "http://nomad.example.test:4646", "http://nomad.example.test:4646", "", true},
		{"allowlist exclusive", "https://other.example.test", "", "https://nomad.example.test", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFactory(testEnv(map[string]string{"KARAKORAM_NOMAD_ADDR": tc.operator, "KARAKORAM_NOMAD_ALLOWED_ADDRS": tc.allowed}))
			_, err := f.New(factoryTarget(tc.address))
			if (err == nil) != tc.ok {
				t.Fatalf("address policy: %v", err)
			}
			if !tc.ok {
				if !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("wrong rejection: %v", err)
				}
				assertSafeMessage(t, err.Error(), tc.address)
				if _, _, err = f.Validate(t.Context(), factoryTarget(tc.address)); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("Validate bypassed policy: %v", err)
				}
			}
		})
	}
}

func TestWithAllowedAddrsIsRestrictive(t *testing.T) {
	for _, origins := range [][]string{nil, {"https://approved.example.test"}} {
		f := newFactory(testEnv(map[string]string{"KARAKORAM_NOMAD_ADDR": "https://operator.example.test"}), WithAllowedAddrs(origins...))
		if _, err := f.New(factoryTarget("https://unapproved.example.test")); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("explicit allowlist permitted arbitrary public HTTPS: %v", err)
		}
		if _, err := f.New(factoryTarget("https://operator.example.test")); err != nil {
			t.Fatal("operator origin no longer approved:", err)
		}
		if len(origins) > 0 {
			if _, err := f.New(factoryTarget(origins[0])); err != nil {
				t.Fatal("explicit origin rejected:", err)
			}
		}
	}
}

func TestFactoryValidateScopedToken(t *testing.T) {
	f := newFakeNomad(t)
	f.addNamespace("other")
	factory := newFactory(testEnv(nil), WithHTTPClient(f.Client()), WithAllowedAddrs(f.URL))
	backend, project, err := factory.Validate(t.Context(), factoryTarget(f.URL))
	if err != nil || backend == nil || project.ID != "default" {
		t.Fatalf("valid scoped namespace token: %+v %v", project, err)
	}
	target := factoryTarget(f.URL)
	target.ID = "other"
	if _, _, err = factory.Validate(t.Context(), target); !errors.Is(err, infra.ErrUnauthorized) {
		t.Fatalf("cross-namespace scope: %v", err)
	} else {
		assertSafeMessage(t, err.Error(), f.URL)
	}
	target = factoryTarget(f.URL)
	target.Token = "unknown-test-token"
	if _, _, err = factory.Validate(t.Context(), target); !errors.Is(err, infra.ErrUnauthorized) {
		t.Fatalf("unknown token: %v", err)
	} else if strings.Contains(err.Error(), target.Token) {
		t.Fatal("unauthorized error exposed token")
	}
}

func TestFactoryTLSEnvironmentErrors(t *testing.T) {
	for _, values := range []map[string]string{
		{"KARAKORAM_NOMAD_CACERT": "/not-a-real-certificate.pem"},
		{"KARAKORAM_NOMAD_CLIENT_CERT": "/not-a-real-certificate.pem"},
		{"KARAKORAM_NOMAD_CLIENT_KEY": "/not-a-real-key.pem"},
		{"KARAKORAM_NOMAD_CLIENT_CERT": "/not-a-real-certificate.pem", "KARAKORAM_NOMAD_CLIENT_KEY": "/not-a-real-key.pem"},
	} {
		f := newFactory(testEnv(values))
		target := factoryTarget("https://nomad.example.test")
		if _, err := f.New(target); err == nil {
			t.Fatal("invalid TLS environment accepted by New")
		} else {
			assertSafeMessage(t, err.Error(), target.Config["address"])
		}
		if _, _, err := f.Validate(t.Context(), target); err == nil {
			t.Fatal("Validate bypassed invalid TLS environment")
		}
	}
}

func TestFactoryConfigAndEnvironment(t *testing.T) {
	values := map[string]string{"KARAKORAM_NOMAD_NAMESPACE": "tenant", "KARAKORAM_NOMAD_TOKEN": "environment-token"}
	want := map[string]string{"address": "https://nomad.example.test", "region": "global", "datacenters": "dc1,dc2", "nodePool": "default", "network": "member", "publicIPv4": "192.0.2.1", "publicIPv6": "2001:db8::1", "ephemeralDiskMB": "6144"}
	for _, entry := range configEnv {
		values[entry.env] = want[entry.key]
	}
	f := newFactory(testEnv(nil))
	target, err := f.TargetFromEnv(testEnv(values))
	if err != nil || target.Kind != "nomad" || target.ID != "tenant" || target.Token != "environment-token" || !reflect.DeepEqual(target.Config, want) {
		t.Fatal("incorrect Nomad environment mapping")
	}
	info := f.Info()
	if info.Kind != "nomad" || !info.Experimental || !info.Relay || info.DisplayName != "HashiCorp Nomad" {
		t.Fatalf("factory metadata: %+v", info)
	}
	keys := []string{"address", "region", "datacenters", "nodePool", "network", "publicIPv4", "publicIPv6", "ephemeralDiskMB"}
	if !reflect.DeepEqual(info.ConfigKeys, keys) {
		t.Fatalf("config discovery keys: %v", info.ConfigKeys)
	}
	info.ConfigKeys[0] = "changed"
	if f.Info().ConfigKeys[0] != "address" {
		t.Fatal("Info aliases config keys")
	}
	if _, err = f.New(target); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"unknown": "value", "nodePool": "bad/pool", "network": "bad/network", "datacenters": "dc1,,dc2", "publicIPv4": "2001:db8::1", "publicIPv6": "192.0.2.1"} {
		target := factoryTarget("https://nomad.example.test")
		target.Config[key] = value
		if _, err = f.New(target); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("invalid %s accepted: %v", key, err)
		}
	}
	original := factoryTarget("https://nomad.example.test")
	backend, err := f.New(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Config["address"] = "https://changed.example.test"
	if backend.(*Backend).target.Config["address"] != "https://nomad.example.test" {
		t.Fatal("backend aliases caller config")
	}
}
