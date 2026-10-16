// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestRequiredCapabilities(t *testing.T) {
	for _, tt := range []struct {
		name string
		spec domain.ClusterSpec
		want []string
	}{
		{name: "private"},
		{name: "private allowlist", spec: domain.ClusterSpec{Exposure: domain.Exposure{Allowlist: []string{"192.0.2.0/24"}}}},
		{name: "public", spec: domain.ClusterSpec{Exposure: domain.Exposure{Public: true}}, want: []string{"publicTcp"}},
		{name: "allowlisted public", spec: domain.ClusterSpec{Exposure: domain.Exposure{Public: true, Allowlist: []string{"192.0.2.0/24"}}}, want: []string{"publicTcp", "tcpAllowlist"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := RequiredCapabilities(tt.spec); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
	for _, name := range []string{"engine", "configServer", "router", "extra"} {
		t.Run("dedicated "+name, func(t *testing.T) {
			spec := domain.ClusterSpec{}
			component := domain.ComponentSpec{Resources: domain.Resources{CPUMode: "dedicated"}}
			switch name {
			case "engine":
				spec.Components.Engine = component
			case "configServer":
				spec.Components.ConfigServer = component
			case "router":
				spec.Components.Router = component
			case "extra":
				spec.Components.Extra = map[string]domain.ComponentSpec{"auth": component, "rest": component}
			}
			if got := RequiredCapabilities(spec); !reflect.DeepEqual(got, []string{"dedicatedCpu"}) {
				t.Fatal(got)
			}
		})
	}
}

type capabilityTestProvider struct {
	Provider
	required []string
}

func (p capabilityTestProvider) RequiredCapabilities(domain.ClusterSpec) []string { return p.required }

func TestCheckCapabilities(t *testing.T) {
	spec := domain.ClusterSpec{Exposure: domain.Exposure{Public: true, Allowlist: []string{"192.0.2.0/24"}}}
	spec.Components.Engine.Resources.CPUMode = "DEDICATED"
	provider := capabilityTestProvider{required: []string{"publicTcp", "publicHttps", "horizontalScaling", "horizontalScaling"}}
	err := CheckCapabilities(provider, spec, "limited", infra.Capabilities{PublicTCP: true})
	if !errors.Is(err, ErrNotSupported) || !strings.Contains(err.Error(), "backend limited does not provide tcpAllowlist, dedicatedCpu, publicHttps, horizontalScaling") {
		t.Fatal(err)
	}
	if strings.Count(err.Error(), "horizontalScaling") != 1 {
		t.Fatal("duplicate missing capability", err)
	}
	have := infra.Capabilities{PublicTCP: true, TCPAllowlist: true, DedicatedCPU: true, PublicHTTPS: true, HorizontalScaling: true}
	if err := CheckCapabilities(provider, spec, "capable", have); err != nil {
		t.Fatal(err)
	}
	// Providers without the optional interface still receive neutral admission checks.
	if err := CheckCapabilities(nil, spec, "limited", infra.Capabilities{}); !errors.Is(err, ErrNotSupported) {
		t.Fatal(err)
	}
	if err := CheckCapabilities(nil, domain.ClusterSpec{}, "limited", infra.Capabilities{}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicTCPAllowlistAdmission(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ipType     string
		allowlist  []string
		allowAll   bool
		restricted bool
	}{
		{"ipv4 all", "IPV4", []string{"0.0.0.0/0"}, true, false},
		{"ipv6 all", "IPV6", []string{"::/0"}, true, false},
		{"ipv4 split coverage", "IPV4", []string{"0.0.0.0/1", "128.0.0.0/1"}, true, false},
		{"ipv6 split coverage", "IPV6", []string{"::/1", "8000::/1"}, true, false},
		{"unacknowledged all", "IPV4", []string{"0.0.0.0/0"}, false, true},
		{"restricted acknowledged", "IPV4", []string{"192.0.2.0/24"}, true, true},
		{"restricted", "IPV4", []string{"192.0.2.0/24"}, false, true},
		{"wrong family", "IPV6", []string{"0.0.0.0/0"}, true, true},
		{"invalid", "IPV4", []string{"invalid"}, true, true},
		{"no allowlist", "IPV4", nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := domain.Exposure{Public: true, IPType: tc.ipType, Allowlist: tc.allowlist, AllowAllAddresses: tc.allowAll}
			want := []string{"publicTcp"}
			if tc.restricted {
				want = append(want, "tcpAllowlist")
			}
			if got := RequiredCapabilities(domain.ClusterSpec{Exposure: ex}); !reflect.DeepEqual(got, want) {
				t.Fatalf("required capabilities = %v, want %v", got, want)
			}
			tcpOnly := fake.New(fake.Options{Capabilities: infra.Capabilities{PublicTCP: true}})
			err := RequirePublicTCP(tcpOnly, ex)
			if tc.restricted && !errors.Is(err, ErrNotSupported) || !tc.restricted && err != nil {
				t.Fatalf("runtime admission = %v, restricted = %t", err, tc.restricted)
			}
			if err := RequirePublicTCP(fake.New(fake.Options{}), ex); err != nil {
				t.Fatalf("allowlist-capable backend rejected exposure: %v", err)
			}
			noTCP := fake.New(fake.Options{Capabilities: infra.Capabilities{Placement: true}})
			if err := RequirePublicTCP(noTCP, ex); !errors.Is(err, ErrNotSupported) {
				t.Fatalf("backend without public TCP admitted exposure: %v", err)
			}
		})
	}
}
