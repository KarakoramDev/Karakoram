// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package configtest supplies the cross-engine native IPv6 rendering contract.
// Package-local adapters feed actual rendered settings to the same check, so
// private renderers need not be exported solely for qualification.
package configtest

import (
	"net"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

const Member = "member00"
const Peer = "member01"
const IPv6 = "fd00::12"
const PeerIPv6 = "fd00::13"
const IPv4 = "10.0.0.12"
const PeerIPv4 = "10.0.0.13"

// NetworkConfig describes network-facing settings, not intentionally local-only
// admin/metrics/recovery sockets. Endpoints must include a port; hosts must not.
type NetworkConfig struct {
	Listeners           []string
	LocalOnlyListeners  []string
	AdvertisedHosts     []string
	AdvertisedEndpoints []string
}

// CheckIPv6NativeConfig checks the AAAA-only member/peer fixture.
func CheckIPv6NativeConfig(t *testing.T, c NetworkConfig) {
	t.Helper()
	checkNativeConfig(t, c, true)
}

// CheckIPv4NativeConfig checks the A-only fixture without assuming IPv6 sockets
// are available on the member host.
func CheckIPv4NativeConfig(t *testing.T, c NetworkConfig) {
	t.Helper()
	checkNativeConfig(t, c, false)
}

func checkNativeConfig(t *testing.T, c NetworkConfig, ipv6 bool) {
	t.Helper()
	if len(c.Listeners) == 0 && len(c.LocalOnlyListeners) == 0 {
		t.Fatal("adapter supplied no native listener")
	}
	host := func(value string) string {
		if value == Member {
			if !ipv6 {
				return IPv4
			}
			return IPv6
		}
		if value == Peer {
			if !ipv6 {
				return PeerIPv4
			}
			return PeerIPv6
		}
		return value
	}
	endpoint := func(value string) string {
		t.Helper()
		if strings.Contains(value, "://") {
			u, err := url.Parse(value)
			if err != nil {
				t.Fatalf("invalid endpoint %q: %v", value, err)
			}
			value = u.Host
		}
		h, p, err := net.SplitHostPort(value)
		if err != nil || p == "" {
			t.Fatalf("endpoint must be IPv6-safe host:port: %q (%v)", value, err)
		}
		return h
	}
	for _, value := range c.Listeners {
		h := value
		if net.ParseIP(h) == nil && h != "*" {
			h = endpoint(h)
		}
		if h == "*" || h == "" {
			continue
		}
		ip := net.ParseIP(h)
		if ip == nil || (ip.To4() == nil) != ipv6 {
			t.Errorf("listener has wrong address family (IPv6=%t): %q", ipv6, value)
		}
	}
	for _, value := range c.LocalOnlyListeners {
		ip := net.ParseIP(endpoint(value))
		if ip == nil || !ip.IsLoopback() {
			t.Errorf("local-only listener was widened: %q", value)
		}
	}
	advertised := append([]string(nil), c.AdvertisedHosts...)
	for _, value := range c.AdvertisedEndpoints {
		advertised = append(advertised, endpoint(value))
	}
	for _, value := range advertised {
		ip := net.ParseIP(host(value))
		if ip == nil || (ip.To4() == nil) != ipv6 || ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
			t.Errorf("advertised/peer host is not routable in requested family (IPv6=%t): %q", ipv6, value)
		}
	}
}

// Settings reads line-oriented native key/value config or process environment.
func Settings(text, separator string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		key, value, ok := strings.Cut(line, separator)
		if ok {
			out[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return out
}

// CheckIPv6Rendered scans every rendered file, argument and environment entry,
// including fields not selected by the structured listener/advertise adapter.
// Exceptions must name the exact legitimate fragment and explain why it is local
// or a deliberate dual-stack companion; never allow an IPv4 address globally.
func CheckIPv6Rendered(t *testing.T, files map[string][]byte, args, env []string, allowed map[string]string) {
	t.Helper()
	for fragment, reason := range allowed {
		if fragment == "" || reason == "" {
			t.Fatal("IPv4 exception requires an exact fragment and reason")
		}
	}
	check := func(name, text string) {
		t.Helper()
		for fragment := range allowed {
			text = strings.ReplaceAll(text, fragment, "")
		}
		if literal := ipv4Only.FindString(text); literal != "" {
			t.Errorf("%s contains unqualified IPv4-only setting %q", name, literal)
		}
	}
	for name, data := range files {
		check(name, string(data))
	}
	for _, arg := range args {
		check("argument", arg)
	}
	for _, entry := range env {
		check("environment", entry)
	}
}

var ipv4Only = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b|\btcp4\b`)
