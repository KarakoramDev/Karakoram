// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var urlSharedAddressPrefix = netip.MustParsePrefix("100.64.0.0/10")
var urlThisNetworkPrefix = netip.MustParsePrefix("0.0.0.0/8")

type addrPolicy struct {
	allowed       map[string]bool
	restricted    bool
	invalidPolicy bool
}

func newAddrPolicy(getenv func(string) string) addrPolicy {
	p := addrPolicy{allowed: make(map[string]bool)}
	allowlist := getenv("KARAKORAM_NOMAD_ALLOWED_ADDRS")
	// Never offer an operator-wide client identity to an implicitly approved
	// public origin. Even partially configured material fails closed.
	p.restricted = allowlist != "" || getenv("KARAKORAM_NOMAD_CLIENT_CERT") != "" || getenv("KARAKORAM_NOMAD_CLIENT_KEY") != ""
	var entries []string
	if allowlist != "" {
		entries = append(entries, strings.Split(allowlist, ",")...)
	}
	if base := getenv("KARAKORAM_NOMAD_ADDR"); base != "" {
		entries = append(entries, base)
	}
	for _, entry := range entries {
		origin, err := normalizeOrigin(strings.TrimSpace(entry))
		if err != nil {
			p.invalidPolicy = true
			continue
		}
		p.allowed[origin] = true
	}
	return p
}

func (p addrPolicy) check(raw string) (string, error) {
	origin, err := normalizeOrigin(raw)
	if err != nil || p.invalidPolicy || (!p.allowed[origin] && (p.restricted || !implicitHTTPSOrigin(origin))) {
		return "", addrRejected()
	}
	return origin, nil
}

func rejectedConfig(code, message string) error {
	return &infra.RejectedError{Code: code, Message: "nomad: " + message}
}

func addrRejected() error {
	return rejectedConfig("nomadAddrNotAllowed", "address must be an allowed HTTP(S) origin; configure KARAKORAM_NOMAD_ALLOWED_ADDRS (and check KARAKORAM_NOMAD_ADDR)")
}

func normalizeOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return "", addrRejected()
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(host, "%\\") {
		return "", addrRejected()
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", addrRejected()
		}
		port = strconv.Itoa(n)
	} else if strings.HasSuffix(u.Host, ":") {
		return "", addrRejected()
	}
	if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		port = ""
	}
	if strings.Contains(host, ":") {
		address, err := netip.ParseAddr(host)
		if err != nil || !address.Is6() {
			return "", addrRejected()
		}
		host = address.String()
		if port == "" {
			host = "[" + host + "]"
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	}
	return u.Scheme + "://" + host, nil
}

func implicitHTTPSOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return false
	}
	if address, err := netip.ParseAddr(host); err == nil {
		return !address.IsPrivate() && !address.IsLoopback() && !address.IsLinkLocalUnicast() && !address.IsLinkLocalMulticast() && !address.IsUnspecified() && !address.Is4In6() && !urlSharedAddressPrefix.Contains(address) && !urlThisNetworkPrefix.Contains(address)
	}
	return true
}
