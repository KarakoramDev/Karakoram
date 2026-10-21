// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package netguard implements DNS-rebinding-safe outbound connection policy.
package netguard

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

var ErrNotAllowed = errors.New("endpoint is not permitted")

// Policy has no implicit trusted hosts. S3Compatibility preserves the historical
// backup policy (including its unconditional loopback refusal and origin fence).
type Policy struct {
	Allowlist       []string
	S3Compatibility bool
	Endpoint        *url.URL
}

func ParseAllowlist(value string) []string {
	var entries []string
	for _, entry := range strings.Split(value, ",") {
		if entry = strings.ToLower(strings.TrimSpace(entry)); entry != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (p *Policy) Allowlisted(host, port string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, entry := range p.Allowlist {
		if entry == net.JoinHostPort(host, port) || (strings.TrimSuffix(entry, ".") == host && (port == "80" || port == "443")) {
			return true
		}
	}
	return false
}

var (
	sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")
	metadataAWS        = netip.MustParseAddr("fd00:ec2::254")
	metadataAzure      = netip.MustParseAddr("168.63.129.16")
	metadataOracle     = netip.MustParseAddr("192.0.0.192")
)

// AddressAllowed never permits cloud metadata, link-local or shared addresses.
// Only notification policies permit explicitly allowlisted loopback destinations.
func AddressAllowed(ip netip.Addr, approved, allowLoopback bool) bool {
	ip = ip.Unmap()
	if ip.Zone() != "" || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return false
	}
	if ip.IsLoopback() {
		return approved && allowLoopback
	}
	if !ip.IsGlobalUnicast() || sharedAddressSpace.Contains(ip) || ip == metadataAWS || ip == metadataAzure || ip == metadataOracle {
		return false
	}
	return !ip.IsPrivate() || approved
}

func urlPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

// CheckURL performs syntax and literal-address checks; dialing revalidates DNS.
func (p *Policy) CheckURL(u *url.URL, requireHTTPS bool) error {
	if u == nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ErrNotAllowed
	}
	approved := p.Allowlisted(u.Hostname(), urlPort(u))
	if requireHTTPS && u.Scheme != "https" && !approved {
		return ErrNotAllowed
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !AddressAllowed(ip, approved, !p.S3Compatibility) {
		return ErrNotAllowed
	}
	return nil
}

func (p *Policy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		if p.S3Compatibility {
			return nil, err
		}
		return nil, ErrNotAllowed
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	approved := p.Allowlisted(host, port)
	if p.S3Compatibility && p.Endpoint != nil {
		endpointHost := strings.ToLower(strings.TrimSuffix(p.Endpoint.Hostname(), "."))
		endpointPort := urlPort(p.Endpoint)
		approved = p.Allowlisted(endpointHost, endpointPort)
		if port != endpointPort || (host != endpointHost && !strings.HasSuffix(host, "."+endpointHost)) || (!approved && port != "80" && port != "443") {
			return nil, &PolicyError{"S3 endpoint destination or port is not permitted"}
		}
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		if p.S3Compatibility {
			return nil, errors.New("S3 endpoint has no addresses")
		}
		return nil, ErrNotAllowed
	}
	for _, ip := range addresses {
		if !AddressAllowed(ip, approved, !p.S3Compatibility) {
			if p.S3Compatibility {
				return nil, &PolicyError{"S3 endpoint address is not permitted"}
			}
			return nil, ErrNotAllowed
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	var dialErr error
	for _, ip := range addresses {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		dialErr = err
	}
	return nil, dialErr
}

// PolicyError carries only a fixed, credential-free diagnostic.
type PolicyError struct{ Message string }

func (e *PolicyError) Error() string { return e.Message }
func (e *PolicyError) Unwrap() error { return ErrNotAllowed }

func (p *Policy) HTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = p.DialContext
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	// Return the un-followed response so callers can report only its status code.
	return &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
