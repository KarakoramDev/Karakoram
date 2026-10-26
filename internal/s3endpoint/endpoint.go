// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package s3endpoint defines the storage registration endpoint contract shared by
// admission, the Go object client and native-engine configuration renderers.
package s3endpoint

import (
	"errors"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// ErrPathPrefix is separate so callers can give existing registrations precise,
// credential-free remediation without including the configured endpoint.
var ErrPathPrefix = errors.New("S3 endpoint path prefixes are unsupported by native backup engines; use an HTTP(S) origin and put object key prefixes in prefix (a reverse-proxy mount such as /s3 requires an origin-level S3 endpoint)")

// Parse accepts only endpoint origins supported by all native consumers. It does
// not resolve DNS or relax the Go client's independent outbound-address policy.
// Errors are fixed strings and never contain the input URL.
func Parse(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(endpoint, "#") {
		return nil, errors.New("S3 endpoint must be an HTTP(S) origin without credentials, query or fragment; include http:// or https://")
	}
	if (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return nil, ErrPathPrefix
	}
	host := u.Hostname()
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" {
			return nil, errors.New("S3 endpoint scoped IPv6 addresses are unsupported by native backup engines; use an unscoped IPv6 or DNS endpoint")
		}
		if ip.Is4() && strings.HasPrefix(u.Host, "[") {
			return nil, errors.New("S3 endpoint IPv4 literals must not be bracketed; brackets are only supported for IPv6")
		}
		if ip.Is4In6() && strings.Contains(host, ".") {
			return nil, errors.New("S3 endpoint dotted IPv4-mapped IPv6 addresses are unsupported by native backup engines; use an IPv4 literal or hex-only IPv6 endpoint")
		}
	} else if !s3utils.IsValidDomain(host) {
		// Match the existing object SDK's hostname contract, including legacy
		// internal names with underscores; do not introduce stricter DNS rules.
		return nil, errors.New("S3 endpoint host must be a DNS name, IPv4 literal or bracketed unscoped IPv6 literal")
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("S3 endpoint port must be an integer from 1 to 65535")
	}
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || p[0] == '0' || port < 1 || port > 65535 {
			return nil, errors.New("S3 endpoint port must be an integer from 1 to 65535 without leading zeros")
		}
	}
	return u, nil
}

// PathStyle treats false as a preference for virtual hosting, not a request to
// construct unusable hosts. IP endpoints and non-DNS buckets cannot be virtual
// hosted; dotted HTTPS buckets do not fit a single-label wildcard certificate.
// Custom DNS endpoints otherwise retain virtual hosting (unlike MinIO Auto).
// This predicate also works for legacy Supabase endpoints with retained paths.
func PathStyle(endpoint *url.URL, bucket string, force bool) bool {
	if force {
		return true
	}
	_, err := netip.ParseAddr(endpoint.Hostname())
	return err == nil || s3utils.CheckValidBucketNameStrict(bucket) != nil || endpoint.Scheme == "https" && strings.Contains(bucket, ".")
}
