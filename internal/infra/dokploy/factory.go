// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var dokployCapabilities = infra.Capabilities{PublicTCP: true, PublicHTTPS: true, Placement: true, Logs: true, HorizontalScaling: true}

var configEnv = [...]struct{ key, env string }{
	{"url", "KARAKORAM_DOKPLOY_URL"},
	{"network", "KARAKORAM_DOKPLOY_NETWORK"},
	{"registryId", "KARAKORAM_DOKPLOY_REGISTRY_ID"},
	{"publicIPv4", "KARAKORAM_DOKPLOY_PUBLIC_IPV4"},
	{"publicIPv6", "KARAKORAM_DOKPLOY_PUBLIC_IPV6"},
	{"domainSuffix", "KARAKORAM_DOKPLOY_DOMAIN_SUFFIX"},
	{"certificateType", "KARAKORAM_DOKPLOY_CERTIFICATE_TYPE"},
}

var environmentID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var networkName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
var registryToken = regexp.MustCompile(`^[^\s/\\]+$`)
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

var urlSharedAddressPrefix = netip.MustParsePrefix("100.64.0.0/10")
var urlThisNetworkPrefix = netip.MustParsePrefix("0.0.0.0/8")

type factory struct {
	allowed       map[string]bool
	restricted    bool
	invalidPolicy bool
}

// NewFactory captures the operator's URL policy at construction time. Private
// origins require explicit operator approval; omitted networks are isolated by
// environment using karakoram-<first four SHA-256 bytes of the environment ID>.
func NewFactory() infra.Factory { return newFactory(os.Getenv) }

func newFactory(getenv func(string) string) factory {
	f := factory{allowed: make(map[string]bool)}
	allowlist := getenv("KARAKORAM_DOKPLOY_ALLOWED_URLS")
	f.restricted = allowlist != ""
	entries := []string{}
	if allowlist != "" {
		entries = append(entries, strings.Split(allowlist, ",")...)
	}
	if base := getenv("KARAKORAM_DOKPLOY_URL"); base != "" {
		entries = append(entries, base)
	}
	for _, entry := range entries {
		origin, err := normalizeOrigin(strings.TrimSpace(entry))
		if err != nil {
			f.invalidPolicy = true
			continue
		}
		f.allowed[origin] = true
	}
	return f
}

func (f factory) Info() infra.BackendInfo {
	keys := make([]string, 0, len(configEnv))
	for _, entry := range configEnv {
		keys = append(keys, entry.key)
	}
	return infra.BackendInfo{Kind: "dokploy", DisplayName: "Dokploy (Docker Swarm)", Experimental: true, Relay: true, Capabilities: dokployCapabilities, ConfigKeys: keys}
}

func (f factory) TargetFromEnv(getenv func(string) string) (infra.Target, error) {
	t := infra.Target{Kind: "dokploy", ID: getenv("KARAKORAM_DOKPLOY_ENVIRONMENT_ID"), Token: getenv("KARAKORAM_DOKPLOY_API_KEY"), Config: make(map[string]string)}
	for _, entry := range configEnv {
		if value := getenv(entry.env); value != "" {
			t.Config[entry.key] = value
		}
	}
	return t, nil
}

func rejectedConfig(code, message string) error {
	return &infra.RejectedError{Code: code, Message: "dokploy: " + message}
}

func urlRejected() error {
	return rejectedConfig("dokployUrlNotAllowed", "URL must be an allowed HTTP(S) origin; configure KARAKORAM_DOKPLOY_ALLOWED_URLS (and check KARAKORAM_DOKPLOY_URL)")
}

func normalizeOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return "", urlRejected()
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(host, "%\\") {
		return "", urlRejected()
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", urlRejected()
		}
		port = strconv.Itoa(n)
	} else if strings.HasSuffix(u.Host, ":") {
		return "", urlRejected()
	}
	if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		port = ""
	}
	if strings.Contains(host, ":") {
		address, err := netip.ParseAddr(host)
		if err != nil || !address.Is6() {
			return "", urlRejected()
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

// implicitHTTPSOrigin permits public HTTPS origins without operator allowlisting.
// Literal local/private destinations and localhost names must be explicitly
// trusted through the captured operator URL policy, even when they use HTTPS.
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
		return !address.IsPrivate() && !address.IsLoopback() &&
			!address.IsLinkLocalUnicast() && !address.IsLinkLocalMulticast() &&
			!address.IsUnspecified() && !address.Is4In6() &&
			!urlSharedAddressPrefix.Contains(address) && !urlThisNetworkPrefix.Contains(address)
	}
	return true
}

func defaultNetworkName(envID string) string {
	sum := sha256.Sum256([]byte(envID))
	return "karakoram-" + hex.EncodeToString(sum[:4])
}

func (f factory) New(t infra.Target) (infra.Backend, error) {
	origin, err := normalizeOrigin(t.Config["url"])
	if err != nil || f.invalidPolicy || (!f.allowed[origin] && (f.restricted || !implicitHTTPSOrigin(origin))) {
		return nil, urlRejected()
	}
	if t.Kind != "dokploy" || !environmentID.MatchString(t.ID) || strings.TrimSpace(t.Token) == "" {
		return nil, rejectedConfig("dokployInvalidTarget", "kind, environment ID and API key are required; environment ID accepts only letters, digits, underscores and hyphens")
	}
	for key, value := range t.Config {
		valid := true
		switch key {
		case "url":
		case "network":
			valid = networkName.MatchString(value)
		case "registryId":
			valid = registryToken.MatchString(value) && strings.IndexFunc(value, unicode.IsSpace) < 0
		case "publicIPv4":
			ip, err := netip.ParseAddr(value)
			valid = err == nil && ip.Is4()
		case "publicIPv6":
			ip, err := netip.ParseAddr(value)
			valid = err == nil && ip.Is6() && !ip.Is4In6() && ip.Zone() == ""
		case "domainSuffix":
			valid = value != "" && len(value) <= 253
			for _, label := range strings.Split(value, ".") {
				valid = valid && dnsLabel.MatchString(label)
			}
		case "certificateType":
			valid = value == "letsencrypt" || value == "none" || value == "custom"
		default:
			return nil, rejectedConfig("unknownConfigKey", "unknown target setting")
		}
		if !valid {
			return nil, rejectedConfig("dokployInvalidConfig", "invalid "+key+" setting")
		}
	}
	// Keep an immutable private copy: callers may reuse or modify their target.
	config := make(map[string]string, len(t.Config))
	for key, value := range t.Config {
		config[key] = value
	}
	config["url"] = origin
	if _, supplied := config["network"]; !supplied {
		config["network"] = defaultNetworkName(t.ID)
	}
	t.Config = config
	c := newClient(origin, t.Token)
	return &Backend{c: c, target: t}, nil
}

func (f factory) Validate(ctx context.Context, t infra.Target) (infra.Backend, infra.Project, error) {
	backend, err := f.New(t)
	if err != nil {
		return nil, infra.Project{}, err
	}
	b := backend.(*Backend)
	project, err := b.validate(ctx)
	if err != nil {
		return nil, project, err
	}
	return b, project, nil
}
