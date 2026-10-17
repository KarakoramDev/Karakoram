// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var nomadCapabilities = infra.Capabilities{PublicTCP: true, Placement: true, Logs: true, HorizontalScaling: true, DedicatedCPU: true}
var configEnv = [...]struct{ key, env string }{
	{"address", "KARAKORAM_NOMAD_ADDR"}, {"region", "KARAKORAM_NOMAD_REGION"}, {"datacenters", "KARAKORAM_NOMAD_DATACENTERS"}, {"nodePool", "KARAKORAM_NOMAD_NODE_POOL"}, {"network", "KARAKORAM_NOMAD_NETWORK"}, {"publicIPv4", "KARAKORAM_NOMAD_PUBLIC_IPV4"}, {"publicIPv6", "KARAKORAM_NOMAD_PUBLIC_IPV6"},
}
var namespaceName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var settingName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type FactoryOption func(*factory)
type factory struct {
	policy      addrPolicy
	tls         *tls.Config
	err         error
	http        *http.Client
	artifacts   *artifactResolver
	artifactURL string
}

// WithHTTPClient supplies an API transport, primarily for embedded integrations.
func WithHTTPClient(h *http.Client) FactoryOption { return func(f *factory) { f.http = h } }

// WithAllowedAddrs restricts API origins to these and explicitly trusted operator origins.
func WithAllowedAddrs(origins ...string) FactoryOption {
	return func(f *factory) {
		f.policy.restricted = true
		for _, raw := range origins {
			origin, err := normalizeOrigin(raw)
			if err != nil {
				f.err = err
				continue
			}
			f.policy.allowed[origin] = true
		}
	}
}

// WithArtifactURL supplies a public archive mirror whose binary is verified at deploy.
func WithArtifactURL(source string) FactoryOption { return func(f *factory) { f.artifactURL = source } }

// NewFactory captures operator address policy and TLS credentials without contacting Nomad.
func NewFactory(opts ...FactoryOption) infra.Factory { return newFactory(os.Getenv, opts...) }
func newFactory(getenv func(string) string, opts ...FactoryOption) factory {
	t, err := loadTLS(getenv)
	f := factory{policy: newAddrPolicy(getenv), tls: t, err: err, artifacts: newArtifactResolver(getenv, nil, nil)}
	for _, opt := range opts {
		opt(&f)
	}
	return f
}
func (f factory) Info() infra.BackendInfo {
	keys := make([]string, 0, len(configEnv))
	for _, e := range configEnv {
		keys = append(keys, e.key)
	}
	return infra.BackendInfo{Kind: "nomad", DisplayName: "HashiCorp Nomad", Experimental: true, Relay: true, Capabilities: nomadCapabilities, ConfigKeys: keys}
}
func (f factory) TargetFromEnv(getenv func(string) string) (infra.Target, error) {
	t := infra.Target{Kind: "nomad", ID: getenv("KARAKORAM_NOMAD_NAMESPACE"), Token: getenv("KARAKORAM_NOMAD_TOKEN"), Config: map[string]string{}}
	for _, e := range configEnv {
		if v := getenv(e.env); v != "" {
			t.Config[e.key] = v
		}
	}
	return t, nil
}
func (f factory) New(t infra.Target) (infra.Backend, error) {
	if f.err != nil {
		return nil, f.err
	}
	origin, err := f.policy.check(t.Config["address"])
	if err != nil {
		return nil, err
	}
	if t.Kind != "nomad" || !namespaceName.MatchString(t.ID) || strings.TrimSpace(t.Token) == "" {
		return nil, reject("kind, namespace and token are required; namespace must contain only letters, digits, underscores and hyphens")
	}
	config := make(map[string]string, len(t.Config))
	for key, v := range t.Config {
		valid := true
		switch key {
		case "address":
		case "region", "nodePool", "network":
			valid = settingName.MatchString(v)
		case "datacenters":
			for _, dc := range strings.Split(v, ",") {
				valid = valid && (dc == "*" || settingName.MatchString(strings.TrimSpace(dc)))
			}
		case "publicIPv4":
			ip, e := netip.ParseAddr(v)
			valid = e == nil && ip.Is4()
		case "publicIPv6":
			ip, e := netip.ParseAddr(v)
			valid = e == nil && ip.Is6() && !ip.Is4In6() && ip.Zone() == ""
		default:
			return nil, rejectedConfig("unknownConfigKey", "unknown target setting")
		}
		if !valid {
			return nil, reject("invalid " + key + " setting")
		}
		config[key] = v
	}
	config["address"] = origin
	if config["datacenters"] == "" {
		config["datacenters"] = "*"
	}
	if config["nodePool"] == "" {
		config["nodePool"] = "default"
	}
	if config["network"] == "" {
		config["network"] = "karakoram"
	}
	t.Config = config
	c := newClient(origin, config["region"], t.ID, t.Token, f.tls)
	if f.http != nil {
		h := *f.http
		c.http = &h
	}
	return &Backend{c: c, target: t, artifacts: f.artifacts, artifactURL: f.artifactURL}, nil
}
func (f factory) Validate(ctx context.Context, t infra.Target) (infra.Backend, infra.Project, error) {
	backend, err := f.New(t)
	if err != nil {
		return nil, infra.Project{}, err
	}
	b := backend.(*Backend)
	p, err := b.ProjectInfo(ctx)
	if err != nil {
		return nil, p, err
	}
	if _, err = b.c.get(ctx, "/v1/jobs", nil, nil); err != nil {
		return nil, p, err
	}
	if _, err = b.c.get(ctx, "/v1/vars", query("prefix", "karakoram/"), nil); err != nil {
		return nil, p, err
	}
	return b, p, nil
}
