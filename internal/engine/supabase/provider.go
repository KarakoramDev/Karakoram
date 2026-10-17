// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var codes = map[string]string{"gateway": "gw", "auth": "au", "rest": "rs", "realtime": "rt", "storage": "st", "imgproxy": "ip", "meta": "mt", "studio": "sd", "functions": "fn", "pooler": "pl", "analytics": "an", "vector": "vc"}
var ports = map[string]int{"gateway": 8000, "auth": 9999, "rest": 3000, "realtime": 4000, "storage": 5000, "imgproxy": 5001, "meta": 8080, "studio": 3002, "functions": 9000, "pooler": 5432, "analytics": 4000, "vector": 9002}
var stackComponents = func() engine.ComponentSet {
	var out engine.ComponentSet
	for _, name := range sbspec.Components {
		replicas := 0
		if name == "gateway" || name == "auth" || name == "rest" {
			replicas = 1
		}
		def := engine.ComponentDef{
			Name: name, DisplayName: name, MinReplicas: 1, MaxReplicas: 5, PublicHTTPS: name == "gateway",
			Defaults: domain.ComponentSpec{
				Replicas: replicas,
				Resources: domain.Resources{
					CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1,
					MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 1,
				},
			},
		}
		switch name {
		case "realtime":
			// The native release currently has no routable, distinct BEAM
			// identities or distribution/gen_rpc port contract.
			def.MaxReplicas = 1
		case "imgproxy":
			def.Requires = []string{"storage"}
		case "studio":
			def.Requires = []string{"meta", "gateway"}
		case "vector":
			def.Requires = []string{"analytics"}
		}
		out = append(out, def)
	}
	return out
}()

type Provider struct {
	*postgres.Core
	options     postgres.Options
	functionsMu sync.Mutex
}

func New(o postgres.Options) *Provider {
	flavour := &supabaseFlavour{}
	o.Flavour = flavour
	o.ReservedRole = ReservedRole
	core := postgres.New(o)
	flavour.core = core
	return &Provider{Core: core, options: o}
}
func (*Provider) Name() string { return "supabase" }
func ReservedRole(name string) bool {
	if strings.HasPrefix(name, "supabase_") || strings.HasPrefix(name, "kk_") || strings.HasPrefix(name, "pg_") {
		return true
	}
	switch name {
	case "postgres", "authenticator", "anon", "authenticated", "service_role", "dashboard_user", "pgbouncer", "supavisor":
		return true
	}
	return false
}
func (p *Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "supabase"
	}
	p.Core.ApplyDefaults(s)
	stackComponents.ApplyDefaults(&s.Components)
}
func (p *Provider) Describe() domain.ProviderInfo {
	info := p.Core.Describe()
	info.Name = "supabase"
	info.DisplayName = "Supabase"
	versions := info.Versions
	info.Versions = nil
	for _, v := range versions {
		if v.Major == "17" {
			info.Versions = append(info.Versions, v)
		}
	}
	paths := info.UpgradePaths
	info.UpgradePaths = nil
	for _, path := range paths {
		if strings.HasPrefix(path.From, "17.") && strings.HasPrefix(path.To, "17.") {
			info.UpgradePaths = append(info.UpgradePaths, path)
		}
	}
	info.Operations = append(info.Operations, "rotate-jwt-secret")
	info.Capabilities = append(info.Capabilities, "supabaseAccess")
	info.Processes = append(info.Processes, sbspec.Components...)
	componentInfo := stackComponents.Info()
	for i := range info.Topologies {
		for _, def := range stackComponents {
			c := componentInfo[def.Name]
			c.Schema, _ = json.Marshal(map[string]any{
				"type": "object", "requires": def.Requires, "publicHttps": def.PublicHTTPS,
			})
			info.Topologies[i].Components[def.Name] = c
		}
	}
	info.Presets = nil
	for _, name := range []string{"minimal", "standard", "full"} {
		spec := domain.ClusterSpec{}
		p.ApplyDefaults(&spec)
		for component, c := range spec.Components.Extra {
			enabled := component == "proxy" || component == "gateway" || component == "auth" || component == "rest"
			if name != "minimal" {
				enabled = enabled || component == "storage" || component == "realtime" || component == "meta" || component == "studio"
			}
			if name == "full" {
				enabled = true
			}
			if enabled {
				c.Replicas = 1
			} else {
				c.Replicas = 0
			}
			spec.Components.Extra[component] = c
		}
		info.Presets = append(info.Presets, domain.Preset{Name: name, Description: "Supabase " + name + " component set", Topology: spec.Topology, Components: spec.Components})
	}
	options := map[string]any{
		"siteUrl": map[string]any{"type": "string", "format": "uri"},
		"additionalRedirectUrls": map[string]any{
			"type": "array", "items": map[string]any{"type": "string", "format": "uri"},
		},
		"auth": map[string]any{
			"disableSignup": false, "autoconfirm": false, "jwtExpiry": 3600,
			"smtp": map[string]any{"host": "", "port": 587, "user": "", "adminEmail": "", "senderName": ""},
		},
		"rest": map[string]any{"schemas": []string{"public", "graphql_public"}, "maxRows": 1000},
		"storage": map[string]any{
			"backendStorage": "", "bucket": "", "fileSizeLimit": 52428800, "imageTransformation": false,
		},
		"gateway":   map[string]any{"publicHttps": false, "corsOrigins": []string{}},
		"studio":    map[string]any{"organization": "", "project": ""},
		"functions": map[string]any{"verifyJwt": true},
		"extensions": map[string]any{
			"type": "array", "items": map[string]any{"type": "string", "enum": availableExtensions()},
		},
	}
	for name, schema := range options {
		info.EngineOptions[name] = schema
	}
	return info
}
func (p *Provider) Validate(ctx context.Context, old, next *domain.Cluster) error {
	if next == nil {
		return engine.Invalid("cluster is nil")
	}
	if !strings.HasPrefix(next.Spec.Version, "17.") {
		return engine.Invalid("Supabase requires PostgreSQL 17")
	}
	o, err := ParseOptions(next.Spec.EngineOptions)
	if err != nil {
		return err
	}
	if err = p.Core.Validate(ctx, old, next); err != nil {
		return err
	}
	components := next.Spec.Components
	components.Extra = make(map[string]domain.ComponentSpec, len(next.Spec.Components.Extra))
	for name, c := range next.Spec.Components.Extra {
		if name != "proxy" {
			components.Extra[name] = c
		}
	}
	if err = stackComponents.Validate(components); err != nil {
		return err
	}
	// Logflare's native release exceeds 512 MiB while booting, even with
	// bounded BEAM schedulers. Reserve startup headroom, not just idle RSS.
	if analytics := components.Extra["analytics"]; analytics.Replicas > 0 && analytics.Resources.MinRAM < 1 {
		return engine.Invalid("analytics requires at least 1 GiB minRam for Logflare startup")
	}
	if o.Gateway.PublicHTTPS && components.Extra["gateway"].Replicas == 0 {
		return engine.Invalid("gateway.publicHttps requires gateway")
	}
	if o.Storage.ImageTransformation && components.Extra["imgproxy"].Replicas == 0 {
		return engine.Invalid("storage.imageTransformation requires imgproxy")
	}
	if components.Extra["storage"].Replicas > 0 && o.Storage.BackendStorage == "" {
		return engine.Invalid("storage.backendStorage is required when storage is enabled")
	}
	if o.Storage.BackendStorage != "" {
		if p.options.Store == nil {
			return engine.ErrUnavailable
		}
		if _, err = p.options.Store.GetBackupStorage(ctx, next.Ref().Namespace, o.Storage.BackendStorage); err != nil {
			return engine.Invalid("registered storage %q is unavailable", o.Storage.BackendStorage)
		}
	}
	bundle, err := nativeartifacts.Get("supabase-pg17-extensions")
	if err != nil {
		return engine.Invalid("%v", err)
	}
	if _, err = nativeartifacts.ResolveURL(bundle); err != nil {
		return engine.Invalid("%v", err)
	}
	for _, name := range stackComponents.Enabled(components) {
		a, e := nativeartifacts.Get(componentArtifact(name))
		if e != nil {
			return engine.Invalid("%v", e)
		}
		if _, e = nativeartifacts.ResolveURL(a); e != nil {
			return engine.Invalid("%v", e)
		}
	}
	return validateExtensions(o.Extensions)
}
func (p *Provider) RequiredCapabilities(s domain.ClusterSpec) []string {
	caps := p.Core.RequiredCapabilities(s)
	o, _ := ParseOptions(s.EngineOptions)
	if o.Gateway.PublicHTTPS {
		caps = append(caps, "publicHttps")
	}
	for name, c := range s.Components.Extra {
		if name != "proxy" && c.Replicas > 1 {
			caps = append(caps, "horizontalScaling")
			break
		}
	}
	return caps
}
func availableExtensions() []string {
	out := strings.Fields("pgcrypto uuid-ossp pg_stat_statements pg_cron pgaudit vector postgis pg_repack pg_stat_monitor set_user hstore citext btree_gist btree_gin unaccent fuzzystrmatch")
	if artifact, err := nativeartifacts.Get("supabase-pg17-extensions"); err == nil {
		out = append(out, artifact.Extensions...)
	}
	return out
}

func validateExtensions(extensions []string) error {
	available := make(map[string]bool)
	for _, name := range availableExtensions() {
		available[name] = true
	}
	for _, name := range extensions {
		if !available[name] {
			return engine.Invalid("extension %s is unavailable in this build", name)
		}
	}
	return nil
}
func configDigest(v any) string {
	b, _ := json.Marshal(v)
	return digest(b)
}

func componentArtifact(component string) string {
	switch component {
	case "meta":
		return "supabase-postgres-meta"
	case "functions":
		return "supabase-edge-runtime"
	case "pooler":
		return "supabase-supavisor"
	case "analytics":
		return "supabase-logflare"
	default:
		return "supabase-" + component
	}
}

func (p *Provider) Metrics() []domain.MetricDescriptor {
	return append(p.Core.Metrics(),
		domain.MetricDescriptor{Exposition: "karakoram_supabase_up", Name: "supabase_up", Unit: "count", Scale: 1},
		domain.MetricDescriptor{Exposition: "karakoram_supabase_restarts_total", Name: "supabase_restarts_total", Unit: "count", Scale: 1},
	)
}
