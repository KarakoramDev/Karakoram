// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Options supplies PostgreSQL binding and member runtime dependencies.
type Options struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          *store.Store
	Logger                         *slog.Logger
	Bindings                       pgbinding.Service
}

// Provider reconciles Temporal components and their bound PostgreSQL stores.
type Provider struct {
	options       Options
	certificateMu sync.Mutex
}

// New constructs a Temporal provider. Nil bindings reject cluster admission.
func New(o Options) *Provider { return &Provider{options: o} }

// Name identifies this provider in cluster specs and agent configuration.
func (*Provider) Name() string { return tspec.Engine }

var _ engine.Provider = (*Provider)(nil)
var _ engine.AgentConfigSource = (*Provider)(nil)

func componentSet(topology string) engine.ComponentSet {
	limits := map[string]int{tspec.History: 9, tspec.Matching: 5, tspec.Frontend: 5, tspec.Worker: 3, tspec.Server: 5, tspec.UI: 3}
	out := make(engine.ComponentSet, 0, len(tspec.RollOrder))
	for _, name := range tspec.RollOrder {
		replicas := 0
		if name == tspec.UI || topology == "combined" && name == tspec.Server || topology == "split" && name != tspec.UI && name != tspec.Server {
			replicas = 1
		}
		out = append(out, engine.ComponentDef{
			Name: name, DisplayName: name,
			MinReplicas: 1, MaxReplicas: limits[name],
			PublicHTTPS: name == tspec.UI,
			Defaults: domain.ComponentSpec{
				Replicas: replicas,
				Resources: domain.Resources{
					CPUMode: "SHARED",
					MinCPU:  1, MaxCPU: 1,
					MinRAM: 1, MaxRAM: 1,
					MinDisk: 1, MaxDisk: 1,
				},
			},
		})
	}
	return out
}

func serverArtifact(version string) (nativeartifacts.Artifact, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return nativeartifacts.Artifact{}, engine.Invalid("Temporal version must be a locked semantic version")
	}
	a, e := nativeartifacts.Get("temporal-server-" + parts[0] + "." + parts[1])
	if e != nil || a.Version != version {
		return a, engine.Invalid("Temporal version %q has no matching locked native artifact", version)
	}
	return a, nil
}

func upgradeAllowed(from, to string) bool {
	var a, b [3]int
	for i, s := range strings.Split(from, ".") {
		if i >= 3 {
			return false
		}
		n, e := strconv.Atoi(s)
		if e != nil {
			return false
		}
		a[i] = n
	}
	for i, s := range strings.Split(to, ".") {
		if i >= 3 {
			return false
		}
		n, e := strconv.Atoi(s)
		if e != nil {
			return false
		}
		b[i] = n
	}
	return len(strings.Split(from, ".")) == 3 && len(strings.Split(to, ".")) == 3 &&
		a[0] == b[0] && (b[1] == a[1] && b[2] > a[2] || b[1] == a[1]+1)
}

// ApplyDefaults fills absent topology, component, TLS and engine option settings.
func (p *Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = tspec.Engine
	}
	if s.Version == "" {
		s.Version = "1.32.1"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "combined"
	}
	componentSet(s.Topology.Type).ApplyDefaults(&s.Components)
	// The core engine slot is unused (zero replicas) but still serialized;
	// give it valid resource bounds so specs and presets stay well-formed.
	if s.Components.Engine.Resources.CPUMode == "" {
		s.Components.Engine.Resources = componentSet(s.Topology.Type)[0].Defaults.Resources
	}
	if s.TLS.Mode == "" {
		s.TLS.Mode = "requireTLS"
	}
	if s.TLS.CA == "" {
		s.TLS.CA = "selfSigned"
	}
	if o, e := ParseOptions(s.EngineOptions); e == nil {
		s.EngineOptions, _ = json.Marshal(o)
	}
}

// Describe publishes the locked version catalog, topology bounds and presets.
func (p *Provider) Describe() domain.ProviderInfo {
	out := domain.ProviderInfo{
		Name: tspec.Engine, DisplayName: "Temporal",
		Capabilities: []string{"tls", "backups", "restores", "temporalAccess"},
		BackupTypes:  []string{"full", "diff", "incr"},
		Operations:   []string{"restart", "rotate-tls", "backup", "delete-backup", "restore"},
		Processes:    []string{"temporal-server", "ui-server"},
		EngineOptions: map[string]any{
			"persistence": map[string]any{
				"postgres": map[string]any{
					"cluster": "", "namespace": "", "database": "", "visibilityDatabase": "",
				},
			},
			"historyShards": 512,
			"frontend":      map[string]any{"requireClientAuth": true},
			"namespaces":    []tspec.Namespace{},
			"ui": map[string]any{
				"publicHttps": false,
				"oidc":        map[string]any{"issuer": "", "clientId": ""},
			},
		},
	}
	for _, v := range []string{"1.32.1", "1.31.4"} {
		if _, e := serverArtifact(v); e == nil {
			out.Versions = append(out.Versions, domain.VersionInfo{
				Version: v, Major: strings.Join(strings.Split(v, ".")[:2], "."),
				Default: v == "1.32.1", Status: "supported",
			})
		}
	}
	for _, a := range out.Versions {
		for _, b := range out.Versions {
			if upgradeAllowed(a.Version, b.Version) {
				out.UpgradePaths = append(out.UpgradePaths, domain.UpgradePath{From: a.Version, To: b.Version})
			}
		}
	}
	for _, topology := range []string{"combined", "split"} {
		info := componentSet(topology).Info()
		for _, d := range componentSet(topology) {
			c := info[d.Name]
			c.Schema, _ = json.Marshal(map[string]any{"publicHttps": d.PublicHTTPS})
			info[d.Name] = c
		}
		out.Topologies = append(out.Topologies, domain.TopologyInfo{Type: topology, Components: info})
		spec := domain.ClusterSpec{Topology: domain.Topology{Type: topology}}
		p.ApplyDefaults(&spec)
		name := "dev"
		if topology == "split" {
			name = "prod"
			for k, n := range map[string]int{tspec.History: 3, tspec.Matching: 2, tspec.Frontend: 2} {
				c := spec.Components.Extra[k]
				c.Replicas = n
				spec.Components.Extra[k] = c
			}
		}
		out.Presets = append(out.Presets, domain.Preset{
			Name: name, Description: "Temporal " + name,
			Topology: spec.Topology, Components: spec.Components,
		})
	}
	return out
}

// Validate checks locked artifacts, component topology and immutable options.
func (p *Provider) Validate(_ context.Context, old, next *domain.Cluster) error {
	if p.options.Bindings == nil {
		return engine.Invalid("PostgreSQL bindings are unavailable")
	}
	if next == nil {
		return engine.Invalid("cluster is nil")
	}
	if _, e := serverArtifact(next.Spec.Version); e != nil {
		return e
	}
	if next.Spec.EngineConfig != "" {
		return engine.Invalid("Temporal uses engineOptions, not engineConfig")
	}
	if next.Spec.Exposure.Public {
		return engine.Invalid("Temporal frontend is private; use ui.publicHttps for the UI")
	}
	if next.Spec.TLS.Mode != "requireTLS" || next.Spec.TLS.CA != "selfSigned" {
		return engine.Invalid("Temporal always uses requireTLS with the cluster's self-signed CA")
	}
	if next.Spec.Topology.Type != "combined" && next.Spec.Topology.Type != "split" {
		return engine.Invalid("Temporal topology must be combined or split")
	}
	if next.Spec.Components.Engine.Replicas != 0 || next.Spec.Components.ConfigServer.Replicas != 0 ||
		next.Spec.Components.Router.Replicas != 0 {
		return engine.Invalid("Temporal components must be in components.extra")
	}
	if e := componentSet(next.Spec.Topology.Type).Validate(next.Spec.Components); e != nil {
		return e
	}
	for _, name := range tspec.RollOrder {
		if name == tspec.UI {
			continue
		}
		n := next.Spec.Components.Extra[name].Replicas
		required := next.Spec.Topology.Type == "combined" && name == tspec.Server || next.Spec.Topology.Type == "split" && name != tspec.Server
		if required && n < 1 || !required && n != 0 {
			return engine.Invalid("component %s is inconsistent with %s topology", name, next.Spec.Topology.Type)
		}
	}
	o, e := optionsFor(next)
	if e != nil {
		return e
	}
	if e = validateOptions(next, o); e != nil {
		return e
	}
	if old != nil {
		if e = immutableOptions(old, next); e != nil {
			return e
		}
		if old.Spec.Version != next.Spec.Version && !upgradeAllowed(old.Spec.Version, next.Spec.Version) {
			return engine.Invalid("unsupported Temporal upgrade %s to %s", old.Spec.Version, next.Spec.Version)
		}
	}
	return nil
}

// RequiredCapabilities declares public HTTPS and horizontal-scaling requirements.
func (*Provider) RequiredCapabilities(s domain.ClusterSpec) []string {
	var out []string
	o, _ := ParseOptions(s.EngineOptions)
	if o.UI.PublicHTTPS {
		out = append(out, "publicHttps")
	}
	for _, c := range s.Components.Extra {
		if c.Replicas > 1 {
			out = append(out, "horizontalScaling")
			break
		}
	}
	return out
}
