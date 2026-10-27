// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package postgres implements the shared PostgreSQL control-plane core.
// Declarative profiles own extension and configuration policy; wrapper hooks
// compose application readiness and relay targets without duplicating lifecycle.
package postgres

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
)

// Flavour customizes data members without duplicating their lifecycle.
type Flavour interface {
	Name() string
	PostReady(context.Context, *engine.Context) error
	RelayTargets(*domain.Cluster) []string
}

// Options supplies namespace-independent provider dependencies.
type Options struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          engine.ClusterStore
	Logger                         *slog.Logger
	Flavour                        Flavour
	// ReservedRole extends the immutable system-role set for a composed engine.
	ReservedRole func(string) bool
	// AvailableExtensions is the admission authority for database extension requests.
	// Nil uses the conservative Percona contrib baseline.
	AvailableExtensions func(context.Context, *engine.Context) ([]string, error)
	// ProfileExtensions lists extensions to update after a version upgrade.
	ProfileExtensions func(*engine.Context) []string
}

// Core reconciles the PostgreSQL data plane and its stateless proxy.
type Core struct {
	options  Options
	accessMu contextMutex
	// Overrides only repository I/O in tests; nil uses the guarded S3 client.
	restoreSetPresent func(context.Context, *domain.BackupStorage, string) (bool, error)
	deleteRepository  func(context.Context, domain.BackupStorage, string, string, string) error
	citusArtifact     func(product string, major int) (nativeartifacts.Artifact, error)
}

// contextMutex keeps all credential mutations serialized while allowing repair
// callers to stop waiting when their shorter context expires.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) Lock(ctx context.Context) error {
	m.once.Do(func() { m.gate = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *contextMutex) Unlock() { <-m.gate }

// New constructs the shared PostgreSQL core.
func New(o Options) *Core {
	p := &Core{options: o, citusArtifact: nativeartifacts.ExtensionArtifact}
	if p.options.AvailableExtensions == nil {
		p.options.AvailableExtensions = p.profileAvailableExtensions
	}
	if p.options.ProfileExtensions == nil {
		p.options.ProfileExtensions = p.selectedProfileExtensions
	}
	return p
}
func (*Core) Name() string { return "postgres" }

var catalog = []domain.VersionInfo{
	{Version: "17.11.1", Major: "17", Default: true, Status: "supported"},
	{Version: "17.10.1", Major: "17", Status: "supported"},
	{Version: "16.15.1", Major: "16", Status: "supported"},
	{Version: "16.14.1", Major: "16", Status: "supported"},
	{Version: "18.6.1", Major: "18", Status: "supported"},
	{Version: "18.4.1", Major: "18", Status: "supported"},
}

// Package versions are pinned per release, not just per major: otherwise a
// catalog minor upgrade would reinstall the same packages.
var packagePins = map[string]string{
	"16.14.1": "2:16.14-1.noble",
	"16.15.1": "2:16.15-1.noble",
	"17.10.1": "2:17.10-1.noble",
	"17.11.1": "2:17.11-1.noble",
	"18.4.1":  "3:18.4-1.noble",
	"18.6.1":  "3:18.6-1.noble",
}
var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{0,11}$`)
var proxyComponents = engine.ComponentSet{{
	Name: "proxy", DisplayName: "ProxySQL",
	Defaults:    domain.ComponentSpec{Replicas: 1, Resources: defaultResources(false)},
	MinReplicas: 1, MaxReplicas: 5,
}}

func defaultResources(data bool) domain.Resources {
	r := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 1}
	if data {
		r.MinRAM = 2
		r.MaxRAM = 2
		r.MinDisk = 5
		r.MaxDisk = 5
	}
	return r
}
func major(v string) int {
	n, _ := strconv.Atoi(strings.Split(v, ".")[0])
	return n
}
func (p *Core) Describe() domain.ProviderInfo {
	info := domain.ProviderInfo{
		Name: "postgres", DisplayName: "PostgreSQL",
		Versions:     append([]domain.VersionInfo(nil), catalog...),
		UpgradePaths: catalogUpgradePaths(),
		BackupTypes:  []string{"full", "diff", "incr"},
		Capabilities: []string{"tls", "monitoring", "publicExposure", "systemUsers", "backups", "restores", "pitr", "postgresAccess"},
		Operations: []string{
			domain.OperationRestart, domain.OperationReplaceMember, "reinit-member",
			domain.OperationUpgradeAgents,
			domain.OperationRotateTLS, domain.OperationBackup, domain.OperationRebalance,
			domain.OperationRotateSystemUser,
			domain.OperationDeleteBackup, domain.OperationRestore, "initial-restore",
		},
		Processes: []string{"postgres", "patroni", "etcd", "proxysql"},
		EngineOptions: map[string]any{
			"synchronousMode": map[string]any{"enum": []string{"off", "on", "strict"}, "default": "off"},
			"parameters":      map[string]any{"type": "object"},
			"proxy":           map[string]any{"readWriteSplit": false, "readerPort": false},
			"profiles":        profileDescription(),
			"citus": map[string]any{
				"workerGroups": map[string]any{"type": "integer", "minimum": 1, "maximum": 8, "default": 2},
				"database":     map[string]any{"type": "string", "default": "app"},
			},
		},
	}
	for _, t := range []string{"single", "ha"} {
		n := 1
		maxN := 1
		if t == "ha" {
			n = 3
			maxN = 5
		}
		comps := proxyComponents.Info()
		comps["engine"] = domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: n, Resources: defaultResources(true)}, MinReplicas: n, MaxReplicas: maxN, AllowedReplicas: []int{n}}
		if t == "ha" {
			e := comps["engine"]
			e.AllowedReplicas = []int{3, 5}
			comps["engine"] = e
			p := comps["proxy"]
			p.MinReplicas = 1
			comps["proxy"] = p
		}
		info.Topologies = append(info.Topologies, domain.TopologyInfo{Type: t, Components: comps})
		presetName := "dev"
		description := "Single PostgreSQL member with ProxySQL"
		if t == "ha" {
			presetName = "prod"
			description = "Three PostgreSQL members with automatic failover"
		}
		info.Presets = append(info.Presets, domain.Preset{
			Name: presetName, Description: description,
			Topology: domain.Topology{Type: t},
			Components: domain.Components{
				Engine: comps["engine"].Defaults,
				Extra:  map[string]domain.ComponentSpec{"proxy": comps["proxy"].Defaults},
			},
		})
	}
	if p.options.Flavour == nil {
		comps := proxyComponents.Info()
		comps["engine"] = domain.ComponentInfo{
			Defaults:    domain.ComponentSpec{Replicas: 3, Resources: defaultResources(true)},
			MinReplicas: 1, MaxReplicas: 3, AllowedReplicas: []int{1, 3},
		}
		info.Topologies = append(info.Topologies, domain.TopologyInfo{Type: "distributed", Components: comps})
	}
	return info
}
func fillResources(r *domain.Resources, d domain.Resources) {
	if r.CPUMode == "" {
		r.CPUMode = d.CPUMode
	}
	if r.MinCPU == 0 {
		r.MinCPU = d.MinCPU
	}
	if r.MaxCPU == 0 {
		r.MaxCPU = max(r.MinCPU, d.MaxCPU)
	}
	if r.MinRAM == 0 {
		r.MinRAM = d.MinRAM
	}
	if r.MaxRAM == 0 {
		r.MaxRAM = max(r.MinRAM, d.MaxRAM)
	}
	if r.MinDisk == 0 {
		r.MinDisk = d.MinDisk
	}
	if r.MaxDisk == 0 {
		r.MaxDisk = max(r.MinDisk, d.MaxDisk)
	}
}
func (*Core) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "postgres"
	}
	if s.Version == "" {
		s.Version = "17.11.1"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "single"
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 1
		if s.Topology.Type == "ha" || s.Topology.Type == "distributed" {
			s.Components.Engine.Replicas = 3
		}
	}
	fillResources(&s.Components.Engine.Resources, defaultResources(true))
	proxyComponents.ApplyDefaults(&s.Components)
	if s.TLS.Mode == "" {
		s.TLS.Mode = "requireTLS"
	}
	if s.TLS.CA == "" {
		s.TLS.CA = "selfSigned"
	}
	if s.Exposure.IPType == "" {
		s.Exposure.IPType = "IPV6"
	}
	s.Monitoring.Enabled = true
}
func (p *Core) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return engine.Invalid("cluster is nil")
	}
	if _, err := p.DataMcp(n); err != nil {
		return err
	}
	s := n.Spec
	if !clusterName.MatchString(n.Name) {
		return engine.Invalid("cluster name must be 1..12 lowercase alphanumeric characters starting with a letter")
	}
	expected := "postgres"
	if p.options.Flavour != nil {
		expected = p.options.Flavour.Name()
	}
	if s.Provider != expected {
		return engine.Invalid("provider must be %s", expected)
	}
	known := false
	for _, v := range catalog {
		known = known || v.Version == s.Version
	}
	if !known {
		return engine.Invalid("unsupported PostgreSQL version")
	}
	if s.Topology.Type != "single" && s.Topology.Type != "ha" && s.Topology.Type != "distributed" {
		return engine.Invalid("topology must be single, ha or distributed")
	}
	nrep := s.Components.Engine.Replicas
	if s.Topology.Type == "single" && nrep != 1 || s.Topology.Type == "ha" && nrep != 3 && nrep != 5 || s.Topology.Type == "distributed" && nrep != 1 && nrep != 3 {
		return engine.Invalid("single requires one data member; ha requires three or five; distributed requires one or three per group")
	}
	if s.Components.Router.Replicas != 0 || s.Components.ConfigServer.Replicas != 0 || s.Topology.Shards != 0 {
		return engine.Invalid("MongoDB components are not supported")
	}
	components := s.Components
	if p.options.Flavour != nil {
		components.Extra = map[string]domain.ComponentSpec{"proxy": s.Components.Extra["proxy"]}
	}
	if err := proxyComponents.Validate(components); err != nil {
		return err
	}
	if (s.Topology.Type == "ha" || s.Topology.Type == "distributed" && nrep == 3) && s.Components.Extra["proxy"].Replicas == 0 {
		return engine.Invalid("ha and replicated distributed groups require at least one proxy replica")
	}
	if err := (engine.ComponentSet{{Name: "data", MinReplicas: 1, MaxReplicas: 5}}).Validate(domain.Components{Extra: map[string]domain.ComponentSpec{"data": s.Components.Engine}}); err != nil {
		return err
	}
	if s.Components.Engine.Resources.MinRAM < 1 || s.Components.Engine.Resources.MinDisk < 5 {
		return engine.Invalid("data members require at least 1 GiB RAM and 5 GB disk")
	}
	if s.EngineConfig != "" {
		return engine.Invalid("PostgreSQL uses engineOptions, not engineConfig")
	}
	o, err := p.parseOptions(s.EngineOptions)
	if err != nil {
		return err
	}
	if p.options.Flavour == nil {
		options, _ := p.parseOptions(s.EngineOptions)
		if _, err := pgprofile.Resolve(options.Profiles, major(s.Version)); err != nil {
			return engine.Invalid("%v", err)
		}
		if old != nil {
			before, err := p.parseOptions(old.Spec.EngineOptions)
			if err != nil {
				return err
			}
			for _, id := range before.Profiles {
				found := false
				for _, selected := range options.Profiles {
					found = found || selected == id
				}
				if !found {
					return engine.Invalid("PostgreSQL profiles are additive: cannot remove profile %s", id)
				}
			}
		}
	}
	if s.Topology.Type != "distributed" && o.Citus != nil {
		return engine.Invalid("citus options require distributed topology")
	}
	if s.Topology.Type == "distributed" {
		if p.options.Flavour != nil {
			return engine.Invalid("distributed topology is supported only by the postgres provider")
		}
		if major(s.Version) < 16 || major(s.Version) > 18 {
			return engine.Invalid("distributed topology requires PostgreSQL 16..18")
		}
		if old != nil {
			if old.Spec.Topology.Type != "distributed" {
				return engine.Invalid("conversion to distributed topology is not supported")
			}
			if major(old.Spec.Version) != major(s.Version) {
				return engine.Invalid("major upgrades of distributed topology are not supported")
			}
			if old.Spec.Components.Engine.Replicas != nrep {
				return engine.Invalid("components.engine.replicas cannot change for an existing distributed cluster: group-local replica scaling is not supported; scale engineOptions.citus.workerGroups instead")
			}
		}
		a, err := p.citusArtifact("citus", major(s.Version))
		if err != nil {
			return engine.Invalid("distributed topology requires a locked Citus artifact: %v", err)
		}
		if _, err := nativeartifacts.ResolveURL(a); err != nil {
			return engine.Invalid("%v", err)
		}
	}
	if s.TLS.Mode != "disabled" && s.TLS.Mode != "preferTLS" && s.TLS.Mode != "requireTLS" {
		return engine.Invalid("invalid TLS mode")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return engine.Invalid("invalid CA source")
	}
	var exposure *domain.Exposure
	if old != nil {
		exposure = &old.Spec.Exposure
		if old.Spec.Topology.Type == "distributed" && s.Topology.Type != "distributed" {
			return engine.Invalid("conversion from distributed topology is not supported")
		}
		if s.Components.Engine.Resources.MinDisk < old.Spec.Components.Engine.Resources.MinDisk || s.Components.Engine.Resources.MaxDisk < old.Spec.Components.Engine.Resources.MaxDisk {
			return engine.Invalid("data volumes cannot shrink")
		}
		if old.Spec.Topology.Type == "ha" && s.Topology.Type == "single" {
			return engine.Invalid("ha cannot be converted to single")
		}
		if old.Spec.Version != s.Version && !upgradeAllowed(old.Spec.Version, s.Version) {
			return engine.Invalid("only newer minor or adjacent major PostgreSQL upgrades are supported")
		}
	}
	if err := domain.ValidateExposureUpdate(exposure, s.Exposure); err != nil {
		return engine.Invalid("%v", err)
	}
	return p.validateBackups(ctx, old, n)
}

// parseOptions allows a flavour to project its own options onto the core's
// strict PostgreSQL schema without mutating the shared cluster specification.
func (p *Core) parseOptions(raw json.RawMessage) (EngineOptions, error) {
	if flavour, ok := p.options.Flavour.(interface {
		PostgresOptions(json.RawMessage) (json.RawMessage, error)
	}); ok {
		var err error
		raw, err = flavour.PostgresOptions(raw)
		if err != nil {
			return EngineOptions{}, err
		}
	}
	return ParseOptions(raw)
}

// observedReleaseMatches preserves the actual observation while allowing
// upstream-only PostgreSQL output to omit Percona's package revision.
// A reported product revision must still match exactly.
func observedReleaseMatches(observed, assigned string) bool {
	return observed == assigned || (strings.Count(observed, ".") == 1 &&
		strings.HasPrefix(assigned, observed+"."))
}

func upgradeAllowed(from, to string) bool {
	if major(to) != major(from) && major(to) != major(from)+1 {
		return false
	}
	a, b := strings.Split(from, "."), strings.Split(to, ".")
	// Legacy agents report upstream major.minor rather than Percona's
	// major.minor.revision. Missing numeric components sort as zero, so
	// 16.15 -> 16.15.1 is not mistaken for a downgrade.
	for i := 0; i < max(len(a), len(b)); i++ {
		x, y := 0, 0
		if i < len(a) {
			x, _ = strconv.Atoi(a[i])
		}
		if i < len(b) {
			y, _ = strconv.Atoi(b[i])
		}
		if x != y {
			return y > x
		}
	}
	return false
}

func catalogUpgradePaths() []domain.UpgradePath {
	var paths []domain.UpgradePath
	for _, from := range catalog {
		for _, to := range catalog {
			if upgradeAllowed(from.Version, to.Version) {
				paths = append(paths, domain.UpgradePath{
					From: from.Version, To: to.Version,
					RequiresBackup: from.Major != to.Major,
				})
			}
		}
	}
	return paths
}
func (*Core) RequiredCapabilities(s domain.ClusterSpec) []string {
	if s.Components.Extra["proxy"].Replicas > 1 {
		return []string{"horizontalScaling"}
	}
	return nil
}

var _ engine.Provider = (*Core)(nil)
var _ engine.AgentConfigSource = (*Core)(nil)
var _ engine.DataPlane = (*Core)(nil)
