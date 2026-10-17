// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package postgres implements the shared PostgreSQL control-plane core. Wrappers
// supply a Flavour to install extensions, force parameters and preload libraries,
// bootstrap SQL after readiness, and extend relay targets. Wrappers reconcile
// their additional components themselves; the core owns data members and proxy.
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
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Flavour customizes data members without duplicating their lifecycle.
type Flavour interface {
	Name() string
	PrepareCommands(infra.Layout, int) ([]string, error)
	Parameters() map[string]string
	SharedPreloadLibraries() []string
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
}

// Core reconciles the PostgreSQL data plane and its stateless proxy.
type Core struct {
	options  Options
	accessMu sync.Mutex
}

// New constructs the shared PostgreSQL core.
func New(o Options) *Core  { return &Core{options: o} }
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
func (*Core) Describe() domain.ProviderInfo {
	info := domain.ProviderInfo{
		Name: "postgres", DisplayName: "PostgreSQL",
		Versions:     append([]domain.VersionInfo(nil), catalog...),
		UpgradePaths: catalogUpgradePaths(),
		BackupTypes:  []string{"full", "diff", "incr"},
		Capabilities: []string{"tls", "monitoring", "publicExposure", "systemUsers", "backups", "restores", "pitr", "postgresAccess"},
		Operations: []string{
			domain.OperationRestart, domain.OperationReplaceMember, "reinit-member",
			domain.OperationRotateTLS, domain.OperationBackup,
			domain.OperationDeleteBackup, domain.OperationRestore, "initial-restore",
		},
		Processes: []string{"postgres", "patroni", "etcd", "proxysql"},
		EngineOptions: map[string]any{
			"synchronousMode": map[string]any{"enum": []string{"off", "on", "strict"}, "default": "off"},
			"parameters":      map[string]any{"type": "object"},
			"proxy":           map[string]any{"readWriteSplit": false, "readerPort": false},
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
		if s.Topology.Type == "ha" {
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
	if s.Topology.Type != "single" && s.Topology.Type != "ha" {
		return engine.Invalid("topology must be single or ha")
	}
	nrep := s.Components.Engine.Replicas
	if s.Topology.Type == "single" && nrep != 1 || s.Topology.Type == "ha" && nrep != 3 && nrep != 5 {
		return engine.Invalid("single requires one data member; ha requires three or five")
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
	if s.Topology.Type == "ha" && s.Components.Extra["proxy"].Replicas == 0 {
		return engine.Invalid("ha requires at least one proxy replica")
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
	if _, err := p.parseOptions(s.EngineOptions); err != nil {
		return err
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

func upgradeAllowed(from, to string) bool {
	if major(to) != major(from) && major(to) != major(from)+1 {
		return false
	}
	a, b := strings.Split(from, "."), strings.Split(to, ".")
	for i := 0; i < len(a) && i < len(b); i++ {
		x, _ := strconv.Atoi(a[i])
		y, _ := strconv.Atoi(b[i])
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
