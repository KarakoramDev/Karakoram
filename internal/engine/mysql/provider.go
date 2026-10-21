// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package mysql implements the Percona Server for MySQL control-plane core.
package mysql

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
)

type Options struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          engine.ClusterStore
	Logger                         *slog.Logger
	Provider                       string
}
type Core struct {
	options  Options
	accessMu sync.Mutex
}

func New(o Options) *Core { return &Core{options: o} }
func (p *Core) Name() string {
	if p.options.Provider == "pxc" {
		return "pxc"
	}
	return "mysql"
}

var catalog = []domain.VersionInfo{
	{Version: "8.4.11-11", Major: "8.4", Default: true, Status: "supported"},
	{Version: "8.4.10-10", Major: "8.4", Status: "supported"},
	{Version: "8.0.46-37", Major: "8.0", Status: "supported"},
	{Version: "8.0.45-36", Major: "8.0", Status: "supported"},
}
var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{0,11}$`)
var proxyComponents = engine.ComponentSet{{Name: "proxy", DisplayName: "ProxySQL", Defaults: domain.ComponentSpec{Replicas: 1, Resources: defaultResources(false)}, MinReplicas: 0, MaxReplicas: 5}}

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
func (p *Core) Describe() domain.ProviderInfo {
	info := domain.ProviderInfo{Name: "mysql", DisplayName: "Percona Server for MySQL", Versions: append([]domain.VersionInfo(nil), catalog...), UpgradePaths: catalogUpgradePaths(), BackupTypes: []string{"full", "incremental"}, Capabilities: []string{"tls", "monitoring", "publicExposure", "systemUsers", "backups", "restores", "pitr", "mysqlAccess"}, Operations: []string{domain.OperationRestart, domain.OperationReplaceMember, domain.OperationRotateTLS, domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, "initial-restore"}, Processes: []string{"mysqld", "proxysql"}, EngineOptions: map[string]any{
		"parameters": map[string]any{"type": "object"}, "proxy": map[string]any{"readWriteSplit": false, "readerPort": false},
		"groupReplication": map[string]any{"consistency": map[string]any{"enum": []string{"BEFORE_ON_PRIMARY_FAILOVER", "EVENTUAL", "BEFORE", "AFTER", "BEFORE_AND_AFTER"}, "default": "BEFORE_ON_PRIMARY_FAILOVER"}, "unreachableMajorityTimeout": map[string]any{"type": "integer", "default": 30}},
		"binlog":           map[string]any{"archiveInterval": map[string]any{"type": "integer", "default": 300}, "expireSeconds": map[string]any{"type": "integer", "default": 604800}},
	}}
	if p.Name() == "pxc" {
		info.Name, info.DisplayName = "pxc", "Percona XtraDB Cluster"
		info.Versions = append([]domain.VersionInfo(nil), pxcCatalog...)
		info.UpgradePaths = upgradePaths(pxcCatalog)
		delete(info.EngineOptions, "groupReplication")
		info.Operations = append(info.Operations, "recover-quorum")
		comps := proxyComponents.Info()
		proxy := comps["proxy"]
		proxy.MinReplicas = 1
		comps["proxy"] = proxy
		comps["engine"] = domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 3, Resources: defaultResources(true)}, MinReplicas: 3, MaxReplicas: 5, AllowedReplicas: []int{3, 5}}
		info.Topologies = []domain.TopologyInfo{{Type: "cluster", Components: comps}}
		info.Presets = []domain.Preset{{Name: "prod", Description: "Three Galera members with ProxySQL", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: comps["engine"].Defaults, Extra: map[string]domain.ComponentSpec{"proxy": comps["proxy"].Defaults}}}}
		return info
	}
	for _, t := range []string{"single", "group"} {
		n, maxN := 1, 1
		allowed := []int{1}
		preset, description := "dev", "Single MySQL member with ProxySQL"
		if t == "group" {
			n, maxN = 3, 5
			allowed = []int{3, 5}
			preset, description = "prod", "Three MySQL members with Group Replication"
		}
		comps := proxyComponents.Info()
		comps["engine"] = domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: n, Resources: defaultResources(true)}, MinReplicas: n, MaxReplicas: maxN, AllowedReplicas: allowed}
		if t == "group" {
			p := comps["proxy"]
			p.MinReplicas = 1
			comps["proxy"] = p
		}
		info.Topologies = append(info.Topologies, domain.TopologyInfo{Type: t, Components: comps})
		info.Presets = append(info.Presets, domain.Preset{Name: preset, Description: description, Topology: domain.Topology{Type: t}, Components: domain.Components{Engine: comps["engine"].Defaults, Extra: map[string]domain.ComponentSpec{"proxy": comps["proxy"].Defaults}}})
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
func (p *Core) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = p.Name()
	}
	if s.Version == "" {
		s.Version = p.versions()[0].Version
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "single"
		if p.Name() == "pxc" {
			s.Topology.Type = "cluster"
		}
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 1
		if s.Topology.Type == "group" || s.Topology.Type == "cluster" {
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
	if s.Provider != p.Name() {
		return engine.Invalid("provider must be %s", p.Name())
	}
	known := false
	for _, v := range p.versions() {
		known = known || v.Version == s.Version
	}
	if !known {
		return engine.Invalid("unsupported MySQL version")
	}
	if p.Name() == "pxc" && s.Topology.Type != "cluster" || p.Name() == "mysql" && s.Topology.Type != "single" && s.Topology.Type != "group" {
		return engine.Invalid("unsupported provider topology")
	}
	r := s.Components.Engine.Replicas
	if s.Topology.Type == "single" && r != 1 {
		return engine.Invalid("single requires one data member")
	}
	if s.Topology.Type != "single" && r != 3 && r != 5 {
		return engine.Invalid("%s requires three or five data members", s.Topology.Type)
	}
	if s.Topology.Shards != 0 || s.Components.Router.Replicas != 0 || s.Components.ConfigServer.Replicas != 0 {
		return engine.Invalid("MongoDB components are not supported")
	}
	if err := proxyComponents.Validate(s.Components); err != nil {
		return err
	}
	if s.Topology.Type != "single" && s.Components.Extra["proxy"].Replicas == 0 {
		return engine.Invalid("%s requires at least one proxy", s.Topology.Type)
	}
	if err := (engine.ComponentSet{{Name: "data", MinReplicas: 1, MaxReplicas: 5}}).Validate(domain.Components{Extra: map[string]domain.ComponentSpec{"data": s.Components.Engine}}); err != nil {
		return err
	}
	if s.Components.Engine.Resources.MinRAM < 1 || s.Components.Engine.Resources.MinDisk < 5 {
		return engine.Invalid("data members require at least 1 GiB RAM and 5 GB disk")
	}
	if s.EngineConfig != "" {
		return engine.Invalid("MySQL uses engineOptions, not engineConfig")
	}
	if _, err := ParseOptions(s.EngineOptions); err != nil {
		return err
	}
	if p.Name() == "pxc" {
		var options map[string]json.RawMessage
		if err := json.Unmarshal(s.EngineOptions, &options); len(s.EngineOptions) != 0 && err != nil {
			return engine.Invalid("invalid PXC options")
		}
		if _, ok := options["groupReplication"]; ok {
			return engine.Invalid("PXC does not use Group Replication")
		}
	}
	if s.TLS.Mode != "requireTLS" {
		return engine.Invalid("MySQL requires TLS")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return engine.Invalid("invalid CA source")
	}
	var exposure *domain.Exposure
	if old != nil {
		exposure = &old.Spec.Exposure
		if old.Spec.Topology.Type != s.Topology.Type && !(old.Spec.Topology.Type == "single" && s.Topology.Type == "group") {
			return engine.Invalid("only single to group topology conversion is supported")
		}
		if old.Spec.Version != s.Version && !upgradeAllowed(old.Spec.Version, s.Version) {
			return engine.Invalid("only newer minor or adjacent 8.0 to 8.4 upgrades are supported")
		}
		if s.Components.Engine.Resources.MinDisk < old.Spec.Components.Engine.Resources.MinDisk || s.Components.Engine.Resources.MaxDisk < old.Spec.Components.Engine.Resources.MaxDisk {
			return engine.Invalid("data volumes cannot shrink")
		}
	}
	if err := domain.ValidateExposureUpdate(exposure, s.Exposure); err != nil {
		return engine.Invalid("%v", err)
	}
	return p.validateBackups(ctx, old, n)
}
func major(v string) string {
	p := strings.Split(v, ".")
	if len(p) < 2 {
		return ""
	}
	return p[0] + "." + p[1]
}
func upgradeAllowed(from, to string) bool {
	a, b := major(from), major(to)
	if a != b {
		return a == "8.0" && b == "8.4"
	}
	ap, bp := strings.FieldsFunc(from, func(r rune) bool { return r == '.' || r == '-' }), strings.FieldsFunc(to, func(r rune) bool { return r == '.' || r == '-' })
	if len(ap) != len(bp) || len(ap) < 4 {
		return false
	}
	for i := range ap {
		x, e1 := strconv.Atoi(ap[i])
		y, e2 := strconv.Atoi(bp[i])
		if e1 != nil || e2 != nil {
			return false
		}
		if x != y {
			return y > x
		}
	}
	return false
}
func catalogUpgradePaths() []domain.UpgradePath {
	return upgradePaths(catalog)
}
func upgradePaths(versions []domain.VersionInfo) []domain.UpgradePath {
	var paths []domain.UpgradePath
	for _, a := range versions {
		for _, b := range versions {
			if upgradeAllowed(a.Version, b.Version) {
				paths = append(paths, domain.UpgradePath{From: a.Version, To: b.Version, RequiresBackup: a.Major != b.Major})
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

// ArtifactPins resolves a supported server release to its immutable server and backup catalog keys.
func ArtifactPins(version string) (server, backup string, err error) {
	for _, v := range catalog {
		if v.Version == version {
			backupVersion := "8.4.0-7"
			if v.Major == "8.0" {
				backupVersion = "8.0.35-36"
			}
			return "percona-server-" + version, "percona-xtrabackup-" + backupVersion, nil
		}
	}
	return "", "", engine.Invalid("unsupported MySQL version")
}
