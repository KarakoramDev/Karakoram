// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net/netip"
	"reflect"
	"slices"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/robfig/cron/v3"
)

type Options struct {
	ControlPlaneURL, InternalCAPEM string
	Logger                         *slog.Logger
	Store                          engine.ClusterStore
}
type Provider struct {
	options          Options
	portMu           sync.Mutex
	portReservations map[string]map[int]string
}

func New(o Options) *Provider {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ControlPlaneURL == "" {
		o.ControlPlaneURL = "https://karakoram:9443"
	}
	return &Provider{options: o, portReservations: map[string]map[int]string{}}
}
func (*Provider) Name() string                 { return "weaviate" }
func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }
func defaultResources() domain.Resources {
	return domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 2, MaxRAM: 2, MinDisk: 10, MaxDisk: 10}
}
func (*Provider) Describe() domain.ProviderInfo {
	r := defaultResources()
	single := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 1, Resources: r}, MinReplicas: 1, MaxReplicas: 1}
	cluster := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 3, Resources: r}, MinReplicas: 3, MaxReplicas: 9}
	return domain.ProviderInfo{Name: "weaviate", DisplayName: "Weaviate", Versions: slices.Clone(catalog), UpgradePaths: catalogUpgradePaths(), Topologies: []domain.TopologyInfo{{Type: "single", Components: map[string]domain.ComponentInfo{"engine": single}}, {Type: "cluster", Components: map[string]domain.ComponentInfo{"engine": cluster}}}, Presets: []domain.Preset{{Name: "dev", Description: "Single member; no redundancy", Topology: domain.Topology{Type: "single"}, Components: domain.Components{Engine: single.Defaults}}, {Name: "prod", Description: "Three members; placement separation is not guaranteed", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: cluster.Defaults}}}, BackupTypes: []string{"full"}, Capabilities: []string{"backups", "restores", "publicExposure", "tls", "monitoring", "systemUsers", "weaviateAccess"}, Operations: []string{"backup", "delete-backup", "restore", "restart", "replace-member", "rotate-system-user", "rotate-tls"}, Processes: []string{"weaviate"}, EngineOptions: optionsDescription()}
}
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "weaviate"
	}
	if s.Version == "" {
		s.Version = "1.40.1"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "single"
	}
	c := &s.Components.Engine
	if c.Replicas == 0 {
		c.Replicas = 1
		if s.Topology.Type == "cluster" {
			c.Replicas = 3
		}
	}
	r := &c.Resources
	if r.CPUMode == "" {
		r.CPUMode = "SHARED"
	}
	if r.MinCPU == 0 {
		r.MinCPU = 1
	}
	if r.MaxCPU == 0 {
		r.MaxCPU = r.MinCPU
	}
	if r.MinRAM == 0 {
		r.MinRAM = 2
	}
	if r.MaxRAM == 0 {
		r.MaxRAM = r.MinRAM
	}
	if r.MinDisk == 0 {
		r.MinDisk = 10
	}
	if r.MaxDisk == 0 {
		r.MaxDisk = r.MinDisk
	}
	if s.TLS.Mode == "" {
		s.TLS.Mode = "requireTLS"
	}
	if s.TLS.CA == "" {
		s.TLS.CA = "selfSigned"
	}
	if s.Exposure.IPType == "" {
		s.Exposure.IPType = "IPV6"
	}
	if o, err := parseOptionsWithDefaults(s.EngineOptions, s.Topology.Type); err == nil {
		s.EngineOptions, _ = json.Marshal(o)
	}
}
func (p *Provider) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return invalid("cluster is nil")
	}
	if old == nil {
		if err := p.validateDataSource(ctx, n); err != nil {
			return err
		}
	} else if !reflect.DeepEqual(old.Spec.DataSource, n.Spec.DataSource) {
		return invalid("dataSource is immutable")
	}
	s := n.Spec
	if !ValidClusterName(n.Name) {
		return invalid("cluster name must match %s", clusterName)
	}
	if s.Provider != "weaviate" {
		return invalid("provider must be weaviate")
	}
	if !slices.ContainsFunc(catalog, func(v domain.VersionInfo) bool { return v.Version == s.Version }) {
		return invalid("unsupported engine version %q", s.Version)
	}
	if s.Topology.Shards != 0 || (s.Topology.Type != "single" && s.Topology.Type != "cluster") {
		return invalid("Weaviate supports single or cluster topology without shards")
	}
	if !reflect.DeepEqual(s.Components.ConfigServer, domain.ComponentSpec{}) || !reflect.DeepEqual(s.Components.Router, domain.ComponentSpec{}) || len(s.Components.Extra) > 0 {
		return invalid("Weaviate supports only the engine component")
	}
	replicas := s.Components.Engine.Replicas
	if s.Topology.Type == "single" && replicas != 1 || s.Topology.Type == "cluster" && (replicas < 3 || replicas > 9) {
		return invalid("single requires 1 replica; cluster requires 3..9 replicas")
	}
	r := s.Components.Engine.Resources
	if r.CPUMode != "SHARED" && r.CPUMode != "DEDICATED" {
		return invalid("cpuMode must be SHARED or DEDICATED")
	}
	for _, v := range []float64{r.MinCPU, r.MaxCPU, r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return invalid("resource bounds must be finite")
		}
	}
	if r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinRAM < 1 || r.MaxRAM < r.MinRAM || r.MinDisk < 1 || r.MaxDisk < r.MinDisk {
		return invalid("invalid resource bounds (RAM >=1 GiB, disk >=1 GiB)")
	}
	if r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || r.StartCPU != nil && (math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0) || *r.StartCPU != math.Trunc(*r.StartCPU) || *r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
		return invalid("CPU bounds must be whole cores and startCpu within bounds")
	}
	if s.TLS.Mode != "requireTLS" && s.TLS.Mode != "disabled" {
		return invalid("TLS mode must be requireTLS or disabled")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return invalid("TLS CA must be selfSigned or userProvided")
	}
	if s.Exposure.Public && s.TLS.Mode != "requireTLS" {
		return invalid("public exposure requires requireTLS")
	}
	if s.Exposure.IPType != "IPV4" && s.Exposure.IPType != "IPV6" {
		return invalid("invalid exposure IP type")
	}
	for _, cidr := range s.Exposure.Allowlist {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return invalid("invalid allowlist CIDR %q", cidr)
		}
	}
	o, err := parseOptionsWithDefaults(s.EngineOptions, s.Topology.Type)
	if err != nil {
		return err
	}
	if err = validateOptions(o, replicas); err != nil {
		return err
	}
	if s.EngineConfig != "" {
		return invalid("engineConfig is unsupported: Weaviate is environment-configured; use typed engineOptions")
	}
	if s.Backup.PITR.Enabled {
		return invalid("Weaviate does not support PITR")
	}
	for _, b := range s.Backup.Schedules {
		if _, err := cron.ParseStandard(b.Cron); err != nil {
			return invalid("invalid backup schedule %q", b.Name)
		}
		if b.Type != "full" || b.RetainCount < 1 {
			return invalid("backup schedules require full and positive retainCount")
		}
		if s.Backup.Storage == "" || b.Storage != "" && b.Storage != s.Backup.Storage {
			return invalid("backup schedules must use the cluster's bound storage")
		}
	}
	if s.Backup.Storage != "" {
		if p.options.Store == nil {
			return invalid("backup storage validation requires store")
		}
		storage, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, s.Backup.Storage)
		if err != nil {
			return invalid("backup storage %q: %v", s.Backup.Storage, err)
		}
		if storage.Type != "s3" {
			return invalid("backup storage must be S3-compatible")
		}
	}
	if old != nil {
		if old.Name != n.Name || old.Ref().Namespace != n.Ref().Namespace || old.Spec.Provider != s.Provider {
			return invalid("cluster identity and provider are immutable")
		}
		if !reflect.DeepEqual(old.Spec.Topology, s.Topology) {
			return invalid("topology is immutable; create a new cluster and restore a backup")
		}
		st, err := load(old)
		if err != nil {
			return err
		}
		if replicas < old.Spec.Components.Engine.Replicas || replicas < len(st.Members) {
			if s.Topology.Type != "cluster" || replicas < 3 || replicas < len(st.Voters) {
				return invalid("scale-down cannot remove a fixed Raft voter; retain at least three members")
			}
			nonVoters := 0
			for _, m := range st.Members {
				if !slices.Contains(st.Voters, m.Name) {
					nonVoters++
				}
			}
			if len(st.Members)-replicas > nonVoters {
				return invalid("scale-down would remove a fixed Raft voter")
			}
		}
		if !UpgradeAllowed(old.Spec.Version, s.Version) {
			return invalid("upgrade must be patch-forward in the same minor or exactly one minor forward")
		}
		if r.MinDisk < old.Spec.Components.Engine.Resources.MinDisk || r.MaxDisk < old.Spec.Components.Engine.Resources.MaxDisk {
			return invalid("disk shrinking is not supported")
		}
		if old.Spec.TLS.CA != s.TLS.CA {
			return invalid("CA source is immutable")
		}
	}
	return nil
}
