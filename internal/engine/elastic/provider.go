// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"log/slog"
	"math"
	"reflect"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/robfig/cron/v3"
)

// ProviderOptions supplies control-plane endpoints and inventory lookups.
type ProviderOptions struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          engine.ClusterStore
	Logger                         *slog.Logger
}

// Provider reconciles Elasticsearch and its optional stack components.
type Provider struct {
	options          ProviderOptions
	portMu           sync.Mutex
	portReservations map[string]map[int]string
}

// New constructs an Elasticsearch provider.
func New(o ProviderOptions) *Provider {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Provider{options: o, portReservations: map[string]map[int]string{}}
}

// Name returns the stable provider identifier.
func (*Provider) Name() string { return "elasticsearch" }

// Describe returns the supported catalog and lifecycle capabilities.
func (*Provider) Describe() domain.ProviderInfo {
	info := domain.ProviderInfo{Name: "elasticsearch", DisplayName: "Elasticsearch", Versions: append([]domain.VersionInfo(nil), catalog...), UpgradePaths: catalogUpgradePaths(), BackupTypes: []string{"snapshot"}, Capabilities: []string{"backups", "restores", "publicExposure", "tls", "monitoring", "elasticAccess", "systemUsers"}, Operations: []string{"backup", "delete-backup", "restore", "restart", "replace-member", "rotate-system-user", "rotate-tls"}, Processes: []string{"elasticsearch", "kibana", "logstash", "apm-server"}}
	ci := func(name string, replicas, min, max int) domain.ComponentInfo {
		r, _ := DefaultResources(name)
		return domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: replicas, Resources: r}, MinReplicas: min, MaxReplicas: max}
	}
	for _, topology := range []string{"cluster", "dedicatedMasters"} {
		comps := map[string]domain.ComponentInfo{"engine": ci("engine", 3, 1, 9), "kibana": ci("kibana", 0, 0, 3), "logstash": ci("logstash", 0, 0, 5), "apm": ci("apm", 0, 0, 3)}
		if topology == "dedicatedMasters" {
			comps["engine"] = ci("engine", 2, 1, 20)
			m := ci("master", 3, 3, 5)
			m.AllowedReplicas = []int{3, 5}
			comps["master"] = m
		}
		info.Topologies = append(info.Topologies, domain.TopologyInfo{Type: topology, Components: comps})
	}
	for _, name := range []string{"dev", "prod", "observability", "large"} {
		topology := "cluster"
		n := 3
		if name == "dev" {
			n = 1
		}
		if name == "large" {
			topology = "dedicatedMasters"
		}
		extra := map[string]domain.ComponentSpec{"kibana": ci("kibana", 1, 0, 3).Defaults, "logstash": ci("logstash", 0, 0, 5).Defaults, "apm": ci("apm", 0, 0, 3).Defaults}
		if name == "dev" {
			kibana := extra["kibana"]
			kibana.Resources.MaxRAM = 1.5
			extra["kibana"] = kibana
		}
		if name == "observability" {
			extra["logstash"] = ci("logstash", 1, 0, 5).Defaults
			extra["apm"] = ci("apm", 1, 0, 3).Defaults
		}
		if name == "large" {
			extra["master"] = ci("master", 3, 3, 5).Defaults
		}
		info.Presets = append(info.Presets, domain.Preset{Name: name, Description: map[string]string{"dev": "Single 2 GB node and Kibana; no database redundancy", "prod": "Three combined nodes and Kibana", "observability": "Three combined nodes, Kibana, Logstash and APM", "large": "Three dedicated masters, three data nodes and Kibana"}[name], Topology: domain.Topology{Type: topology}, Components: domain.Components{Engine: ci("engine", n, 1, 20).Defaults, Extra: extra}})
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

// ApplyDefaults fills omitted spec values without enabling optional components.
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "elasticsearch"
	}
	if s.Version == "" {
		s.Version = catalog[0].Version
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "cluster"
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 3
		if s.Topology.Type == "dedicatedMasters" {
			s.Components.Engine.Replicas = 2
		}
	}
	d, _ := DefaultResources("engine")
	fillResources(&s.Components.Engine.Resources, d)
	if s.Components.Extra == nil {
		s.Components.Extra = map[string]domain.ComponentSpec{}
	}
	names := []string{"kibana", "logstash", "apm"}
	if s.Topology.Type == "dedicatedMasters" {
		names = append(names, "master")
	}
	for _, name := range names {
		c := s.Components.Extra[name]
		if name == "master" && c.Replicas == 0 {
			c.Replicas = 3
		}
		d, _ := DefaultResources(name)
		fillResources(&c.Resources, d)
		s.Components.Extra[name] = c
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
	s.Monitoring.Enabled = true
}
func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }

// Validate rejects unsupported configurations and unsafe simultaneous changes.
func (p *Provider) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return invalid("cluster is nil")
	}
	s := n.Spec
	if !ValidClusterName(n.Name) {
		return invalid("cluster name must be 2..12 lowercase alphanumeric characters starting with a letter")
	}
	if s.Provider != "elasticsearch" {
		return invalid("provider must be elasticsearch")
	}
	known := false
	for _, v := range catalog {
		known = known || v.Version == s.Version
	}
	if !known {
		return invalid("unsupported Elasticsearch version")
	}
	if s.Topology.Type != "cluster" && s.Topology.Type != "dedicatedMasters" {
		return invalid("unsupported topology")
	}
	limit := 9
	if s.Topology.Type == "dedicatedMasters" {
		limit = 20
	}
	if s.Components.Engine.Replicas < 1 || s.Components.Engine.Replicas > limit {
		return invalid("engine replicas must be 1..%d", limit)
	}
	if s.Components.ConfigServer.Replicas != 0 || s.Components.Router.Replicas != 0 || s.Topology.Shards != 0 {
		return invalid("MongoDB components and shards are not supported")
	}
	comps := map[string]domain.ComponentSpec{"engine": s.Components.Engine}
	for name, c := range s.Components.Extra {
		switch name {
		case "master":
			if s.Topology.Type != "dedicatedMasters" {
				return engine.InvalidField("spec.components.extra.master", domain.ReasonUnsupported, "dedicated masters require the dedicatedMasters topology; cluster topology uses combined master/data nodes")
			}
			if c.Replicas != 3 && c.Replicas != 5 {
				return invalid("dedicated masters require 3 or 5 replicas")
			}
		case "kibana", "apm":
			if c.Replicas < 0 || c.Replicas > 3 {
				return invalid("%s replicas must be 0..3", name)
			}
		case "logstash":
			if c.Replicas < 0 || c.Replicas > 5 {
				return invalid("logstash replicas must be 0..5")
			}
		default:
			return invalid("unknown component %q", name)
		}
		comps[name] = c
	}
	if s.Topology.Type == "dedicatedMasters" && s.Components.Extra["master"].Replicas != 3 && s.Components.Extra["master"].Replicas != 5 {
		return invalid("dedicated masters require 3 or 5 replicas")
	}
	for name, c := range comps {
		r := c.Resources
		if err := ValidateMinimumRAM(name, r.MinRAM); err != nil {
			return invalid("%v", err)
		}
		for _, v := range []float64{r.MinCPU, r.MaxCPU, r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return invalid("resource bounds must be finite")
			}
		}
		if r.CPUMode != "SHARED" && r.CPUMode != "DEDICATED" || r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || r.MaxRAM < r.MinRAM || r.MinDisk < 1 || r.MaxDisk < r.MinDisk {
			return invalid("invalid %s resource bounds", name)
		}
		if (name == "engine" || name == "master") && r.MinDisk < 5 {
			return invalid("node volumes require at least 5 GB")
		}
		if r.StartCPU != nil && (math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0) || *r.StartCPU != math.Trunc(*r.StartCPU) || *r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
			return invalid("startCpu outside CPU bounds")
		}
	}
	if s.EngineConfig != "" {
		return invalid("Elasticsearch uses engineOptions, not engineConfig")
	}
	if _, err := ParseOptions(s.EngineOptions); err != nil {
		return invalid("%v", err)
	}
	if s.TLS.Mode != "requireTLS" {
		return invalid("Elasticsearch requires requireTLS")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return invalid("invalid CA source")
	}
	if s.Backup.PITR.Enabled {
		return invalid("Elasticsearch does not support PITR")
	}
	var previousExposure *domain.Exposure
	if old != nil {
		previousExposure = &old.Spec.Exposure
	}
	if err := domain.ValidateExposureUpdate(previousExposure, s.Exposure); err != nil {
		if field, ok := err.(*domain.FieldError); ok {
			field.Err = engine.ErrInvalid
		}
		return err
	}
	storages := map[string]bool{}
	if s.Backup.Storage != "" {
		storages[s.Backup.Storage] = true
	}
	for _, b := range s.Backup.Schedules {
		if _, err := cron.ParseStandard(b.Cron); err != nil {
			return invalid("invalid backup schedule %q", b.Name)
		}
		if b.Type != "snapshot" || b.RetainCount < 1 {
			return invalid("schedules require snapshot type and positive retention")
		}
		storage := b.Storage
		if storage == "" {
			storage = s.Backup.Storage
		}
		if storage == "" {
			return invalid("backup schedule requires storage")
		}
		storages[storage] = true
	}
	for name := range storages {
		if p.options.Store == nil {
			return invalid("backup storage validation requires store")
		}
		if _, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, name); err != nil {
			return invalid("backup storage %q not found", name)
		}
	}
	if old != nil {
		if old.Name != n.Name || old.Spec.Provider != s.Provider {
			return invalid("cluster name and provider are immutable")
		}
		if old.Spec.Topology.Type != s.Topology.Type {
			return invalid("topology is immutable")
		}
		if old.Spec.TLS.CA != s.TLS.CA {
			return invalid("CA source is immutable")
		}
		if old.Spec.Version != s.Version {
			if ok, _ := UpgradeAllowed(old.Spec.Version, s.Version); !ok {
				return invalid("unsupported upgrade path")
			}
			if old.Spec.Components.Engine.Replicas != s.Components.Engine.Replicas || old.Spec.Components.Extra["master"].Replicas != s.Components.Extra["master"].Replicas {
				return invalid("version and node counts cannot change together")
			}
		}
		for name, c := range comps {
			previous := old.Spec.Components.Extra[name]
			if name == "engine" {
				previous = old.Spec.Components.Engine
			}
			if c.Resources.MinDisk < previous.Resources.MinDisk || c.Resources.MaxDisk < previous.Resources.MaxDisk {
				return invalid("disk shrinking is not supported")
			}
		}
		normalize := func(d *domain.DataSource, ns string) *domain.DataSource {
			if d == nil {
				return nil
			}
			copy := *d
			if copy.Namespace == "" {
				copy.Namespace = domain.NormalizedNamespace(ns)
			}
			return &copy
		}
		if !reflect.DeepEqual(normalize(old.Spec.DataSource, old.Namespace), normalize(s.DataSource, n.Namespace)) {
			return invalid("dataSource is immutable")
		}
	}
	if d := s.DataSource; d != nil {
		if p.options.Store == nil {
			return invalid("dataSource requires store")
		}
		ref := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
		if ref.Namespace == "" {
			ref.Namespace = n.Ref().Namespace
		}
		if d.Cluster == "" || ref.Key() == n.Ref().Key() || d.Backup == "" || d.PITRTime != nil {
			return invalid("dataSource requires another cluster and a snapshot backup")
		}
		source, err := p.options.Store.GetCluster(ctx, ref)
		if err != nil || source.Spec.Provider != "elasticsearch" {
			return invalid("source cluster must exist and use Elasticsearch")
		}
		b, err := p.options.Store.GetBackup(ctx, ref, d.Backup)
		if err != nil || b.Phase != domain.PhaseSucceeded || b.Type != "snapshot" {
			return invalid("source snapshot must have succeeded")
		}
		version := b.SourceVersion
		if version == "" {
			version = source.Spec.Version
		}
		a, ok := parseVersion(snapshotReleaseVersion(version))
		target, _ := parseVersion(s.Version)
		if !ok || a[0] > target[0] || target[0] > a[0]+1 || a[0] == target[0] && (a[1] > target[1] || a[1] == target[1] && a[2] > target[2]) {
			return invalid("source version is incompatible with target")
		}
	}
	return nil
}

// RunOperation advances a durable provider operation.
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	return p.runOperation(ctx, rc, s, op)
}

// SyncDuringOperation permits convergence only at operation-safe checkpoints.
func (*Provider) SyncDuringOperation(op *domain.Operation) bool { return syncDuringOperation(op) }
