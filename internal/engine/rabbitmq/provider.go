// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
	"strings"
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
func (*Provider) Name() string { return "rabbitmq" }

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)

// The curated broker versions share one compatible, exactly pinned Erlang package.
var catalog = struct {
	ErlangVersion string
	Versions      []domain.VersionInfo
}{
	ErlangVersion: "1:27.3.4.17-1",
	Versions: []domain.VersionInfo{
		{Version: "4.3.5", Major: "4.3", Status: "deprecated"},
		{Version: "4.3.6", Major: "4.3", Status: "supported", Default: true},
	},
}

func defaultResources() domain.Resources {
	return domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}
}
func upgrade(from, to string) bool             { return from == to || from == "4.3.5" && to == "4.3.6" }
func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }
func (*Provider) Describe() domain.ProviderInfo {
	r := defaultResources()
	ci := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 3, Resources: r}, MinReplicas: 1, MaxReplicas: 7, AllowedReplicas: []int{1, 3, 5, 7}}
	return domain.ProviderInfo{Name: "rabbitmq", DisplayName: "RabbitMQ", Versions: append([]domain.VersionInfo(nil), catalog.Versions...), UpgradePaths: []domain.UpgradePath{{From: "4.3.5", To: "4.3.6"}}, Topologies: []domain.TopologyInfo{{Type: "cluster", Components: map[string]domain.ComponentInfo{"engine": ci}}}, Presets: []domain.Preset{{Name: "dev", Description: "Single broker; no queue redundancy", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: domain.ComponentSpec{Replicas: 1, Resources: r}}}, {Name: "prod", Description: "Three brokers; placement separation is not guaranteed", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: ci.Defaults}}}, Capabilities: []string{"systemUsers", "backups", "restores", "publicExposure", "tls", "monitoring", "engineConfig", "rabbitmqAccess"}, Operations: []string{"backup", "delete-backup", "restore", "restart", "replace-member", "rotate-system-user", "rotate-tls"}, Processes: []string{"rabbitmq-server"}, BackupTypes: []string{"definitions"}, EngineOptions: optionsDescription(), EngineConfig: map[string]any{
		"format": "rabbitmq.conf", "syntax": "key = value",
		"managedKeyPrefixes": slices.Clone(managedConfigPrefixes),
		"managedKeyMatching": "A key is rejected when it starts with a listed prefix or equals that prefix with its trailing dot removed.",
	}}
}
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "rabbitmq"
	}
	if s.Version == "" {
		s.Version = "4.3.6"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "cluster"
	}
	c := &s.Components.Engine
	if c.Replicas == 0 {
		c.Replicas = 3
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
		r.MinRAM = 1
	}
	if r.MaxRAM == 0 {
		r.MaxRAM = r.MinRAM
	}
	if r.MinDisk == 0 {
		r.MinDisk = 5
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
}

type pluginGroup struct {
	ID           string              `json:"id"`
	Label        string              `json:"label"`
	Plugins      []string            `json:"plugins"`
	Dependencies map[string][]string `json:"dependencies"`
}

var optionalPluginGroups = []pluginGroup{
	{ID: "shovel", Label: "Shovel", Plugins: []string{"rabbitmq_shovel", "rabbitmq_shovel_management"}, Dependencies: map[string][]string{"rabbitmq_shovel_management": {"rabbitmq_shovel"}}},
	{ID: "federation", Label: "Federation", Plugins: []string{"rabbitmq_federation", "rabbitmq_federation_management"}, Dependencies: map[string][]string{"rabbitmq_federation_management": {"rabbitmq_federation"}}},
	{ID: "stream", Label: "Stream", Plugins: []string{"rabbitmq_stream", "rabbitmq_stream_management"}, Dependencies: map[string][]string{"rabbitmq_stream_management": {"rabbitmq_stream"}}},
}

var alwaysEnabledPlugins = []string{"rabbitmq_management", "rabbitmq_prometheus"}
var managementPublicUnavailableTLSModes = []string{"requireTLS"}

var managedConfigPrefixes = []string{
	"listeners.", "ssl_options.", "management.listener.", "management.tcp.", "management.ssl.",
	"prometheus.", "cluster_formation.", "vm_memory_high_watermark", "total_memory_available_override_value",
	"disk_free_limit", "log.", "default_user", "default_pass", "loopback_users", "distribution.",
	"quorum_queue.initial_cluster_size", "default_queue_type", "management.load_definitions",
	"load_definitions", "definitions.", "auth_backends", "ssl_cert_login", "stream.listeners.",
	"stream.replication.", "stream.ssl.", "osiris.",
}

func optionsDescription() map[string]any {
	groups := make([]pluginGroup, 0, len(optionalPluginGroups))
	for _, group := range optionalPluginGroups {
		dependencies := make(map[string][]string, len(group.Dependencies))
		for plugin, required := range group.Dependencies {
			dependencies[plugin] = slices.Clone(required)
		}
		groups = append(groups, pluginGroup{ID: group.ID, Label: group.Label, Plugins: slices.Clone(group.Plugins), Dependencies: dependencies})
	}
	return map[string]any{
		"pluginGroups":         groups,
		"alwaysEnabledPlugins": slices.Clone(alwaysEnabledPlugins),
		"managementUI": map[string]any{"public": map[string]any{
			"type": "boolean", "default": false, "unavailableTLSModes": slices.Clone(managementPublicUnavailableTLSModes),
			"description": "Publishes the RabbitMQ management UI/API through backend HTTPS routing with a private plaintext HTTP hop. Uses RabbitMQ credentials and is not limited by the AMQP allowlist.",
		}},
	}
}

type engineOptions struct {
	Plugins      []string `json:"plugins,omitempty"`
	ManagementUI struct {
		Public bool `json:"public"`
	} `json:"managementUI,omitempty"`
}

func parseOptions(raw json.RawMessage) (engineOptions, error) {
	var o engineOptions
	if len(raw) == 0 {
		return o, nil
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&o); err != nil {
		return o, invalid("invalid engineOptions: %v", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return o, invalid("engineOptions must contain one JSON object")
	}
	if strings.TrimSpace(string(raw)) == "null" {
		return o, invalid("engineOptions must be an object")
	}
	allowed := func(plugin string) bool {
		for _, group := range optionalPluginGroups {
			if slices.Contains(group.Plugins, plugin) {
				return true
			}
		}
		return false
	}
	seen := map[string]bool{}
	for _, plugin := range o.Plugins {
		if !allowed(plugin) || seen[plugin] {
			return o, invalid("unsupported or duplicate plugin %q", plugin)
		}
		seen[plugin] = true
	}
	for _, group := range optionalPluginGroups {
		for plugin, dependencies := range group.Dependencies {
			for _, dependency := range dependencies {
				if seen[plugin] && !seen[dependency] {
					return o, invalid("plugin %q requires %q", plugin, dependency)
				}
			}
		}
	}
	return o, nil
}

var configKey = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$`)

func validateEngineConfig(config string) error {
	for _, line := range strings.Split(config, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || !configKey.MatchString(k) || strings.TrimSpace(v) == "" {
			return invalid("engineConfig requires key = value lines")
		}
		for _, prefix := range managedConfigPrefixes {
			if k == strings.TrimSuffix(prefix, ".") || strings.HasPrefix(k, prefix) {
				return invalid("engineConfig key %q is managed", k)
			}
		}
		if strings.ContainsAny(v, "\x00\r") || strings.Contains(v, "$(") {
			return invalid("invalid engineConfig value")
		}
	}
	return nil
}
func (p *Provider) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return invalid("cluster is nil")
	}
	if _, err := p.DataMcp(n); err != nil {
		return err
	}
	s := n.Spec
	if !clusterName.MatchString(n.Name) {
		return invalid("cluster name must match %s", clusterName)
	}
	if s.Provider != "rabbitmq" {
		return invalid("provider must be rabbitmq")
	}
	if !slices.ContainsFunc(catalog.Versions, func(v domain.VersionInfo) bool { return v.Version == s.Version }) {
		return invalid("unsupported engine version %q", s.Version)
	}
	if s.Topology.Type != "cluster" || s.Topology.Shards != 0 {
		return invalid("RabbitMQ supports cluster topology only")
	}
	if !reflect.DeepEqual(s.Components.ConfigServer, domain.ComponentSpec{}) || !reflect.DeepEqual(s.Components.Router, domain.ComponentSpec{}) || len(s.Components.Extra) > 0 {
		return invalid("RabbitMQ supports only the engine component")
	}
	c := s.Components.Engine
	if c.Replicas != 1 && c.Replicas != 3 && c.Replicas != 5 && c.Replicas != 7 {
		return invalid("replicas must be 1, 3, 5 or 7")
	}
	r := c.Resources
	if r.CPUMode != "SHARED" && r.CPUMode != "DEDICATED" {
		return invalid("cpuMode must be SHARED or DEDICATED")
	}
	for _, v := range []float64{r.MinCPU, r.MaxCPU, r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return invalid("resource bounds must be finite")
		}
	}
	if r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinRAM < 0.5 || r.MaxRAM < r.MinRAM || r.MinDisk < 5 || r.MaxDisk < r.MinDisk {
		return invalid("invalid resource bounds (RAM >=0.5 GB, disk >=5 GB)")
	}
	if r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || r.StartCPU != nil && (*r.StartCPU != math.Trunc(*r.StartCPU) || math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0) || *r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
		return invalid("CPU bounds must be whole cores and startCpu within bounds")
	}
	if s.TLS.Mode != "disabled" && s.TLS.Mode != "preferTLS" && s.TLS.Mode != "requireTLS" {
		return invalid("invalid TLS mode")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return invalid("invalid TLS CA source")
	}
	if s.Exposure.Public && len(s.Exposure.Allowlist) == 0 {
		return invalid("public exposure requires an IP allowlist")
	}
	if s.Exposure.IPType != "IPV6" && s.Exposure.IPType != "IPV4" {
		return invalid("invalid exposure IP type")
	}
	for _, cidr := range s.Exposure.Allowlist {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return invalid("invalid allowlist CIDR %q", cidr)
		}
	}
	options, err := parseOptions(s.EngineOptions)
	if err != nil {
		return err
	}
	if slices.Contains(managementPublicUnavailableTLSModes, s.TLS.Mode) && options.ManagementUI.Public {
		return invalid("managementUI.public is unavailable with requireTLS: public management requires certificate-verified HTTPS backend routing or TLS TCP passthrough; public HTTPS routes forward plaintext HTTP")
	}
	if err := validateEngineConfig(s.EngineConfig); err != nil {
		return err
	}
	if s.Backup.PITR.Enabled {
		return invalid("RabbitMQ does not support PITR")
	}
	storages := map[string]bool{}
	if s.Backup.Storage != "" {
		storages[s.Backup.Storage] = true
	}
	for _, b := range s.Backup.Schedules {
		if _, err := cron.ParseStandard(b.Cron); err != nil {
			return invalid("invalid backup schedule %q", b.Name)
		}
		if b.Type != "definitions" || b.RetainCount < 1 {
			return invalid("backup schedules require definitions and positive retainCount")
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
	for storage := range storages {
		if p.options.Store == nil {
			return invalid("backup storage validation requires store")
		}
		if _, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, storage); err != nil {
			return invalid("backup storage %q: %v", storage, err)
		}
	}
	if old != nil {
		if old.Name != n.Name || old.Ref().Namespace != n.Ref().Namespace || old.Spec.Provider != s.Provider {
			return invalid("cluster identity and provider are immutable")
		}
		if !reflect.DeepEqual(old.Spec.Topology, s.Topology) {
			return invalid("topology is immutable")
		}
		if !upgrade(old.Spec.Version, s.Version) {
			return invalid("upgrade must be forward within 4.3")
		}
		if r.MinDisk < old.Spec.Components.Engine.Resources.MinDisk || r.MaxDisk < old.Spec.Components.Engine.Resources.MaxDisk {
			return invalid("disk shrinking is not supported")
		}
		if old.Spec.TLS.CA != s.TLS.CA {
			return invalid("CA source is immutable")
		}
		if (old.Spec.TLS.Mode == "requireTLS") != (s.TLS.Mode == "requireTLS") {
			return invalid("inter-node TLS is fixed at creation; create a new cluster and restore definitions")
		}
		normalize := func(d *domain.DataSource, ns string) *domain.DataSource {
			if d == nil {
				return nil
			}
			v := *d
			if v.Namespace == "" {
				v.Namespace = domain.NormalizedNamespace(ns)
			}
			return &v
		}
		if !reflect.DeepEqual(normalize(old.Spec.DataSource, old.Namespace), normalize(s.DataSource, n.Namespace)) {
			return invalid("dataSource is immutable")
		}
	}
	if d := s.DataSource; old == nil && d != nil {
		ref := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
		if ref.Namespace == "" {
			ref.Namespace = n.Ref().Namespace
		}
		if d.Cluster == "" || ref.Key() == n.Ref().Key() || d.Backup == "" || d.PITRTime != nil {
			return invalid("dataSource requires another cluster and a definitions backup")
		}
		if p.options.Store == nil {
			return invalid("dataSource validation requires store")
		}
		b, err := p.options.Store.GetBackup(ctx, ref, d.Backup)
		if err != nil || b.Phase != domain.PhaseSucceeded || b.Provider != "rabbitmq" || b.Type != "definitions" {
			return invalid("source backup %q in namespace %q, cluster %q must be a succeeded RabbitMQ definitions backup", d.Backup, domain.NormalizedNamespace(ref.Namespace), ref.Name)
		}
	}
	return nil
}

var _ engine.Provider = (*Provider)(nil)
