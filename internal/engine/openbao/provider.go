// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/robfig/cron/v3"
)

// ProviderOptions supplies control-plane dependencies, not the engineOptions document.
type ProviderOptions struct {
	ControlPlaneURL, InternalCAPEM string
	Logger                         *slog.Logger
	Store                          engine.ClusterStore
	Bindings                       pgbinding.Service
	Targets                        func(context.Context, domain.ClusterRef) (Target, error)
}
type Provider struct {
	options          ProviderOptions
	portMu           sync.Mutex
	portReservations map[string]map[int]string
	connectionsMu    sync.Map // cluster reference -> *sync.Mutex
}

func New(o ProviderOptions) *Provider {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ControlPlaneURL == "" {
		o.ControlPlaneURL = "https://karakoram:9443"
	}
	return &Provider{options: o, portReservations: map[string]map[int]string{}}
}
func (*Provider) Name() string { return "openbao" }

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)

// Catalog entries are ordered from oldest to newest; updates move forward only.
var catalog = []domain.VersionInfo{{Version: "2.7.1", Major: "2.7", Status: "supported", Default: true}}

func defaultResources() domain.Resources {
	return domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}
}
func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }
func (p *Provider) Describe() domain.ProviderInfo {
	r := defaultResources()
	c := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 3, Resources: r}, MinReplicas: 1, MaxReplicas: 5, AllowedReplicas: []int{1, 3, 5}}
	capabilities := []string{"monitoring", "tls", "publicExposure", "backups", "restores"}
	if p.options.Bindings != nil {
		capabilities = append(capabilities, "postgresql-storage")
	}
	return domain.ProviderInfo{Name: "openbao", DisplayName: "OpenBao", Versions: slices.Clone(catalog), Topologies: []domain.TopologyInfo{{Type: "cluster", Components: map[string]domain.ComponentInfo{"engine": c}}}, Presets: []domain.Preset{{Name: "dev", Description: "Single server; no Raft redundancy", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: domain.ComponentSpec{Replicas: 1, Resources: r}}}, {Name: "prod", Description: "Three Raft voters; placement separation is not guaranteed", Topology: domain.Topology{Type: "cluster"}, Components: domain.Components{Engine: c.Defaults}}}, Capabilities: capabilities, Operations: []string{"backup", "delete-backup", "restore", "restart", "replace-member", "rotate-tls"}, BackupTypes: []string{"raft-snapshot"}, Processes: []string{"openbao"}, EngineOptions: optionsDescription()}
}
func optionsDescription() map[string]any {
	return map[string]any{
		"unseal": map[string]any{"shares": map[string]any{"type": "integer", "default": 5, "minimum": 1, "maximum": 10, "immutable": true}, "threshold": map[string]any{"type": "integer", "default": 3, "minimum": 1, "maximum": 10, "immutable": true}},
		"seal": map[string]any{
			"type":    map[string]any{"type": "string", "enum": []string{"shamir", "transit", "awskms"}, "default": "shamir", "immutable": true},
			"transit": map[string]any{"address": map[string]any{"type": "string"}, "mountPath": map[string]any{"type": "string", "default": "transit/"}, "keyName": map[string]any{"type": "string"}, "caPEM": map[string]any{"type": "string"}, "tlsServerName": map[string]any{"type": "string"}, "token": map[string]any{"type": "string", "writeOnly": true}},
			"awskms":  map[string]any{"region": map[string]any{"type": "string"}, "kmsKeyId": map[string]any{"type": "string"}, "endpoint": map[string]any{"type": "string"}, "accessKeyId": map[string]any{"type": "string", "writeOnly": true}, "secretAccessKey": map[string]any{"type": "string", "writeOnly": true}},
		},
		"storage":  map[string]any{"type": map[string]any{"type": "string", "enum": []string{"raft", "postgresql"}, "default": "raft", "immutable": true}, "postgresql": map[string]any{"cluster": map[string]any{"type": "string"}}},
		"logLevel": map[string]any{"type": "string", "enum": []string{"trace", "debug", "info", "warn", "error"}, "default": "info"},
	}
}
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "openbao"
	}
	if s.Version == "" {
		s.Version = "2.7.1"
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
func parseOptions(raw json.RawMessage) (Options, error) {
	var o Options
	if len(raw) > 0 {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return o, invalid("engineOptions must be an object")
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		// Never include decoder errors: malformed write-only values may be secret.
		if err := d.Decode(&o); err != nil {
			return o, invalid("invalid engineOptions")
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			return o, invalid("engineOptions must contain one JSON object")
		}
	}
	if o.Unseal.Shares == 0 {
		o.Unseal.Shares = 5
	}
	if o.Unseal.Threshold == 0 {
		o.Unseal.Threshold = 3
	}
	if o.Seal.Type == "" {
		o.Seal.Type = "shamir"
	}
	if o.Storage.Type == "" {
		o.Storage.Type = "raft"
	}
	if o.LogLevel == "" {
		o.LogLevel = "info"
	}
	if o.Unseal.Shares < 1 || o.Unseal.Shares > 10 || o.Unseal.Threshold < 1 || o.Unseal.Threshold > o.Unseal.Shares || o.Unseal.Shares >= 2 && o.Unseal.Threshold < 2 {
		return o, invalid("unseal requires 1 <= threshold <= shares <= 10 and threshold >= 2 for multiple shares")
	}
	if !slices.Contains([]string{"trace", "debug", "info", "warn", "error"}, o.LogLevel) {
		return o, invalid("invalid logLevel")
	}
	return o, nil
}
func (p *Provider) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return invalid("cluster is nil")
	}
	s := n.Spec
	if !clusterName.MatchString(n.Name) {
		return invalid("cluster name must match %s", clusterName)
	}
	if s.Provider != "openbao" {
		return invalid("provider must be openbao")
	}
	if !slices.ContainsFunc(catalog, func(v domain.VersionInfo) bool { return v.Version == s.Version }) {
		return invalid("unsupported OpenBao version")
	}
	if s.Topology.Type != "cluster" || s.Topology.Shards != 0 {
		return invalid("OpenBao requires cluster topology")
	}
	if !reflect.DeepEqual(s.Components.ConfigServer, domain.ComponentSpec{}) || !reflect.DeepEqual(s.Components.Router, domain.ComponentSpec{}) || len(s.Components.Extra) > 0 {
		return invalid("OpenBao supports only the engine component")
	}
	if !slices.Contains([]int{1, 3, 5}, s.Components.Engine.Replicas) {
		return invalid("replicas must be 1, 3 or 5")
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
	if r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinRAM < 0.5 || r.MaxRAM < r.MinRAM || r.MinDisk < 5 || r.MaxDisk < r.MinDisk {
		return invalid("invalid resource bounds (RAM >=0.5 GB, disk >=5 GB)")
	}
	if r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || r.StartCPU != nil && (math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0) || *r.StartCPU != math.Trunc(*r.StartCPU) || *r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
		return invalid("CPU bounds must be whole cores and startCpu within bounds")
	}
	if s.TLS.Mode != "requireTLS" {
		return invalid("OpenBao requires requireTLS")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return invalid("invalid TLS CA source")
	}
	if s.Exposure.Public && len(s.Exposure.Allowlist) == 0 {
		return invalid("public exposure requires an IP allowlist")
	}
	if s.Exposure.IPType != "IPV4" && s.Exposure.IPType != "IPV6" {
		return invalid("invalid exposure IP type")
	}
	for _, cidr := range s.Exposure.Allowlist {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return invalid("invalid allowlist CIDR")
		}
	}
	if s.EngineConfig != "" {
		return invalid("OpenBao does not accept engineConfig overrides")
	}
	if s.Backup.PITR.Enabled {
		return invalid("OpenBao does not support PITR")
	}
	if p.postgresStorage(n) && (len(s.Backup.Schedules) > 0 || s.DataSource != nil) {
		return invalid("PostgreSQL-storage OpenBao clusters do not support raft-snapshot backups or restores")
	}
	for _, b := range s.Backup.Schedules {
		if _, err := cron.ParseStandard(b.Cron); err != nil {
			return invalid("invalid backup schedule %q", b.Name)
		}
		if b.Type != "raft-snapshot" || b.RetainCount < 1 {
			return invalid("backup schedules require raft-snapshot and positive retainCount")
		}
		storage := b.Storage
		if storage == "" {
			storage = s.Backup.Storage
		}
		if storage == "" {
			return invalid("backup schedule requires storage")
		}
		if p.options.Store == nil {
			return invalid("backup storage validation requires store")
		}
		if _, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, storage); err != nil {
			return invalid("backup storage is unavailable")
		}
	}
	if d := s.DataSource; d != nil {
		if d.PITRTime != nil || d.Backup == "" || d.Cluster == "" {
			return invalid("OpenBao dataSource requires a source cluster and raft-snapshot backup; PITR is not supported")
		}
		if old != nil && !reflect.DeepEqual(old.Spec.DataSource, d) {
			return invalid("dataSource is immutable")
		}
		if p.options.Store == nil {
			return invalid("dataSource validation requires store")
		}
		source := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
		if source.Namespace == "" {
			source.Namespace = n.Ref().Namespace
		}
		b, err := p.options.Store.GetBackup(ctx, source, d.Backup)
		if err != nil || b.Provider != "openbao" || b.Type != "raft-snapshot" || b.Phase != domain.PhaseSucceeded || b.Location == "" {
			return invalid("dataSource must reference a successful OpenBao raft-snapshot backup")
		}
	}
	o, err := parseOptions(s.EngineOptions)
	if err != nil {
		return err
	}
	var prior *Options
	if old != nil {
		v, err := parseOptions(old.Spec.EngineOptions)
		if err != nil {
			return err
		}
		prior = &v
		if old.Name != n.Name || old.Ref().Namespace != n.Ref().Namespace || old.Spec.Provider != s.Provider {
			return invalid("cluster identity and provider are immutable")
		}
		if !reflect.DeepEqual(old.Spec.Topology, s.Topology) {
			return invalid("topology is immutable")
		}
		if old.Spec.Version != s.Version {
			from := slices.IndexFunc(catalog, func(v domain.VersionInfo) bool { return v.Version == old.Spec.Version })
			to := slices.IndexFunc(catalog, func(v domain.VersionInfo) bool { return v.Version == s.Version })
			if from < 0 || to <= from {
				return invalid("version change must follow a forward catalog upgrade path")
			}
		}
		if r.MinDisk < old.Spec.Components.Engine.Resources.MinDisk || r.MaxDisk < old.Spec.Components.Engine.Resources.MaxDisk {
			return invalid("disk shrinking is not supported")
		}
		if old.Spec.TLS.CA != s.TLS.CA {
			return invalid("CA source is immutable")
		}
		if v.Unseal != o.Unseal {
			return invalid("unseal share settings are immutable")
		}
	}
	if o.Storage.Type == "postgresql" && p.options.Bindings == nil {
		return invalid("PostgreSQL storage requires a binding service")
	}
	return validateSealAndStorage(prior, &o)
}

var _ engine.Provider = (*Provider)(nil)
