// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package neo4j reconciles a TLS-only Neo4j Community singleton.
// Admitted exposure is retained independently of runtime routes so suspension
// and runtime recovery restore accepted access even outside maintenance windows.
// Deferred exposure edits do not acknowledge their desired generation.
package neo4j

import (
	"context"
	"log/slog"
	"reflect"
	"regexp"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
)

type Options struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          engine.ClusterStore
	Logger                         *slog.Logger
}
type Provider struct{ options Options }

func New(o Options) *Provider  { return &Provider{options: o} }
func (*Provider) Name() string { return "neo4j" }

const defaultVersion = "2026.10.0"

func resources() domain.Resources {
	return domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 2, MaxRAM: 2, MinDisk: 10, MaxDisk: 10}
}
func (*Provider) Describe() domain.ProviderInfo {
	return domain.ProviderInfo{
		Name: "neo4j", DisplayName: "Neo4j Community",
		Versions:     []domain.VersionInfo{{Version: "2026.09.0", Major: "2026", Status: "supported"}, {Version: defaultVersion, Major: "2026", Default: true, Status: "supported"}},
		UpgradePaths: []domain.UpgradePath{{From: "2026.09.0", To: defaultVersion, RequiresBackup: true}},
		Topologies:   []domain.TopologyInfo{{Type: "single", Components: map[string]domain.ComponentInfo{"engine": {Defaults: domain.ComponentSpec{Replicas: 1, Resources: resources()}, MinReplicas: 1, MaxReplicas: 1, AllowedReplicas: []int{1}}}}},
		BackupTypes:  []string{"dump"},
		Capabilities: []string{"tls", "monitoring", "publicExposure", "systemUsers", "neo4jAccess", "backups", "restores", "clone"},
		Operations:   []string{domain.OperationBackup, domain.OperationRestore, domain.OperationDeleteBackup, domain.OperationRestart, domain.OperationRotateSystemUser, domain.OperationRotateTLS}, Processes: []string{"neo4j"},
		EngineOptions: map[string]any{
			"heapMB":               map[string]any{"type": "integer", "minimum": 256, "maximum": 31744, "description": "40% of minimum RAM by default; heap plus page cache must not exceed 80%"},
			"pageCacheMB":          map[string]any{"type": "integer", "minimum": 64, "description": "25% of minimum RAM by default"},
			"acceptBackupDowntime": map[string]any{"type": "boolean", "default": false, "description": "Explicitly allow full outages during scheduled offline dumps"},
			"settings":             map[string]any{"type": "object", "additionalProperties": false, "allowedKeys": settingKeys()},
		}, EngineConfig: map[string]any{"supported": false},
	}
}
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "neo4j"
	}
	if s.Version == "" {
		s.Version = defaultVersion
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "single"
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 1
	}
	r, d := &s.Components.Engine.Resources, resources()
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

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{0,11}$`)

func (*Provider) Validate(_ context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return engine.Invalid("cluster is nil")
	}
	s := n.Spec
	if !clusterName.MatchString(n.Name) {
		return engine.Invalid("cluster name must be 1..12 lowercase alphanumeric characters starting with a letter")
	}
	if s.Provider != "neo4j" {
		return engine.Invalid("provider must be neo4j")
	}
	if s.Version != "2026.09.0" && s.Version != defaultVersion {
		return engine.Invalid("unsupported Neo4j version")
	}
	if s.Topology.Type != "single" || s.Topology.Shards != 0 || s.Components.Engine.Replicas != 1 {
		return engine.Invalid("Neo4j Community requires single topology and exactly one member")
	}
	if !reflect.DeepEqual(s.Components.Router, domain.ComponentSpec{}) || !reflect.DeepEqual(s.Components.ConfigServer, domain.ComponentSpec{}) || len(s.Components.Extra) > 0 {
		return engine.Invalid("additional components are not supported")
	}
	if err := (engine.ComponentSet{{Name: "engine", MinReplicas: 1, MaxReplicas: 1}}).Validate(domain.Components{Extra: map[string]domain.ComponentSpec{"engine": s.Components.Engine}}); err != nil {
		return err
	}
	if s.Components.Engine.Resources.MinRAM < 1 || s.Components.Engine.Resources.MinDisk < 1 {
		return engine.Invalid("Neo4j requires at least 1 GiB RAM and 1 GB disk")
	}
	if s.EngineConfig != "" {
		return engine.Invalid("Neo4j uses engineOptions, not engineConfig")
	}
	o, err := ParseOptions(s.EngineOptions)
	if err != nil {
		return err
	}
	if _, _, err = memory(s.Components.Engine.Resources.MinRAM, o); err != nil {
		return err
	}
	if s.TLS.Mode != "requireTLS" || (s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided") {
		return engine.Invalid("Neo4j requires TLS and a selfSigned or userProvided CA")
	}
	if s.Backup.PITR.Enabled {
		return engine.Invalid("Neo4j Community does not support PITR")
	}
	for _, schedule := range s.Backup.Schedules {
		if schedule.Enabled && !o.AcceptBackupDowntime {
			return engine.Invalid("scheduled Neo4j dumps require acceptBackupDowntime=true")
		}
	}
	var exposure *domain.Exposure
	if old != nil {
		exposure = &old.Spec.Exposure
		if old.Spec.Version != s.Version && !(old.Spec.Version == "2026.09.0" && s.Version == defaultVersion) {
			return engine.Invalid("unsupported Neo4j upgrade or downgrade")
		}
		a, b := old.Spec.Components.Engine.Resources, s.Components.Engine.Resources
		if b.MinDisk < a.MinDisk || b.MaxDisk < a.MaxDisk {
			return engine.Invalid("data volumes cannot shrink")
		}
	}
	if err := domain.ValidateExposureUpdate(exposure, s.Exposure); err != nil {
		return engine.Invalid("%v", err)
	}
	for _, name := range []string{"neo4j-community-" + s.Version, "temurin-jre-21"} {
		a, e := nativeartifacts.Get(name)
		if e != nil {
			return engine.Invalid("%v", e)
		}
		if _, e = nativeartifacts.ResolveURL(a); e != nil {
			return engine.Invalid("%v", e)
		}
	}
	return nil
}

var _ engine.Provider = (*Provider)(nil)
