// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package psmdb reconciles Percona Server for MongoDB on singleton Zerops services.
package psmdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/robfig/cron/v3"
)

// Options configures the provider's control-plane endpoint and validation lookups.
type Options struct {
	ControlPlaneURL string
	InternalCAPEM   string
	Logger          *slog.Logger
	Store           engine.ClusterStore
}

// Provider owns the PSMDB reconciliation and data-plane implementations.
type Provider struct {
	options              Options
	portMu               sync.Mutex
	portReservations     map[string]map[int]string
	deleteClusterObjects func(context.Context, domain.BackupStorage, string, string) error
	ensureManagedUsers   func(context.Context, *engine.Context, *internalState) error
}

// New constructs a PSMDB provider.
func New(o Options) *Provider {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ControlPlaneURL == "" {
		o.ControlPlaneURL = "https://karakoram:9443"
	}
	p := &Provider{options: o, portReservations: make(map[string]map[int]string), deleteClusterObjects: backupstorage.DeleteCluster}
	p.ensureManagedUsers = p.reconcileManagedUsers
	return p
}

// Name returns the stable engine identifier.
func (*Provider) Name() string { return "psmdb" }

// RelayTargets exposes the provider's observed and planned relay destinations.
func (*Provider) RelayTargets(c *domain.Cluster) []string { return RelayTargets(c) }

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)

// Catalog pins noble packages published by repo.percona.com.
// 8.3 is deliberately excluded: repository presence alone does not establish a supported major-upgrade path.
var catalog = []domain.VersionInfo{{Version: "6.0.29-23", Major: "6.0", Status: "deprecated"}, {Version: "7.0.40-22", Major: "7.0", Status: "supported"}, {Version: "7.0.43-23", Major: "7.0", Status: "supported"}, {Version: "8.0.29-13", Major: "8.0", Status: "supported"}, {Version: "8.0.32-14", Major: "8.0", Default: true, Status: "supported"}}

func defaultResources() domain.Resources {
	return domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}
}
func major(v string) string {
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[0] + "." + parts[1]
}
func numericVersion(v string) []int {
	v = strings.TrimSuffix(v, ".noble")
	s := strings.FieldsFunc(v, func(r rune) bool { return r == '.' || r == '-' })
	n := make([]int, len(s))
	for i, x := range s {
		n[i], _ = strconv.Atoi(x)
	}
	return n
}
func newer(a, b string) bool {
	aa, bb := numericVersion(a), numericVersion(b)
	for i := range min(len(aa), len(bb)) {
		if aa[i] != bb[i] {
			return aa[i] > bb[i]
		}
	}
	return false
}
func upgrade(from, to string) bool {
	if from == to {
		return true
	}
	if major(from) == major(to) {
		return newer(to, from)
	}
	a, _ := strconv.Atoi(strings.Split(from, ".")[0])
	b, _ := strconv.Atoi(strings.Split(to, ".")[0])
	return b == a+1
}

// Describe exposes supported versions, adjacent-major upgrades and topology constraints.
func (*Provider) Describe() domain.ProviderInfo {
	r := defaultResources()
	ci := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 3, Resources: r}, MinReplicas: 1, MaxReplicas: 7, AllowedReplicas: []int{1, 3, 5, 7}}
	router := domain.ComponentInfo{Defaults: domain.ComponentSpec{Replicas: 1, Resources: r}, MinReplicas: 1, MaxReplicas: 32}
	v := domain.ProviderInfo{Name: "psmdb", DisplayName: "Percona Server for MongoDB", Versions: append([]domain.VersionInfo(nil), catalog...), BackupTypes: []string{"logical", "physical"}, Topologies: []domain.TopologyInfo{{Type: "replicaSet", Components: map[string]domain.ComponentInfo{"engine": ci}}, {Type: "sharded", Components: map[string]domain.ComponentInfo{"engine": ci, "configServer": ci, "router": router}, Schema: json.RawMessage(`{"type":"object","properties":{"shards":{"type":"integer","minimum":1,"maximum":128,"default":1}}}`)}}}
	v.Capabilities = []string{"databaseUsers", "databaseRoles", "systemUsers", "balancer", "pitr", "backups", "restores", "fcv", "publicExposure", "tls", "monitoring", "engineConfig"}
	v.Operations = []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, domain.OperationRaiseFCV, domain.OperationRestart, domain.OperationReplaceMember, domain.OperationRotateSystemUser, domain.OperationRotateTLS}
	v.Processes = []string{"mongod", "mongos", "pbm-agent"}
	for i := range v.Versions {
		v.Versions[i].PBMVersion = "2.16.0-1"
	}
	for _, a := range catalog {
		for _, b := range catalog {
			if a.Version != b.Version && upgrade(a.Version, b.Version) {
				path := domain.UpgradePath{From: a.Version, To: b.Version}
				if a.Major != b.Major {
					path.RequiresFCV = a.Major
				}
				v.UpgradePaths = append(v.UpgradePaths, path)
			}
		}
	}
	v.Presets = []domain.Preset{
		{
			Name:        "dev",
			Description: "Single member, 1 GB RAM; no database redundancy",
			Topology:    domain.Topology{Type: "replicaSet"},
			Components:  domain.Components{Engine: domain.ComponentSpec{Replicas: 1, Resources: r}},
		},
		{
			Name:        "prod",
			Description: "Three voting members; Zerops does not guarantee host separation",
			Topology:    domain.Topology{Type: "replicaSet"},
			Components:  domain.Components{Engine: domain.ComponentSpec{Replicas: 3, Resources: r}},
		},
		{
			Name:        "sharded",
			Description: "Two three-member shards, three config servers and one router",
			Topology:    domain.Topology{Type: "sharded", Shards: 2},
			Components:  domain.Components{Engine: ci.Defaults, ConfigServer: ci.Defaults, Router: router.Defaults},
		},
	}
	return v
}

// ApplyDefaults fills only omitted fields.
func (*Provider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "psmdb"
	}
	if s.Version == "" {
		s.Version = "8.0.32-14"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "replicaSet"
	}
	if s.Topology.Type == "sharded" && s.Topology.Shards == 0 {
		s.Topology.Shards = 1
	}
	comps := []*domain.ComponentSpec{&s.Components.Engine}
	if s.Topology.Type == "sharded" {
		comps = append(comps, &s.Components.ConfigServer, &s.Components.Router)
	}
	for _, c := range comps {
		if c.Replicas == 0 {
			c.Replicas = 3
			if c == &s.Components.Router {
				c.Replicas = 1
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
	if s.Backup.PITR.OplogSpanMinutes == 0 {
		s.Backup.PITR.OplogSpanMinutes = 10
	}
}
func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }

// Validate checks both intrinsic constraints and unsafe changes against the prior spec.
func (p *Provider) Validate(ctx context.Context, old, n *domain.Cluster) error {
	if n == nil {
		return invalid("cluster is nil")
	}
	s := n.Spec
	if !clusterName.MatchString(n.Name) {
		return invalid("cluster name must match %s", clusterName)
	}
	if s.Provider != "psmdb" {
		return invalid("provider must be psmdb")
	}
	known := false
	for _, v := range catalog {
		known = known || v.Version == s.Version
	}
	if !known {
		versions := make([]string, 0, len(catalog))
		for _, v := range catalog {
			versions = append(versions, v.Version)
		}
		return engine.InvalidField("spec.version", domain.ReasonUnsupported, "spec.version is not offered by provider psmdb; offered versions: %s", strings.Join(versions, ", "))
	}
	if s.Topology.Type != "replicaSet" && s.Topology.Type != "sharded" {
		return invalid("unsupported topology")
	}
	if s.Topology.Type == "sharded" && (s.Topology.Shards < 1 || s.Topology.Shards > 128) {
		return engine.InvalidField("spec.topology.shards", domain.ReasonOutOfRange, "spec.topology.shards must be from 1 to 128")
	}
	cs := []domain.ComponentSpec{s.Components.Engine}
	if s.Topology.Type == "sharded" {
		cs = append(cs, s.Components.ConfigServer, s.Components.Router)
	}
	for i, c := range cs {
		component := "engine"
		if i == 1 {
			component = "configServer"
		} else if i == 2 {
			component = "router"
		}
		field := "spec.components." + component
		if i < 2 && (c.Replicas < 1 || c.Replicas > 7 || c.Replicas%2 == 0) {
			return engine.InvalidField(field+".replicas", domain.ReasonOutOfRange, "%s.replicas must be 1, 3, 5 or 7", field)
		}
		if i == 2 && (c.Replicas < 1 || c.Replicas > 32) {
			return engine.InvalidField(field+".replicas", domain.ReasonOutOfRange, "%s.replicas must be from 1 to 32", field)
		}
		r := c.Resources
		if r.CPUMode != "SHARED" && r.CPUMode != "DEDICATED" {
			return engine.InvalidField(field+".resources.cpuMode", domain.ReasonInvalid, "%s.resources.cpuMode must be SHARED or DEDICATED", field)
		}
		for _, bound := range []struct {
			name           string
			value, minimum float64
		}{
			{"minCpu", r.MinCPU, 1}, {"maxCpu", r.MaxCPU, r.MinCPU},
			{"minRam", r.MinRAM, 0.5}, {"maxRam", r.MaxRAM, r.MinRAM},
			{"minDisk", r.MinDisk, 5}, {"maxDisk", r.MaxDisk, r.MinDisk},
		} {
			if bound.value < bound.minimum {
				return engine.InvalidField(field+".resources."+bound.name, domain.ReasonOutOfRange, "%s.resources.%s must be at least %g", field, bound.name, bound.minimum)
			}
		}
		// Zerops allocates whole CPU cores and rejects fractional autoscaling bounds.
		if r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || (r.StartCPU != nil && *r.StartCPU != math.Trunc(*r.StartCPU)) {
			name := "minCpu"
			if r.MaxCPU != math.Trunc(r.MaxCPU) {
				name = "maxCpu"
			} else if r.StartCPU != nil && *r.StartCPU != math.Trunc(*r.StartCPU) {
				name = "startCpu"
			}
			return engine.InvalidField(field+".resources."+name, domain.ReasonInvalid, "%s.resources.%s must be a whole number of cores (at least 1)", field, name)
		}
		if r.StartCPU != nil && (*r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
			return engine.InvalidField(field+".resources.startCpu", domain.ReasonOutOfRange, "%s.resources.startCpu must be between minCpu and maxCpu", field)
		}
	}
	if s.TLS.Mode != "disabled" && s.TLS.Mode != "preferTLS" && s.TLS.Mode != "requireTLS" {
		return invalid("invalid TLS mode")
	}
	if s.TLS.CA != "selfSigned" && s.TLS.CA != "userProvided" {
		return invalid("invalid TLS CA source")
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
	if s.Backup.PITR.Enabled && s.Backup.Storage == "" {
		return invalid("PITR requires backup storage")
	}
	if s.Backup.PITR.OplogSpanMinutes < 1 {
		return invalid("PITR oplog span must be positive")
	}
	storages := map[string]bool{}
	if s.Backup.Storage != "" {
		storages[s.Backup.Storage] = true
	}
	for i, b := range s.Backup.Schedules {
		if _, e := cron.ParseStandard(b.Cron); e != nil {
			return invalid("invalid backup schedule %q", b.Name)
		}
		if b.Type != "logical" && b.Type != "physical" {
			return invalid("invalid backup type")
		}
		if b.RetainCount < 1 {
			return engine.InvalidField(fmt.Sprintf("spec.backup.schedules.%d.retainCount", i), domain.ReasonOutOfRange, "spec.backup.schedules.%d.retainCount must be positive (at least 1)", i)
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
	if len(storages) > 0 && p.options.Store == nil {
		return invalid("backup storage validation requires configured store")
	}
	for storage := range storages {
		if _, e := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, storage); e != nil {
			return invalid("backup storage %q: %v", storage, e)
		}
	}
	if _, err := agentapi.ParseEngineConfig(s.EngineConfig); err != nil {
		return invalid("%v", err)
	}
	if old != nil {
		if old.Name != n.Name {
			return invalid("cluster name is immutable")
		}
		if old.Spec.Topology.Type != s.Topology.Type {
			return invalid("topology type is immutable")
		}
		if s.Topology.Shards < old.Spec.Topology.Shards {
			return invalid("shard removal is not supported")
		}
		if !upgrade(old.Spec.Version, s.Version) {
			return invalid("upgrade must be newer within a major or to the next major")
		}
		if major(old.Spec.Version) != major(s.Version) && old.Status.FCV != major(old.Spec.Version) {
			return invalid("major upgrade requires observed FCV %s; raise FCV first", major(old.Spec.Version))
		}
		olds := []domain.ComponentSpec{old.Spec.Components.Engine, old.Spec.Components.ConfigServer, old.Spec.Components.Router}
		for i, c := range cs {
			if c.Resources.MinDisk < olds[i].Resources.MinDisk || c.Resources.MaxDisk < olds[i].Resources.MaxDisk {
				return invalid("disk shrinking is not supported")
			}
		}
		if old.Spec.TLS.CA != s.TLS.CA {
			return invalid("CA source is immutable; rotate leaf certificates instead")
		}
		normalizeSource := func(source *domain.DataSource, namespace string) *domain.DataSource {
			if source == nil {
				return nil
			}
			copy := *source
			if copy.Namespace == "" {
				copy.Namespace = domain.NormalizedNamespace(namespace)
			}
			return &copy
		}
		a, _ := json.Marshal(normalizeSource(old.Spec.DataSource, old.Namespace))
		b, _ := json.Marshal(normalizeSource(s.DataSource, n.Namespace))
		if string(a) != string(b) {
			return invalid("dataSource is immutable")
		}
	}
	if d := s.DataSource; old == nil && d != nil {
		sourceRef := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
		if sourceRef.Namespace == "" {
			sourceRef.Namespace = n.Ref().Namespace
		}
		if d.Cluster == "" || sourceRef.Key() == n.Ref().Key() || (d.Backup == "") == (d.PITRTime == nil) {
			return invalid("dataSource requires another cluster and exactly one of backup or pitrTime")
		}
		if p.options.Store == nil {
			return invalid("dataSource validation requires store")
		}
		var source *domain.Cluster
		if d.Backup != "" {
			b, e := p.options.Store.GetBackup(ctx, sourceRef, d.Backup)
			if e != nil || b.Phase != domain.PhaseSucceeded {
				return invalid("source backup %q on cluster %q in namespace %q must exist and be succeeded", d.Backup, sourceRef.Name, sourceRef.Namespace)
			}
			topology, version := b.SourceTopology, b.SourceVersion
			if topology.Type == "" || version == "" {
				source, e = p.options.Store.GetCluster(ctx, sourceRef)
				if e != nil {
					return invalid("source backup %q lacks topology/version provenance and source cluster %q in namespace %q is unavailable: %v", d.Backup, sourceRef.Name, sourceRef.Namespace, e)
				}
				topology, version = source.Spec.Topology, source.Spec.Version
			}
			if topology.Type != s.Topology.Type || major(version) != major(s.Version) {
				return invalid("restore source must match topology and engine major")
			}
		} else {
			var e error
			source, e = p.options.Store.GetCluster(ctx, sourceRef)
			if e != nil {
				return invalid("source cluster %q in namespace %q: %v", sourceRef.Name, sourceRef.Namespace, e)
			}
			if source.Spec.Topology.Type != s.Topology.Type || major(source.Spec.Version) != major(s.Version) {
				return invalid("restore source must match topology and engine major")
			}
			window := source.Status.Backup.PITRWindow
			if d.PITRTime.IsZero() || window == nil || d.PITRTime.Before(window.Start) || d.PITRTime.After(window.End) {
				return invalid("dataSource PITR time must be in the source cluster's observed recoverable window")
			}
		}
	}
	return nil
}
