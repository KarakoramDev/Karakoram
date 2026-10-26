// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"sort"
	"strconv"
	"strings"
	"time"
)

// resourceDecimal keeps the API's shortest float32 decimal representation;
// a direct widening adds binary tails that Zerops rejects as excess precision.
func resourceDecimal(v float32) float64 {
	d, _ := strconv.ParseFloat(strconv.FormatFloat(float64(v), 'f', -1, 32), 64)
	return d
}

func resourcesDomain(r *api.ResourcesInput) domain.Resources {
	if r == nil {
		return domain.Resources{}
	}
	o := domain.Resources{CPUMode: string(value(r.CpuMode)), MinCPU: resourceDecimal(value(r.MinCpu)), MaxCPU: resourceDecimal(value(r.MaxCpu)), MinRAM: resourceDecimal(value(r.MinRam)), MaxRAM: resourceDecimal(value(r.MaxRam)), MinDisk: resourceDecimal(value(r.MinDisk)), MaxDisk: resourceDecimal(value(r.MaxDisk))}
	if r.StartCpu != nil {
		o.StartCPU = ptr(resourceDecimal(*r.StartCpu))
	}
	return o
}
func resourcesAPI(r domain.Resources) api.Resources {
	o := api.Resources{CpuMode: api.ResourcesCpuMode(r.CPUMode), MinCpu: float32(r.MinCPU), MaxCpu: float32(r.MaxCPU), MinRam: float32(r.MinRAM), MaxRam: float32(r.MaxRAM), MinDisk: float32(r.MinDisk), MaxDisk: float32(r.MaxDisk)}
	if r.StartCPU != nil {
		o.StartCpu = ptr(float32(*r.StartCPU))
	}
	return o
}
func componentDomain(c *api.ComponentInput) domain.ComponentSpec {
	if c == nil {
		return domain.ComponentSpec{}
	}
	return domain.ComponentSpec{Replicas: value(c.Replicas), Resources: resourcesDomain(c.Resources)}
}
func componentAPI(c domain.ComponentSpec) api.Component {
	return api.Component{Replicas: c.Replicas, Resources: resourcesAPI(c.Resources)}
}
func extraResourcesDomain(r *api.ExtraResourcesInput) domain.Resources {
	if r == nil {
		return domain.Resources{}
	}
	o := domain.Resources{CPUMode: string(value(r.CpuMode)), MinCPU: resourceDecimal(value(r.MinCpu)), MaxCPU: resourceDecimal(value(r.MaxCpu)), MinRAM: resourceDecimal(value(r.MinRam)), MaxRAM: resourceDecimal(value(r.MaxRam)), MinDisk: resourceDecimal(value(r.MinDisk)), MaxDisk: resourceDecimal(value(r.MaxDisk))}
	if r.StartCpu != nil {
		o.StartCPU = ptr(resourceDecimal(*r.StartCpu))
	}
	return o
}
func specDomain(a api.ClusterInputSpec) domain.ClusterSpec {
	// Presence is retained in the generated input model: the validator rejects
	// explicit zero before conversion, while nil values remain provider defaults.
	s := domain.ClusterSpec{Provider: a.Provider, Version: value(a.Version), EngineConfig: value(a.EngineConfig)}
	if a.Topology != nil {
		s.Topology = domain.Topology{Type: string(value(a.Topology.Type)), Shards: value(a.Topology.Shards)}
	}
	if a.Components != nil {
		s.Components.Engine = componentDomain(a.Components.Engine)
		s.Components.ConfigServer = componentDomain(a.Components.ConfigServer)
		s.Components.Router = componentDomain(a.Components.Router)
	}
	if a.Components != nil && a.Components.Extra != nil {
		s.Components.Extra = make(map[string]domain.ComponentSpec, len(*a.Components.Extra))
		for name, c := range *a.Components.Extra {
			s.Components.Extra[name] = domain.ComponentSpec{Replicas: value(c.Replicas), Resources: extraResourcesDomain(c.Resources)}
		}
	}
	if a.EngineOptions != nil {
		s.EngineOptions, _ = json.Marshal(*a.EngineOptions)
	}
	if a.Backup != nil {
		s.Backup.Storage = value(a.Backup.Storage)
		if a.Backup.Schedules != nil {
			s.Backup.Schedules = make([]domain.BackupSchedule, 0, len(*a.Backup.Schedules))
			for _, b := range *a.Backup.Schedules {
				s.Backup.Schedules = append(s.Backup.Schedules, domain.BackupSchedule{Name: b.Name, Cron: b.Cron, Type: string(b.Type), Storage: value(b.Storage), RetainCount: b.RetainCount, Enabled: b.Enabled})
			}
		}
		if a.Backup.Pitr != nil {
			s.Backup.PITR = domain.PITRConfig{Enabled: a.Backup.Pitr.Enabled, OplogSpanMinutes: value(a.Backup.Pitr.OplogSpanMinutes)}
		}
	}
	if a.Tls != nil {
		s.TLS = domain.TLSConfig{Mode: string(value(a.Tls.Mode)), CA: string(value(a.Tls.Ca))}
	}
	if a.Exposure != nil {
		s.Exposure = domain.Exposure{Public: value(a.Exposure.Public), IPType: string(value(a.Exposure.IpType)), Allowlist: value(a.Exposure.Allowlist), AllowAllAddresses: value(a.Exposure.AllowAllAddresses)}
	}
	if a.Monitoring != nil {
		s.Monitoring.Enabled = a.Monitoring.Enabled
	}
	if a.DataSource != nil {
		s.DataSource = &domain.DataSource{Namespace: value(a.DataSource.Namespace), Cluster: a.DataSource.Cluster, Backup: value(a.DataSource.Backup), PITRTime: a.DataSource.PitrTime}
	}
	return s
}

// preserveExplicitSpecValues restores values whose zero representation is also
// used by provider defaults for omission. Input pointers retain that distinction
// for create/PUT and for the fully merged PATCH document.
func preserveExplicitSpecValues(s *domain.ClusterSpec, input api.ClusterInputSpec) {
	if input.Monitoring != nil {
		s.Monitoring.Enabled = input.Monitoring.Enabled
	}
	if input.Components == nil || input.Components.Extra == nil {
		return
	}
	for name, c := range *input.Components.Extra {
		component := s.Components.Extra[name]
		if c.Replicas != nil {
			component.Replicas = *c.Replicas
		}
		if c.Resources != nil {
			if c.Resources.MinDisk != nil {
				component.Resources.MinDisk = resourceDecimal(*c.Resources.MinDisk)
			}
			if c.Resources.MaxDisk != nil {
				component.Resources.MaxDisk = resourceDecimal(*c.Resources.MaxDisk)
			}
		}
		s.Components.Extra[name] = component
	}
}

// optionalString omits values that are absent, rather than inventing an empty
// identifier in a successful response.
func optionalString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func specAPI(s domain.ClusterSpec) api.ClusterSpec {
	a := api.ClusterSpec{Provider: s.Provider, Version: s.Version, Topology: api.Topology{Type: api.TopologyType(s.Topology.Type)}, Components: api.Components{Engine: componentAPI(s.Components.Engine)}, EngineConfig: optionalString(s.EngineConfig), Tls: &api.TLS{Mode: api.TLSMode(s.TLS.Mode), Ca: api.TLSCa(s.TLS.CA)}, Exposure: &api.Exposure{Public: s.Exposure.Public, Allowlist: ptr(nonNil(s.Exposure.Allowlist))}, Monitoring: &api.MonitoringSpec{Enabled: s.Monitoring.Enabled}}
	a.RetryEpoch = ptr(s.RetryEpoch)
	if s.Exposure.IPType != "" {
		a.Exposure.IpType = ptr(api.ExposureIpType(s.Exposure.IPType))
	}
	a.Exposure.AllowAllAddresses = ptr(s.Exposure.AllowAllAddresses)
	if s.Topology.Type == "sharded" {
		a.Topology.Shards = &s.Topology.Shards
		a.Components.ConfigServer = ptr(componentAPI(s.Components.ConfigServer))
		a.Components.Router = ptr(componentAPI(s.Components.Router))
	}
	if len(s.Components.Extra) > 0 {
		extra := make(map[string]api.Component, len(s.Components.Extra))
		for name, c := range s.Components.Extra {
			extra[name] = componentAPI(c)
		}
		a.Components.Extra = &extra
	}
	if len(s.EngineOptions) > 0 {
		var options map[string]any
		if json.Unmarshal(s.EngineOptions, &options) == nil {
			a.EngineOptions = &options
		}
	}
	schedules := make([]api.BackupSchedule, 0, len(s.Backup.Schedules))
	for _, b := range s.Backup.Schedules {
		schedules = append(schedules, api.BackupSchedule{Name: b.Name, Cron: b.Cron, Type: api.BackupScheduleType(b.Type), Storage: optionalString(b.Storage), RetainCount: b.RetainCount, Enabled: b.Enabled})
	}
	a.Backup = &api.BackupSpec{Storage: optionalString(s.Backup.Storage), Schedules: &schedules}
	if s.Backup.PITR.Enabled || s.Backup.PITR.OplogSpanMinutes != 0 {
		a.Backup.Pitr = &api.PITRSpec{Enabled: s.Backup.PITR.Enabled}
		if s.Backup.PITR.OplogSpanMinutes != 0 {
			a.Backup.Pitr.OplogSpanMinutes = &s.Backup.PITR.OplogSpanMinutes
		}
	}
	if s.DataSource != nil {
		a.DataSource = &api.DataSource{Namespace: ptr(domain.NormalizedNamespace(s.DataSource.Namespace)), Cluster: s.DataSource.Cluster, Backup: optionalString(s.DataSource.Backup), PitrTime: s.DataSource.PITRTime}
	}
	return a
}

// specPatchBase preserves absent collections instead of feeding response-only
// empty arrays back into desired state. Explicit empty arrays remain present.
func specPatchBase(s domain.ClusterSpec) api.ClusterSpec {
	a := specAPI(s)
	if s.Backup.Schedules == nil {
		a.Backup.Schedules = nil
	}
	if s.Exposure.Allowlist == nil {
		a.Exposure.Allowlist = nil
	}
	return a
}
func connectionAPI(c domain.Connection) api.ConnectionSummary {
	a := api.ConnectionSummary{Hosts: nonNil(c.Hosts), ReplicaSet: optionalString(c.ReplicaSet), Uri: c.URI, PublicHosts: ptr(nonNil(c.PublicHosts))}
	if len(c.Endpoints) > 0 {
		endpoints := make([]api.Endpoint, 0, len(c.Endpoints))
		for _, e := range c.Endpoints {
			endpoints = append(endpoints, api.Endpoint{Name: e.Name, Url: e.URL, Public: e.Public})
		}
		a.Endpoints = &endpoints
	}
	return a
}
func (s *Server) memberAPI(cluster domain.ClusterRef, m domain.MemberStatus) api.Member {
	usage := s.Monitoring.MemberUsage(cluster, m.Name)
	a := api.Member{Name: m.Name, Set: m.Set, Role: memberRole(m), State: m.State, Health: m.Health, Version: &m.Version, LagSeconds: ptr(float32(m.LagSeconds)), Optime: &m.Optime, Uptime: &m.Uptime, RamUsageBytes: usage.RAMBytes, DiskUsageBytes: usage.DiskBytes, DiskTotalBytes: usage.DiskTotalBytes, PlacementGroup: &m.PlacementGroup, Agent: &api.AgentStatus{Connected: m.Agent.Healthy, ConfigVersion: &m.Agent.ConfigVersion, LastSeenAt: &m.Agent.LastSeen}}
	a.Agent.Message = optionalString(redact.Diagnostic(m.Agent.Message))
	a.ComponentVersion = optionalString(m.ComponentVersion)
	if usage.CPUPercent != nil {
		a.CpuUsage = ptr(float32(*usage.CPUPercent))
	}
	if m.PublicHost != "" {
		a.PublicHost = &m.PublicHost
	}
	if m.Resources != nil {
		a.Resources = &api.ObservedResources{CpuCores: float32(m.Resources.CPUCores), MemoryGB: float32(m.Resources.MemoryGB), DiskGB: float32(m.Resources.DiskGB)}
	}
	if m.Autoscaling != nil {
		r := resourcesAPI(*m.Autoscaling)
		a.Autoscaling = &r
	}
	return a
}

func memberRole(m domain.MemberStatus) api.MemberRole {
	switch m.Role {
	case "gateway", "auth", "rest", "realtime", "storage", "imgproxy", "meta", "studio", "functions", "pooler", "analytics", "vector":
		return api.MemberRole(m.Role)
	case "broker", "master", "data", "master,data", "kibana", "logstash", "apm", "replica", "standalone", "proxy":
		return api.MemberRole(m.Role)
	}
	if m.Role == "mongos" || m.Role == "router" {
		return api.MemberRole("router")
	}
	state := strings.ToLower(m.State)
	if state == "" || state == "paused" || state == "error" {
		state = strings.ToLower(m.Role)
	}
	config := m.Role == "mongod-cfg" || m.Set == "cfg" || strings.HasPrefix(m.Role, "config-")
	switch state {
	case "primary", "secondary":
		if config {
			state = "config-" + state
		}
		return api.MemberRole(state)
	case "arbiter":
		if !config {
			return api.MemberRole("arbiter")
		}
	case "config-primary", "config-secondary":
		return api.MemberRole(state)
	}
	return api.MemberRole("unknown")
}
func placementAPI(p domain.Placement) api.Placement {
	p.Warning = redact.Diagnostic(p.Warning)
	a := api.Placement{Warning: p.Warning, GuaranteedAntiAffinity: false, Groups: []api.PlacementGroup{}}
	names := make([]string, 0, len(p.Groups))
	for n := range p.Groups {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		a.Groups = append(a.Groups, api.PlacementGroup{Name: n, Members: nonNil(p.Groups[n])})
	}
	return a
}

// freshStatus is an API-only projection. Only successful observations from this
// CP process can establish availability; wall time cannot prove that provenance.
// Never pass this projection to reconciliation or persist it.
func (s *Server) freshStatus(status domain.ClusterStatus) domain.ClusterStatus {
	return s.freshStatusAt(status, engine.RealClock{}.Now())
}

func (s *Server) freshStatusAt(status domain.ClusterStatus, at time.Time) domain.ClusterStatus {
	observedAt := status.ObservedAt
	if s.Context.ObservationEpoch == "" || status.ObservationEpoch != s.Context.ObservationEpoch {
		status.ObservedAt = time.Time{}
	}
	status = engine.FreshStatus(status, at)
	status.ObservedAt = observedAt
	return status
}

func (s *Server) statusAPI(cluster domain.ClusterRef, status domain.ClusterStatus) api.ClusterStatus {
	status.Message = redact.Diagnostic(status.Message)
	status = s.freshStatus(status)
	a := api.ClusterStatus{Phase: api.ClusterStatusPhase(status.Phase), Message: optionalString(status.Message), ObservedGeneration: status.ObservedGeneration, Version: optionalString(status.Version), Fcv: optionalString(status.FCV), Connection: ptr(connectionAPI(status.Connection)), Placement: ptr(placementAPI(status.Placement)), Conditions: []api.Condition{}, Members: []api.Member{}}
	for _, c := range status.Conditions {
		a.Conditions = append(a.Conditions, api.Condition{Type: c.Type, Status: api.ConditionStatus(c.Status), Reason: c.Reason, Message: redact.Diagnostic(c.Message), LastTransitionTime: c.LastTransitionTime})
	}
	for _, m := range status.Members {
		a.Members = append(a.Members, s.memberAPI(cluster, m))
	}
	a.Backup = &api.ClusterBackupStatus{}
	if status.Backup.LastBackup != nil {
		a.Backup.LastBackup = ptr(status.Backup.LastBackup.Format("2006-01-02T15:04:05Z07:00"))
	}
	if status.Backup.PITRWindow != nil {
		w := status.Backup.PITRWindow
		a.Backup.PitrWindow = &api.PITRWindow{Available: true, Start: &w.Start, End: &w.End}
	}
	return a
}

// projectedClusterStatus applies the restore receipt fence even to a fresh
// persisted Ready lease, before reconciliation has run after a restart.
func (s *Server) projectedClusterStatus(c domain.Cluster) domain.ClusterStatus {
	status := c.Status
	if c.Spec.DataSource == nil {
		return status
	}
	status.Conditions = append([]domain.Condition(nil), status.Conditions...)
	var provider engine.Provider
	if s.Registry != nil {
		provider, _ = s.Registry.Get(c.Spec.Provider)
	}
	// Missing providers also fail closed; the shared gate needs no native IO.
	_ = engine.ApplyInitialDataSourceStatus(provider, &c, &status)
	return status
}
func (s *Server) clusterAPI(c domain.Cluster) api.Cluster {
	c.Status = s.projectedClusterStatus(c)
	a := api.Cluster{Namespace: ptr(domain.NormalizedNamespace(c.Namespace)), Name: c.Name, Spec: specAPI(c.Spec), Status: s.statusAPI(domain.ClusterRef{Namespace: c.Namespace, Name: c.Name}, c.Status), Generation: c.Generation, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
	if c.OperationID != "" {
		a.OperationId = &c.OperationID
	}
	return a
}
func operationAPI(o domain.Operation) api.Operation {
	o.Message = redact.Diagnostic(o.Message)
	o.Step = redact.Diagnostic(o.Step)
	if o.Target.Kind == "" {
		o.Target = domain.OperationTarget{Kind: "cluster", Name: o.Cluster}
	}
	a := api.Operation{Namespace: ptr(domain.NormalizedNamespace(o.Namespace)), Id: o.ID, Cluster: o.Cluster, Target: api.OperationTarget{Kind: o.Target.Kind, Name: o.Target.Name}, Type: api.OperationType(o.Kind), Phase: api.OperationPhase(o.Phase), Message: &o.Message, Actor: &o.Actor, CreatedAt: o.CreatedAt, StartedAt: o.StartedAt, CompletedAt: o.CompletedAt}
	if o.Step != "" {
		a.Step = &o.Step
	}
	if o.TargetGeneration != 0 {
		a.TargetGeneration = &o.TargetGeneration
	}
	if o.CancelRequested {
		a.CancelRequested = &o.CancelRequested
	}
	if o.Terminal() {
		a.Progress = ptr(float32(100))
	}
	return a
}
func backupAPI(b domain.Backup) api.Backup {
	b.Error = redact.Diagnostic(b.Error)
	a := api.Backup{Namespace: ptr(domain.NormalizedNamespace(b.Namespace)), Name: b.Name, Cluster: b.Cluster, Storage: b.Storage, Type: api.BackupType(b.Type), Trigger: api.BackupTrigger(b.Trigger), Schedule: optionalString(b.Schedule), Phase: api.BackupPhase(b.Phase), PbmName: optionalString(b.PBMName), StartedAt: b.StartedAt, CompletedAt: b.CompletedAt, LastWriteTime: b.LastWriteTime, Size: &b.Size, Error: optionalString(b.Error), OperationId: optionalString(b.OperationID)}
	if b.Provider != "" {
		a.Provider = &b.Provider
	}
	if b.Location != "" {
		a.Location = &b.Location
	}
	if metadata := b.Postgres; metadata != nil {
		a.Postgres = &api.PostgresBackup{
			Label: metadata.Label, Repository: metadata.Repository,
			Prior:    optionalString(metadata.Prior),
			WalStart: optionalString(metadata.WALStart), WalStop: optionalString(metadata.WALStop),
			SystemId: metadata.SystemID, Version: metadata.Version, RepoBytes: metadata.RepoBytes,
		}
		if len(metadata.References) > 0 {
			a.Postgres.References = &metadata.References
		}
	}
	return a
}
func restoreAPI(r domain.Restore) api.Restore {
	r.Error = redact.Diagnostic(r.Error)
	return api.Restore{Namespace: ptr(domain.NormalizedNamespace(r.Namespace)), Name: r.Name, Cluster: r.Cluster, Source: api.RestoreSource{Backup: optionalString(r.Source.Backup), PitrTime: r.Source.PITRTime, SourceCluster: optionalString(r.Source.SourceCluster)}, Phase: api.RestorePhase(r.Phase), StartedAt: r.StartedAt, CompletedAt: r.CompletedAt, Error: optionalString(r.Error), OperationId: optionalString(r.OperationID)}
}
func eventAPI(e domain.Event) api.Event {
	e.Message = redact.Diagnostic(e.Message)
	e.Details = redact.EventDetailsJSON(e.Details)
	var namespace *string
	if e.Namespace != "" {
		namespace = &e.Namespace
	}
	var details *api.EventDetails
	if len(e.Details) != 0 {
		if json.Unmarshal(e.Details, &details) != nil {
			details = nil
		}
	}
	return api.Event{Namespace: namespace, Id: e.ID, Cluster: optionalString(e.Cluster), OperationId: optionalString(e.Operation), Type: e.Type, Reason: optionalString(e.Reason), Message: e.Message, Details: details, Actor: optionalString(e.Actor), Resource: optionalString(e.Resource), Target: optionalString(e.Target), CreatedAt: e.CreatedAt}
}
func rolesDomain(a []api.RoleReference) []domain.RoleRef {
	out := make([]domain.RoleRef, 0, len(a))
	for _, r := range a {
		out = append(out, domain.RoleRef{Role: r.Role, DB: r.Db})
	}
	return out
}
func rolesAPI(a []domain.RoleRef) []api.RoleReference {
	out := make([]api.RoleReference, 0, len(a))
	for _, r := range a {
		out = append(out, api.RoleReference{Role: r.Role, Db: r.DB})
	}
	return out
}
func userAPI(u domain.DatabaseUser) api.DatabaseUser {
	return api.DatabaseUser{Username: u.Name, Database: u.Database, Roles: rolesAPI(u.Roles)}
}
func privilegesDomain(a []api.Privilege) []domain.Privilege {
	out := make([]domain.Privilege, 0, len(a))
	for _, p := range a {
		r, _ := json.Marshal(p.Resource)
		out = append(out, domain.Privilege{Resource: r, Actions: p.Actions})
	}
	return out
}
func roleAPI(r domain.DatabaseRole) api.DatabaseRole {
	a := api.DatabaseRole{Name: r.Name, Database: r.Database, Roles: rolesAPI(r.Roles), Privileges: []api.Privilege{}}
	for _, p := range r.Privileges {
		v := api.Privilege{Actions: nonNil(p.Actions)}
		_ = json.Unmarshal(p.Resource, &v.Resource)
		a.Privileges = append(a.Privileges, v)
	}
	return a
}
func providerAPI(p engine.Provider) api.Provider {
	d := p.Describe()
	a := api.Provider{Name: d.Name, DisplayName: d.DisplayName, Versions: []api.ProviderVersion{}, UpgradePaths: []api.UpgradePath{}, Topologies: []api.ProviderTopology{}, Presets: []api.Preset{}, BackupTypes: []api.ProviderBackupTypes{}}
	if d.Capabilities != nil {
		a.Capabilities = &d.Capabilities
	}
	if d.Operations != nil {
		a.Operations = &d.Operations
	}
	if d.Processes != nil {
		a.Processes = &d.Processes
	}
	if d.EngineOptions != nil {
		a.EngineOptions = &d.EngineOptions
	}
	if d.EngineConfig != nil {
		a.EngineConfig = &d.EngineConfig
	}
	for _, v := range d.Versions {
		a.Versions = append(a.Versions, api.ProviderVersion{Version: v.Version, Major: v.Major, Default: v.Default, Status: api.ProviderVersionStatus(v.Status), PbmVersion: ptr(v.PBMVersion)})
	}
	for _, u := range d.UpgradePaths {
		a.UpgradePaths = append(a.UpgradePaths, api.UpgradePath{From: u.From, To: u.To, RequiresFCV: ptr(u.RequiresFCV), RequiresBackup: ptr(u.RequiresBackup)})
	}
	for _, b := range d.BackupTypes {
		a.BackupTypes = append(a.BackupTypes, api.ProviderBackupTypes(b))
	}
	for _, t := range d.Topologies {
		spec := domain.ClusterSpec{Provider: d.Name, Topology: domain.Topology{Type: t.Type}}
		for n, c := range t.Components {
			switch n {
			case "engine":
				spec.Components.Engine = c.Defaults
			case "configServer":
				spec.Components.ConfigServer = c.Defaults
			case "router":
				spec.Components.Router = c.Defaults
			default:
				if spec.Components.Extra == nil {
					spec.Components.Extra = make(map[string]domain.ComponentSpec)
				}
				spec.Components.Extra[n] = c.Defaults
			}
		}
		p.ApplyDefaults(&spec)
		schemas := map[string]any{}
		limits := map[string]any{}
		for n, c := range t.Components {
			limits[n] = map[string]any{"minReplicas": c.MinReplicas, "maxReplicas": c.MaxReplicas, "allowedReplicas": c.AllowedReplicas}
			if len(c.Schema) > 0 {
				var v any
				if json.Unmarshal(c.Schema, &v) == nil {
					schemas[n] = v
				}
			}
		}
		topology := api.ProviderTopology{Type: api.ProviderTopologyType(t.Type), DisplayName: t.Type, Defaults: specAPI(spec), ComponentSchemas: schemas, Limits: limits}
		if len(t.Schema) > 0 {
			var schema map[string]any
			if json.Unmarshal(t.Schema, &schema) == nil {
				topology.UiSchema = &schema
			}
		}
		a.Topologies = append(a.Topologies, topology)
	}
	for _, v := range d.Presets {
		spec := domain.ClusterSpec{Provider: d.Name, Topology: v.Topology, Components: v.Components}
		p.ApplyDefaults(&spec)
		a.Presets = append(a.Presets, api.Preset{Name: v.Name, DisplayName: v.Name, Description: &v.Description, Spec: specAPI(spec)})
	}
	return a
}
