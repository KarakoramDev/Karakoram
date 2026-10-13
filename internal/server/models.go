// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"sort"
	"strconv"
	"strings"
)

// resourceDecimal keeps the API's shortest float32 decimal representation;
// a direct widening adds binary tails that Zerops rejects as excess precision.
func resourceDecimal(v float32) float64 {
	d, _ := strconv.ParseFloat(strconv.FormatFloat(float64(v), 'f', -1, 32), 64)
	return d
}

func resourcesDomain(r api.Resources) domain.Resources {
	o := domain.Resources{CPUMode: string(r.CpuMode), MinCPU: resourceDecimal(r.MinCpu), MaxCPU: resourceDecimal(r.MaxCpu), MinRAM: resourceDecimal(r.MinRam), MaxRAM: resourceDecimal(r.MaxRam), MinDisk: resourceDecimal(r.MinDisk), MaxDisk: resourceDecimal(r.MaxDisk)}
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
func componentDomain(c api.Component) domain.ComponentSpec {
	return domain.ComponentSpec{Replicas: c.Replicas, Resources: resourcesDomain(c.Resources)}
}
func componentAPI(c domain.ComponentSpec) api.Component {
	return api.Component{Replicas: c.Replicas, Resources: resourcesAPI(c.Resources)}
}
func specDomain(a api.ClusterSpec) domain.ClusterSpec {
	s := domain.ClusterSpec{Provider: a.Provider, Version: a.Version, Topology: domain.Topology{Type: string(a.Topology.Type), Shards: value(a.Topology.Shards)}, Components: domain.Components{Engine: componentDomain(a.Components.Engine)}, EngineConfig: value(a.EngineConfig)}
	if a.Components.ConfigServer != nil {
		s.Components.ConfigServer = componentDomain(*a.Components.ConfigServer)
	}
	if a.Components.Router != nil {
		s.Components.Router = componentDomain(*a.Components.Router)
	}
	if a.Backup != nil {
		s.Backup.Storage = value(a.Backup.Storage)
		if a.Backup.Schedules != nil {
			s.Backup.Schedules = make([]domain.BackupSchedule, 0, len(*a.Backup.Schedules))
			for _, b := range *a.Backup.Schedules {
				s.Backup.Schedules = append(s.Backup.Schedules, domain.BackupSchedule{Name: b.Name, Cron: b.Cron, Type: string(b.Type), Storage: b.Storage, RetainCount: b.RetainCount, Enabled: b.Enabled})
			}
		}
		if a.Backup.Pitr != nil {
			s.Backup.PITR = domain.PITRConfig{Enabled: a.Backup.Pitr.Enabled, OplogSpanMinutes: value(a.Backup.Pitr.OplogSpanMinutes)}
		}
	}
	if a.Tls != nil {
		s.TLS = domain.TLSConfig{Mode: string(a.Tls.Mode), CA: string(a.Tls.Ca)}
	}
	if a.Exposure != nil {
		s.Exposure = domain.Exposure{Public: a.Exposure.Public, IPType: string(value(a.Exposure.IpType)), Allowlist: value(a.Exposure.Allowlist)}
	}
	if a.Monitoring != nil {
		s.Monitoring.Enabled = a.Monitoring.Enabled
	}
	if a.DataSource != nil {
		s.DataSource = &domain.DataSource{Cluster: a.DataSource.Cluster, Backup: value(a.DataSource.Backup), PITRTime: a.DataSource.PitrTime}
	}
	return s
}
func specAPI(s domain.ClusterSpec) api.ClusterSpec {
	a := api.ClusterSpec{Provider: s.Provider, Version: s.Version, Topology: api.Topology{Type: api.TopologyType(s.Topology.Type)}, Components: api.Components{Engine: componentAPI(s.Components.Engine)}, EngineConfig: ptr(s.EngineConfig), Tls: &api.TLS{Mode: api.TLSMode(s.TLS.Mode), Ca: api.TLSCa(s.TLS.CA)}, Exposure: &api.Exposure{Public: s.Exposure.Public, IpType: ptr(api.ExposureIpType(s.Exposure.IPType)), Allowlist: &s.Exposure.Allowlist}, Monitoring: &api.MonitoringSpec{Enabled: s.Monitoring.Enabled}}
	if s.Topology.Type == "sharded" {
		a.Topology.Shards = &s.Topology.Shards
		a.Components.ConfigServer = ptr(componentAPI(s.Components.ConfigServer))
		a.Components.Router = ptr(componentAPI(s.Components.Router))
	}
	schedules := make([]api.BackupSchedule, 0, len(s.Backup.Schedules))
	for _, b := range s.Backup.Schedules {
		schedules = append(schedules, api.BackupSchedule{Name: b.Name, Cron: b.Cron, Type: api.BackupScheduleType(b.Type), Storage: b.Storage, RetainCount: b.RetainCount, Enabled: b.Enabled})
	}
	a.Backup = &api.BackupSpec{Storage: &s.Backup.Storage, Schedules: &schedules, Pitr: &api.PITRSpec{Enabled: s.Backup.PITR.Enabled, OplogSpanMinutes: &s.Backup.PITR.OplogSpanMinutes}}
	if s.DataSource != nil {
		a.DataSource = &api.DataSource{Cluster: s.DataSource.Cluster, Backup: &s.DataSource.Backup, PitrTime: s.DataSource.PITRTime}
	}
	return a
}
func connectionAPI(c domain.Connection) api.ConnectionSummary {
	return api.ConnectionSummary{Hosts: c.Hosts, ReplicaSet: &c.ReplicaSet, Uri: c.URI, PublicHosts: &c.PublicHosts}
}
func (s *Server) memberAPI(cluster string, m domain.MemberStatus) api.Member {
	usage := s.Monitoring.MemberUsage(cluster, m.Name)
	a := api.Member{Name: m.Name, Set: m.Set, Role: memberRole(m), State: m.State, Health: m.Health, Version: &m.Version, LagSeconds: ptr(float32(m.LagSeconds)), Optime: &m.Optime, Uptime: &m.Uptime, RamUsageBytes: usage.RAMBytes, DiskUsageBytes: usage.DiskBytes, DiskTotalBytes: usage.DiskTotalBytes, PlacementGroup: &m.PlacementGroup, Agent: &api.AgentStatus{Connected: m.Agent.Healthy, ConfigVersion: &m.Agent.ConfigVersion, LastSeenAt: &m.Agent.LastSeen}}
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
	if m.Role == "mongos" || m.Role == "router" {
		return api.MemberRole("router")
	}
	state := strings.ToLower(m.State)
	if state == "" {
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
	a := api.Placement{Warning: p.Warning, GuaranteedAntiAffinity: false, Groups: []api.PlacementGroup{}}
	names := make([]string, 0, len(p.Groups))
	for n := range p.Groups {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		a.Groups = append(a.Groups, api.PlacementGroup{Name: n, Members: p.Groups[n]})
	}
	return a
}
func (s *Server) statusAPI(cluster string, status domain.ClusterStatus) api.ClusterStatus {
	a := api.ClusterStatus{Phase: api.ClusterStatusPhase(status.Phase), Message: &status.Message, ObservedGeneration: status.ObservedGeneration, Version: &status.Version, Fcv: &status.FCV, Connection: ptr(connectionAPI(status.Connection)), Placement: ptr(placementAPI(status.Placement)), Conditions: []api.Condition{}, Members: []api.Member{}}
	for _, c := range status.Conditions {
		a.Conditions = append(a.Conditions, api.Condition{Type: c.Type, Status: api.ConditionStatus(c.Status), Reason: c.Reason, Message: c.Message, LastTransitionTime: c.LastTransitionTime})
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
func (s *Server) clusterAPI(c domain.Cluster) api.Cluster {
	return api.Cluster{Name: c.Name, Spec: specAPI(c.Spec), Status: s.statusAPI(c.Name, c.Status), Generation: c.Generation, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
}
func operationAPI(o domain.Operation) api.Operation {
	return api.Operation{Id: o.ID, Cluster: o.Cluster, Type: api.OperationType(o.Kind), Phase: api.OperationPhase(o.Phase), Message: &o.Message, Actor: &o.Actor, CreatedAt: o.CreatedAt, StartedAt: o.StartedAt, CompletedAt: o.CompletedAt}
}
func backupAPI(b domain.Backup) api.Backup {
	return api.Backup{Name: b.Name, Cluster: b.Cluster, Storage: b.Storage, Type: api.BackupType(b.Type), Trigger: api.BackupTrigger(b.Trigger), Schedule: &b.Schedule, Phase: api.BackupPhase(b.Phase), PbmName: &b.PBMName, StartedAt: b.StartedAt, CompletedAt: b.CompletedAt, LastWriteTime: b.LastWriteTime, Size: &b.Size, Error: &b.Error}
}
func restoreAPI(r domain.Restore) api.Restore {
	return api.Restore{Name: r.Name, Cluster: r.Cluster, Source: api.RestoreSource{Backup: &r.Source.Backup, PitrTime: r.Source.PITRTime, SourceCluster: r.Source.SourceCluster}, Phase: api.RestorePhase(r.Phase), StartedAt: r.StartedAt, CompletedAt: r.CompletedAt, Error: &r.Error}
}
func eventAPI(e domain.Event) api.Event {
	return api.Event{Id: e.ID, Cluster: &e.Cluster, OperationId: &e.Operation, Type: e.Type, Reason: &e.Reason, Message: e.Message, Actor: &e.Actor, Resource: &e.Resource, Target: &e.Target, CreatedAt: e.CreatedAt}
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
		v := api.Privilege{Actions: p.Actions}
		_ = json.Unmarshal(p.Resource, &v.Resource)
		a.Privileges = append(a.Privileges, v)
	}
	return a
}
func providerAPI(p engine.Provider) api.Provider {
	d := p.Describe()
	a := api.Provider{Name: d.Name, DisplayName: d.DisplayName, Versions: []api.ProviderVersion{}, UpgradePaths: []api.UpgradePath{}, Topologies: []api.ProviderTopology{}, Presets: []api.Preset{}, BackupTypes: []api.ProviderBackupTypes{}}
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
