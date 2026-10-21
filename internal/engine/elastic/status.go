// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func phase(s *state) domain.Phase {
	if s.Suspended {
		return domain.PhaseSuspended
	}
	if s.RestoreActive {
		return domain.PhaseRestoring
	}
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "provisioning":
		return domain.PhaseCreating
	case "forming", "components":
		return domain.PhaseInitializing
	case "scaling":
		return domain.PhaseScaling
	case "rolling":
		if s.Rolling != nil && s.Rolling.Reason == "upgrade" {
			return domain.PhaseUpgrading
		}
		return domain.PhaseUpdating
	case "cleanup":
		return domain.PhaseDeleting
	default:
		return domain.PhaseUpdating
	}
}
func connection(c *domain.Cluster, s *state) domain.Connection {
	v := domain.Connection{Hosts: []string{}, PublicHosts: []string{}, Endpoints: []domain.Endpoint{}}
	for _, m := range s.members(kindNode) {
		if !m.Removing {
			host := m.Name + ":9200"
			v.Hosts = append(v.Hosts, host)
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "elasticsearch", URL: "https://" + host})
		}
	}
	if len(v.Hosts) > 0 {
		v.URI = "https://" + strings.Join(v.Hosts, ",")
	}
	for _, m := range s.componentMembers() {
		switch m.Kind {
		case kindKibana:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "kibana", URL: "http://" + m.Name + ":5601"})
		case kindAPM:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "apm", URL: "http://" + m.Name + ":8200"})
		case kindLogstash:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "logstash-beats", URL: "tcp://" + m.Name + ":5044"}, domain.Endpoint{Name: "logstash-http", URL: "http://" + m.Name + ":8080"})
		}
	}
	return v
}
func (p *Provider) observedConnection(ctx context.Context, rc *engine.Context, s *state) (domain.Connection, error) {
	v := connection(rc.Cluster, s)
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return v, err
	}
	return p.connectionWithProject(ctx, rc, s, project)
}

func (p *Provider) connectionWithProject(ctx context.Context, rc *engine.Context, s *state, project infra.Project) (domain.Connection, error) {
	v := connection(rc.Cluster, s)
	for _, m := range s.members(kindNode) {
		if m.PublicPort > 0 && !m.Removing {
			ip := project.PublicIPv6
			if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
				ip = project.PublicIPv4
			}
			if ip != "" {
				host := net.JoinHostPort(ip, strconv.Itoa(m.PublicPort))
				v.PublicHosts = append(v.PublicHosts, host)
				v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "elasticsearch", URL: "https://" + host, Public: true})
			}
		}
	}
	if sub, err := engine.PublicHTTPS(rc.Backend); err == nil {
		for _, m := range s.componentMembers() {
			if !m.Subdomain {
				continue
			}
			urls, err := sub.SubdomainURLs(ctx, m.Name)
			if err != nil {
				return v, err
			}
			for _, url := range urls {
				v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: m.Kind, URL: url, Public: true})
			}
		}
	}
	return v, nil
}

// Status combines Elasticsearch health, member agents and infrastructure placement.
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() {
		engine.ObservePlacement(&status, rc.Backend.Capabilities().Placement)
		engine.ApplyDeploymentStatus(rc, &status)
	}()
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	defer func() {
		statusErr = errors.Join(statusErr, engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status))
	}()
	v := domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	if v.Phase == domain.PhaseUpdating {
		v.Message = rc.SyncWait
	}
	condition := func(kind string, ok bool, reason, message string) {
		value := "False"
		if ok {
			value = "True"
		}
		v.Conditions = append(v.Conditions, domain.Condition{Type: kind, Status: value, Reason: reason, Message: message, LastTransitionTime: now(rc)})
	}
	condition("Bootstrapped", s.ClusterUUID != "", "ClusterFormation", "Elasticsearch cluster formation")
	if step, message := rollingProgress(s); step != "" && !s.Suspended {
		condition("RollingProgress", true, step, message)
		v.Message = message
	}
	certificatesValid := true
	checkCertificate := func(key string) {
		cert, e := secret(ctx, rc, key)
		if e != nil {
			certificatesValid = false
			return
		}
		expired, e := NeedsRenewal(cert, 0, now(rc))
		certificatesValid = certificatesValid && e == nil && !expired
	}
	checkCertificate(secretCA)
	for _, m := range s.Members {
		if m.Removing {
			continue
		}
		checkCertificate("tls/agents/" + m.Name + ".crt")
		if isES(m) {
			checkCertificate("tls/nodes/" + m.Name + "/transport.crt")
			checkCertificate("tls/nodes/" + m.Name + "/http.crt")
		}
	}
	condition("Certificates", certificatesValid, "ClusterTLS", "Cluster and member TLS certificate validity")
	if rc.Cluster.Spec.Topology.Type == "cluster" && rc.Cluster.Spec.Components.Engine.Replicas%2 == 0 {
		condition("QuorumWarning", true, "EvenVotingCount", "Even combined-node counts do not improve voting fault tolerance over the preceding odd count")
	}
	if s.DrainBlocked != "" && !s.Suspended {
		condition("ShardDrainBlocked", true, "ShardAllocationBlocked", s.DrainBlocked)
		v.Message = s.DrainBlocked
	}
	if s.Retry != nil {
		condition("PlatformRetry", true, "Backoff", "A failed platform action is scheduled for retry")
		if v.Message == "" {
			v.Message = "Waiting for a scheduled platform retry"
		} else {
			v.Message += "; waiting for a scheduled platform retry"
		}
	}
	type instanceObservation struct {
		instance string
		status   agentapi.Status
	}
	type memberObservation struct {
		status           agentapi.Status
		err              error
		at               time.Time
		componentReady   bool
		componentMessage string
		componentErr     error
		componentAt      time.Time
		instances        []instanceObservation
	}
	samples := make([]memberObservation, len(s.Members))
	memberNames := make([]string, len(s.Members))
	for i, m := range s.Members {
		memberNames[i] = m.Name
	}
	var inventory engine.PlatformObservation
	var native nativeObservation
	var memberNative map[string]nativeObservation
	inventoryReady := make(chan struct{})
	var probes sync.WaitGroup
	probes.Go(func() {
		inventory = engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, memberNames)
		close(inventoryReady)
	})
	if !s.Suspended {
		probes.Go(func() {
			native, memberNative = p.observeNative(ctx, rc, s)
		})
		for i, m := range s.Members {
			probes.Go(func() {
				probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
				defer cancel()
				a, err := p.agentClient(probe, rc, m.Name)
				if err != nil {
					samples[i].err = err
				} else {
					samples[i].status, samples[i].err = a.Status(probe)
					a.Close()
					samples[i].at = now(rc)
				}
			})
			if !isES(m) {
				probes.Go(func() {
					<-inventoryReady
					probe, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					var mu sync.Mutex
					observe := func(instance string, status agentapi.Status) {
						mu.Lock()
						samples[i].instances = append(samples[i].instances, instanceObservation{instance: instance, status: status})
						mu.Unlock()
					}
					samples[i].componentReady, samples[i].componentMessage, samples[i].componentErr = componentInstancesObservation(probe, rc, m, []engine.InstanceInventory{inventory.Members[i]}, observe)
					samples[i].componentAt = now(rc)
				})
			}
		}
	}
	probes.Wait()
	if inventory.ProjectErr != nil {
		return v, inventory.ProjectErr
	}
	if v.Connection, err = p.connectionWithProject(ctx, rc, s, inventory.Project); err != nil {
		return v, err
	}
	services := make(map[string]infra.Service, len(inventory.Services))
	for _, service := range inventory.Services {
		services[service.Hostname] = service
	}
	nodes := map[string]esapi.Node{}
	master := ""
	healthy := false
	if s.Suspended {
		condition("ClusterHealth", false, "Suspended", "Elasticsearch is suspended; runtime health is not observed")
		v.Message = "Runtime suspended; members and agents are not running"
	} else if native.err == nil {
		h, e := native.health, native.healthErr
		if e == nil {
			healthy, e = native.acceptable, native.checkErr
			message := "Elasticsearch shard health: " + h.Status
			switch h.Status {
			case "yellow":
				message += " — all primary shards are allocated, but replica shards are unassigned; data redundancy is reduced. Add capacity or review index replicas."
			case "red":
				message += " — primary shards are unavailable; some data cannot be served. Restore primary shard availability before maintenance."
			}
			condition("ClusterHealth", healthy && e == nil, h.Status, message)
			if h.Status != "green" && v.Message == "" {
				v.Message = message
			}
		} else {
			condition("ClusterHealth", false, "Unreachable", "Elasticsearch is unreachable")
		}
		blocks, blockErr := native.blocks, native.blockErr
		switch {
		case blockErr != nil:
			healthy = false
			v.Conditions = append(v.Conditions, domain.Condition{Type: "IndexWriteBlocked", Status: "Unknown", Reason: "ObservationFailed", Message: "Unable to inspect index write blocks; verify Elasticsearch access and index settings", LastTransitionTime: now(rc)})
			if v.Message == "" {
				v.Message = "Index write-block observation unavailable"
			}
		case len(blocks) > 0:
			healthy = false
			message := "Writes blocked on indices: " + strings.Join(blocks, ", ") + ". For read_only_allow_delete, free disk space or add capacity below the high watermark; Elasticsearch automatically releases flood-stage blocks. Review operator-set read_only/write blocks before removing them."
			condition("IndexWriteBlocked", true, "NativeIndexBlock", message)
			v.Message = message
		default:
			condition("IndexWriteBlocked", false, "NoWriteBlocks", "No native index write blocks observed")
		}
		for _, node := range native.nodes {
			nodes[node.Name] = node
		}
		master = native.master
	} else {
		condition("ClusterHealth", false, "Unreachable", "Elasticsearch is not yet reachable")
	}
	nativeMessage := ""
	if !s.Suspended {
		var unavailable []string
		for _, probe := range []struct {
			name string
			err  error
		}{
			{"native access", native.err},
			{"cluster health", native.healthErr},
			{"shard availability", native.checkErr},
			{"node membership (/_nodes)", native.nodesErr},
			{"elected master (/_cat/master)", native.masterErr},
		} {
			if probe.err != nil {
				unavailable = append(unavailable, probe.name)
			}
		}
		if len(unavailable) > 0 {
			healthy = false
			nativeMessage = "Elasticsearch observation unavailable: " + strings.Join(unavailable, ", ") + "; inspect native API access, member process health and connectivity; restore observations before maintenance"
			for i := range v.Conditions {
				if v.Conditions[i].Type == "ClusterHealth" {
					if v.Conditions[i].Status == "True" || native.err != nil || native.healthErr != nil || native.checkErr != nil {
						v.Conditions[i].Status = "Unknown"
					}
					v.Conditions[i].Reason = "ObservationUnavailable"
					v.Conditions[i].Message = nativeMessage
				}
			}
			if v.Message != "" {
				v.Message += "; "
			}
			v.Message += nativeMessage
		}
	}
	componentsReady := !s.Suspended
	componentMessages := []string{}
	pressureMessages := []string{}
	evidenceUnavailableMessages := []string{}
	heapMessages := []string{}
	allReady := healthy
	for i, m := range s.Members {
		ms := domain.MemberStatus{Name: m.Name, Role: m.Kind, Version: m.Version, State: "absent"}
		if isES(m) {
			if m.Kind == kindMaster {
				ms.Role = "master"
			} else if masterEligible(rc.Cluster.Spec, m) {
				ms.Role = "master,data"
			} else {
				ms.Role = "data"
			}
			if node, ok := nodes[m.Name]; ok {
				ms.State = "joined"
				ms.Version = node.Version
				ms.Health = true
				if m.Name == master {
					ms.State = "elected-master"
				}
			}
		}
		var allocated *agentapi.ContainerResources
		if s.Suspended {
			ms.State = "suspended"
			ms.Health = false
			ms.Agent.Message = "Runtime suspended; member agent is not running"
		} else {
			status, e := samples[i].status, samples[i].err
			if e == nil {
				allocated = status.Resources
				ms.Agent = domain.AgentStatus{Version: status.Agent.Version, Commit: status.Agent.Commit, Build: status.Agent.Build, Healthy: true, ConfigVersion: status.ConfigVersion, LastSeen: samples[i].at}
				ms.Uptime = status.UptimeSeconds
				ms.DiskUsage = float64(status.Disk.UsedBytes)
				if !rc.Backend.Capabilities().Placement {
					ms.PlacementGroup = status.HostFingerprint
				}
				if !isES(m) {
					ms.Version = status.EngineVersion
				}
			} else {
				ms.Agent.Message = "Member status unavailable"
			}
			if isES(m) {
				ms.Health = ms.Health && strings.EqualFold(samples[i].status.Processes[product(m)].State, "running")
			}
			if len(samples[i].instances) > 0 {
				ms.Agent.Healthy = true
				ms.Agent.LastSeen = samples[i].componentAt
			}
		}
		memberMessage := nativeMessage
		if local, ok := memberNative[m.Name]; ok && local.failed() {
			if local.err != nil || local.nodesErr != nil {
				ms.State = "unknown"
			}
			memberMessage = "Elasticsearch native observation unavailable for " + m.Name + "; inspect its JVM for a paused/stopped process and verify port 9200; restore native responses before maintenance"
			v.Conditions = append(v.Conditions, domain.Condition{Type: "NativeObservation/" + m.Name, Status: "Unknown", Reason: "ObservationUnavailable", Message: memberMessage, LastTransitionTime: now(rc)})
		}
		if isES(m) && memberMessage != "" {
			ms.Health = false
			if ms.Agent.Message != "" {
				ms.Agent.Message += "; "
			}
			ms.Agent.Message += memberMessage
		}
		if !isES(m) && !s.Suspended {
			ms.Health, ms.Agent.Message = samples[i].componentReady, samples[i].componentMessage
			var heap *componentHeapExhausted
			if errors.As(samples[i].componentErr, &heap) {
				heapMessages = append(heapMessages, m.Name+": "+samples[i].componentMessage)
			}
			var pressure *componentMemoryPressure
			if errors.As(samples[i].componentErr, &pressure) {
				pressureMessages = append(pressureMessages, m.Name+": "+samples[i].componentMessage)
			}
			var unavailable *componentMemoryEvidenceUnavailable
			if errors.As(samples[i].componentErr, &unavailable) {
				evidenceUnavailableMessages = append(evidenceUnavailableMessages, m.Name+": "+samples[i].componentMessage)
			}
			if ms.Health {
				ms.Agent.LastSeen = samples[i].componentAt
				ms.State = "joined"
			}
		}
		if service, ok := services[m.Name]; ok {
			resources := service.Resources
			ms.Autoscaling = &resources
		}
		containers, e := inventory.Members[i].Containers, inventory.Members[i].Err
		if e != nil || len(containers) == 0 {
			ms.Health = false
		} else {
			for _, container := range containers {
				if isES(m) && !strings.EqualFold(container.Status, infra.ContainerActive) && !strings.EqualFold(container.Status, infra.ContainerRunning) {
					ms.Health = false
				}
				if rc.Backend.Capabilities().Placement && container.PlacementGroup != "" {
					ms.PlacementGroup = container.PlacementGroup
				}
			}
			ms.Resources = engine.MemberResources(allocated, ms.Autoscaling)
		}
		ms.Health = ms.Health && ms.Agent.Healthy
		if m.Removing && !s.Suspended {
			ms.State = "draining"
			if m.Stage == stageDeleting || m.Stage == "delete-volume" || m.Stage == "removed" {
				ms.State = "removing"
			}
			ms.Health = false
		}
		engine.ApplyAgentDiagnostic(&ms, samples[i].status.Error)
		if !s.Suspended {
			v.Conditions = append(v.Conditions, engine.ApplyProcessDiagnostics(&ms, samples[i].status.Processes, "", samples[i].at)...)
			for _, instance := range samples[i].instances {
				v.Conditions = append(v.Conditions, engine.ApplyProcessDiagnostics(&ms, instance.status.Processes, instance.instance, samples[i].componentAt)...)
			}
		}
		if !isES(m) {
			componentsReady = componentsReady && ms.Health
			if !ms.Health && ms.Agent.Message != "" {
				componentMessages = append(componentMessages, m.Name+": "+ms.Agent.Message)
			}
		}
		if m.PublicPort > 0 {
			for _, host := range v.Connection.PublicHosts {
				_, port, _ := net.SplitHostPort(host)
				if port == strconv.Itoa(m.PublicPort) {
					ms.PublicHost = host
					break
				}
			}
		}
		allReady = allReady && ms.Health
		v.Members = append(v.Members, ms)
	}
	componentMessage := strings.Join(componentMessages, "; ")
	condition("ComponentsReady", componentsReady, "ComponentReadiness", componentMessage)
	if len(pressureMessages) > 0 {
		condition("MemoryPressure", true, "CgroupMemoryPressure", strings.Join(pressureMessages, "; "))
	} else if len(evidenceUnavailableMessages) > 0 {
		v.Conditions = append(v.Conditions, domain.Condition{Type: "MemoryPressure", Status: "Unknown", Reason: "CgroupMemoryEvidenceUnavailable", Message: strings.Join(evidenceUnavailableMessages, "; "), LastTransitionTime: now(rc)})
	}
	if len(heapMessages) > 0 {
		condition("KibanaHeapExhausted", true, "HeapExhausted", strings.Join(heapMessages, "; "))
	}
	if message := kibanaMinimumRAMMessage(rc.Cluster.Spec); message != "" {
		condition("KibanaResources", false, "BelowMinimumRAM", message)
	}
	if !componentsReady && v.Message == "" {
		v.Message = componentMessage
	}
	if v.Phase == domain.PhaseReady && !allReady {
		v.Phase = domain.PhaseDegraded
	}
	if rc.Store != nil {
		backups, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if e != nil {
			return v, e
		}
		for _, backup := range backups {
			if backup.Phase == domain.PhaseSucceeded && backup.CompletedAt != nil && (v.Backup.LastBackup == nil || backup.CompletedAt.After(*v.Backup.LastBackup)) {
				at := *backup.CompletedAt
				v.Backup.LastBackup = &at
			}
		}
	}
	for _, m := range s.Members {
		engine.ApplyRuntimeRecoveryStatus(&v, m.Name, m.Volume, m.RuntimeRecovery, now(rc))
	}
	return v, nil
}

// Connection returns credentials only through the protected data-plane endpoint.
func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	v := domain.ConnectionDetails{}
	if v.Connection, err = p.observedConnection(ctx, rc, s); err != nil {
		return v, err
	}
	ca, err := secret(ctx, rc, secretCA)
	if err != nil {
		return v, err
	}
	v.CACertificate = string(ca)
	for _, entry := range []struct{ name, key string }{{"elastic", secretElastic}, {"apm-secret-token", secretAPMToken}} {
		pw, err := secret(ctx, rc, entry.key)
		if err != nil {
			return v, err
		}
		v.Credentials = append(v.Credentials, domain.ConnectionCredential{Username: entry.name, Password: string(pw), URI: v.Connection.URI})
	}
	return v, nil
}

// SystemUsers lists the managed identities that the rotate-system-user
// operation accepts, without credential material. The control-plane admin
// user is internal to the file realm and is not rotated through this list.
func (*Provider) SystemUsers(_ context.Context, rc *engine.Context) ([]domain.SystemUser, error) {
	out := []domain.SystemUser{}
	for _, name := range []string{"elastic", MonitorUser, "kibana-token", "logstash", "apm", "apm-secret-token"} {
		out = append(out, domain.SystemUser{Name: name})
	}
	if rc != nil {
		read, write := engine.DataAccessLevels(rc.Cluster)
		if read {
			out = append(out, domain.SystemUser{Name: engine.DataAccessReadUser})
		}
		if write {
			out = append(out, domain.SystemUser{Name: engine.DataAccessReadWriteUser})
		}
	}
	return out, nil
}

// Placement uses the same authoritative groups or agent evidence as status.
func (p *Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	status, err := p.Status(ctx, rc)
	return status.Placement, err
}

// MemberLogs reads a bounded, allowlisted member process log.
func (p *Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	m := s.find(name)
	if m == nil {
		return nil, engine.ErrNotFound
	}
	if process != "agent" && process != product(m) {
		return nil, invalid("invalid member process")
	}
	a, err := p.agentClient(ctx, rc, name)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	logs, err := a.Logs(ctx, process, lines)
	if err != nil {
		return nil, err
	}
	out := []domain.MemberLogLine{}
	for _, line := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: line.Timestamp, Member: name, Process: process, Stream: line.Stream, Message: line.Message})
	}
	return out, nil
}

// ListDatabaseUsers rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}

// GetDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}

// CreateDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) CreateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) (string, error) {
	return "", engine.ErrNotSupported
}

// UpdateDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return engine.ErrNotSupported
}

// DeleteDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}

// ListDatabaseRoles rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}

// GetDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}

// CreateDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}

// UpdateDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}

// DeleteDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}

// Balancer is unsupported; Elasticsearch manages shard allocation itself.
func (*Provider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}

// SetBalancer is unsupported; Elasticsearch manages shard allocation itself.
func (*Provider) SetBalancer(context.Context, *engine.Context, bool) error {
	return engine.ErrNotSupported
}

// PITRWindow is unsupported; Elasticsearch recovery uses snapshots.
func (*Provider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}

var _ engine.DataPlane = (*Provider)(nil)

type nativeObservation struct {
	err                 error
	health              esapi.Health
	healthErr, checkErr error
	acceptable          bool
	blocks              []string
	blockErr            error
	nodes               []esapi.Node
	master              string
	nodesErr, masterErr error
}

func (o nativeObservation) failed() bool {
	return o.err != nil || o.healthErr != nil || o.checkErr != nil || o.blockErr != nil || o.nodesErr != nil || o.masterErr != nil
}

// Probe endpoints independently, never replaying a mutation or waiting for a
// paused peer's /_nodes fan-out. Leave time inside the sweep for projection.
func (p *Provider) observeNative(ctx context.Context, rc *engine.Context, s *state) (nativeObservation, map[string]nativeObservation) {
	cfg, err := p.esConfig(ctx, rc, s)
	if err != nil {
		return nativeObservation{err: err}, nil
	}
	members := slices.DeleteFunc(s.esNodes(), func(m *member) bool {
		return m.Stage != stageDeployed && m.Stage != stageJoined && !m.Removing
	})
	samples := make([]nativeObservation, len(members))
	var probes sync.WaitGroup
	for i, m := range members {
		probes.Go(func() {
			probe, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			memberConfig := cfg
			memberConfig.Endpoints = []string{"https://" + m.Name + ":9200"}
			memberConfig.Timeout = 2 * time.Second
			c, err := esapi.New(memberConfig)
			if err != nil {
				samples[i].err = err
				return
			}
			defer c.Close()
			samples[i] = observeNativeEndpoint(probe, c, rc.Cluster.Spec.Components.Engine.Replicas)
		})
	}
	probes.Wait()
	out := nativeObservation{err: errWaiting, healthErr: errWaiting, checkErr: errWaiting, blockErr: errWaiting, nodesErr: errWaiting, masterErr: errWaiting}
	byMember := make(map[string]nativeObservation, len(members))
	for i, m := range members {
		sample := samples[i]
		byMember[m.Name] = sample
		if sample.err != nil {
			continue
		}
		out.err = nil
		if sample.healthErr == nil {
			if out.healthErr != nil {
				out.health, out.healthErr = sample.health, nil
				out.acceptable, out.checkErr = sample.acceptable, sample.checkErr
			} else {
				// Never let a later green sample overwrite observed red/yellow.
				rank := func(health string) int {
					switch health {
					case "green":
						return 2
					case "yellow":
						return 1
					default:
						return 0
					}
				}
				if rank(sample.health.Status) < rank(out.health.Status) {
					out.health = sample.health
				}
				out.acceptable = out.acceptable && sample.acceptable
				out.checkErr = errors.Join(out.checkErr, sample.checkErr)
			}
		}
		if sample.blockErr == nil {
			out.blocks = append(out.blocks, sample.blocks...)
			out.blockErr = nil
		}
		if sample.masterErr == nil {
			out.master, out.masterErr = sample.master, nil
		}
		if sample.nodesErr == nil {
			out.nodesErr = nil
			for _, node := range sample.nodes {
				if node.Name == m.Name {
					out.nodes = append(out.nodes, node)
				}
			}
		}
	}
	slices.Sort(out.blocks)
	out.blocks = slices.Compact(out.blocks)
	return out, byMember
}

func observeNativeEndpoint(ctx context.Context, c *esapi.Client, replicas int) nativeObservation {
	var out nativeObservation
	var probes sync.WaitGroup
	probes.Go(func() {
		out.health, out.healthErr = c.Health(ctx, esapi.HealthOptions{})
		if out.healthErr == nil {
			out.acceptable, out.checkErr = healthAcceptable(ctx, c, out.health, replicas)
		}
	})
	probes.Go(func() { out.blocks, out.blockErr = c.IndexWriteBlocks(ctx) })
	probes.Go(func() { out.nodes, out.nodesErr = c.LocalNodes(ctx) })
	probes.Go(func() { out.master, out.masterErr = c.MasterNode(ctx) })
	probes.Wait()
	return out
}
