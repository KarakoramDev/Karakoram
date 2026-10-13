// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"reflect"
	"slices"
	"strings"
	"time"
)

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (p *Provider) agent(ctx context.Context, rc *engine.Context, name string) (*agentapi.Client, error) {
	return rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
}
func (p *Provider) call(ctx context.Context, rc *engine.Context, name, action string, req, resp any) error {
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Engine(ctx, "rabbitmq", action, req, resp)
}
func (p *Provider) action(ctx context.Context, rc *engine.Context, name, action string, req any) error {
	if req == nil {
		req = rmqspec.EmptyRequest{}
	}
	var out rmqspec.ActionResponse
	if err := p.call(ctx, rc, name, action, req, &out); err != nil {
		return err
	}
	if !out.OK {
		return fmt.Errorf("RabbitMQ %s %s was not acknowledged", name, action)
	}
	return nil
}
func (p *Provider) brokerStatus(ctx context.Context, rc *engine.Context, name string) (rmqspec.StatusResponse, error) {
	var st rmqspec.StatusResponse
	err := p.call(ctx, rc, name, "status", rmqspec.EmptyRequest{}, &st)
	return st, err
}
func (p *Provider) agentStatus(ctx context.Context, rc *engine.Context, name string) (agentapi.Status, error) {
	a, err := p.agent(ctx, rc, name)
	if err != nil {
		return agentapi.Status{}, err
	}
	defer a.Close()
	return a.Status(ctx)
}
func running(st agentapi.Status) bool {
	return strings.EqualFold(st.Processes["rabbitmq-server"].State, "running")
}
func noProblems(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "[]" || s == "{}"
}
func (p *Provider) healthy(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	st, err := p.agentStatus(ctx, rc, m.Name)
	if err != nil {
		return false, err
	}
	if !running(st) || st.ConfigVersion < m.ConfigVersion {
		return false, nil
	}
	b, err := p.brokerStatus(ctx, rc, m.Name)
	if err != nil {
		return false, err
	}
	return slices.Contains(b.RunningNodes, "rabbit@"+m.Name) && noProblems(b.Alarms) && noProblems(b.Partitions) && !b.Maintenance, nil
}
func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	specs := []infra.ServiceSpec{}
	for _, volume := range []bool{true, false} {
		name := m.Name
		if volume {
			name = m.Volume
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		if service != nil {
			continue
		}
		if volume {
			r := rc.Cluster.Spec.Components.Engine.Resources
			specs = append(specs, infra.ServiceSpec{Hostname: m.Volume, Type: "local-storage:single@1", Resources: domain.Resources{MinDisk: r.MinDisk, MaxDisk: r.MaxDisk}, Env: ownershipEnv(rc.Cluster.Name, m.Name, tok.Token)})
		} else {
			specs = append(specs, serviceSpec(rc.Cluster, m, tok.Token))
		}
	}
	m.Resources = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	m.Disk = rc.Cluster.Spec.Components.Engine.Resources
	if len(specs) == 0 {
		m.Stage = "imported"
		s.resetPlatformRetry(&pending{Action: "import", Member: m.Name})
		return nil
	}
	op, err := rc.Backend.ImportServices(ctx, specs)
	s.Pending = &pending{Op: op, Action: "import", Member: m.Name}
	if err != nil {
		return s.failPending(rc, err.Error())
	}
	return nil
}
func (p *Provider) deploy(ctx context.Context, rc *engine.Context, s *internalState, m *member, action, version string) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	m.DesiredVersion = version
	if err := s.save(rc.Cluster); err != nil {
		return err
	}
	doc, err := p.zeropsYAML(rc, m, version)
	if err != nil {
		return err
	}
	artifact, err := rc.Artifact.Open(ctx)
	if err != nil {
		return err
	}
	defer artifact.Close()
	op, err := rc.Backend.Deploy(ctx, m.Name, infra.Deployment{Artifact: artifact, ZeropsYAML: doc, SetupName: m.Name, Filename: "karakoram.tar.gz"})
	s.Pending = &pending{Op: op, Action: action, Member: m.Name, Version: version}
	if err != nil {
		return s.failPending(rc, err.Error())
	}
	return nil
}

// Sync changes at most one broker or platform action and persists observations even on retryable failures.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 5 * time.Second
	s, e := loadState(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.Suspended {
		s.Suspended = false
		s.CleanupBackups = false
		s.CleanupSecrets = false
		s.Bootstrapped = false
		for _, m := range s.Members {
			if m.VolumeOrphaned {
				m.Removing = true
				m.RemoveStep = "delete-volume"
				continue
			}
			m.Stage = "planned"
			m.Resume = true
			m.Removing = false
			m.RemoveStep = ""
			m.ChangeAction = ""
			m.ChangeStep = ""
			m.PublicHash = ""
			m.ConfigVersion = 0
		}
		s.Phase = "resume"
		return result, nil
	}
	if s.UID == "" {
		uid, e := secrets.Token()
		if e != nil {
			return result, e
		}
		s.UID = uid
	}
	// A replacement owns its temporary extra member until the grow/shrink workflow finishes.
	if !s.ReplaceActive {
		active := []*member{}
		for _, m := range ordered(s) {
			if !m.Removing {
				active = append(active, m)
			}
		}
		desired := rc.Cluster.Spec.Components.Engine.Replicas
		for len(active) < desired {
			m, e := allocateNewMember(ctx, rc, s)
			if e != nil {
				return result, e
			}
			active = append(active, m)
			s.Rebalance = s.Bootstrapped
		}
		if len(active) > desired {
			for i := len(active) - 1; i >= desired; i-- {
				active[i].Removing = true
			}
		}
	}
	if e = p.ensureSecrets(ctx, rc, s); e != nil {
		return result, e
	}
	// Finish an in-flight rolling member before provisioning or changing any other member.
	for _, m := range ordered(s) {
		if m.ChangeAction != "" && m.ChangeAction != "password" {
			s.Phase = "upgrade"
			_, e = p.changeStep(ctx, rc, s, m)
			return result, e
		}
	}
	// Retained Khepri voters need a majority online before an individual broker
	// can finish startup. Provision and start the entire retained plan first.
	for _, m := range ordered(s) {
		if !m.Resume || m.Removing {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "resume"
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			s.Phase = "resume"
			if e = applyProvidedCertificate(ctx, rc, m); e != nil {
				return result, e
			}
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		case "resume-deployed":
			a, e := p.agent(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			e = a.Maintenance(ctx, agentapi.Maintenance{Enabled: false})
			a.Close()
			if e != nil {
				return result, e
			}
			if e = p.action(ctx, rc, m.Name, "start", nil); e != nil {
				return result, e
			}
			m.Stage = "resume-revive"
			return result, nil
		}
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "import"
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			s.Phase = "deploy"
			if e = applyProvidedCertificate(ctx, rc, m); e != nil {
				return result, e
			}
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		case "resume-revive":
			if e = p.action(ctx, rc, m.Name, "revive", nil); e != nil {
				return result, e
			}
			m.Resume = false
			m.Stage = "deployed"
			return result, nil
		case "deployed":
			s.Phase = "agents"
			healthy, e := p.healthy(ctx, rc, m)
			if e != nil {
				return result, e
			}
			if !healthy {
				return result, nil
			}
			if !s.Bootstrapped {
				s.Phase = "bootstrap"
				if e = p.action(ctx, rc, m.Name, "bootstrap", nil); e != nil {
					return result, e
				}
				s.Bootstrapped = true
				m.Stage = "ready"
				return result, nil
			}
			s.Phase = "join"
			seed := p.availableMembers(ctx, rc, s)
			if len(seed) == 0 {
				return result, fmt.Errorf("no healthy seed member")
			}
			st, e := p.brokerStatus(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if !slices.Contains(st.RunningNodes, "rabbit@"+seed[0].Name) {
				return result, p.action(ctx, rc, m.Name, "join", rmqspec.JoinRequest{Seed: "rabbit@" + seed[0].Name})
			}
			m.Stage = "ready"
			s.Rebalance = true
			return result, nil
		}
	}
	if removing := removingMembers(s); len(removing) > 0 {
		s.Phase = "remove"
		return result, p.removeStep(ctx, rc, s, removing[0])
	}
	for _, m := range ordered(s) {
		r := rc.Cluster.Spec.Components.Engine.Resources
		if !reflect.DeepEqual(m.Resources, runtimeResources(r)) {
			s.Phase = "resources"
			if m.Resources.CPUMode != r.CPUMode || m.Resources.MinCPU != r.MinCPU || m.Resources.MaxCPU != r.MaxCPU || !reflect.DeepEqual(m.Resources.StartCPU, r.StartCPU) {
				beginChange(rc, s, m, "resources")
				return result, nil
			}
			return result, p.submit(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: runtimeResources(r)})
		}
		if m.Disk.MinDisk != r.MinDisk || m.Disk.MaxDisk != r.MaxDisk {
			s.Phase = "resources"
			return result, p.submit(ctx, rc, s, &pending{Action: "disk", Member: m.Name, Resources: r})
		}
		pending, e := hasPendingCertificate(ctx, rc, m)
		if e != nil {
			return result, e
		}
		if !pending {
			pending, e = providedCertificateChanged(ctx, rc, m)
			if e != nil {
				return result, e
			}
		}
		if m.Version != rc.Cluster.Spec.Version || m.TLSMode != rc.Cluster.Spec.TLS.Mode || m.EngineConfig != rc.Cluster.Spec.EngineConfig || hash(m.EngineOptions) != hash(rc.Cluster.Spec.EngineOptions) || !reflect.DeepEqual(m.ConfigResources, m.Resources) || m.ConfigDisk.MinDisk != m.Disk.MinDisk || m.ConfigDisk.MaxDisk != m.Disk.MaxDisk || pending {
			if m.Version != rc.Cluster.Spec.Version {
				s.FeatureFlags = true
			}
			beginChange(rc, s, m, "deploy")
			s.Phase = "upgrade"
			return result, nil
		}
	}
	if s.FeatureFlags {
		ms := s.runningMembers()
		if len(ms) == 0 {
			return result, engine.ErrNotFound
		}
		if e = p.action(ctx, rc, ms[0].Name, "enable-feature-flags", nil); e != nil {
			return result, e
		}
		s.FeatureFlags = false
		s.Rebalance = true
		return result, nil
	}
	if s.Rebalance {
		ms := s.runningMembers()
		if len(ms) > 0 {
			if e = p.action(ctx, rc, ms[0].Name, "rebalance", nil); e != nil {
				return result, e
			}
		}
		s.Rebalance = false
		return result, nil
	}
	if changed, e := p.syncExposure(ctx, rc, s); changed || e != nil {
		s.Phase = "exposure"
		return result, e
	}
	// A minority outage must not prevent claiming recovery operations. Status
	// reports incomplete membership as Degraded; removal retains its own gates.
	names := []string{}
	for _, m := range s.runningMembers() {
		names = append(names, "rabbit@"+m.Name)
	}
	healthyMembers := 0
	complete := true
	var observationErr error
	for _, m := range s.runningMembers() {
		healthy, e := p.healthy(ctx, rc, m)
		if e != nil || !healthy {
			complete = false
			if e != nil && observationErr == nil {
				observationErr = e
			}
			continue
		}
		healthyMembers++
		st, e := p.brokerStatus(ctx, rc, m.Name)
		if e != nil {
			complete = false
			if observationErr == nil {
				observationErr = e
			}
			continue
		}
		for _, name := range names {
			if !slices.Contains(st.RunningNodes, name) {
				complete = false
			}
		}
	}
	if !complete {
		if observationErr != nil && healthyMembers <= len(names)/2 {
			return result, observationErr
		}
		return result, nil
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return result, e
	}
	for _, v := range retained {
		if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), v.Member); e != nil && !missing(e) {
			return result, e
		}
	}
	if rc.Cluster.Spec.DataSource != nil && !s.CloneImported {
		s.Phase = "restore"
		if e = s.save(rc.Cluster); e != nil {
			return result, e
		}
		done, e := p.cloneStep(ctx, rc)
		if e != nil {
			return result, e
		}
		fresh, e := loadState(rc.Cluster)
		if e != nil {
			return result, e
		}
		s = fresh
		if !done {
			return result, nil
		}
	}
	s.Phase = "ready"
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}
func beginChange(rc *engine.Context, s *internalState, m *member, action string) {
	m.ChangeAction = action
	m.ChangeStep = "quorum"
	m.TargetVersion = rc.Cluster.Spec.Version
	m.TargetResources = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	m.TargetTLS = rc.Cluster.Spec.TLS.Mode
	m.TargetConfig = rc.Cluster.Spec.EngineConfig
	m.TargetOptions = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
	if action == "restart" || action == "tls" {
		m.TargetVersion = m.Version
		m.TargetResources = m.ConfigResources
		m.TargetTLS = m.TLSMode
		m.TargetConfig = m.EngineConfig
		m.TargetOptions = append(json.RawMessage(nil), m.EngineOptions...)
	} else if m.Version != rc.Cluster.Spec.Version {
		s.FeatureFlags = true
	}
}
func (p *Provider) changeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member) (bool, error) {
	switch m.ChangeStep {
	case "quorum":
		if len(s.runningMembers()) > 1 {
			if err := p.action(ctx, rc, m.Name, "await-quorum", rmqspec.AwaitQuorumRequest{TimeoutSeconds: 30}); err != nil {
				return false, err
			}
		}
		m.ChangeStep = "critical"
	case "critical":
		if len(s.runningMembers()) > 1 {
			var out rmqspec.QuorumCriticalResponse
			if err := p.call(ctx, rc, m.Name, "quorum-critical", rmqspec.EmptyRequest{}, &out); err != nil {
				return false, err
			}
			if out.Critical {
				return false, fmt.Errorf("%w: member %s is quorum critical", engine.ErrConflict, m.Name)
			}
		}
		m.ChangeStep = "drain"
	case "drain":
		if err := p.action(ctx, rc, m.Name, "drain", nil); err != nil {
			return false, err
		}
		m.ChangeStep = "stop"
	case "stop":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true}); err != nil {
			return false, err
		}
		if err = p.action(ctx, rc, m.Name, "stop", nil); err != nil {
			return false, err
		}
		m.ChangeStep = "platform"
	case "platform": // New config and leaves are published only after the old broker is safely stopped.
		if err := publishRenewedCertificates(ctx, rc, m); err != nil {
			return false, err
		}
		if err := applyProvidedCertificate(ctx, rc, m); err != nil {
			return false, err
		}
		s.ConfigVersion++
		m.ConfigVersion = s.ConfigVersion
		m.DesiredVersion = m.TargetVersion
		m.TLSMode = m.TargetTLS
		m.EngineConfig = m.TargetConfig
		m.EngineOptions = append(json.RawMessage(nil), m.TargetOptions...)
		m.ConfigResources = m.TargetResources
		m.ConfigDisk = m.Disk
		if err := s.save(rc.Cluster); err != nil {
			return false, err
		}
		if m.ChangeAction == "resources" {
			return false, p.submit(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: m.TargetResources})
		}
		action := "upgrade"
		if m.ChangeAction == "restart" {
			action = "restart"
		}
		return false, p.submit(ctx, rc, s, &pending{Action: action, Member: m.Name, Version: m.TargetVersion})
	case "redeploy":
		return false, p.submit(ctx, rc, s, &pending{Action: "upgrade", Member: m.Name, Version: m.TargetVersion})
	case "reload":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Reload(ctx); err != nil {
			return false, err
		}
		m.ChangeStep = "start"
	case "start":
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		defer a.Close()
		if err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: false}); err != nil {
			return false, err
		}
		if err = p.action(ctx, rc, m.Name, "start", nil); err != nil {
			return false, err
		}
		m.ChangeStep = "wait"
	case "wait":
		st, err := p.agentStatus(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if !running(st) || st.ConfigVersion < m.ConfigVersion {
			return false, nil
		}
		b, err := p.brokerStatus(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if !slices.Contains(b.RunningNodes, "rabbit@"+m.Name) || !noProblems(b.Alarms) || !noProblems(b.Partitions) {
			return false, nil
		}
		for _, peer := range s.runningMembers() {
			if !slices.Contains(b.RunningNodes, "rabbit@"+peer.Name) {
				return false, nil
			}
		}
		m.ChangeStep = "revive"
	case "revive":
		if err := p.action(ctx, rc, m.Name, "revive", nil); err != nil {
			return false, err
		}
		m.ChangeAction = ""
		m.ChangeStep = ""
		m.Stage = "ready"
		return true, nil
	default:
		return false, fmt.Errorf("unknown member change checkpoint %q", m.ChangeStep)
	}
	return false, nil
}
func (p *Provider) removeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	survivors := s.runningMembers()
	if m.RemoveStep == "" {
		if m.Stage == "planned" || m.Stage == "imported" {
			m.RemoveStep = "delete-runtime"
		} else {
			m.RemoveStep = "check-queues"
		}
	}
	if m.RemoveStep != "delete-runtime" && m.RemoveStep != "delete-volume" {
		if len(survivors) == 0 {
			return blockRemoval(m, "no survivor for member removal")
		}
		for _, peer := range survivors {
			healthy, err := p.healthy(ctx, rc, peer)
			if err != nil || !healthy {
				return blockRemoval(m, fmt.Sprintf("remaining member %s is unavailable; removal is paused", peer.Name))
			}
		}
	}
	seed := ""
	if len(survivors) > 0 {
		seed = survivors[0].Name
	}
	switch m.RemoveStep {
	case "check-queues", "grow", "streams", "shrink", "drain", "stop-app":
		lost, err := p.departureUnavailable(ctx, rc, m)
		if err != nil {
			return err
		}
		m.Lost = m.Lost || lost
		if m.Lost {
			if err := p.guardLostMajority(ctx, rc, s, m, survivors); err != nil {
				return err
			}
		}
		if err := p.guardClassicQueues(ctx, rc, s, m); err != nil {
			return err
		}
	}
	switch m.RemoveStep {
	case "check-queues":
		m.RemoveStep = "grow"
	case "grow":
		if m.GrowIndex < len(survivors) {
			if err := p.action(ctx, rc, seed, "grow", rmqspec.NodeRequest{Node: "rabbit@" + survivors[m.GrowIndex].Name}); err != nil {
				return err
			}
			m.GrowIndex++
			return nil
		}
		m.RemoveStep = "streams"
	case "streams":
		done, err := p.migrateStreams(ctx, rc, s, m)
		if err != nil {
			return err
		}
		if done {
			m.RemoveStep = "shrink"
		}
	case "shrink":
		if err := p.action(ctx, rc, seed, "shrink", rmqspec.NodeRequest{Node: "rabbit@" + m.Name}); err != nil {
			return err
		}
		m.RemoveStep = "drain"
		if m.Lost {
			m.RemoveStep = "forget"
		}
	case "drain":
		if m.Lost {
			m.RemoveStep = "forget"
			return nil
		}
		if err := p.action(ctx, rc, m.Name, "drain", nil); err != nil {
			return err
		}
		m.RemoveStep = "stop-app"
	case "stop-app":
		if m.Lost {
			m.RemoveStep = "forget"
			return nil
		}
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return err
		}
		err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
		a.Close()
		if err != nil {
			return err
		}
		// The Erlang VM remains a live Khepri voter until the survivor forgets it.
		if err = p.action(ctx, rc, m.Name, "stop-app", nil); err != nil {
			return err
		}
		m.RemoveStep = "forget"
	case "forget":
		lost, err := p.departureUnavailable(ctx, rc, m)
		if err != nil {
			return err
		}
		m.Lost = m.Lost || lost
		if m.Lost {
			if err := p.guardLostMajority(ctx, rc, s, m, survivors); err != nil {
				return err
			}
		}
		if err := p.action(ctx, rc, seed, "forget", rmqspec.NodeRequest{Node: "rabbit@" + m.Name}); err != nil {
			return err
		}
		m.RemovalBlocked = ""
		m.RemoveStep = "stop"
	case "stop":
		st, agentErr := p.agentStatus(ctx, rc, m.Name)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if agentErr == nil && running(st) {
			a, err := p.agent(ctx, rc, m.Name)
			if err != nil {
				return err
			}
			err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
			a.Close()
			if err != nil {
				return err
			}
			if err = p.action(ctx, rc, m.Name, "stop", nil); err != nil {
				return err
			}
		}
		m.RemoveStep = "delete-runtime"
	case "delete-runtime":
		if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
			return err
		}
		if m.ManagementPublic {
			return p.submit(ctx, rc, s, &pending{Action: "cleanup-management", Member: m.Name})
		}
		return p.submit(ctx, rc, s, &pending{Action: "delete-runtime", Member: m.Name})
	case "delete-volume":
		if m.VolumeOrphaned {
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return err
			}
			removeRecord(s, m.Name)
			return nil
		}
		return p.submit(ctx, rc, s, &pending{Action: "delete-volume", Member: m.Name})
	default:
		return fmt.Errorf("unknown removal checkpoint %q", m.RemoveStep)
	}
	return nil
}

func blockRemoval(m *member, message string) error {
	m.RemovalBlocked = message
	return fmt.Errorf("%w: %s", engine.ErrConflict, message)
}

func (p *Provider) departureUnavailable(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	for _, name := range []string{m.Name, m.Volume} {
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return false, err
		}
		if service == nil {
			return true, nil
		}
	}
	st, err := p.agentStatus(ctx, rc, m.Name)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return err != nil || !running(st), nil
}

func (p *Provider) guardClassicQueues(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := s.save(rc.Cluster); err != nil {
		return err
	}
	admin, err := p.adminClient(ctx, rc)
	if err != nil {
		return err
	}
	defer admin.Close()
	queues, err := admin.Queues(ctx)
	if err != nil {
		return err
	}
	blocked := []string{}
	node := "rabbit@" + m.Name
	for _, q := range queues {
		if q.Durable && (q.Type == "classic" || q.Type == "") && (q.Node == node || q.Leader == node) {
			blocked = append(blocked, fmt.Sprintf("%q/%q", q.Vhost, q.Name))
		}
	}
	if len(blocked) > 0 {
		slices.Sort(blocked)
		return blockRemoval(m, "durable classic queues on "+m.Name+" prevent automatic removal: "+strings.Join(blocked, ", ")+"; explicitly migrate or delete these queues before retrying")
	}
	m.RemovalBlocked = ""
	return nil
}
func (p *Provider) migrateStreams(ctx context.Context, rc *engine.Context, s *internalState, m *member) (bool, error) {
	admin, err := p.adminClient(ctx, rc)
	if err != nil {
		return false, err
	}
	defer admin.Close()
	queues, err := admin.Queues(ctx)
	if err != nil {
		return false, err
	}
	survivors := s.runningMembers()
	for _, q := range queues {
		if q.Type != "stream" || !slices.Contains(q.Members, "rabbit@"+m.Name) {
			continue
		}
		for _, peer := range survivors {
			if !slices.Contains(q.Members, "rabbit@"+peer.Name) {
				return false, p.streamReplica(ctx, rc, survivors[0].Name, rmqspec.StreamReplicasRequest{Action: "add", VHost: q.Vhost, Queue: q.Name, Node: "rabbit@" + peer.Name})
			}
		}
		return false, p.streamReplica(ctx, rc, survivors[0].Name, rmqspec.StreamReplicasRequest{Action: "delete", VHost: q.Vhost, Queue: q.Name, Node: "rabbit@" + m.Name})
	}
	return true, nil
}

func (p *Provider) streamReplica(ctx context.Context, rc *engine.Context, name string, req rmqspec.StreamReplicasRequest) error {
	var out rmqspec.StreamReplicasResponse
	return p.call(ctx, rc, name, "stream-replicas", req, &out)
}

// availableMembers skips failed brokers when choosing a replacement's join seed.
func (p *Provider) availableMembers(ctx context.Context, rc *engine.Context, s *internalState) []*member {
	out := []*member{}
	for _, m := range s.runningMembers() {
		lost, err := p.departureUnavailable(ctx, rc, m)
		if err != nil || lost {
			continue
		}
		ok, err := p.healthy(ctx, rc, m)
		if err == nil && ok {
			out = append(out, m)
		}
	}
	return out
}

func (p *Provider) guardLostMajority(ctx context.Context, rc *engine.Context, s *internalState, m *member, survivors []*member) error {
	if len(survivors) == 0 {
		return blockRemoval(m, "lost-node removal requires surviving brokers")
	}
	st, err := p.brokerStatus(ctx, rc, survivors[0].Name)
	if err != nil {
		return err
	}
	voters := max(len(st.DiskNodes), len(s.Members))
	online := 0
	for _, peer := range survivors {
		if slices.Contains(st.RunningNodes, "rabbit@"+peer.Name) {
			online++
		}
	}
	if online < voters/2+1 {
		return blockRemoval(m, "lost-node removal requires a Khepri voter majority among the surviving brokers")
	}
	return nil
}
