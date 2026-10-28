// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	if tok == nil {
		return engine.ErrNotFound
	}
	specs := []infra.ServiceSpec{}
	incomplete := false
	for _, volume := range []bool{true, false} {
		name := m.Name
		if volume {
			name = m.Volume
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		incomplete = incomplete || service != nil && service.Importing
		if service != nil {
			continue
		}
		if volume {
			specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, rc.Cluster.Spec.Components.Engine.Resources))
		} else {
			specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: runtimeResources(rc.Cluster.Spec.Components.Engine.Resources), Containers: 1}))
		}
	}
	m.Resources = runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)
	m.Disk = rc.Cluster.Spec.Components.Engine.Resources
	if incomplete && m.ImportPlan == nil {
		return fmt.Errorf("%w: incomplete import for %s has no durable batch", engine.ErrConflict, m.Name)
	}
	if m.ImportPlan != nil && (len(specs) > 0 || incomplete) {
		specs = m.ImportPlan.Services(rc.Backend.Layout(), rc.Cluster.Name, m.Name, tok.Token)
	}
	if len(specs) == 0 {
		m.Stage = "imported"
		m.ImportPlan = nil
		s.resetPlatformRetry(&pending{Action: "import", Member: m.Name})
		return nil
	}
	if m.ImportPlan == nil {
		m.ImportPlan = engine.NewImportPlan(specs)
		return nil
	}
	op, err := m.ImportPlan.Submit(ctx, rc, m.Name, tok.Token)
	s.Pending = &pending{Op: op, Action: "import", Member: m.Name}
	if err != nil {
		return s.failPendingCause(rc, err)
	}
	return nil
}
func (p *Provider) deploy(ctx context.Context, rc *engine.Context, s *internalState, m *member, version string) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	m.DesiredVersion = version
	spec, err := p.runtimeSpec(rc, m, version)
	if err != nil {
		return err
	}
	op, err := engine.Deploy(ctx, rc, m.Name, spec)
	v := &pending{Op: op, Action: "deploy", Member: m.Name, Version: version}
	if err != nil {
		s.Pending = v
		return s.failPendingCause(rc, err)
	}
	m.ResumeDeploy = v
	s.resetPlatformRetry(v)
	return nil
}

// Sync provisions at most two independent runtime receipts, but serializes all
// native initialization and unseal transitions through durable ownership gates.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = time.Second
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := p.retireUnplacedScaleDown(ctx, rc, s); busy || e != nil {
		return result, e
	}
	for _, m := range s.Members {
		r := rc.Cluster.Spec.Components.Engine.Resources
		if m.VolumeID != "" && m.ImportPlan == nil && !m.Removing {
			disk, e := engine.PreflightVolumeGrowth(ctx, rc.Backend, m.Volume, m.VolumeID, m.Disk, r)
			if e != nil {
				return result, e
			}
			m.Disk.MinDisk, m.Disk.MaxDisk = disk.MinDisk, disk.MaxDisk
		}
	}
	if s.InitializationOutcomeLost != "" {
		result.ConvergenceError = p.initializationLost(s)
		return result, nil
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if _, e := p.observeDeployments(ctx, rc, s); e != nil {
		return result, e
	}
	if s.UID == "" {
		s.UID, err = secrets.Token()
		if err != nil {
			return result, err
		}
	}
	if s.Suspended {
		s.Suspended = false
		s.Phase = "deploy"
		for _, m := range s.Members {
			m.Resume = true
			m.Stage = "planned"
			m.ServiceID = ""
			m.RemoveStep = ""
			m.PublicHash = ""
		}
		return result, nil
	}
	desired := rc.Cluster.Spec.Components.Engine.Replicas
	if len(s.Members) < desired {
		for len(s.Members) < desired {
			if _, err = allocateNewMember(ctx, rc, s); err != nil {
				return result, err
			}
		}
		s.ConfigVersion++
		for _, m := range s.Members {
			activateConfig(rc.Cluster, s, m)
		}
		s.Phase = "import"
		return result, nil
	}
	if len(s.Members) > desired && !s.Replacing {
		members := ordered(s)
		target := members[len(members)-1]
		if !target.Removing {
			if rc.WaitForWindow().RequeueAfter == 0 {
				target.Removing = true
			}
		}
	}
	if s.InitOwner == "" {
		s.InitOwner = ordered(s)[0].Name
		return result, nil
	}
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if busy, e := p.ensurePostgresStorage(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.recoverRuntimes(ctx, rc, s); busy || e != nil {
		return result, e
	}
	inFlight := 0
	for _, m := range s.Members {
		if m.ResumeDeploy != nil {
			inFlight++
		}
	}
	for _, m := range ordered(s) {
		if m.Removing || m.ResumeDeploy != nil {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "import"
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			if inFlight >= 2 {
				continue
			}
			s.Phase = "deploy"
			if err = p.deploy(ctx, rc, s, m, rc.Cluster.Spec.Version); err != nil {
				return result, err
			}
			inFlight++
		}
	}
	for _, m := range ordered(s) {
		if m.ResumeDeploy != nil || m.Stage == "planned" || m.Stage == "imported" {
			continue
		}
		if m.Stage == "resume-deployed" {
			a, e := p.agent(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			e = a.Maintenance(ctx, agentapi.Maintenance{Enabled: false})
			if e == nil {
				e = a.Process(ctx, agentapi.ProcessAction{Process: "openbao", Action: "start"})
			}
			a.Close()
			if e != nil {
				return result, e
			}
			m.Resume = false
			m.Stage = "deployed"
			return result, nil
		}
		st, e := p.agentStatus(ctx, rc, m.Name)
		if e != nil {
			continue
		}
		if st.ConfigVersion < m.ConfigVersion {
			a, e := p.agent(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			e = a.Reload(ctx)
			a.Close()
			return result, e
		}
		if !strings.EqualFold(st.Processes["openbao"].State, "running") {
			continue
		}
		m.AppliedConfigVersion = st.ConfigVersion
		if m.Stage == "deployed" {
			m.Stage = "configured"
		}
	}
	p.observeMembers(ctx, rc, s)
	// Preparation still needs ordinary formation/unseal reconciliation. Only
	// snapshot submission and recovery own native reconciliation exclusively.
	if s.CloneRestoreStep != "" && s.CloneRestoreStep != "prepare" && s.CloneRestoreStep != "done" {
		if busy, e := p.restoreDataSource(ctx, rc, s); busy || e != nil {
			return result, e
		}
	}
	s.Phase = "initializing"
	if busy, e := p.initialize(ctx, rc, s); busy || e != nil {
		if s.InitializationOutcomeLost != "" {
			result.ConvergenceError = e
			return result, nil
		}
		return result, e
	}
	material, e := readUnseal(ctx, rc)
	if e != nil {
		return result, e
	}
	// Establish the owner first. A follower is never a sys/init target.
	owner := s.find(s.InitOwner)
	if owner == nil {
		return result, p.initializationLost(s)
	}
	active := false
	for _, m := range s.Members {
		active = active || m.Active
	}
	if !active {
		target := owner
		// A previously formed quorum may restart entirely sealed. Existing
		// voters must be unsealed to recover that quorum before autopilot can
		// answer; fresh followers still wait for an active owner.
		if s.Unsealing != "" {
			if m := s.find(s.Unsealing); m != nil {
				target = m
			}
		}
		if target.Observed && target.Sealed && target.Initialized && !p.autoUnseal(rc.Cluster) {
			c, e := p.nativeClient(ctx, rc, target.Name, "")
			if e != nil {
				return result, e
			}
			_, e = c.UnsealWith(ctx, material.Shares[:material.Threshold])
			s.Unsealing = target.Name
			return result, e
		}
		for _, m := range ordered(s) {
			if !m.Observed || (!m.Voter && !p.postgresStorage(rc.Cluster)) || !m.Initialized || !m.Sealed || p.autoUnseal(rc.Cluster) {
				continue
			}
			if !p.postgresStorage(rc.Cluster) {
				s.QuorumBlocked = "Waiting for previously established Raft voters to unseal and elect an active member"
			}
			if s.Unsealing != m.Name {
				s.Unsealing = m.Name
				return result, nil
			}
			return result, p.nativeUnsealStep(ctx, rc, s)
		}
		return result, nil
	}
	token, e := p.ensureManagementToken(ctx, rc, s)
	if e != nil {
		return result, e
	}
	if e = p.ackInitialization(ctx, rc, s); e != nil {
		return result, e
	}
	admin, _, e := p.activeClient(ctx, rc, s, token)
	if e != nil {
		return result, e
	}
	if p.postgresStorage(rc.Cluster) {
		for _, m := range s.Members {
			m.Voter = false
			m.Healthy = m.Observed && m.Initialized && !m.Sealed
			if m.Healthy {
				m.Stage = "ready"
			}
		}
		if s.Unsealing != "" {
			m := s.find(s.Unsealing)
			if m != nil && m.Observed && m.Sealed && !p.autoUnseal(rc.Cluster) {
				return result, p.nativeUnsealStep(ctx, rc, s)
			}
			if m != nil && m.Observed && !m.Sealed {
				s.Unsealing = ""
			}
		}
		for _, m := range ordered(s) {
			if m.Observed && m.Initialized && m.Sealed && !p.autoUnseal(rc.Cluster) && !m.Removing && m.Change == nil {
				s.Unsealing = m.Name
				return result, nil
			}
		}
		return p.finishNativeSync(ctx, rc, s, true)
	}
	peers, e := admin.RaftConfiguration(ctx)
	if e != nil {
		return result, e
	}
	autopilot, e := admin.AutopilotState(ctx)
	if e != nil {
		return result, e
	}
	for _, m := range s.Members {
		m.Voter = false
		for _, peer := range peers {
			if peer.NodeID == m.Name {
				m.Voter = peer.Voter
			}
		}
		for id, server := range autopilot.Servers {
			if server.Name == m.Name || id == m.Name {
				m.Healthy = m.Observed && !m.Sealed && m.Voter && server.Healthy && (server.Status == "voter" || server.Status == "leader")
			}
		}
		if m.Healthy && !m.Sealed && m.Initialized {
			m.Stage = "ready"
			m.RuntimeRecovery.Initialized = true
		}
	}
	s.QuorumBlocked = ""
	if busy, e := p.lifecycleStep(ctx, rc, s, autopilot.Healthy); busy || e != nil {
		return result, e
	}
	if s.Unsealing != "" {
		m := s.find(s.Unsealing)
		if m == nil || !m.Observed {
			return result, nil
		}
		if m != nil && m.Observed {
			if m.Sealed && !p.autoUnseal(rc.Cluster) {
				return result, p.nativeUnsealStep(ctx, rc, s)
			}
			if !m.Healthy {
				return result, nil
			}
		}
		s.Unsealing = ""
		return result, nil
	}
	for _, m := range ordered(s) {
		if m.ResumeDeploy != nil || m.Stage == "planned" || m.Stage == "imported" || m.Stage == "deployed" {
			return result, nil
		}
		if m.Observed && m.Sealed && !p.autoUnseal(rc.Cluster) {
			if !m.Initialized {
				return result, nil
			}
			s.Unsealing = m.Name
			return result, nil // Persist the serialization fence first.
		}
	}
	for _, m := range s.Members {
		if !m.Healthy || m.Version != m.DesiredVersion {
			return result, nil
		}
	}
	if autopilot.Healthy {
		if busy, e := p.restoreDataSource(ctx, rc, s); busy || e != nil {
			return result, e
		}
	}
	if busy, e := p.reconcileConnections(ctx, rc); busy || e != nil {
		return result, e
	}
	if busy, e := p.reconcileExposure(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if !autopilot.Healthy {
		return result, nil
	}
	if len(s.Members) != rc.Cluster.Spec.Components.Engine.Replicas {
		return result, nil
	}
	s.Phase = "ready"
	s.RollingKind = ""
	s.RollingGeneration = 0
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}

// recoverEstablishedPeers lets restart recovery precede a blocked rolling
// mutation (including a platform retry). Formation still owns new followers,
// and the rolling wait stage exclusively owns its current runtime.
func (p *Provider) recoverEstablishedPeers(ctx context.Context, rc *engine.Context, s *internalState, rolling *member) (bool, error) {
	if p.autoUnseal(rc.Cluster) {
		return false, nil
	}
	waitOwnsTarget := s.Retry == nil && rolling.Change != nil && (rolling.Change.Step == "wait" || rolling.Change.Step == "reload" || rolling.ResumeDeploy != nil)
	established := func(m *member) bool {
		return m != nil && (m.Voter || p.postgresStorage(rc.Cluster) && m.Stage == "ready")
	}
	if s.Unsealing != "" {
		if s.Unsealing == rolling.Name && waitOwnsTarget {
			return false, nil
		}
		m := s.find(s.Unsealing)
		// Never steal a formation fence or clear one on absent observations.
		if !established(m) || !m.Observed || !m.Initialized {
			return true, nil
		}
		if m.Sealed {
			return true, p.nativeUnsealStep(ctx, rc, s)
		}
		if !p.postgresStorage(rc.Cluster) {
			token, err := p.ensureManagementToken(ctx, rc, s)
			if err != nil {
				return true, err
			}
			admin, _, err := p.activeClient(ctx, rc, s, token)
			if err != nil {
				return true, nil
			}
			peers, err := admin.RaftConfiguration(ctx)
			if err != nil {
				return true, nil
			}
			voter := false
			for _, peer := range peers {
				voter = voter || peer.NodeID == m.Name && peer.Voter
			}
			autopilot, err := admin.AutopilotState(ctx)
			if err != nil {
				return true, nil
			}
			healthy := false
			for id, server := range autopilot.Servers {
				if id == m.Name || server.Name == m.Name {
					healthy = server.Healthy && (server.Status == "voter" || server.Status == "leader")
				}
			}
			if !voter || !healthy {
				return true, nil
			}
		}
		s.Unsealing = ""
		return true, nil
	}
	for _, m := range ordered(s) {
		if m.Name == rolling.Name && waitOwnsTarget || !established(m) || !m.Observed || !m.Initialized || !m.Sealed || m.ResumeDeploy != nil {
			continue
		}
		s.Unsealing = m.Name
		return true, nil // Commit the serial recovery fence before sharing keys.
	}
	return false, nil
}

// nativeUnsealStep is shared by convergence and rolling workflows. Its caller
// must persist s.Unsealing before invoking it on a new follower.
func (p *Provider) nativeUnsealStep(ctx context.Context, rc *engine.Context, s *internalState) error {
	if p.autoUnseal(rc.Cluster) {
		return nil
	}
	if s.Unsealing == "" {
		return nil
	}
	m := s.find(s.Unsealing)
	if m == nil {
		return engine.ErrNotFound
	}
	if !m.Observed || !m.Sealed {
		return nil
	}
	material, err := readUnseal(ctx, rc)
	if err != nil {
		return err
	}
	c, err := p.nativeClient(ctx, rc, m.Name, "")
	if err != nil {
		return err
	}
	_, err = c.UnsealWith(ctx, material.Shares[:material.Threshold])
	return err
}
