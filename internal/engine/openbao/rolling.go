// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"reflect"
	"time"
)

type memberChange struct {
	Kind        string           `json:"kind"`
	Step        string           `json:"step"`
	Version     string           `json:"version"`
	Fingerprint string           `json:"fingerprint"`
	Resources   domain.Resources `json:"resources"`
	Disk        domain.Resources `json:"disk"`
	Started     bool             `json:"started,omitempty"`
}

func (p *Provider) restartFingerprint(ctx context.Context, rc *engine.Context) (string, error) {
	cas, err := p.trustCAs(ctx, rc)
	if err != nil {
		return "", err
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return "", err
	}
	seal, err := p.sealCredentialFingerprint(ctx, rc)
	if err != nil {
		return "", err
	}
	recoveryCA, err := memberCertificate(ctx, rc, secretRecoveryCA)
	if err != nil {
		return "", err
	}
	return hash(struct {
		Config     string
		Options    string
		CAs        []string
		Generation int64
		Seal       string
		RecoveryCA string
	}{rc.Cluster.Spec.EngineConfig, string(rc.Cluster.Spec.EngineOptions), cas, s.CredentialGeneration, seal, recoveryCA.CertPEM}), nil
}
func (p *Provider) connectionTrustReady(ctx context.Context, rc *engine.Context) (bool, error) {
	fp, err := p.restartFingerprint(ctx, rc)
	if err != nil {
		return false, err
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	for _, m := range s.Members {
		if m.RestartFingerprint != fp || m.Change != nil || m.Stage != "ready" || m.AppliedConfigVersion < m.ConfigVersion {
			return false, nil
		}
	}
	return true, nil
}

func (p *Provider) lifecycleStep(ctx context.Context, rc *engine.Context, s *internalState, healthy bool) (bool, error) {
	for _, m := range ordered(s) {
		if m.Removing {
			return true, p.removeStep(ctx, rc, s, m)
		}
	}
	for _, m := range ordered(s) {
		if m.Change != nil {
			s.RollingKind = m.Change.Kind
			if joining := s.find(s.Unsealing); joining != nil && !joining.Voter && !p.postgresStorage(rc.Cluster) {
				return false, nil // Let ordinary formation finish its existing fence.
			}
			if busy, err := p.recoverEstablishedPeers(ctx, rc, s, m); busy || err != nil {
				return true, err
			}
			return true, p.changeStep(ctx, rc, s, m, healthy)
		}
	}
	if s.Unsealing != "" {
		return false, nil
	}
	fp, err := p.restartFingerprint(ctx, rc)
	if err != nil {
		return true, err
	}
	candidates := ordered(s)
	// Stable partition: every standby precedes the current active member.
	for _, active := range []bool{false, true} {
		for _, m := range candidates {
			if m.Active != active || m.Stage != "ready" {
				continue
			}
			if m.RestartFingerprint == "" {
				m.RestartFingerprint = fp
				continue
			}
			kind := ""
			if m.DesiredVersion != rc.Cluster.Spec.Version {
				kind = "deploy"
			} else if !reflect.DeepEqual(m.Resources, runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)) || m.Disk.MinDisk != rc.Cluster.Spec.Components.Engine.Resources.MinDisk || m.Disk.MaxDisk != rc.Cluster.Spec.Components.Engine.Resources.MaxDisk {
				kind = "resources"
			} else if m.RestartFingerprint != fp {
				kind = "restart"
			}
			if kind != "" {
				if !healthy {
					return true, nil
				}
				// Evaluate admission before persisting a new change plan.
				// Otherwise this sweep starts the desired receipt and only
				// the next sweep discovers that no disruption was admitted.
				if rc.WaitForWindow().RequeueAfter > 0 {
					return true, nil
				}
				m.Change = &memberChange{Kind: kind, Version: rc.Cluster.Spec.Version, Fingerprint: fp, Resources: runtimeResources(rc.Cluster.Spec.Components.Engine.Resources), Disk: rc.Cluster.Spec.Components.Engine.Resources}
				s.RollingKind = kind
				return true, nil
			}
		}
	}
	return false, nil
}

func (p *Provider) changeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member, healthy bool) error {
	c := m.Change
	needsWindow := c.Kind != "tls" || rc.Operation != nil && rc.Operation.Kind == domain.OperationRotateTLS
	if !c.Started && needsWindow {
		switch c.Step {
		case "", "apply", "runtime-resources", "deploy":
			if rc.WaitForWindow().RequeueAfter > 0 {
				return nil
			}
		}
	}
	// Every durable boundary can be separated by a controller restart or a
	// leadership/quorum change. Check again where disruption actually occurs.
	switch c.Step {
	case "apply", "runtime-resources", "disk", "resource-config", "deploy":
		if c.Kind != "tls" {
			allowed, err := p.allowDisruption(ctx, rc, s, m)
			if err != nil || !allowed {
				return err
			}
		}
	}
	switch c.Step {
	case "":
		if !m.Observed || !m.Healthy || !healthy {
			return nil
		}
		if c.Kind != "tls" && m.Active && len(s.Members) > 1 {
			token, err := p.ensureManagementToken(ctx, rc, s)
			if err != nil {
				return err
			}
			client, err := p.nativeClient(ctx, rc, m.Name, token)
			if err != nil {
				return err
			}
			if err = client.StepDown(ctx); err != nil {
				return err
			}
			c.Started = true
			c.Step = "stepped-down"
			return nil
		}
		c.Step = "apply"
		return nil
	case "stepped-down":
		if m.Active {
			return nil
		}
		found := false
		for _, other := range s.Members {
			found = found || other.Active
		}
		if !found {
			return nil
		}
		c.Step = "apply"
		return nil
	case "apply":
		if c.Kind == "resources" {
			c.Step = "runtime-resources"
			return nil
		}
		c.Started = true
		s.ConfigVersion++
		activateConfig(rc.Cluster, s, m)
		if c.Kind == "deploy" {
			c.Step = "deploy"
			return nil
		}
		if c.Kind == "tls" {
			if err := rotateCertificate(ctx, rc, m); err != nil {
				return err
			}
		} else {
			m.ProcessGeneration++
		}
		c.Step = "reload"
		return nil
	case "runtime-resources":
		c.Started = true
		return p.submit(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: c.Resources})
	case "disk":
		// Legacy disk checkpoints also contain CPU/RAM fields. Only persistent
		// disk bounds decide whether this step needs a volume operation.
		if m.Disk.MinDisk == c.Disk.MinDisk && m.Disk.MaxDisk == c.Disk.MaxDisk {
			c.Step = "resource-config"
			return nil
		}
		return p.submit(ctx, rc, s, &pending{Action: "disk", Member: m.Name, Resources: c.Disk})
	case "resource-config":
		s.ConfigVersion++
		activateConfig(rc.Cluster, s, m)
		m.ProcessGeneration++
		c.Step = "reload"
		return nil
	case "deploy":
		if err := p.deploy(ctx, rc, s, m, c.Version); err != nil {
			return err
		}
		c.Step = "wait"
		return nil
	case "reload":
		if err := s.save(rc.Cluster); err != nil {
			return err
		}
		a, err := p.agent(ctx, rc, m.Name)
		if err != nil {
			return err
		}
		err = a.Reload(ctx)
		a.Close()
		if err != nil {
			return err
		}
		c.Step = "wait"
		return nil
	case "wait":
		if m.ResumeDeploy != nil {
			return nil
		}
		st, err := p.agentStatus(ctx, rc, m.Name)
		if err != nil {
			return err
		}
		if st.ConfigVersion < m.ConfigVersion || st.Processes["openbao"].State != "running" {
			return nil
		}
		m.AppliedConfigVersion = st.ConfigVersion
		if c.Kind == "tls" {
			m.Change = nil
			return nil
		}
		if !m.Observed {
			return nil
		}
		if m.Sealed {
			if p.autoUnseal(rc.Cluster) {
				return nil
			}
			if s.Unsealing != m.Name {
				s.Unsealing = m.Name
				return nil
			}
			return p.nativeUnsealStep(ctx, rc, s)
		}
		if m.Healthy && s.Unsealing == m.Name {
			// This runtime has recovered. A separately sealed established peer
			// can now take the serial fence even if global autopilot is unhealthy.
			s.Unsealing = ""
		}
		if !m.Healthy || !healthy {
			return nil
		}
		s.Unsealing = ""
		m.RestartFingerprint = c.Fingerprint
		m.Change = nil
	}
	return nil
}

func (p *Provider) finishNativeSync(ctx context.Context, rc *engine.Context, s *internalState, healthy bool) (engine.Result, error) {
	r := engine.Result{RequeueAfter: time.Second}
	if busy, err := p.lifecycleStep(ctx, rc, s, healthy); busy || err != nil {
		return r, err
	}
	for _, m := range s.Members {
		if !m.Healthy {
			return r, nil
		}
	}
	if busy, err := p.reconcileConnections(ctx, rc); busy || err != nil {
		return r, err
	}
	if busy, err := p.reconcileExposure(ctx, rc, s); busy || err != nil {
		return r, err
	}
	if len(s.Members) != rc.Cluster.Spec.Components.Engine.Replicas {
		return r, nil
	}
	s.Phase = "ready"
	s.RollingKind = ""
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	r.RequeueAfter = 30 * time.Second
	return r, nil
}

func (p *Provider) allowDisruption(ctx context.Context, rc *engine.Context, s *internalState, target *member) (bool, error) {
	s.QuorumBlocked = "Waiting for live healthy quorum and a standby target before disruption"
	token, err := p.ensureManagementToken(ctx, rc, s)
	if err != nil {
		return false, err
	}
	admin, _, err := p.activeClient(ctx, rc, s, token)
	if err != nil {
		return false, nil
	}
	live := map[string]bool{}
	for _, m := range s.Members {
		c, err := p.nativeClient(ctx, rc, m.Name, token)
		if err != nil {
			continue
		}
		seal, err := c.SealStatus(ctx)
		if err == nil && seal.Initialized && !seal.Sealed {
			live[m.Name] = true
		}
	}
	if !live[target.Name] {
		return false, nil
	}
	if !p.postgresStorage(rc.Cluster) {
		peers, err := admin.RaftConfiguration(ctx)
		if err != nil {
			return false, nil
		}
		autopilot, err := admin.AutopilotState(ctx)
		if err != nil {
			return false, nil
		}
		voters, survivors := 0, 0
		for _, peer := range peers {
			if !peer.Voter {
				continue
			}
			voters++
			healthy := false
			for id, server := range autopilot.Servers {
				if id == peer.NodeID || server.Name == peer.NodeID {
					healthy = server.Healthy
				}
			}
			if peer.NodeID != target.Name && live[peer.NodeID] && healthy {
				survivors++
			}
		}
		// A single-member server necessarily loses availability during a roll.
		if !autopilot.Healthy || voters == 0 || voters > 1 && survivors < voters/2+1 {
			return false, nil
		}
	}
	client, err := p.nativeClient(ctx, rc, target.Name, token)
	if err != nil {
		return false, nil
	}
	leader, err := client.Leader(ctx)
	if err != nil {
		return false, nil
	}
	if len(s.Members) > 1 && leader.IsSelf {
		if err = client.StepDown(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if len(s.Members) > 1 && leader.LeaderAddress == "" {
		return false, nil
	}
	s.QuorumBlocked = ""
	return true, nil
}
