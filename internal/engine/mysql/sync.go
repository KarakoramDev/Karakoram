// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"time"
)

func desiredComponent(rc *engine.Context, m *member) domain.ComponentSpec {
	if m.Role == "proxy" {
		return rc.Cluster.Spec.Components.Extra["proxy"]
	}
	c := rc.Cluster.Spec.Components.Engine
	c.Replicas = 1
	return c
}
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func verifyMember(ctx context.Context, rc *engine.Context, m *member) error {
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	if token == nil {
		return engine.ErrNotFound
	}
	for _, entry := range []struct {
		name string
		id   *string
	}{{m.Name, &m.ServiceID}, {m.Volume, &m.VolumeID}} {
		if entry.name == "" {
			continue
		}
		service, err := rc.Backend.GetService(ctx, entry.name)
		if err != nil {
			return err
		}
		if service == nil {
			continue
		}
		if *entry.id != service.ID && !engine.HasOwnership(service, token.Token, rc.Cluster.Name, m.Name) {
			return engine.UnverifiedOwnership(service)
		}
		*entry.id = service.ID
		if err = engine.EnsureOwnerMarker(ctx, rc.Backend, service, token.Token, rc.Cluster.Name, m.Name); err != nil {
			return err
		}
	}
	return nil
}
func (p *Core) track(s *state, m *member, action string, op infra.Op) {
	s.Pending = &pending{Op: op, Action: action, Member: m.Name}
}
func resumePlannedImport(ctx context.Context, rc *engine.Context, m *member) (*infra.Op, error) {
	if m.ImportBatch == nil {
		return nil, nil
	}
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, engine.ErrNotFound
	}
	return engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, token.Token)
}
func (p *Core) observePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	m := s.find(q.Member)
	if m == nil {
		return true, engine.ErrNotFound
	}
	if q.Action == "import" {
		op, err := resumePlannedImport(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if op != nil {
			q.Op = *op
			return true, nil
		}
	}
	status, err := rc.Backend.OpStatus(ctx, q.Op)
	if err != nil {
		return true, err
	}
	if !status.Done && !status.Failed {
		return true, nil
	}
	if status.Failed {
		s.Pending = nil
		return true, fmt.Errorf("infrastructure %s failed for %s", q.Action, q.Member)
	}
	switch q.Action {
	case "import":
		if err = verifyMember(ctx, rc, m); err != nil {
			return true, err
		}
		if m.ServiceID == "" || m.Volume != "" && m.VolumeID == "" {
			return true, engine.ErrUnavailable
		}
		m.Stage = "imported"
		m.ImportBatch = nil
	case "delete-runtime":
		m.ServiceID = ""
		m.Public = false
		m.Stage = "planned"
		m.DeployOp = nil
	case "delete-volume":
		m.VolumeID = ""
	}
	s.Pending = nil
	return true, nil
}
func (p *Core) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if intent, e := loadAccessIntent(ctx, rc); e != nil || intent != nil {
		if e != nil {
			return result, e
		}
		return engine.Result{RequeueAfter: 2 * time.Second}, p.completeAccessIntent(ctx, rc, intent)
	}
	result.RequeueAfter = 2 * time.Second
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	// Observation records discontinuities without fencing normal service.
	if e := checkPXCQuorumGTIDs(ctx, rc, s); e != nil {
		if _, observation := e.(*myspec.GTIDQuorumError); !observation {
			return result, e
		}
	}
	if busy, e := p.reconcileGaleraTrafficRotation(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.reconcileRestoreRecovery(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.UID == "" {
		s.UID, err = secrets.RandomString(16)
		if err != nil {
			return result, err
		}
		s.Topology = rc.Cluster.Spec.Topology.Type
	}
	if err = p.rememberRepository(ctx, rc, s); err != nil {
		return result, err
	}
	if s.Formation.GroupName == "" {
		s.Formation.GroupName, err = p.newReplicationIdentity()
		if err != nil {
			return result, err
		}
	}
	if s.Topology != rc.Cluster.Spec.Topology.Type {
		if err = p.adoptSingleton(ctx, rc, s); err != nil {
			return result, err
		}
		return result, nil
	}
	if s.Suspended {
		s.Suspended = false
		if clustered(s.Topology) {
			s.Formation.Reformation = &reformation{State: "observing"}
			for _, m := range s.data() {
				m.Offline = true
			}
		}
		publishRevision(s)
		return result, nil
	}
	if busy, e := p.reconcileMembership(ctx, rc, s); busy || e != nil {
		return result, e
	}
	s.allocate(rc.Cluster)
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if s.GaleraTLS != nil {
		return result, nil
	}
	if err = p.reconcileExposure(ctx, rc, s, false); err != nil {
		return result, err
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.reconcileRolling(ctx, rc, s); busy || e != nil {
		return result, e
	}
	raw, e := json.Marshal(rc.Cluster.Spec)
	if e != nil {
		return result, e
	}
	managed, e := secret(ctx, rc, "mysql/access/proxy-users")
	if e != nil && !missing(e) {
		return result, e
	}
	digest := sha256.New()
	digest.Write(raw)
	digest.Write(managed)
	hash := hex.EncodeToString(digest.Sum(nil))
	if s.ConfigHash != hash {
		s.ConfigHash = hash
		publishRevision(s)
	}
	writer := false
	if clustered(s.Topology) || s.Topology == "single" {
		for _, m := range s.data() {
			st, e := live(ctx, rc, m.Name)
			if e == nil && (onlinePrimary(st) || s.Topology == "single" && st.MemberState == "ONLINE") {
				writer = true
				break
			}
		}
	}
	if clustered(s.Topology) && s.Formation.State == "formed" && (s.Formation.Reformation == nil || s.Formation.Reformation.State == "formed") {
		allOffline := len(s.data()) > 0
		for _, m := range s.data() {
			st, e := live(ctx, rc, m.Name)
			if s.Topology == "cluster" {
				if m.Stage != "joined" || !galeraStopped(ctx, rc, m.Name) {
					allOffline = false
				}
			} else if e != nil || m.Stage != "joined" || st.MemberState != "OFFLINE" && st.MemberState != "ERROR" {
				allOffline = false
			}
		}
		if allOffline {
			s.Formation.Reformation = &reformation{State: "observing"}
			for _, m := range s.data() {
				m.Offline = true
			}
			publishRevision(s)
			return result, nil
		}
	}
	// Import every data identity before waiting for engine-level readiness.
	for _, m := range s.Members {
		if m.Role == "proxy" && !writer {
			continue
		}
		desired := desiredComponent(rc, m)
		if m.Stage != "planned" {
			continue
		}
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return result, e
		}
		if tok == nil {
			return result, engine.ErrNotFound
		}
		if m.ImportBatch == nil {
			specs := []infra.ServiceSpec{}
			if m.Volume != "" {
				v, e := rc.Backend.GetService(ctx, m.Volume)
				if e != nil {
					return result, e
				}
				if v == nil {
					if m.Initialized {
						return result, engine.Invalid("initialized MySQL volume %s is missing", m.Volume)
					}
					specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, desired.Resources))
				} else {
					if v.Importing {
						return result, engine.Invalid("incomplete import has no frozen batch for %s", m.Name)
					}
					if (m.VolumeID == "" || m.VolumeID != v.ID) && !engine.HasOwnership(v, tok.Token, rc.Cluster.Name, m.Name) {
						return result, engine.UnverifiedOwnership(v)
					}
					m.VolumeID = v.ID
				}
			}
			runtime, e := rc.Backend.GetService(ctx, m.Name)
			if e != nil {
				return result, e
			}
			if runtime == nil {
				specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: desired.Resources, Containers: desired.Replicas}))
			} else if runtime.Importing {
				return result, engine.Invalid("incomplete import has no frozen batch for %s", m.Name)
			}
			m.Resources = desired.Resources
			m.Replicas = desired.Replicas
			m.Version = rc.Cluster.Spec.Version
			options, e := ParseOptions(rc.Cluster.Spec.EngineOptions)
			if e != nil {
				return result, e
			}
			if m.Role == "data" {
				m.Parameters = restartParameters(options.Parameters)
			}
			m.ConfigVersion = s.ConfigVersion
			if len(specs) == 0 {
				if e = verifyMember(ctx, rc, m); e != nil {
					return result, e
				}
				m.Stage = "imported"
			} else {
				m.ImportBatch = engine.NewImportPlan(specs)
			}
			return result, nil
		}
		op, e := m.ImportBatch.Submit(ctx, rc, m.Name, tok.Token)
		if e != nil {
			return result, e
		}
		p.track(s, m, "import", op)
		return result, nil
	}
	for _, m := range s.Members {
		if m.Role == "proxy" && !writer {
			continue
		}
		if m.Stage != "imported" {
			continue
		}
		spec, e := p.runtimeSpec(rc, m)
		if e != nil {
			return result, e
		}
		if engine.DeploymentBlocked(rc, m.Name, spec) {
			continue
		}
		op, e := engine.Deploy(ctx, rc, m.Name, spec)
		if e != nil {
			return result, e
		}
		m.DeployOp = &op
		m.Stage = "deploying"
		return result, nil
	}
	deploymentsBusy := false
	for _, m := range s.Members {
		if m.Stage != "deploying" {
			continue
		}
		if m.DeployOp == nil {
			return result, engine.Invalid("deploying member has no receipt")
		}
		st, e := rc.Backend.OpStatus(ctx, *m.DeployOp)
		if st.Status == "DeploymentRetriesExhausted" && e == nil {
			e = &infra.DeploymentExhaustedError{Service: m.Name, LastError: st.Message}
		}
		if errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
			spec, x := p.runtimeSpec(rc, m)
			if x != nil {
				return result, x
			}
			m.Stage = "imported"
			m.DeployOp = nil
			if x = engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, e); x != nil {
				return result, x
			}
			continue
		}
		if e != nil {
			return result, e
		}
		if !st.Done && !st.Failed {
			deploymentsBusy = true
			continue
		}
		m.DeployOp = nil
		if st.Failed {
			m.Stage = "imported"
			return result, fmt.Errorf("deployment failed for %s", m.Name)
		}
		m.Stage = "deployed"
	}
	if deploymentsBusy {
		return result, nil
	}
	if e := save(rc.Cluster, s); e != nil {
		return result, e
	}
	if busy, e := p.syncInitialRestore(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if changed, e := p.reconcileFormation(ctx, rc, s); changed || e != nil {
		return result, e
	}
	converged := true
	for _, m := range s.Members {
		if m.Stage != "deployed" && m.Stage != "joined" {
			converged = false
			continue
		}
		ok, e := ready(ctx, rc, m)
		if e != nil {
			return result, e
		}
		if !ok {
			converged = false
			continue
		}
		if m.Stage != "joined" {
			m.Stage = "joined"
			if m.Role == "data" {
				m.Initialized = true
				publishRevision(s)
			}
			if rc.Store != nil && m.Volume != "" {
				if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
					return result, e
				}
			}
			return result, nil
		}
	}
	if converged && len(s.Members) > 0 {
		if err = p.reconcileExposure(ctx, rc, s, true); err != nil {
			return result, err
		}
		if s.Topology == "single" {
			s.Formation.State = "formed"
		}
		if e := save(rc.Cluster, s); e != nil {
			return result, e
		}
		status, e := p.Status(ctx, rc)
		if e != nil {
			return result, e
		}
		rc.Cluster.Status = status
		if status.Phase == domain.PhaseReady {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
			result.RequeueAfter = 15 * time.Second
		}
	}
	return result, nil
}

// Revoke changed routes before health gates; publish only after native convergence.
func (p *Core) reconcileExposure(ctx context.Context, rc *engine.Context, s *state, publish bool) error {
	raw, err := json.Marshal(rc.Cluster.Spec.Exposure)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	hash := hex.EncodeToString(digest[:])
	target := connectionMember(rc, s)
	for _, m := range s.Members {
		want := rc.Cluster.Spec.Exposure.Public && m.Name == target
		if m.Public && (!want || m.ExposureHash != hash) {
			if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
				return err
			}
			m.Public = false
		}
		if !publish || !want || m.Public {
			continue
		}
		if err = engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure); err != nil {
			return err
		}
		exposure := rc.Cluster.Spec.Exposure
		if err = rc.Backend.EnsurePublicPort(ctx, m.Name, infra.PublicPort{Port: 3306, TargetPort: 3306, Protocol: "tcp", IPType: exposure.IPType, Allowlist: exposure.Allowlist}); err != nil {
			return err
		}
		m.Public = true
		m.ExposureHash = hash
	}
	return nil
}
