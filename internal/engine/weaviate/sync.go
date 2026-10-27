// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"reflect"
	"time"
)

func verifyMemberServices(ctx context.Context, rc *engine.Context, m *member) error {
	tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return e
	}
	if tok == nil {
		return engine.ErrUnavailable
	}
	for _, volume := range []bool{false, true} {
		name, id, kind := m.Name, m.ServiceID, infra.KindRuntime
		if volume {
			name, id, kind = m.Volume, m.VolumeID, infra.KindVolume
		}
		v, e := rc.Backend.GetService(ctx, name)
		if e != nil {
			return e
		}
		if v == nil {
			continue
		}
		if v.Kind != kind || v.ID != id && !engine.HasOwnership(v, tok.Token, rc.Cluster.Name, m.Name) {
			return engine.UnverifiedOwnership(v)
		}
		if volume {
			m.VolumeID = v.ID
		} else {
			m.ServiceID = v.ID
		}
	}
	return nil
}
func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if e := verifyMemberServices(ctx, rc, m); e != nil {
		return e
	}
	if m.ImportPlan == nil {
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return e
		}
		specs := []infra.ServiceSpec{}
		for _, volume := range []bool{true, false} {
			name := m.Name
			if volume {
				name = m.Volume
			}
			v, e := rc.Backend.GetService(ctx, name)
			if e != nil {
				return e
			}
			if v != nil {
				if v.Importing {
					return fmt.Errorf("incomplete import has no original batch")
				}
				continue
			}
			if volume {
				specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, m.Resources))
			} else {
				specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Volume: m.Volume, Token: tok.Token, Resources: runtimeResources(m.Resources), Containers: 1}))
			}
		}
		if len(specs) == 0 {
			m.Stage = "imported"
			return nil
		}
		m.ImportPlan = engine.NewImportPlan(specs)
		return nil
	}
	s.Pending = &pending{Action: "import", Member: m.Name}
	return nil
}
func (p *Provider) advancePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	m := s.find(q.Member)
	if m == nil {
		return true, fmt.Errorf("pending member is missing")
	}
	if q.Op == nil {
		if q.Waiting(now(rc), q.RetryAt) {
			return true, nil
		}
		// Queue-time evidence is not authority to mutate a hostname later.
		// Recheck on every submission, including retries and coordinated rolls.
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return true, e
		}
		var op infra.Op
		var e error
		switch q.Action {
		case "import":
			tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				return true, err
			}
			op, e = m.ImportPlan.Submit(ctx, rc, m.Name, tok.Token)
		case "deploy":
			spec, err := p.runtimeSpec(rc, m, m.DesiredVersion)
			if err != nil {
				return true, err
			}
			if engine.DeploymentBlocked(rc, m.Name, spec) {
				engine.ApplyDeploymentStatus(rc, &rc.Cluster.Status)
				return true, nil
			}
			op, e = engine.Deploy(ctx, rc, m.Name, spec)
		case "stop":
			op, e = rc.Backend.Stop(ctx, m.Name)
		case "delete-runtime":
			op, e = rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		case "delete-volume":
			op, e = rc.Backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID)
		case "resources":
			op, e = rc.Backend.SetResources(ctx, m.Name, runtimeResources(m.Resources))
		case "volume-resources":
			op, e = rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: m.Resources.MinDisk, MaxDisk: m.Resources.MaxDisk})
		default:
			return true, fmt.Errorf("unknown pending action %s", q.Action)
		}
		if e != nil {
			var rejected *infra.ResourceUpdateRejectedError
			if (q.Action == "resources" || q.Action == "volume-resources") && errors.As(e, &rejected) {
				return true, p.rejectResources(ctx, rc, s, m, e)
			}
			if q.Action == "deploy" && errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
				spec, err := p.runtimeSpec(rc, m, m.DesiredVersion)
				if err != nil {
					return true, err
				}
				return true, engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, e)
			}
			q.RetryAt = q.Schedule(now(rc), now(rc).Add(30*time.Second))
			if at := infra.RetryAt(e); at.After(q.RetryAt) {
				q.RetryAt = q.Schedule(now(rc), at)
			}
			return true, e
		}
		q.Op = &op
		return true, nil
	}
	if q.Action == "import" {
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return true, e
		}
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return true, e
		}
		op, e := engine.ResumeImportPlan(ctx, rc, m.ImportPlan, m.Name, tok.Token)
		if e != nil {
			return true, e
		}
		if op != nil {
			q.Op = op
			return true, nil
		}
	}
	st, e := rc.Backend.OpStatus(ctx, *q.Op)
	if st.Status == "DeploymentRetriesExhausted" && e == nil {
		e = &infra.DeploymentExhaustedError{Service: m.Name, LastError: st.Message}
	}
	if q.Action == "deploy" && errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
		spec, err := p.runtimeSpec(rc, m, m.DesiredVersion)
		if err != nil {
			return true, err
		}
		// Release the terminal receipt, not the disruption owner. The same
		// action resumes after re-arm without advancing a rolling/rotation step.
		q.Op = nil
		q.RetryAt = time.Time{}
		return true, engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, e)
	}
	if e != nil {
		var rejected *infra.ResourceUpdateRejectedError
		if (q.Action == "resources" || q.Action == "volume-resources") && errors.As(e, &rejected) {
			return true, p.rejectResources(ctx, rc, s, m, e)
		}
		return true, e
	}
	if st.Failed {
		q.Op = nil
		q.RetryAt = q.Schedule(now(rc), now(rc).Add(30*time.Second))
		return true, fmt.Errorf("platform %s failed for %s", q.Action, m.Name)
	}
	if !st.Done {
		return true, nil
	}
	switch q.Action {
	case "import":
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return true, e
		}
		for _, spec := range m.ImportPlan.Specs {
			v, e := rc.Backend.GetService(ctx, spec.Hostname)
			if e != nil {
				return true, e
			}
			if v == nil {
				return true, nil
			}
			if engine.ImportedServiceID(*q.Op, st, v) != v.ID && !engine.HasOwnership(v, tok.Token, rc.Cluster.Name, m.Name) {
				return true, engine.UnverifiedOwnership(v)
			}
			if spec.Kind == infra.KindVolume {
				m.VolumeID = v.ID
			} else {
				m.ServiceID = v.ID
			}
		}
		m.Stage = "imported"
		m.ImportPlan = nil
	case "deploy":
		m.Stage = "deployed"
	case "stop":
		m.CleanupStep = "stopped"
	case "delete-runtime":
		m.ServiceID = ""
		m.Stage = "planned"
		m.CleanupStep = "runtime-deleted"
	case "delete-volume":
		m.VolumeID = ""
		m.CleanupStep = "volume-deleted"
	}
	s.Pending = nil
	return true, nil
}
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = time.Second
	s, e := load(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := advanceUnplacedRetirement(ctx, rc, s); busy || e != nil {
		return result, e
	}
	for _, m := range s.Members {
		if m.VolumeID != "" && m.ImportPlan == nil && !m.Removing {
			disk, e := engine.PreflightVolumeGrowth(ctx, rc.Backend, m.Volume, m.VolumeID, m.Resources, rc.Cluster.Spec.Components.Engine.Resources)
			if e != nil {
				return result, e
			}
			m.Resources.MinDisk, m.Resources.MaxDisk = disk.MinDisk, disk.MaxDisk
		}
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.advancePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.UID == "" {
		s.UID, e = secrets.Token()
		if e != nil {
			return result, e
		}
		s.Phase = "creating"
		return result, nil
	}
	if s.Suspended {
		for _, m := range s.Members {
			m.Stage = "planned"
			m.CleanupStep = ""
		}
		s.Suspended = false
		s.CleanupSecrets = false
		return result, nil
	}
	if len(s.Members) < rc.Cluster.Spec.Components.Engine.Replicas {
		for len(s.Members) < rc.Cluster.Spec.Components.Engine.Replicas {
			index := s.NextIndex
			s.NextIndex++
			name := NodeHostname(rc.Cluster.Name, index)
			v, e := rc.Backend.GetService(ctx, name)
			if e != nil {
				return result, e
			}
			if v != nil {
				continue
			}
			m := &member{Name: name, Volume: VolumeHostname(name), Index: index, Stage: "planned", Resources: rc.Cluster.Spec.Components.Engine.Resources}
			s.Members = append(s.Members, m)
			if !s.Bootstrapped && len(s.Voters) < min(3, rc.Cluster.Spec.Components.Engine.Replicas) {
				s.Voters = append(s.Voters, name)
			}
		}
		return result, nil
	}
	if e = p.ensureSecrets(ctx, rc, s); e != nil {
		return result, e
	}
	for _, m := range ordered(s) {
		// Retirement owns this identity, including any deliberate restart
		// needed to finish a late shard drain. Ordinary provisioning must not
		// recreate a runtime after its retirement deletion has completed.
		if m.Removing {
			continue
		}
		if m.ConfigVersion == 0 {
			cfg, e := p.desiredConfig(ctx, rc, s, m)
			if e != nil {
				return result, e
			}
			return result, activateConfig(ctx, rc, s, m, cfg)
		}
		if m.Stage == "planned" {
			return result, p.importMember(ctx, rc, s, m)
		}
		if m.Stage == "imported" {
			s.Pending = &pending{Action: "deploy", Member: m.Name}
			return result, nil
		}
	}
	if !rc.Cluster.Spec.Exposure.Public {
		if busy, e := p.reconcileExposure(ctx, rc, s); busy || e != nil {
			return result, e
		}
	}
	if s.Rolling == nil || s.Rolling.Step == "gate" || s.Rolling.Step == "activate" {
		if s.ScaleDown == nil || s.ScaleDown.Stage == "feasibility" {
			if busy, e := p.reconcileRuntime(ctx, rc, s); busy || e != nil {
				return result, e
			}
		}
	}
	if allowed, _ := rc.DisruptionAllowed(); !allowed && (s.Rolling == nil || s.Rolling.OperationID == "" && s.Rolling.Reason != "renew-tls" && (s.Rolling.Step == "gate" || s.Rolling.Step == "activate")) {
		if busy, e := p.reconcileExemptRenewal(ctx, rc, s); busy || e != nil {
			return result, e
		}
	}
	if s.Rolling == nil {
		if busy, e := p.advanceScaleDown(ctx, rc, s); busy || e != nil {
			return result, e
		}
	}
	if s.Rolling != nil {
		return result, p.rollingStep(ctx, rc, s)
	}
	for _, m := range ordered(s) {
		cfg, e := p.desiredConfig(ctx, rc, s, m)
		if e != nil {
			return result, e
		}
		if configHash(cfg) != m.ConfigHash || !reflect.DeepEqual(m.Resources, rc.Cluster.Spec.Components.Engine.Resources) {
			if e = p.startRolling(ctx, rc, s, "config", "", ""); e != nil {
				return result, e
			}
			return result, nil
		}
	}
	ready, e := p.clusterReady(ctx, rc, s)
	if e != nil || !ready {
		rc.Cluster.Status = engine.UnknownObservation(rc.Cluster.Status, now(rc), "NativeNotReady")
		if errors.Is(e, context.Canceled) {
			return result, e
		}
		return result, nil
	}
	if busy, e := p.reconcileExposure(ctx, rc, s); busy || e != nil {
		return result, e
	}
	for _, m := range s.Members {
		m.Stage = "ready"
		m.Version = m.DesiredVersion
		m.RuntimeRecovery.Initialized = true
	}
	s.Bootstrapped = true
	if busy, e := p.initialRestore(ctx, rc, s); busy || e != nil {
		return result, e
	}
	s.Phase = "ready"
	rc.Cluster.Status.Phase = domain.PhaseReady
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}
