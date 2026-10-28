// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"reflect"
	"time"
)

func action(ctx context.Context, rc *engine.Context, verb string, req, resp any) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if s.Member == nil || rc.Agents == nil {
		return engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), s.Member.Name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Engine(ctx, neo4jspec.Engine, verb, req, resp)
}
func readiness(ctx context.Context, rc *engine.Context) (neo4jspec.Readiness, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var r neo4jspec.Readiness
	err := action(ctx, rc, neo4jspec.ActionReadiness, struct{}{}, &r)
	return r, err
}
func reload(ctx context.Context, rc *engine.Context) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if s.Member == nil || rc.Agents == nil {
		return engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), s.Member.Name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Reload(ctx)
}
func verify(ctx context.Context, rc *engine.Context, m *member) error {
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	if tok == nil {
		return engine.ErrNotFound
	}
	for _, entry := range []struct {
		name string
		id   *string
	}{{m.Name, &m.ServiceID}, {m.Volume, &m.VolumeID}} {
		service, e := rc.Backend.GetService(ctx, entry.name)
		if e != nil {
			return e
		}
		if service == nil {
			continue
		}
		if *entry.id != service.ID {
			if !engine.HasOwnership(service, tok.Token, rc.Cluster.Name, m.Name) {
				return engine.UnverifiedOwnership(service)
			}
		}
		*entry.id = service.ID
		if e = engine.EnsureOwnerMarker(ctx, rc.Backend, service, tok.Token, rc.Cluster.Name, m.Name); e != nil {
			return e
		}
	}
	return nil
}
func observePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	m := s.Member
	if m == nil || m.Pending == nil {
		return false, nil
	}
	if m.PendingAction == "import" {
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return true, e
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		op, e := engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, tok.Token)
		if e != nil {
			return true, e
		}
		if op != nil {
			m.Pending = op
			return true, nil
		}
	}
	st, err := rc.Backend.OpStatus(ctx, *m.Pending)
	var resourceRejected *infra.ResourceUpdateRejectedError
	if errors.As(err, &resourceRejected) {
		// Admission did not apply the pending bounds. Do not hold newer
		// intent behind a durable terminal rejection receipt.
		m.Pending = nil
		m.PendingAction = ""
		m.PendingResources = domain.Resources{}
		return true, err
	}
	if err != nil {
		return true, err
	}
	if st.Failed {
		m.Pending = nil
		return true, fmt.Errorf("Neo4j infrastructure %s failed", m.PendingAction)
	}
	if !st.Done {
		return true, nil
	}
	switch m.PendingAction {
	case "import":
		if err = verify(ctx, rc, m); err != nil {
			return true, err
		}
		m.ImportBatch = nil
		m.Stage = "imported"
	case "deploy":
		m.Stage = "deployed"
		m.Version = m.PendingVersion
	case "resize":
		m.Resources = m.PendingResources
	case "resize-volume":
		m.Disk = m.PendingResources
	case "delete-runtime":
		m.ServiceID = ""
		m.Stage = "planned"
	case "delete-volume":
		m.VolumeID = ""
	}
	m.Pending = nil
	m.PendingAction = ""
	return true, nil
}
func track(m *member, verb string, op infra.Op) { m.Pending = &op; m.PendingAction = verb }
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
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
	if s.RestoreRecovery != nil {
		_, recoveryErr := p.RunOperation(ctx, rc, s.RestoreRecovery)
		// RunOperation owns and persists the maintenance checkpoint.
		current, loadErr := load(rc.Cluster)
		if loadErr != nil {
			return result, loadErr
		}
		*s = *current
		return result, recoveryErr
	}
	if s.UID == "" {
		s.UID, err = secrets.Token()
		if err != nil {
			return result, err
		}
	}
	if s.Member == nil {
		s.Member = &member{Name: rc.Cluster.Name + "n0", Volume: rc.Cluster.Name + "n0v", Stage: "planned", Resources: rc.Cluster.Spec.Components.Engine.Resources, Disk: rc.Cluster.Spec.Components.Engine.Resources}
	}
	m := s.Member
	// Runtime resource changes may restart the member. Refuse unsupported
	// volume growth before submitting those changes or publishing new config.
	if m.VolumeID != "" && m.ImportBatch == nil {
		disk, e := engine.PreflightVolumeGrowth(ctx, rc.Backend, m.Volume, m.VolumeID, m.Disk, rc.Cluster.Spec.Components.Engine.Resources)
		if e != nil {
			return result, e
		}
		m.Disk.MinDisk, m.Disk.MaxDisk = disk.MinDisk, disk.MaxDisk
	}
	s.Suspended = false
	exposureDeferred, err := restrictExposure(ctx, rc, m)
	if err != nil {
		return result, err
	}
	if err = ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if busy, e := observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.Maintenance != "" || m.Offline {
		return result, nil
	}
	tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return result, e
	}
	if tok == nil {
		return result, engine.ErrNotFound
	}
	if m.RuntimeRecovery.Initialized || m.Stage == "deployed" || m.Stage == "ready" {
		out, e := engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
		if e != nil {
			return result, e
		}
		if out.VolumeLost {
			return result, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, false)
		}
		if out.Imported {
			m.Stage = "imported"
			m.Public = false
			if rc.Store != nil {
				if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
					return result, e
				}
			}
		}
		if out.Busy {
			return result, nil
		}
	}
	if m.Stage == "planned" {
		if m.ImportBatch == nil {
			var specs []infra.ServiceSpec
			for _, spec := range []infra.ServiceSpec{engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, m.Disk), engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1})} {
				svc, e := rc.Backend.GetService(ctx, spec.Hostname)
				if e != nil {
					return result, e
				}
				if svc == nil {
					specs = append(specs, spec)
				}
			}
			if len(specs) == 0 {
				if e = verify(ctx, rc, m); e != nil {
					return result, e
				}
				m.Stage = "imported"
				return result, nil
			}
			m.ImportBatch = engine.NewImportPlan(specs)
			return result, nil
		}
		op, e := m.ImportBatch.Submit(ctx, rc, m.Name, tok.Token)
		if e != nil {
			return result, e
		}
		track(m, "import", op)
		return result, nil
	}
	if err = verify(ctx, rc, m); err != nil {
		return result, err
	}
	// Recovery deploys the accepted runtime before considering any new desired
	// disruption. A closed RAM/version window must not strand an imported member.
	if m.Stage == "imported" && m.RuntimeRecovery.Initialized && m.Version != "" {
		return result, p.deployMember(ctx, rc, s, m.Version)
	}
	r := rc.Cluster.Spec.Components.Engine.Resources
	options, e := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if e != nil {
		return result, e
	}
	if !reflect.DeepEqual(m.Resources, r) {
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return wait, nil
		}
		op, e := rc.Backend.SetResources(ctx, m.Name, r)
		if e != nil {
			return result, e
		}
		s.ConfigOptions = &options
		track(m, "resize", op)
		m.PendingResources = r
		return result, nil
	}
	if m.Disk.MinDisk != r.MinDisk || m.Disk.MaxDisk != r.MaxDisk {
		if e = engine.RequireDiskGrowth(rc.Backend); e != nil {
			return result, e
		}
		op, e := rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: r.MinDisk, MaxDisk: r.MaxDisk})
		if e != nil {
			return result, e
		}
		track(m, "resize-volume", op)
		m.PendingResources = r
		return result, nil
	}
	if m.Version != "" && m.Version != rc.Cluster.Spec.Version {
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return wait, nil
		}
		m.PreviousVersion = m.Version
		m.Stage = "imported"
	}
	hash := digest(struct {
		Version   string
		Resources domain.Resources
		Options   any
	}{rc.Cluster.Spec.Version, r, rc.Cluster.Spec.EngineOptions})
	if s.ConfigHash != hash {
		needsWindow := s.ConfigOptions == nil
		if s.ConfigOptions != nil {
			previous := *s.ConfigOptions
			previous.AcceptBackupDowntime = options.AcceptBackupDowntime
			needsWindow = !reflect.DeepEqual(previous, options)
		}
		if needsWindow && s.ConfigHash != "" && m.Stage != "imported" {
			if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
				return wait, nil
			}
		}
		s.ConfigOptions = &options
		s.ConfigHash = hash
		s.ConfigVersion++
		if err = save(rc.Cluster, s); err != nil {
			return result, err
		}
	}
	if m.Stage == "imported" {
		return result, p.deployMember(ctx, rc, s, rc.Cluster.Spec.Version)
	}
	if err = save(rc.Cluster, s); err != nil {
		return result, err
	}
	ready, e := readiness(ctx, rc)
	if e != nil {
		return result, nil
	}
	cfg, e := p.MemberConfig(ctx, rc, m.Name)
	if e != nil {
		return result, e
	}
	var sp neo4jspec.Spec
	if e = json.Unmarshal(cfg.EngineSpec, &sp); e != nil {
		return result, e
	}
	if !ready.Ready || ready.CredentialMismatch || ready.ConfigurationError != "" || ready.Version != rc.Cluster.Spec.Version || ready.ConfigVersion < s.ConfigVersion || ready.AppliedRestartToken != sp.RestartToken {
		if e = reload(ctx, rc); e != nil {
			return result, e
		}
		return result, nil
	}
	m.Stage = "ready"
	m.RuntimeRecovery.Initialized = true
	if err = p.queueClone(ctx, rc, s); err != nil {
		return result, err
	}
	if rc.Cluster.Spec.DataSource != nil && !s.CloneComplete {
		return result, nil
	}
	if exposureDeferred {
		return rc.WaitForWindow(), nil
	}
	if rc.Cluster.Spec.Exposure.Public {
		if e = engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure); e != nil {
			return result, e
		}
		x := rc.Cluster.Spec.Exposure
		port := infra.PublicPort{Port: 7687, TargetPort: 7687, Protocol: "tcp", IPType: x.IPType, Allowlist: x.Allowlist}
		if wait, e := engine.PublicTCPDeferred(ctx, rc, m.Name, &port); e != nil {
			return result, e
		} else if wait {
			return rc.WaitForWindow(), nil
		}
		if e = rc.Backend.EnsurePublicPort(ctx, m.Name, port); e != nil {
			return result, e
		}
		m.Public = true
		m.Exposure = x
	}
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 10 * time.Second
	return result, nil
}

// Restrictions use observed routes, not only the last saved publication flag:
// a crash after publishing must not leave an unrecorded endpoint unrestricted.
func restrictExposure(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return false, err
	}
	e := rc.Cluster.Spec.Exposure
	for _, route := range routes {
		if route.Hostname != m.Name {
			continue
		}
		if e.Public && route.Port == 7687 && route.TargetPort == 7687 && route.Protocol == "tcp" && route.IPType == e.IPType && reflect.DeepEqual(route.Allowlist, e.Allowlist) {
			continue
		}
		if wait, e := engine.PublicTCPDeferred(ctx, rc, m.Name, nil); e != nil {
			return false, e
		} else if wait {
			return true, nil
		}
		if err = verify(ctx, rc, m); err != nil {
			return false, err
		}
		if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
			return false, err
		}
		m.Public = false
		return false, nil
	}
	return false, nil
}

func (p *Provider) deployMember(ctx context.Context, rc *engine.Context, s *state, version string) error {
	accepted := *rc.Cluster
	accepted.Spec.Version = version
	view := *rc
	view.Cluster = &accepted
	runtime, err := p.runtimeSpec(&view, s)
	if err != nil {
		return err
	}
	op, err := engine.Deploy(ctx, rc, s.Member.Name, runtime)
	if err != nil {
		return err
	}
	track(s.Member, "deploy", op)
	s.Member.PendingVersion = version
	return nil
}
