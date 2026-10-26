// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// desiredConfigHash binds runtime acknowledgements to the current access revision.
func desiredConfigHash(rc *engine.Context, revision string) (string, error) {
	// Optional applications own their bounds independently of the database.
	// Keep the proxy's bounds in this revision, but never reload PostgreSQL
	// or ProxySQL just because a composed application's resources changed.
	coreSpec := rc.Cluster.Spec
	coreSpec.Components.Extra = nil
	raw, err := json.Marshal(struct {
		Spec               domain.ClusterSpec
		Proxy              domain.ComponentSpec
		AccessRevision     string
		ProxyBudgetVersion int
	}{coreSpec, rc.Cluster.Spec.Components.Extra["proxy"], revision, 1})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func desiredComponent(rc *engine.Context, m *member) domain.ComponentSpec {
	if m.Role == "proxy" {
		return rc.Cluster.Spec.Components.Extra["proxy"]
	}
	c := rc.Cluster.Spec.Components.Engine
	c.Replicas = 1
	return c
}
func verifyMember(ctx context.Context, rc *engine.Context, m *member) error {
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil && !missing(err) {
		return err
	}
	tokenValue := ""
	if token != nil {
		tokenValue = token.Token
	}
	services := []struct {
		name string
		id   *string
	}{{m.Name, &m.ServiceID}}
	if m.Volume != "" {
		services = append(services, struct {
			name string
			id   *string
		}{m.Volume, &m.VolumeID})
	}
	observed := []*infra.Service{}
	for _, item := range services {
		service, err := rc.Backend.GetService(ctx, item.name)
		if err != nil {
			if !engine.ConfirmedVolumeLoss(err, m.Volume, m.VolumeID) {
				return err
			}
			m.RuntimeRecovery.VolumeLost = true
		}
		if service == nil {
			continue
		}
		if *item.id == "" || *item.id != service.ID {
			if !engine.HasOwnership(service, tokenValue, rc.Cluster.Name, m.Name) {
				return engine.UnverifiedOwnership(service)
			}
		}
		*item.id = service.ID
		observed = append(observed, service)
	}
	if m.RuntimeRecovery.VolumeLost {
		// Preserve identity and still check returned services above. A missing
		// physical volume cannot safely accept owner-marker backfill.
		return nil
	}
	for _, service := range observed {
		if err := engine.EnsureOwnerMarker(ctx, rc.Backend, service, tokenValue, rc.Cluster.Name, m.Name); err != nil {
			return err
		}
	}
	return nil
}
func (p *Core) observePending(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	// A stateless scale operation must not make an obsolete larger count a
	// prerequisite for a newer, smaller count. Nomad may be waiting for the
	// very host-port/canary slot that the reduction releases. Keep the already
	// published surge budget until the superseding operation completes; an
	// increase still takes the normal all-instance configuration barrier below.
	if m := s.find(q.Member); q.Action == "scale" && m != nil && m.Role == "proxy" {
		replicas := desiredComponent(rc, m).Replicas
		if replicas > 0 && replicas < q.Replicas {
			op, err := rc.Backend.SetContainerCount(ctx, m.Name, replicas, replicas)
			if err != nil {
				return true, err
			}
			q.Op, q.Replicas = op, replicas
			return true, nil
		}
	}
	if q.Action == "import" {
		m := s.find(q.Member)
		if m == nil {
			return true, engine.ErrNotFound
		}
		op, err := resumePlannedImport(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if op != nil {
			q.Op = *op
			return true, nil
		}
	}
	st, err := rc.Backend.OpStatus(ctx, q.Op)
	if err != nil {
		return true, err
	}
	if rc.Cluster.Deleting {
		rc.Cluster.Status.Message = fmt.Sprintf("Waiting for PostgreSQL member %s %s process %s: %s", q.Member, q.Action, q.Op.ID, st.Status)
		if st.Message != "" {
			rc.Cluster.Status.Message += "; " + st.Message
		}
	}
	if !st.Done && !st.Failed {
		return true, nil
	}
	if st.Failed {
		m := s.find(q.Member)
		if q.Action == "resize" && m != nil && !reflect.DeepEqual(q.Resources, desiredComponent(rc, m).Resources) {
			// The failed target no longer describes operator intent. Re-plan
			// against the observed runtime instead of retrying obsolete bounds.
			service, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return true, err
			}
			if service != nil {
				m.Resources = service.Resources
			}
			s.Pending = nil
			return false, nil
		}
		if q.Action == "delete-volume" && m != nil {
			cause := fmt.Errorf("infrastructure volume deletion failed")
			if err := orphanVolume(ctx, rc, m, cause); err != nil {
				return true, err
			}
			m.VolumeID = ""
			m.Volume = ""
			s.Pending = nil
			return true, nil
		}
		s.Pending = nil
		return true, fmt.Errorf("infrastructure %s failed for %s", q.Action, q.Member)
	}
	m := s.find(q.Member)
	if m == nil {
		return true, engine.ErrNotFound
	}
	switch q.Action {
	case "import":
		for _, entry := range []struct {
			name string
			id   *string
		}{{m.Name, &m.ServiceID}, {m.Volume, &m.VolumeID}} {
			if entry.name == "" {
				continue
			}
			service, e := rc.Backend.GetService(ctx, entry.name)
			if e != nil {
				return true, e
			}
			if service != nil && service.Importing {
				return true, engine.ErrUnavailable
			}
			if id := engine.ImportedServiceID(q.Op, st, service); id != "" {
				*entry.id = id
			}
		}
		if err = verifyMember(ctx, rc, m); err != nil {
			return true, err
		}
		m.Stage = "imported"
		m.ImportBatch = nil
		if rc.Store != nil {
			if e := rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
				return true, e
			}
		}
	case "resize":
		m.Resources = q.Resources
	case "resize-volume":
		m.Disk = q.Resources
	case "scale":
		m.Replicas = q.Replicas
	case "delete-runtime":
		m.ServiceID = ""
		m.Stage = "planned"
		m.DeployOp = nil
	case "delete-volume":
		m.VolumeID = ""
	}
	s.Pending = nil
	return true, nil
}
func (p *Core) track(s *state, m *member, action string, op infra.Op) {
	c := pending{Op: op, Action: action, Member: m.Name, Resources: m.Resources, Replicas: m.Replicas}
	s.Pending = &c
}
func live(ctx context.Context, rc *engine.Context, name string) (pgspec.StatusResponse, error) {
	// Role discovery is a read, not a long-running engine action. In
	// particular, do not inherit the engine client's mutation timeout.
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var out pgspec.StatusResponse
	if rc.Agents == nil {
		return out, engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return out, err
	}
	err = a.Engine(ctx, "postgres", "status", struct{}{}, &out)
	return out, err
}
func ready(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if m.Role == "proxy" {
		return proxyApplied(ctx, rc, m, m.ConfigVersion, true)
	}
	st, err := live(ctx, rc, m.Name)
	if err != nil {
		rc.SyncWait = fmt.Sprintf("waiting for member %s native status read", m.Name)
		return false, nil
	}
	if !st.Running {
		rc.SyncWait = fmt.Sprintf("waiting for member %s native process to run", m.Name)
		return false, nil
	}
	if m.Role == "data" && m.Topology == "ha" && st.Role != "primary" && (st.Role != "replica" || !st.Streaming) {
		rc.SyncWait = fmt.Sprintf("waiting for member %s primary or streaming replica role", m.Name)
		return false, nil
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		rc.SyncWait = fmt.Sprintf("waiting for member %s agent connection", m.Name)
		return false, nil
	}
	// Keep the generic observation separate from native role discovery and
	// subsequent reload/restart mutations, which retain the parent context.
	agent, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
	if errors.Is(err, context.DeadlineExceeded) {
		return false, err
	}
	if err != nil {
		rc.SyncWait = fmt.Sprintf("waiting for member %s agent status read", m.Name)
		return false, nil
	}
	diagnostic := domain.MemberStatus{Name: m.Name, Health: true}
	if agent.Error != "" || len(engine.ApplyProcessDiagnostics(&diagnostic, agent.Processes, "", now(rc))) > 0 {
		rc.SyncWait = fmt.Sprintf("waiting for member %s healthy supervised processes", m.Name)
		return false, nil
	}
	if agent.ConfigVersion < m.ConfigVersion {
		rc.SyncWait = fmt.Sprintf("waiting for member %s configuration %d (observed %d)", m.Name, m.ConfigVersion, agent.ConfigVersion)
		// A failed reload request must remain retryable after its publication
		// checkpoint; the agent's applied version is the acknowledgement.
		if err := a.Reload(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if st.PendingRestart && m.Role == "data" && m.Topology == "single" {
		rc.SyncWait = fmt.Sprintf("waiting for member %s pending PostgreSQL restart", m.Name)
		return false, a.Process(ctx, agentapi.ProcessAction{Process: "postgres", Action: "restart", RequestID: fmt.Sprintf("config/%s/%d/postgres", m.Name, m.ConfigVersion)})
	}
	if st.PendingRestart && m.Role == "data" && m.Topology == "ha" {
		rc.SyncWait = fmt.Sprintf("waiting for member %s pending PostgreSQL restart or switchover", m.Name)
		if st.Role == "primary" {
			s, err := load(rc.Cluster)
			if err != nil {
				return false, err
			}
			return false, switchAway(ctx, rc, s, m.Name)
		}
		return false, a.Engine(ctx, "postgres", "restart", struct{}{}, nil)
	}
	// Learners reject the local member-list RPC, so their status can omit the
	// ID already learned from the voting member that added them.
	if st.EtcdMemberID != "" {
		m.EtcdID = st.EtcdMemberID
	}
	return true, nil
}

// Sync advances durable infrastructure work and rolls one ready member at a time.
func (p *Core) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 2 * time.Second
	rc.SyncWait = "loading PostgreSQL convergence state"
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if rc.Cluster.Spec.DataSource != nil && s.RestoreRecovery != nil {
		rc.SyncWait = "draining initial restore recovery"
		// A retired initial restore still owns its exact receipt namespace.
		// Drain that journal before ordinary deployment/health convergence.
		_, err = p.reconcileRestoreRecovery(ctx, rc, s)
		return result, err
	}
	s.SwitchoverBlocked = ""
	rc.SyncWait = "retiring unplaced scale-down members"
	if busy, e := p.retireUnplacedScaleDown(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.UID == "" {
		s.UID, err = secrets.RandomString(16)
		if err != nil {
			return result, err
		}
		s.Topology = rc.Cluster.Spec.Topology.Type
	}
	// Revocation is infrastructure-only and must not wait on member health,
	// an in-flight rolling operation, or configuration convergence.
	if err = p.restrictExposure(ctx, rc, s); err != nil {
		return result, err
	}
	if s.Topology != rc.Cluster.Spec.Topology.Type {
		rc.SyncWait = "converting PostgreSQL topology"
		if s.RestoreRecovery != nil {
			return result, engine.Invalid("restore recovery owns the cluster; topology changes must wait")
		}
		return p.convertTopology(ctx, rc, s, result)
	}
	// A durable shared-memory journal owns its current membership. Allocate
	// scale-up identities only after it finishes, never before agent probes
	// that would otherwise preempt provisioning those identities.
	if s.Rolling == nil && s.RestoreRecovery == nil {
		s.allocate(rc.Cluster)
	}
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if e := save(rc.Cluster, s); e != nil {
		return result, e
	}
	// A pending stateless proxy deployment must not hold the initial DATA
	// recovery hostage: native bootstrap has already claimed the source, so
	// finishing it is what reopens safe placement correction admission.
	if rc.Cluster.Spec.DataSource != nil && s.Pending != nil {
		if member := s.find(s.Pending.Member); member != nil && member.Role == "proxy" {
			if _, e := p.syncBackups(ctx, rc, s); e != nil {
				return result, e
			}
		}
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		rc.SyncWait = "observing pending infrastructure operation"
		return result, e
	}
	if s.Suspended {
		rc.SyncWait = "resetting suspended member deployment stages"
		for _, m := range s.Members {
			m.Stage = "planned"
			m.DeployOp = nil
			m.ServiceID = ""
		}
		s.Suspended = false
		s.BackupCleanupStarted = nil
		s.CleanedBackupRepositories = nil
		s.RetainedBackupStorages = nil
		return result, nil
	}
	for _, m := range s.Members {
		if err = verifyMember(ctx, rc, m); err != nil {
			return result, err
		}
	}
	if handled, e := p.reconcileRecoveryResources(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling recovery resources"
		return result, e
	}
	if handled, e := p.reconcileRestoreRecovery(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling restore recovery"
		return result, e
	}
	if busy, e := p.recoverRuntimes(ctx, rc, s); busy || e != nil {
		rc.SyncWait = "recovering member runtimes"
		return result, e
	}
	if handled, e := p.recoverNativeCertificates(ctx, rc, s); handled || e != nil {
		if rc.SyncWait == "" {
			rc.SyncWait = "recovering native certificates"
		}
		return result, e
	}
	if handled, e := p.recoverProxyClock(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "recovering proxy monitor clock state"
		return result, e
	}
	if handled, e := p.reconcileAccessIntent(ctx, rc); handled || e != nil {
		rc.SyncWait = "reconciling managed access journal"
		return result, e
	}
	if s.Replacement != nil && s.Replacement.Recovery != "" {
		_, err = p.advanceReplacement(ctx, rc, s)
		rc.SyncWait = "advancing replacement recovery"
		return result, err
	}
	if handled, e := p.reconcileUpgrade(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling PostgreSQL upgrade"
		return result, e
	}
	if handled, e := p.syncBackups(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling backup configuration"
		return result, e
	}
	if handled, e := p.reconcileSharedMemory(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling shared-memory rollout"
		return result, e
	}
	revision, e := p.accessRevision(ctx, rc)
	if e != nil {
		return result, e
	}
	hash, e := desiredConfigHash(rc, revision)
	if e != nil {
		return result, e
	}
	if s.ConfigHash != hash {
		s.ConfigHash = hash
		s.ConfigVersion++
	}
	if s.Replacement == nil && len(s.data()) > rc.Cluster.Spec.Components.Engine.Replicas {
		rc.SyncWait = "scaling down data members"
		return p.scaleDown(ctx, rc, s, result)
	}
	for _, m := range s.data() {
		if m.EtcdState == "pending" {
			rc.SyncWait = fmt.Sprintf("adding etcd learner %s", m.Name)
			return result, p.addLearner(ctx, rc, s, m)
		}
		if m.EtcdLearner && m.Stage == "joined" && s.Replacement == nil {
			rc.SyncWait = fmt.Sprintf("promoting etcd learner %s", m.Name)
			return result, p.promoteLearner(ctx, rc, s, m)
		}
	}
	// Provision all identities before waiting on deployments: etcd needs a quorum.
	for _, m := range s.Members {
		desired := desiredComponent(rc, m)
		if desired.Replicas == 0 && m.ImportBatch == nil {
			continue
		}
		if m.Stage == "planned" {
			rc.SyncWait = fmt.Sprintf("planning or importing runtime %s", m.Name)
			tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return result, e
			}
			if tok == nil {
				return result, engine.ErrNotFound
			}
			if m.ImportBatch == nil {
				var specs []infra.ServiceSpec
				if m.Volume != "" {
					v, e := rc.Backend.GetService(ctx, m.Volume)
					if e != nil {
						return result, e
					}
					if v != nil && v.Importing {
						return result, engine.Invalid("incomplete import for %s has no frozen batch; recover the original import intent before continuing", m.Name)
					}
					if v == nil {
						specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, desired.Resources))
					}
				}
				runtime, e := rc.Backend.GetService(ctx, m.Name)
				if e != nil {
					return result, e
				}
				if runtime != nil && runtime.Importing {
					return result, engine.Invalid("incomplete import for %s has no frozen batch; recover the original import intent before continuing", m.Name)
				}
				if runtime == nil {
					specs = append(specs, engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: desired.Resources, Containers: desired.Replicas}))
				}
				m.Resources = desired.Resources
				if m.Volume != "" {
					m.Disk = domain.Resources{MinDisk: desired.Resources.MinDisk, MaxDisk: desired.Resources.MaxDisk}
				}
				m.Replicas = desired.Replicas
				m.Version = rc.Cluster.Spec.Version
				m.Options = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
				m.Topology = s.Topology
				m.ConfigVersion = s.ConfigVersion
				if len(specs) == 0 {
					m.Stage = "imported"
				} else {
					m.ImportBatch = engine.NewImportPlan(specs)
				}
				// End this pass so the outer reconciler persists the exact
				// batch and inputs before the first infrastructure submission.
				return result, nil
			}
			if len(m.ImportBatch.Specs) == 0 {
				return result, engine.Invalid("invalid frozen import batch for %s", m.Name)
			}
			op, e := m.ImportBatch.Submit(ctx, rc, m.Name, tok.Token)
			if e != nil {
				return result, e
			}
			p.track(s, m, "import", op)
			return result, nil
		}
	}
	for _, m := range s.Members {
		if desiredComponent(rc, m).Replicas == 0 {
			continue
		}
		if m.Stage == "imported" {
			rc.SyncWait = fmt.Sprintf("deploying imported runtime %s", m.Name)
			spec, e := p.runtimeSpec(rc, m)
			if e != nil {
				return result, e
			}
			if engine.DeploymentBlocked(rc, m.Name, spec) {
				continue
			}
			desired := desiredComponent(rc, m).Resources
			if op, changed, e := engine.PrepareInitialDeploymentResources(ctx, rc, m.Name, spec, desired, m.Resources); changed || e != nil {
				if e == nil {
					p.track(s, m, "resize", op)
					s.Pending.Resources = desired
				}
				return result, e
			}
			op, e := engine.Deploy(ctx, rc, m.Name, spec)
			if e != nil {
				return result, e
			}
			m.DeployOp = &op
			m.ProxyReady = false
			m.Stage = "deploying"
			return result, nil
		}
	}
	deploymentsBusy := false
	for _, m := range s.Members {
		if desiredComponent(rc, m).Replicas == 0 {
			continue
		}
		if m.Stage == "deploying" {
			rc.SyncWait = fmt.Sprintf("observing runtime deployment %s", m.Name)
			var st infra.OpState
			var e error
			if !m.RuntimeRecovery.Initialized {
				var changed bool
				changed, e = engine.ResizeUnplacedDeployment(ctx, rc, m.Name, desiredComponent(rc, m).Resources, &m.Resources, m.DeployOp)
				if changed {
					return result, nil
				}
			}
			if e == nil {
				st, e = rc.Backend.OpStatus(ctx, *m.DeployOp)
			}
			if st.Status == "DeploymentRetriesExhausted" && e == nil {
				e = &infra.DeploymentExhaustedError{Service: m.Name, LastError: st.Message}
			}
			if errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
				engine.ApplyDeploymentResourceRollback(e, &m.Resources)
				spec, specErr := p.runtimeSpec(rc, m)
				if specErr != nil {
					return result, specErr
				}
				m.Stage, m.DeployOp = "imported", nil
				if e = engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, e); e != nil {
					err = errors.Join(err, e)
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
			if st.Failed {
				m.Stage = "imported"
				m.DeployOp = nil
				return result, fmt.Errorf("deployment failed for %s", m.Name)
			}
			m.Stage = "deployed"
			m.DeployOp = nil
		}
	}
	// Publish ready DATA before testing proxy writer health. Allocation order
	// can retain a proxy between data members after single-to-HA conversion;
	// waiting on it first can hide the healthy primary it needs published.
	for _, role := range [...]string{"data", "proxy"} {
		for _, m := range s.Members {
			if m.Role != role {
				continue
			}
			// DATA readiness must advance while a stateless proxy is still waiting
			// for placement. Initial recovery queues only after DATA has joined;
			// a global deployment barrier here would deadlock its mutation fence.
			if deploymentsBusy && m.Role == "proxy" {
				continue
			}
			if desiredComponent(rc, m).Replicas == 0 {
				continue
			}
			if m.Stage == "deployed" {
				rc.SyncWait = fmt.Sprintf("waiting for deployed member %s readiness", m.Name)
				ok, e := ready(ctx, rc, m)
				if e != nil {
					return result, e
				}
				if !ok {
					return result, nil
				}
				// Runtime-only recovery can bypass the ordinary import journal.
				// Retained inventory is released only after the member rejoins.
				if m.Volume != "" && rc.Store != nil {
					if e := rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
						return result, e
					}
				}
				m.Stage = "joined"
				m.NativeReadiness = nil
				if m.Role == "data" && !m.ProxyReady {
					m.ProxyReady = true
					// Joining is the publication barrier for proxy backends.
					// Publish a new version even during replacement operations,
					// which may not reach the ordinary configuration rollout.
					s.ConfigVersion++
					for _, proxy := range s.Members {
						if proxy.Role == "proxy" {
							proxy.ConfigVersion = s.ConfigVersion
						}
					}
				}
				rc.SyncWait = fmt.Sprintf("persisting joined member %s", m.Name)
				return result, nil
			}
		}
	}
	if deploymentsBusy {
		rc.SyncWait = "waiting for infrastructure deployments to complete"
		return result, nil
	}
	// Disable proxies only after the direct data endpoint is ready.
	for i, m := range s.Members {
		if m.Role == "proxy" && desiredComponent(rc, m).Replicas == 0 {
			rc.SyncWait = fmt.Sprintf("removing disabled proxy %s", m.Name)
			if m.ServiceID != "" {
				if e := rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
					return result, e
				}
				op, e := rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
				if e != nil {
					return result, e
				}
				p.track(s, m, "delete-runtime", op)
				return result, nil
			}
			s.Members = append(s.Members[:i], s.Members[i+1:]...)
			delete(rc.Cluster.Status.DeploymentFailures, m.Name)
			return result, nil
		}
	}
	if s.Replacement == nil && len(s.data()) > rc.Cluster.Spec.Components.Engine.Replicas {
		rc.SyncWait = "scaling down excess data members"
		return p.scaleDown(ctx, rc, s, result)
	}
	// Replicas precede the writer; observe leadership afresh before each disruption.
	order := append([]*member(nil), s.Members...)
	roles := map[string]string{}
	observed := observeLiveMembers(ctx, rc, order)
	for i, m := range order {
		st, e := observed[i].status, observed[i].err
		if m.Role == "proxy" && m.Public {
			if repairer, ok := rc.Backend.(interface {
				RepairPublishedEndpoint(context.Context, string, bool) (bool, error)
			}); ok {
				if e == nil && st.Running {
					if _, err := repairer.RepairPublishedEndpoint(ctx, m.Name, true); err != nil {
						return result, err
					}
				} else {
					probeCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
					sample := observeProxyStatus(probeCtx, rc, m)
					cancel()
					healthy := sample.err == nil && sample.proxyCount == max(1, m.Replicas) && len(sample.proxyInstances) == sample.proxyCount
					for _, instance := range sample.proxyInstances {
						healthy = healthy && instance.err == nil && instance.agentHealthy && instance.status.Running
					}
					if healthy && sample.proxyEndpointErr != nil {
						rc.SyncWait = fmt.Sprintf("waiting for published proxy %s logical endpoint repair", m.Name)
						if _, err := repairer.RepairPublishedEndpoint(ctx, m.Name, false); err != nil {
							return result, err
						}
						// A completed platform update is not a recovery receipt.
						// Keep generation/readiness fenced until the next logical probe succeeds.
						return result, nil
					}
				}
			}
		}
		if e != nil || !st.Running {
			rc.SyncWait = nativeObservationWait(m.Name, e)
			service, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return result, err
			}
			desired := desiredComponent(rc, m)
			// A failed resize can leave the agent unable to start. Only an
			// authoritative infrastructure failure permits correcting bounds
			// without live role discovery; an unreachable running member must
			// still follow the normal primary/switchover safety checks.
			if service != nil && service.Status == infra.StatusFailed && !reflect.DeepEqual(service.Resources, desired.Resources) {
				op, err := rc.Backend.SetResources(ctx, m.Name, desired.Resources)
				if err != nil {
					return result, err
				}
				p.track(s, m, "resize", op)
				s.Pending.Resources = desired.Resources
				return result, nil
			}
			if m.Role == "proxy" && service != nil && service.Status == infra.StatusFailed {
				return result, p.recoverFailedRuntime(ctx, rc, s, m)
			}
			return result, nil
		}
		if m.Role == "data" {
			roles[m.Name] = st.Role
		}
	}
	// Publish reduced proxy budgets before scaling proxies or restarting a
	// smaller PostgreSQL writer. Data replicas still precede the writer.
	rank := func(m *member) int {
		if m.Role == "proxy" {
			return 0
		}
		if roles[m.Name] == "primary" || roles[m.Name] == "standalone" {
			return 2
		}
		return 1
	}
	sort.SliceStable(order, func(i, j int) bool { return rank(order[i]) < rank(order[j]) })
	for _, m := range order {
		if _, failed := rc.Cluster.Status.DeploymentFailures[m.Name]; failed {
			if spec, specErr := p.runtimeSpec(rc, m); specErr == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
				continue
			}
		}
		desired := desiredComponent(rc, m)
		if m.Role == "proxy" {
			runtime, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return result, err
			}
			if runtime != nil && runtime.Status == infra.StatusFailed &&
				m.ConfigVersion == s.ConfigVersion && m.Version == rc.Cluster.Spec.Version && reflect.DeepEqual(m.Resources, desired.Resources) &&
				(m.Replicas == desired.Replicas || runtime.MaxContainers == desired.Replicas) {
				rc.SyncWait = fmt.Sprintf("recovering failed proxy %s", m.Name)
				return result, p.recoverFailedRuntime(ctx, rc, s, m)
			}
			// Publish the surge-safe pool before infrastructure can start a canary.
			if m.ConfigVersion != s.ConfigVersion {
				rc.SyncWait = fmt.Sprintf("publishing proxy %s configuration %d", m.Name, s.ConfigVersion)
				m.Options = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
				m.Version = rc.Cluster.Spec.Version
				m.ConfigVersion = s.ConfigVersion
				m.Stage = "deployed"
				if e := activateCertificates(ctx, rc, m); e != nil {
					return result, e
				}
				if e := save(rc.Cluster, s); e != nil {
					return result, e
				}
				a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
				if e != nil {
					return result, e
				}
				defer a.Close()
				return result, a.Reload(ctx)
			}
			if m.Replicas != desired.Replicas || !reflect.DeepEqual(m.Resources, desired.Resources) {
				rc.SyncWait = fmt.Sprintf("waiting for proxy %s configuration before resize", m.Name)
				applied, e := proxyApplied(ctx, rc, m, s.ConfigVersion, false)
				if e != nil || !applied {
					return result, e
				}
			}
		}
		changed := m.ConfigVersion != s.ConfigVersion || m.Version != rc.Cluster.Spec.Version || !reflect.DeepEqual(m.Resources, desired.Resources)
		if m.Role == "proxy" && m.Replicas != desired.Replicas && !changed {
			rc.SyncWait = fmt.Sprintf("scaling proxy %s", m.Name)
			op, e := rc.Backend.SetContainerCount(ctx, m.Name, desired.Replicas, desired.Replicas)
			if e != nil {
				return result, e
			}
			p.track(s, m, "scale", op)
			s.Pending.Replicas = desired.Replicas
			return result, nil
		}
		if m.Volume != "" && (m.Disk.MinDisk != desired.Resources.MinDisk || m.Disk.MaxDisk != desired.Resources.MaxDisk) {
			rc.SyncWait = fmt.Sprintf("growing member %s volume", m.Name)
			if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
				return result, err
			}
			disk := domain.Resources{MinDisk: desired.Resources.MinDisk, MaxDisk: desired.Resources.MaxDisk}
			op, e := rc.Backend.SetResources(ctx, m.Volume, disk)
			if e != nil {
				return result, e
			}
			p.track(s, m, "resize-volume", op)
			s.Pending.Resources = disk
			return result, nil
		}
		if !changed {
			continue
		}
		if roles[m.Name] == "primary" && s.Topology == "ha" && needsDisruption(p, rc, m, desired) {
			rc.SyncWait = fmt.Sprintf("switching leadership away from %s", m.Name)
			return result, switchAwayConfigured(ctx, rc, s, m.Name, true)
		}
		if !reflect.DeepEqual(m.Resources, desired.Resources) {
			rc.SyncWait = fmt.Sprintf("resizing member %s", m.Name)
			op, e := rc.Backend.SetResources(ctx, m.Name, desired.Resources)
			if e != nil {
				return result, e
			}
			p.track(s, m, "resize", op)
			s.Pending.Resources = desired.Resources
			return result, nil
		}
		if m.Role == "data" && major(m.Version) != major(rc.Cluster.Spec.Version) {
			return result, fmt.Errorf("%w: major upgrade requires the PostgreSQL upgrade operation", engine.ErrConflict)
		}
		versionChanged := m.Version != rc.Cluster.Spec.Version
		m.Options = append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)
		m.Version = rc.Cluster.Spec.Version
		m.ConfigVersion = s.ConfigVersion
		// ProxySQL packages and process configuration are independent of the
		// PostgreSQL release. Publish proxy metadata through reload instead.
		if versionChanged && m.Role == "data" {
			rc.SyncWait = fmt.Sprintf("redeploying member %s version", m.Name)
			m.Stage = "imported"
			return result, nil
		}
		if e := activateCertificates(ctx, rc, m); e != nil {
			return result, e
		}
		if m.Role == "data" && m.Stage == "joined" {
			// A config-only reload retains an already joined backend and
			// must not republish it as a new join on every reconciliation.
			m.ProxyReady = true
		}
		m.Stage = "deployed"
		rc.SyncWait = fmt.Sprintf("publishing member %s configuration %d", m.Name, s.ConfigVersion)
		// Publish before requesting the pull, so a failed reload is retried by observation.
		if e := save(rc.Cluster, s); e != nil {
			return result, e
		}
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return result, e
		}
		if e = a.Reload(ctx); e != nil {
			return result, e
		}
		m.Stage = "deployed"
		return result, nil
	}
	if s.Replacement != nil {
		rc.SyncWait = "waiting for member replacement completion"
		return result, nil
	}
	if handled, e := p.syncBackups(ctx, rc, s); handled || e != nil {
		rc.SyncWait = "reconciling final backup configuration"
		return result, e
	}
	if err = p.reconcileExposure(ctx, rc, s); err != nil {
		rc.SyncWait = "reconciling public exposure"
		return result, err
	}
	// Exposure changes can start an asynchronous runtime rollout after the
	// earlier readiness pass. This also covers the direct data endpoint when
	// proxies are disabled. Require infrastructure completion before fresh
	// native/configuration observations; proxy checks visit every live instance.
	target := connectionMember(rc, s)
	for _, m := range s.Members {
		if m.Role == "proxy" || m.Name == target {
			runtime, e := rc.Backend.GetService(ctx, m.Name)
			if e != nil {
				return result, e
			}
			if runtime != nil && runtime.Status == infra.StatusFailed {
				rc.SyncWait = fmt.Sprintf("recovering post-exposure runtime %s", m.Name)
				return result, p.recoverFailedRuntime(ctx, rc, s, m)
			}
			if runtime == nil || runtime.Status != infra.StatusRunning && runtime.Status != infra.StatusActive {
				rc.SyncWait = fmt.Sprintf("waiting for post-exposure runtime %s to be Running/Active", m.Name)
				return result, nil
			}
			rc.SyncWait = fmt.Sprintf("waiting for post-exposure member %s readiness", m.Name)
			ok, e := ready(ctx, rc, m)
			if e != nil || !ok {
				return result, e
			}
		}
	}
	if rc.Cluster.Spec.DataSource != nil && !engine.InitialDataSourceUnproven(rc.Cluster) {
		applied, e := p.InitialDataSourceApplied(rc.Cluster)
		if e != nil || !applied {
			rc.SyncWait = "waiting for initial data source application receipt"
			return result, e
		}
	}
	if p.options.Flavour != nil {
		if err = p.options.Flavour.PostReady(ctx, rc); err != nil {
			return result, err
		}
	}
	if !s.Formed {
		s.Formed = true
		// Revoke first-initialization authorization in durable member configs.
		s.ConfigVersion++
	}
	s.ConversionSeed = ""
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	rc.SyncWait = ""
	result.RequeueAfter = 10 * time.Second
	return result, nil
}

func (p *Core) reconcileExposure(ctx context.Context, rc *engine.Context, s *state) error {
	target := connectionMember(rc, s)
	for _, m := range s.Members {
		want := rc.Cluster.Spec.Exposure.Public && m.Name == target
		if !want {
			if m.Public {
				if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return err
				}
				m.Public = false
			}
			continue
		}
		if err := engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure); err != nil {
			return err
		}
		e := rc.Cluster.Spec.Exposure
		if err := rc.Backend.EnsurePublicPort(ctx, m.Name, infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: e.IPType, Allowlist: e.Allowlist}); err != nil {
			return err
		}
		m.Public = true
	}
	return nil
}

// restrictExposure only removes access. The health-gated reconciler below may
// later publish a new endpoint or expand the allowlist to the full desired set.
func (p *Core) restrictExposure(ctx context.Context, rc *engine.Context, s *state) error {
	target := connectionMember(rc, s)
	for _, m := range s.Members {
		if !m.Public {
			continue
		}
		if err := verifyMember(ctx, rc, m); err != nil {
			return err
		}
		if !rc.Cluster.Spec.Exposure.Public || m.Name != target {
			if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
				return err
			}
			m.Public = false
			continue
		}
		routes, err := rc.Backend.ListPublicPorts(ctx)
		if err != nil {
			return err
		}
		for _, route := range routes {
			if route.Hostname != m.Name || route.TargetPort != 5432 {
				continue
			}
			allowlist, err := intersectAllowlists(route.Allowlist, rc.Cluster.Spec.Exposure.Allowlist)
			if err != nil {
				return err
			}
			if len(allowlist) == 0 && len(route.Allowlist) > 0 && len(rc.Cluster.Spec.Exposure.Allowlist) > 0 {
				// Empty is allow-all to the backend, not deny-all.
				if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return err
				}
				m.Public = false
				break
			}
			if !reflect.DeepEqual(allowlist, route.Allowlist) {
				port := route.PublicPort
				port.Allowlist = allowlist
				if err := rc.Backend.EnsurePublicPort(ctx, m.Name, port); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func intersectAllowlists(current, desired []string) ([]string, error) {
	if len(desired) == 0 {
		return current, nil
	}
	if len(current) == 0 {
		return desired, nil
	}
	result := []string{}
	seen := map[string]bool{}
	for _, old := range current {
		a, err := netip.ParsePrefix(old)
		if err != nil {
			return nil, err
		}
		for _, next := range desired {
			b, err := netip.ParsePrefix(next)
			if err != nil {
				return nil, err
			}
			if !a.Overlaps(b) {
				continue
			}
			narrow := a
			if b.Bits() > a.Bits() {
				narrow = b
			}
			cidr := narrow.Masked().String()
			if !seen[cidr] {
				seen[cidr] = true
				result = append(result, cidr)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}

// A terminal runtime failure must not turn into a successful readiness wait.
// Recovery is a new operation and only a backend proof may admit it without placement.
func (p *Core) recoverFailedRuntime(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if recoverer, ok := rc.Backend.(infra.FailedScaleRecoverer); ok && m.Role == "proxy" {
		desired := desiredComponent(rc, m)
		op, recovering, err := recoverer.RecoverFailedScale(ctx, m.Name, desired.Replicas, desired.Replicas)
		if err != nil {
			return err
		}
		if recovering {
			p.track(s, m, "scale", op)
			s.Pending.Replicas = desired.Replicas
			return nil
		}
	}
	return fmt.Errorf("infrastructure runtime failed for %s; inspect the terminal backend operation and deployment, restore placement capacity or application health, then explicitly reapply the desired runtime", m.Name)
}
