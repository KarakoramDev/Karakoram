// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
			return err
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
	st, err := rc.Backend.OpStatus(ctx, q.Op)
	if err != nil {
		return true, err
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
			if id := engine.ImportedServiceID(q.Op, st, service); id != "" {
				*entry.id = id
			}
		}
		if err = verifyMember(ctx, rc, m); err != nil {
			return true, err
		}
		m.Stage = "imported"
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
		return false, nil
	}
	if !st.Running {
		return false, nil
	}
	if m.Role == "data" && m.Topology == "ha" && st.Role != "primary" && (st.Role != "replica" || !st.Streaming) {
		return false, nil
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return false, nil
	}
	agent, err := a.Status(ctx)
	if err != nil {
		return false, nil
	}
	if agent.ConfigVersion < m.ConfigVersion {
		// A failed reload request must remain retryable after its publication
		// checkpoint; the agent's applied version is the acknowledgement.
		if err := a.Reload(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if st.PendingRestart && m.Role == "data" && m.Topology == "single" {
		return false, a.Process(ctx, agentapi.ProcessAction{Process: "postgres", Action: "restart"})
	}
	if st.PendingRestart && m.Role == "data" && m.Topology == "ha" {
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
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
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
		return p.convertTopology(ctx, rc, s, result)
	}
	// A durable shared-memory journal owns its current membership. Allocate
	// scale-up identities only after it finishes, never before agent probes
	// that would otherwise preempt provisioning those identities.
	if s.Rolling == nil {
		s.allocate(rc.Cluster)
	}
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		return result, err
	}
	if e := save(rc.Cluster, s); e != nil {
		return result, e
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.Suspended {
		for _, m := range s.Members {
			m.Stage = "planned"
			m.DeployOp = nil
			m.ServiceID = ""
		}
		s.Suspended = false
		return result, nil
	}
	for _, m := range s.Members {
		if err = verifyMember(ctx, rc, m); err != nil {
			return result, err
		}
	}
	if s.Replacement != nil && s.Replacement.Recovery != "" {
		_, err = p.advanceReplacement(ctx, rc, s)
		return result, err
	}
	if handled, e := p.reconcileUpgrade(ctx, rc, s); handled || e != nil {
		return result, e
	}
	if handled, e := p.syncBackups(ctx, rc, s); handled || e != nil {
		return result, e
	}
	if handled, e := p.reconcileSharedMemory(ctx, rc, s); handled || e != nil {
		return result, e
	}
	revision, e := p.accessRevision(ctx, rc)
	if e != nil {
		return result, e
	}
	raw, e := json.Marshal(struct {
		Spec           domain.ClusterSpec
		AccessRevision string
	}{rc.Cluster.Spec, revision})
	if e != nil {
		return result, e
	}
	digest := sha256.Sum256(raw)
	hash := hex.EncodeToString(digest[:])
	if s.ConfigHash != hash {
		s.ConfigHash = hash
		s.ConfigVersion++
	}
	if s.Replacement == nil && len(s.data()) > rc.Cluster.Spec.Components.Engine.Replicas {
		return p.scaleDown(ctx, rc, s, result)
	}
	for _, m := range s.data() {
		if m.EtcdState == "pending" {
			return result, p.addLearner(ctx, rc, s, m)
		}
		if m.EtcdLearner && m.Stage == "joined" && s.Replacement == nil {
			return result, p.promoteLearner(ctx, rc, s, m)
		}
	}
	// Provision all identities before waiting on deployments: etcd needs a quorum.
	for _, m := range s.Members {
		desired := desiredComponent(rc, m)
		if desired.Replicas == 0 {
			continue
		}
		if m.Stage == "planned" {
			tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return result, e
			}
			specs := []infra.ServiceSpec{}
			if m.Volume != "" {
				v, e := rc.Backend.GetService(ctx, m.Volume)
				if e != nil {
					return result, e
				}
				if v == nil {
					specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, tok.Token, desired.Resources))
				}
			}
			runtime, e := rc.Backend.GetService(ctx, m.Name)
			if e != nil {
				return result, e
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
				return result, nil
			}
			op, e := rc.Backend.ImportServices(ctx, specs)
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
			spec, e := p.runtimeSpec(rc, m)
			if e != nil {
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
			st, e := rc.Backend.OpStatus(ctx, *m.DeployOp)
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
	if deploymentsBusy {
		return result, nil
	}
	for _, m := range s.Members {
		if desiredComponent(rc, m).Replicas == 0 {
			continue
		}
		if m.Stage == "deployed" {
			ok, e := ready(ctx, rc, m)
			if e != nil {
				return result, e
			}
			if !ok {
				return result, nil
			}
			m.Stage = "joined"
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
			return result, nil
		}
	}
	// Disable proxies only after the direct data endpoint is ready.
	for i, m := range s.Members {
		if m.Role == "proxy" && desiredComponent(rc, m).Replicas == 0 {
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
			return result, nil
		}
	}
	if s.Replacement == nil && len(s.data()) > rc.Cluster.Spec.Components.Engine.Replicas {
		return p.scaleDown(ctx, rc, s, result)
	}
	// Replicas precede the writer; observe leadership afresh before each disruption.
	order := append([]*member(nil), s.Members...)
	roles := map[string]string{}
	for _, m := range order {
		st, e := live(ctx, rc, m.Name)
		if e != nil || !st.Running {
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
			return result, nil
		}
		roles[m.Name] = st.Role
	}
	sort.SliceStable(order, func(i, j int) bool { return roles[order[i].Name] != "primary" && roles[order[j].Name] == "primary" })
	for _, m := range order {
		desired := desiredComponent(rc, m)
		if m.Role == "proxy" && m.Replicas != desired.Replicas {
			op, e := rc.Backend.SetContainerCount(ctx, m.Name, desired.Replicas, desired.Replicas)
			if e != nil {
				return result, e
			}
			p.track(s, m, "scale", op)
			s.Pending.Replicas = desired.Replicas
			return result, nil
		}
		if m.Volume != "" && (m.Disk.MinDisk != desired.Resources.MinDisk || m.Disk.MaxDisk != desired.Resources.MaxDisk) {
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
		changed := m.ConfigVersion != s.ConfigVersion || m.Version != rc.Cluster.Spec.Version || !reflect.DeepEqual(m.Resources, desired.Resources)
		if !changed {
			continue
		}
		if roles[m.Name] == "primary" && s.Topology == "ha" && needsDisruption(p, rc, m, desired) {
			candidate := ""
			for _, peer := range order {
				if peer.Role == "data" && roles[peer.Name] == "replica" && peer.ConfigVersion == s.ConfigVersion {
					candidate = peer.Name
					break
				}
			}
			if candidate == "" {
				return result, nil
			}
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return result, e
			}
			if e = a.Engine(ctx, "postgres", "switchover", pgspec.SwitchoverRequest{Candidate: candidate}, nil); e != nil {
				return result, e
			}
			return result, nil
		}
		if !reflect.DeepEqual(m.Resources, desired.Resources) {
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
		return result, nil
	}
	if handled, e := p.syncBackups(ctx, rc, s); handled || e != nil {
		return result, e
	}
	if err = p.reconcileExposure(ctx, rc, s); err != nil {
		return result, err
	}
	if p.options.Flavour != nil {
		if err = p.options.Flavour.PostReady(ctx, rc); err != nil {
			return result, err
		}
	}
	s.Formed = true
	s.ConversionSeed = ""
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
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
