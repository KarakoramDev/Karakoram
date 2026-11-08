// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"maps"
	"reflect"
	"time"
)

// rollingState freezes the desired artifacts/resources and order across CP restarts.
// Each side effect has a durable preceding stage and an observation checkpoint.
type rollingState struct {
	From          string                          `json:"from"`
	To            string                          `json:"to"`
	Major         bool                            `json:"major"`
	RequestedAt   time.Time                       `json:"requestedAt"`
	Order         []string                        `json:"order"`
	Index         int                             `json:"index"`
	Stage         string                          `json:"stage"`
	Targets       map[string]domain.ComponentSpec `json:"targets"`
	Op            *infra.Op                       `json:"op,omitempty"`
	Parameters    map[string]string               `json:"restartParameters,omitempty"`
	HandoffTarget string                          `json:"handoffTarget,omitempty"`

	ScaleIn bool `json:"scaleIn,omitempty"`
}

func rollingOrder(ctx context.Context, rc *engine.Context, s *state) ([]string, error) {
	var order []string
	primary := ""
	for _, m := range s.Members {
		if m.Role == "proxy" {
			order = append(order, m.Name)
		}
	}
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil || st.MemberState != "ONLINE" {
			return nil, engine.ErrUnavailable
		}
		if onlinePrimary(st) {
			if primary != "" {
				return nil, engine.ErrUnavailable
			}
			primary = m.Name
		} else {
			order = append(order, m.Name)
		}
	}
	if primary == "" {
		return nil, engine.ErrUnavailable
	}
	return append(order, primary), nil
}
func rollingNeedsWindow(m *member, version string, target domain.ComponentSpec, parameters map[string]string) bool {
	return m.Version != version || target.Replicas < m.Replicas ||
		!reflect.DeepEqual(m.Resources, target.Resources) ||
		m.Role == "data" && !reflect.DeepEqual(m.Parameters, parameters)
}

// Count-only and resource-only updates have independent serving guarantees.
func proxyCountOnly(m *member, version string, target domain.ComponentSpec, runtimeHash string, capabilities infra.Capabilities) bool {
	return capabilities.ReplicaScalePreservesServing && m.Role == "proxy" && m.Replicas != target.Replicas &&
		m.Version == version && reflect.DeepEqual(m.Resources, target.Resources) &&
		m.ProxyRuntimeHash != "" && m.ProxyRuntimeHash == runtimeHash
}

func proxyResourceOnly(m *member, version string, target domain.ComponentSpec, runtimeHash string, backend infra.Backend) bool {
	if !backend.Capabilities().ResourceUpdatePreservesServing || m.Role != "proxy" ||
		m.Replicas != target.Replicas || m.Version != version ||
		m.ProxyRuntimeHash == "" || m.ProxyRuntimeHash != runtimeHash ||
		reflect.DeepEqual(m.Resources, target.Resources) {
		return false
	}
	if serving, ok := backend.(infra.ServingResourceUpdate); ok {
		return serving.ResourceUpdatePreservesServingFor(m.Resources, target.Resources)
	}
	return true
}

func proxyRuntimeHash(rc *engine.Context, spec infra.RuntimeSpec) (string, error) {
	// This marker controls backend health gating, not the proxy runtime.
	// Exclude it so upgraded control planes retain existing count fast paths.
	if _, marked := spec.Env["KARAKORAM_PROXY_SERVING_PROBE"]; marked {
		spec.Env = maps.Clone(spec.Env)
		delete(spec.Env, "KARAKORAM_PROXY_SERVING_PROBE")
	}
	raw, err := json.Marshal(struct {
		Runtime infra.RuntimeSpec

		ListenHost string
	}{Runtime: spec, ListenHost: mysqlListenHost(rc)})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func (p *Core) desiredProxyRuntimeHash(rc *engine.Context, m *member) (string, error) {
	if m.Role != "proxy" {
		return "", nil
	}
	spec, err := p.runtimeSpec(rc, m)
	if err != nil {
		return "", err
	}
	return proxyRuntimeHash(rc, spec)
}

func proxyUpdatesPreserveServing(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if m.Role == "proxy" {
		if gate, ok := rc.Backend.(infra.ServingProxyUpdates); ok {
			return gate.ProxyUpdatesPreserveServing(ctx, m.Name)
		}
	}
	return true, nil
}

func (p *Core) reconcileRolling(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Rolling != nil {
		ctx = freshMySQLReads(ctx)
		invalidateMySQLReadObservation(ctx, rc)
		defer invalidateMySQLReadObservation(ctx, rc)
	}
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return true, err
	}
	parameters := restartParameters(options.Parameters)
	if s.Rolling == nil {
		if s.Formation.State != "formed" || s.Replacement != nil {
			return false, nil
		}
		changed := false
		needsWindow := false
		from := ""
		for _, m := range s.Members {
			if m.Stage != "joined" {
				return false, nil
			}
			desired := desiredComponent(rc, m)
			if m.Role == "proxy" && desired.Replicas < 1 {
				return true, engine.Invalid("ProxySQL scaling requires at least one serving replica")
			}
			runtimeHash, err := p.desiredProxyRuntimeHash(rc, m)
			if err != nil {
				return true, err
			}
			runtimeChanged := m.Role == "proxy" && m.ProxyRuntimeHash != "" && m.ProxyRuntimeHash != runtimeHash
			serving, err := proxyUpdatesPreserveServing(ctx, rc, m)
			if err != nil {
				return true, err
			}
			restartingScale := m.Role == "proxy" && m.Replicas != desired.Replicas && (!serving || !rc.Backend.Capabilities().ReplicaScalePreservesServing || m.ProxyRuntimeHash != runtimeHash)
			changed = changed || runtimeChanged
			needsWindow = needsWindow || runtimeChanged || restartingScale
			changed = changed || m.Version != rc.Cluster.Spec.Version || m.Replicas != desired.Replicas || !reflect.DeepEqual(m.Resources, desired.Resources)
			changed = changed || m.Role == "data" && !reflect.DeepEqual(m.Parameters, parameters)
			needsWindow = needsWindow || rollingNeedsWindow(m, rc.Cluster.Spec.Version, desired, parameters)
			if m.Role == "data" {
				from = m.Version
			}
		}
		if !changed {
			return false, nil
		}
		if !p.options.AutoReplaceLostVolumes {
			for _, m := range s.data() {
				if m.ConfirmedVolumeLost {
					// Await explicit replacement without acquiring a rolling
					// owner or failing Sync, either of which prevents its claim.
					// Existing rolling and replacement owners retain progression.
					return true, nil
				}
			}
		}
		if needsWindow && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		for _, m := range s.Members {
			target := desiredComponent(rc, m).Resources
			if m.Resources.MinDisk != target.MinDisk || m.Resources.MaxDisk != target.MaxDisk {
				if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
					return true, err
				}
			}
			if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target) {
				if err := planner.PlanResources(ctx, m.Name, target); err != nil {
					return true, err
				}
			}
		}
		if err := maintenanceAllowed(ctx, rc, s); err != nil {
			return true, err
		}
		if from != rc.Cluster.Spec.Version && !upgradeAllowed(from, rc.Cluster.Spec.Version) {
			return true, engine.Invalid("unsupported MySQL version transition")
		}
		order, err := rollingOrder(ctx, rc, s)
		if err != nil {
			return true, err
		}
		u := &rollingState{From: from, To: rc.Cluster.Spec.Version, Major: mysqlMajor(from) != mysqlMajor(rc.Cluster.Spec.Version), RequestedAt: now(rc), Order: order, Stage: "backup", Targets: map[string]domain.ComponentSpec{}}
		u.Parameters = parameters
		for _, m := range s.Members {
			u.Targets[m.Name] = desiredComponent(rc, m)
		}
		s.Rolling = u
		return true, nil
	}
	u := s.Rolling
	if u.To != rc.Cluster.Spec.Version && u.Stage != "scale-failed" {
		return true, engine.Invalid("cannot change version during MySQL maintenance")
	}
	if u.Stage == "backup" {
		if u.Major {
			if err := requireUpgradeBackup(ctx, rc, u); err != nil {
				return true, err
			}
		}
		u.Stage = "prepare"
		return true, nil
	}
	if u.Index >= len(u.Order) {
		s.Rolling = nil
		return true, nil
	}
	m := s.find(u.Order[u.Index])
	if m == nil {
		return true, engine.ErrNotFound
	}
	if u.Stage == "scale-failed" {
		return true, p.reconcileFailedProxyScale(ctx, rc, s, m)
	}
	target := u.Targets[m.Name]
	if u.Stage == "resource-failed" {
		// A terminal resource receipt must never enter count recovery or be
		// resubmitted automatically. A different explicit target can supersede it.
		if desired := desiredComponent(rc, m); !reflect.DeepEqual(desired.Resources, target.Resources) {
			u.Targets[m.Name] = desired
			m.PendingReplicas = 0
			u.Op = nil
			u.Stage = "resource-recovery"
			return true, nil
		}
		rc.SyncWait = "ProxySQL resource update failed; restore backend health and supersede the failed resource target"
		return true, nil
	}
	runtimeHash, err := p.desiredProxyRuntimeHash(rc, m)
	if err != nil {
		return true, err
	}
	runtimeChanged := m.Role == "proxy" && m.ProxyRuntimeHash != "" && m.ProxyRuntimeHash != runtimeHash
	serving, err := proxyUpdatesPreserveServing(ctx, rc, m)
	if err != nil {
		return true, err
	}
	restartingScale := m.Role == "proxy" && m.Replicas != target.Replicas && (!serving || !rc.Backend.Capabilities().ReplicaScalePreservesServing || m.ProxyRuntimeHash != runtimeHash)
	needsWindow := runtimeChanged || restartingScale || rollingNeedsWindow(m, u.To, target, u.Parameters)
	switch u.Stage {
	case "prepare":
		if !runtimeChanged && m.Version == u.To && m.Replicas == target.Replicas && reflect.DeepEqual(m.Resources, target.Resources) && (m.Role != "data" || reflect.DeepEqual(m.Parameters, u.Parameters)) {
			u.Index++
			return true, nil
		}
		if needsWindow && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if serving && proxyCountOnly(m, u.To, target, runtimeHash, rc.Backend.Capabilities()) {
			if target.Replicas < 1 {
				return true, engine.Invalid("ProxySQL scaling requires at least one serving replica")
			}
			// Do not resize an already unavailable endpoint. Check the current
			// fleet before publishing the expected post-scale instance count.
			ok, err := ready(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
			u.ScaleIn = target.Replicas < m.Replicas
			m.PendingReplicas = target.Replicas
			publishRevision(s)
			// Persist intent before the idempotent backend call. Both readiness
			// and D103 admission now require the target number of acknowledgers.
			u.Stage = "scale"
			return true, nil
		}
		if serving && proxyResourceOnly(m, u.To, target, runtimeHash, rc.Backend) {
			ok, err := ready(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
			m.PendingReplicas = target.Replicas
			publishRevision(s)
			// Persist before SetResources: a lost reply replays the same target
			// and the backend adopts its durable in-flight operation.
			u.Stage = "resource-scale"
			return true, nil
		}
		// Restarting changes retain the existing service-wide maintenance path.
		if m.Resources.MinDisk != target.Resources.MinDisk || m.Resources.MaxDisk != target.Resources.MaxDisk {
			if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
				s.Rolling = nil
				return true, err
			}
		}
		if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target.Resources) {
			if err := planner.PlanResources(ctx, m.Name, target.Resources); err != nil {
				// No disruption has started; discard the frozen target so a
				// subsequent desired generation can supersede this rejection.
				if errors.Is(err, infra.ErrRejected) {
					s.Rolling = nil
				}
				return true, err
			}
		}
		if u.Major && m.Role == "data" {
			m.SlowShutdown = true
			publishRevision(s)
		}
		u.Stage = "handoff"
	case "scale":
		if u.ScaleIn && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if runtimeChanged || !serving || !rc.Backend.Capabilities().ReplicaScalePreservesServing {
			m.PendingReplicas = 0
			u.ScaleIn = false
			u.Stage = "prepare"
			return true, nil
		}
		op, err := rc.Backend.SetContainerCount(ctx, m.Name, target.Replicas, target.Replicas)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "scaled"
	case "scaled":
		if u.Op == nil {
			return true, engine.Invalid("missing MySQL scale receipt")
		}
		status, err := rc.Backend.OpStatus(ctx, *u.Op)
		if status.Failed {
			// A terminal receipt will never become Done. Persist a distinct
			// checkpoint before attempting any backend-proven recovery.
			m.PendingReplicas = 0
			u.Stage = "scale-failed"
			return true, nil
		}
		if err != nil || !status.Done {
			return true, err
		}
		// Every live instance must acknowledge the published backend/user
		// configuration and report a healthy writer before scale completes.
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		m.Replicas = target.Replicas
		m.PendingReplicas = 0
		u.ScaleIn = false
		u.Index++
		u.Op = nil
		u.Stage = "prepare"
	case "resource-recovery":
		// A partial failed rollout makes the committed resources insufficient
		// evidence, even when reverting. Reapply the explicit superseding
		// target through maintenance instead of assuming backend rollback.
		if rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok {
			if err := planner.PlanResources(ctx, m.Name, target.Resources); err != nil {
				return true, err
			}
		}
		m.Offline = true
		publishRevision(s)
		u.Stage = "offline"
	case "resource-scale":
		// Unlike count-only scale-up, resource updates retain window gating.
		if rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if !serving || !proxyResourceOnly(m, u.To, target, runtimeHash, rc.Backend) {
			return true, engine.Invalid("ProxySQL resource update no longer preserves serving; restore the backend serving guarantee before retrying")
		}
		op, err := rc.Backend.SetResources(ctx, m.Name, target.Resources)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "resource-scaled"
	case "resource-scaled":
		if u.Op == nil {
			return true, engine.Invalid("missing MySQL proxy resource update receipt")
		}
		status, err := rc.Backend.OpStatus(ctx, *u.Op)
		if status.Failed {
			u.Stage = "resource-failed"
			rc.SyncWait = "ProxySQL resource update failed; restore backend health and supersede the failed resource target"
			return true, nil
		}
		if err != nil || !status.Done {
			return true, err
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			rc.SyncWait = "ProxySQL resource update is waiting for configuration and writer readiness on every expected live instance"
			return true, err
		}
		m.Resources = target.Resources
		m.PendingReplicas = 0
		u.Index++
		u.Op = nil
		u.Stage = "prepare"
	case "handoff":
		if needsWindow && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target.Resources) {
			if err := planner.PlanResources(ctx, m.Name, target.Resources); err != nil {
				if errors.Is(err, infra.ErrRejected) {
					m.SlowShutdown = false
					publishRevision(s)
					s.Rolling = nil
				}
				return true, err
			}
		}
		if u.Major && m.Role == "data" {
			ok, err := configApplied(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
		}
		if s.Topology == "group" && m.Role == "data" && !u.Major {
			st, err := live(ctx, rc, m.Name)
			if err != nil {
				return true, err
			}
			if onlinePrimary(st) {
				return true, switchAway(ctx, rc, s, m.Name, &u.HandoffTarget)
			}
		}
		if clustered(s.Topology) && m.Role == "data" && !u.Major {
			if err := proxyHandoffReady(ctx, rc, s, m.Name, &u.HandoffTarget); err != nil {
				return true, err
			}
		}
		// For mixed-major groups the old primary leaves rather than promoting an
		// ineligible higher-version secondary with set_as_primary. GR elects it.
		m.Offline = true
		publishRevision(s)
		u.Stage = "offline"
	case "offline":
		ok, err := configApplied(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		if clustered(s.Topology) && m.Role == "data" {
			if err := survivorsOnline(ctx, rc, s, m.Name); err != nil {
				return true, err
			}
			if err := proxyHandoffReady(ctx, rc, s, m.Name, &u.HandoffTarget); err != nil {
				return true, err
			}
		}
		op, err := rc.Backend.Stop(ctx, m.Name)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "stopped"
	case "stopped":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
		if err != nil {
			return true, err
		}
		if runtime != nil && runtime.Status != infra.StatusStopped {
			return true, nil
		}
		if err = verifyMember(ctx, rc, m); err != nil {
			return true, err
		}
		if m.Volume != "" && (m.Resources.MinDisk != target.Resources.MinDisk || m.Resources.MaxDisk != target.Resources.MaxDisk) {
			op, err := rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: target.Resources.MinDisk, MaxDisk: target.Resources.MaxDisk})
			if err != nil {
				return true, err
			}
			u.Op = &op
		} else {
			u.Op = nil
		}
		u.Stage = "disk"
	case "disk":
		if u.Op != nil {
			done, err := rollingOpDone(ctx, rc, u)
			if err != nil || !done {
				return true, err
			}
		}
		op, err := rc.Backend.SetResources(ctx, m.Name, target.Resources)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "resources"
	case "resources":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		if m.Role == "proxy" && m.Replicas != target.Replicas {
			op, err := rc.Backend.SetContainerCount(ctx, m.Name, target.Replicas, target.Replicas)
			if err != nil {
				return true, err
			}
			u.Op = &op
			u.Stage = "replicas"
			return true, nil
		}
		u.Stage = "replicas"
		u.Op = nil
	case "replicas":
		if u.Op != nil {
			done, err := rollingOpDone(ctx, rc, u)
			if err != nil || !done {
				return true, err
			}
		}
		m.Version = u.To
		m.Resources = target.Resources
		m.Replicas = target.Replicas
		if m.Role == "data" {
			m.Parameters = u.Parameters
		}
		publishRevision(s)
		u.Stage = "deploy"
	case "deploy":
		spec, err := p.runtimeSpec(rc, m)
		if err != nil {
			return true, err
		}
		if engine.DeploymentBlocked(rc, m.Name, spec) {
			return true, nil
		}
		runtimeHash := ""
		if m.Role == "proxy" {
			runtimeHash, err = proxyRuntimeHash(rc, spec)
			if err != nil {
				return true, err
			}
		}
		op, err := engine.Deploy(ctx, rc, m.Name, spec)
		if err != nil {
			return true, err
		}
		if m.Role == "proxy" {
			m.ProxyRuntimeHash = runtimeHash
		}
		u.Op = &op
		u.Stage = "deployed"
	case "deployed":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		m.Offline = false
		m.SlowShutdown = false
		publishRevision(s)
		u.Stage = "online"
	case "online":
		if clustered(s.Topology) && m.Role == "data" {
			// Existing data identities are rejoined, never cloned across versions.
			st, err := live(ctx, rc, m.Name)
			if err != nil {
				return true, err
			}
			if st.MemberState != "ONLINE" {
				_, err = p.reconcileFormation(ctx, rc, s)
				return true, err
			}
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		if clustered(s.Topology) && m.Role == "data" {
			if err := proxyRejoinReady(ctx, rc, s, m.Name); err != nil {
				return true, err
			}
		}
		u.Index++
		u.Op = nil
		u.HandoffTarget = ""
		u.Stage = "prepare"
	default:
		return true, engine.Invalid("unknown MySQL rolling checkpoint %s", u.Stage)
	}
	return true, nil
}

func rollingOpDone(ctx context.Context, rc *engine.Context, u *rollingState) (bool, error) {
	if u.Op == nil {
		return false, engine.Invalid("missing MySQL maintenance receipt")
	}
	st, err := rc.Backend.OpStatus(ctx, *u.Op)
	if err != nil {
		return false, err
	}
	if st.Failed {
		return false, engine.ErrUnavailable
	}
	return st.Done, nil
}

// A failed scale receipt is terminal. Observe the surviving fleet, rather than
// pinning readiness to the failed target or resubmitting the same placement.
func (p *Core) reconcileFailedProxyScale(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if m.Role != "proxy" {
		return engine.Invalid("failed scale checkpoint is not a proxy")
	}
	inventory := engine.ObserveInstanceInventory(ctx, rc, m.Name)
	if inventory.Err != nil {
		return inventory.Err
	}
	count := 0
	for _, instance := range inventory.Containers {
		if instance.Status == infra.ContainerRunning || instance.Status == infra.ContainerActive {
			count++
		}
	}
	m.Replicas = count
	m.PendingReplicas = 0
	u := s.Rolling
	desired := desiredComponent(rc, m)
	if desired.Replicas < 1 {
		return engine.Invalid("ProxySQL scaling requires at least one serving replica")
	}
	hash, err := p.desiredProxyRuntimeHash(rc, m)
	if err != nil {
		return err
	}
	// A superseding disruptive specification takes the ordinary maintenance
	// path; recreating the plan retains version/window/resource safeguards.
	if m.Version != rc.Cluster.Spec.Version || !reflect.DeepEqual(m.Resources, desired.Resources) || m.ProxyRuntimeHash != hash {
		s.Rolling = nil
		return nil
	}
	if desired.Replicas < count && rc.WaitForWindow().RequeueAfter > 0 {
		return nil
	}
	// Nomad proves that this new operation needs no placement and preserves
	// serving tasks. Its retain-only recovery cannot be blindly retried after
	// another terminal failure, and a lost reply adopts the retained receipt.
	if recoverer, ok := rc.Backend.(infra.FailedScaleRecoverer); ok {
		op, recovering, err := recoverer.RecoverFailedScale(ctx, m.Name, desired.Replicas, desired.Replicas)
		if err != nil {
			return err
		}
		if recovering {
			u.Targets[m.Name] = desired
			u.ScaleIn = desired.Replicas < count
			u.Op = &op
			u.Stage = "scaled"
			m.PendingReplicas = desired.Replicas
			publishRevision(s)
			return nil
		}
	}
	if desired.Replicas != u.Targets[m.Name].Replicas {
		if desired.Replicas != count {
			// A different explicit count may request new placement. It is not
			// an automatic retry of the terminal operation's failed target.
			s.Rolling = nil
			return nil
		}
		// A reverted desired count can already match the surviving fleet.
		// Still require separately restored backend health and config acks:
		// never interpret the failed receipt itself as successful.
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return err
		}
		if service != nil && (service.Status == infra.StatusRunning || service.Status == infra.StatusActive) {
			ok, err := ready(ctx, rc, m)
			if err != nil {
				return err
			}
			if ok {
				s.Rolling = nil
				return nil
			}
		}
	}
	rc.SyncWait = "ProxySQL scale failed; restore backend health or supersede the failed replica count"
	return nil
}
