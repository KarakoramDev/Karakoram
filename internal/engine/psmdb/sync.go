// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (p *Provider) dial(ctx context.Context, rc *engine.Context, s *internalState, set, user string) (*mongo.Client, error) {
	pw, e := secret(ctx, rc, "users/"+user)
	if e != nil {
		return nil, e
	}
	ca, e := secret(ctx, rc, "tls/ca.crt")
	if e != nil {
		return nil, e
	}
	h := hosts(s.setMembers(set))
	rs := set
	if set == "router" {
		h = []string{rc.Cluster.Name + "r:27017"}
		rs = ""
	}
	// During disabled -> allowTLS, a TLS ping to an upgraded secondary can
	// succeed while the still-disabled primary remains unreachable by that
	// client. Plaintext reaches every member throughout the allowTLS pass.
	opts := mongoadmin.DialOptions{Hosts: h, Dialer: rc.Dialer, ReplicaSet: rs, Username: user, Password: string(pw), TLS: s.TLSMode != "disabled" && s.TLSMode != "allowTLS", CAPEM: ca}
	c, err := mongoadmin.Dial(ctx, opts)
	if err != nil && s.TLSMode == "preferTLS" {
		opts.TLS = false
		return mongoadmin.Dial(ctx, opts)
	}
	return c, err
}
func agent(ctx context.Context, rc *engine.Context, name string) (*agentapi.Client, error) {
	return rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
}
func agentStatus(ctx context.Context, rc *engine.Context, name string) (agentapi.Status, error) {
	a, e := agent(ctx, rc, name)
	if e != nil {
		return agentapi.Status{}, e
	}
	defer a.Close()
	return a.Status(ctx)
}
func running(st agentapi.Status, role string) bool {
	process := "mongod"
	if role == agentapi.RoleRouter {
		process = "mongos"
	}
	p := st.Processes[process]
	return p.State == "running" || p.State == "Running"
}
func (p *Provider) observePending(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	v := s.Pending
	if v == nil {
		return false, nil
	}
	if v.Op.ID != "" {
		st, e := rc.Backend.OpStatus(ctx, v.Op)
		if e != nil {
			return true, e
		}
		if st.Failed {
			return true, s.failPending(rc, st.Message)
		}
		if !st.Done {
			return true, nil
		}
	}
	m := s.find(v.Member)
	if m != nil {
		switch v.Action {
		case "import":
			if err := verifyMemberServices(ctx, rc, m); err != nil {
				return true, err
			}
			m.Stage = "imported"
		case "deploy":
			m.Stage = "deployed"
			m.Version = v.Version
		case "upgrade":
			m.Stage = "upgrade-wait"
			m.Version = v.Version
		case "resources":
			if m.Resources.CPUMode != v.Resources.CPUMode {
				m.CPUModeChangedAt = now(rc)
			}
			m.Resources = v.Resources
		case "disk":
			m.Disk = v.Resources
		case "containers":
			m.Containers = v.Containers
		case "delete-runtime":
			m.Stage = "delete-volume"
		case "delete-volume":
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return true, err
			}
			removeRecord(s, m.Name)
		}
	}
	s.resetPlatformRetry(v)
	s.Pending = nil
	return true, nil
}
func (p *Provider) deploy(ctx context.Context, rc *engine.Context, s *internalState, m *member, action, version string) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	m.DesiredVersion = version
	if e := save(rc.Cluster, s); e != nil {
		return e
	}
	doc, e := p.zeropsYAML(rc, m, version)
	if e != nil {
		return e
	}
	artifact, e := rc.Artifact.Open(ctx)
	if e != nil {
		return e
	}
	defer artifact.Close()
	op, e := rc.Backend.Deploy(ctx, m.Name, infra.Deployment{Artifact: artifact, ZeropsYAML: doc, SetupName: m.Name, Filename: "karakoram.tar.gz"})
	if e != nil {
		return e
	}
	s.Pending = &pending{Op: op, Action: action, Member: m.Name, Version: version}
	return nil
}

// importMember adopts verified services left by a partial import and imports
// only missing services. A failed platform process can still have created either.
func (p *Provider) importMember(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	specs := []infra.ServiceSpec{}
	tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return e
	}
	if m.Volume != "" {
		v, e := rc.Backend.GetService(ctx, m.Volume)
		if e != nil {
			return e
		}
		if v == nil {
			r := component(rc.Cluster.Spec, m).Resources
			specs = append(specs, infra.ServiceSpec{Hostname: m.Volume, Type: "local-storage:single@1", Resources: domain.Resources{MinDisk: r.MinDisk, MaxDisk: r.MaxDisk}, Env: ownershipEnv(rc.Cluster.Name, m.Name, tok.Token)})
		} else {
			if err := verifyServiceOwnership(ctx, rc, m, v, true); err != nil {
				return err
			}
			m.VolumeID = v.ID
		}
		m.Disk = component(rc.Cluster.Spec, m).Resources
	}
	runtime, e := rc.Backend.GetService(ctx, m.Name)
	if e != nil {
		return e
	}
	if runtime == nil {
		specs = append(specs, serviceSpec(rc.Cluster, m, tok.Token))
	} else {
		if err := verifyServiceOwnership(ctx, rc, m, runtime, false); err != nil {
			return err
		}
		m.ServiceID = runtime.ID
	}
	if len(specs) == 0 {
		m.Stage = "imported"
		s.resetPlatformRetry(&pending{Action: "import", Member: m.Name})
		return nil
	}
	op, e := rc.Backend.ImportServices(ctx, specs)
	if e != nil {
		return e
	}
	m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
	m.Containers = 1
	if m.Role == agentapi.RoleRouter {
		m.Containers = rc.Cluster.Spec.Components.Router.Replicas
	}
	s.Pending = &pending{Op: op, Action: "import", Member: m.Name}
	return nil
}
func (p *Provider) replication(ctx context.Context, rc *engine.Context, s *internalState, m *member) (mongoadmin.Status, error) {
	c, e := p.dial(ctx, rc, s, m.Set, "clusterMonitor")
	if e != nil {
		return mongoadmin.Status{}, e
	}
	defer c.Disconnect(ctx)
	return mongoadmin.GetStatus(ctx, c)
}
func caught(s mongoadmin.Status, name string) bool {
	for _, x := range s.Members {
		if x.Name == name+":27017" {
			return x.Health == 1 && (x.State == "SECONDARY" || x.State == "PRIMARY") && s.Lag(x) <= 10
		}
	}
	return false
}
func (p *Provider) prepareDisruption(ctx context.Context, rc *engine.Context, s *internalState, m *member) (bool, error) {
	if m.Role == agentapi.RoleRouter {
		return false, nil
	}
	c, e := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
	if e != nil {
		return false, e
	}
	defer c.Disconnect(ctx)
	if e = mongoadmin.SafeToRestart(ctx, c, m.Name+":27017"); e != nil {
		return false, e
	}
	st, e := mongoadmin.GetStatus(ctx, c)
	if e != nil {
		return false, e
	}
	if len(st.Members) == 1 {
		return false, nil
	}
	for _, x := range st.Members {
		if x.Name == m.Name+":27017" && x.State == "PRIMARY" {
			return true, mongoadmin.StepDown(ctx, c)
		}
	}
	return false, nil
}

// Sync advances one durable step, never combines two disruptive actions in one invocation.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	return p.sync(ctx, rc, p.prepareDisruption, p.replication)
}

func (p *Provider) sync(ctx context.Context, rc *engine.Context,
	prepareDisruption func(context.Context, *engine.Context, *internalState, *member) (bool, error),
	replication func(context.Context, *engine.Context, *internalState, *member) (mongoadmin.Status, error),
) (result engine.Result, err error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	result.RequeueAfter = 5 * time.Second
	if s.UID == "" {
		s.UID, e = secrets.RandomString(12)
		if e != nil {
			return result, e
		}
		s.PBMPrefix = "karakoram/" + rc.Cluster.Name + "-" + s.UID
		s.Phase = "secrets"
		plan(rc.Cluster, s)
		if e := allocateNewPlans(ctx, rc, s, s.Members); e != nil {
			return result, e
		}
		return result, nil
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return result, e
	}
	if s.Suspended {
		s.Suspended = false
		s.CleanupBackups = false
		s.CleanedStorages = nil
		for _, m := range s.Members {
			m.PublicHash = ""
			m.ConfigVersion = 0
		}
		for _, m := range s.Members {
			m.Stage = "planned"
		}
		s.Phase = "resume"
		return result, nil
	}
	previousMembers := len(s.Members)
	if !s.ReplaceActive && plan(rc.Cluster, s) {
		if e := allocateNewPlans(ctx, rc, s, s.Members[previousMembers:]); e != nil {
			return result, e
		}
		s.Phase = "scaling"
		return result, nil
	}
	if e := skipOccupiedPlans(ctx, rc, s, s.Members); e != nil {
		return result, e
	}
	for _, m := range s.Members {
		if len(m.Name) > 25 || len(m.Volume) > 25 {
			return result, invalid("member or volume hostname exceeds 25 characters: %s", m.Name)
		}
	}
	if e = p.ensureSecrets(ctx, rc, s); e != nil {
		return result, e
	}
	// Upgrade legacy plans before any platform mutation. This also prevents a
	// later member's collision from being discovered after changing an earlier one.
	for _, m := range s.Members {
		if err := verifyMemberServices(ctx, rc, m); err != nil {
			return result, err
		}
	}
	// Finish the last gated reload before allowing any other disruption.
	// The publish stage is durable so secret-write/reload failures cannot
	// accidentally let a later member proceed.
	for _, m := range ordered(s) {
		switch m.Stage {
		case "reload-publish":
			stepped, err := prepareDisruption(ctx, rc, s, m)
			if err != nil || stepped {
				return result, err
			}
			return result, reloadMember(ctx, rc, s, m)
		case "reload-wait":
			changed, err := reconcileConfig(ctx, rc, s)
			if err != nil {
				return result, err
			}
			if changed {
				// A rejected configuration must be repairable on this same
				// member without unblocking any other member's disruption.
				recordReloadIntent(s, m, m.ConfigVersion)
				return result, nil
			}
			st, err := agentStatus(ctx, rc, m.Name)
			if err != nil || st.ConfigVersion < m.ConfigVersion || !running(st, m.Role) {
				return result, nil
			}
			if m.Role != agentapi.RoleRouter {
				rs, err := replication(ctx, rc, s, m)
				if err != nil || !caught(rs, m.Name) {
					return result, nil
				}
			}
			m.Stage = "active"
			return result, nil
		}
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		// mongos startup waits for an initialized config-server replica set.
		// Leave routers untouched until its system users are ready as well.
		if m.Role == agentapi.RoleRouter && s.Sets["cfg"].Stage != "ready" {
			continue
		}
		switch m.Stage {
		case "planned":
			s.Phase = "import"
			return result, p.importMember(ctx, rc, s, m)
		case "imported":
			s.Phase = "deploy"
			return result, p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version)
		case "deployed":
			s.Phase = "agents"
			st, e := agentStatus(ctx, rc, m.Name)
			if e != nil || !running(st, m.Role) {
				return result, nil
			}
			m.Stage = "healthy"
			return result, nil
		case "upgrade-wait":
			st, e := agentStatus(ctx, rc, m.Name)
			if e != nil || !running(st, m.Role) || st.EngineVersion != m.Version {
				return result, nil
			}
			if m.Role != agentapi.RoleRouter {
				rs, e := p.replication(ctx, rc, s, m)
				if e != nil || !caught(rs, m.Name) {
					return result, nil
				}
			}
			m.Stage = "active"
			return result, nil
		}
	}
	for _, m := range ordered(s) {
		if m.Removing || m.Role == agentapi.RoleRouter {
			continue
		}
		set := s.Sets[m.Set]
		if set.Stage == "planned" {
			s.Phase = "bootstrap"
			members := s.setMembers(m.Set)
			req := agentapi.BootstrapRequest{ReplSetName: m.Set, ConfigServer: m.Role == agentapi.RoleConfigServer}
			for _, x := range members {
				req.Members = append(req.Members, agentapi.BootstrapMember{ID: x.Index, Host: x.Name + ":27017"})
			}
			pw, e := secret(ctx, rc, "users/userAdmin")
			if e != nil {
				return result, e
			}
			req.UserAdmin = agentapi.Credentials{Username: "userAdmin", Password: string(pw), Database: "admin"}
			v, e := bootstrapSet(ctx, rc, members, req)
			if e != nil {
				return result, e
			}
			if !v.Initialized || v.Primary != "" {
				return result, nil
			}
			set.Stage = "users"
			return result, nil
		}
		if set.Stage == "users" {
			s.Phase = "users"
			c, e := p.dial(ctx, rc, s, m.Set, "userAdmin")
			if e != nil {
				return result, e
			}
			defer c.Disconnect(ctx)
			if e = users.EnsurePBMRole(ctx, c); e != nil {
				return result, e
			}
			pw, e := passwords(ctx, rc)
			if e != nil {
				return result, e
			}
			if e = users.EnsureSystemUsers(ctx, c, pw); e != nil {
				return result, e
			}
			set.Stage = "ready"
			s.ConfigVersion++
			for _, x := range s.setMembers(m.Set) {
				x.Stage = "active"
			}
			return result, nil
		}
		if set.Stage == "ready" && m.Stage == "healthy" {
			s.Phase = "join"
			c, e := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
			if e != nil {
				return result, e
			}
			defer c.Disconnect(ctx)
			if e = mongoadmin.AddNonVoting(ctx, c, m.Index, m.Name+":27017"); e != nil {
				return result, e
			}
			m.Stage = "syncing"
			return result, nil
		}
		if m.Stage == "syncing" {
			// Retire the old member before promoting its replacement, even
			// after failure cleanup releases the replacement operation gate.
			if slices.ContainsFunc(s.Members, func(x *member) bool { return x.Set == m.Set && x.Removing }) {
				continue
			}
			rs, e := p.replication(ctx, rc, s, m)
			if e != nil || !caught(rs, m.Name) {
				return result, nil
			}
			if s.ReplaceActive && !m.ReplacingReady {
				m.Stage = "replacement-caught-up"
				return result, nil
			}
			c, e := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
			if e != nil {
				return result, e
			}
			defer c.Disconnect(ctx)
			if e = mongoadmin.Promote(ctx, c, m.Name+":27017"); e != nil {
				return result, e
			}
			m.Stage = "active"
			return result, nil
		}
	}
	if rc.Cluster.Spec.Topology.Type == "sharded" {
		for _, m := range ordered(s) {
			if m.Role == agentapi.RoleRouter && m.Stage == "healthy" {
				m.Stage = "active"
			}
			if m.Role != agentapi.RoleReplicaSet || s.Sets[m.Set].Added {
				continue
			}
			s.Phase = "add-shard"
			c, e := p.dial(ctx, rc, s, "router", "clusterAdmin")
			if e != nil {
				return result, e
			}
			defer c.Disconnect(ctx)
			if e = mongoadmin.AddShard(ctx, c, m.Set, strings.Join(hosts(s.setMembers(m.Set)), ",")); e != nil {
				return result, e
			}
			s.Sets[m.Set].Added = true
			return result, nil
		}
	}
	for _, m := range removingMembers(s) {
		if err := verifyMemberServices(ctx, rc, m); err != nil {
			return result, err
		}
		s.Phase = "remove"
		switch m.Stage {
		case "delete-volume":
			if m.Volume != "" && !m.VolumeOrphaned {
				v, e := rc.Backend.GetService(ctx, m.Volume)
				if e != nil {
					return result, e
				}
				if v != nil {
					op, e := rc.Backend.DeleteService(ctx, m.Volume)
					s.Pending = &pending{Op: op, Action: "delete-volume", Member: m.Name}
					if e != nil {
						return result, s.failPending(rc, e.Error())
					}
					return result, nil
				}
			}
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return result, err
			}
			removeRecord(s, m.Name)
			return result, nil
		case "removed":
			if e := rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
				return result, e
			}
			m.Stage = "unexposed"
			m.PublicPort = 0
			return result, nil
		case "unexposed":
			op, e := rc.Backend.DeleteService(ctx, m.Name)
			if e != nil {
				return result, e
			}
			s.Pending = &pending{Op: op, Action: "delete-runtime", Member: m.Name}
			return result, nil
		default:
			c, e := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
			if e != nil {
				return result, e
			}
			defer c.Disconnect(ctx)
			rs, e := mongoadmin.GetStatus(ctx, c)
			if e != nil {
				return result, e
			}
			for _, x := range rs.Members {
				if x.Name == m.Name+":27017" && x.State == "PRIMARY" {
					return result, mongoadmin.StepDown(ctx, c)
				}
			}
			if e = mongoadmin.Remove(ctx, c, m.Name+":27017"); e != nil {
				return result, e
			}
			m.Stage = "removed"
			return result, nil
		}
	}
	// Upgrades are ordered config servers -> shards -> routers; within each set choose secondaries first.
	for _, m := range ordered(s) {
		if m.Version == rc.Cluster.Spec.Version {
			continue
		}
		s.Phase = "upgrade"
		if m.Role != agentapi.RoleRouter && major(m.Version) != major(rc.Cluster.Spec.Version) {
			c, err := p.dial(ctx, rc, s, m.Set, "clusterAdmin")
			if err != nil {
				return result, err
			}
			fcv, err := mongoadmin.FCV(ctx, c)
			_ = c.Disconnect(ctx)
			if err != nil {
				return result, err
			}
			if fcv != major(m.Version) {
				return result, fmt.Errorf("%w: raise FCV to %s before upgrading to %s", engine.ErrConflict, major(m.Version), major(rc.Cluster.Spec.Version))
			}
		}
		if rc.Cluster.Spec.Topology.Type == "sharded" && !s.BalancerStopped {
			v, e := p.Balancer(ctx, rc)
			if e != nil {
				return result, e
			}
			s.BalancerWasEnabled = v.Enabled
			if e = p.SetBalancer(ctx, rc, false); e != nil {
				return result, e
			}
			s.BalancerStopped = true
			return result, nil
		}
		candidate := m
		if m.Role != agentapi.RoleRouter {
			rs, e := p.replication(ctx, rc, s, m)
			if e != nil {
				return result, e
			}
			for _, x := range s.setMembers(m.Set) {
				if x.Version == rc.Cluster.Spec.Version {
					continue
				}
				for _, obs := range rs.Members {
					if obs.Name == x.Name+":27017" && obs.State == "SECONDARY" {
						candidate = x
						break
					}
				}
			}
		}
		stepped, e := prepareDisruption(ctx, rc, s, candidate)
		if e != nil || stepped {
			return result, e
		}
		return result, p.deploy(ctx, rc, s, candidate, "upgrade", rc.Cluster.Spec.Version)
	}
	if s.BalancerStopped {
		if s.BalancerWasEnabled {
			if e = p.SetBalancer(ctx, rc, true); e != nil {
				return result, e
			}
		}
		s.BalancerStopped = false
		return result, nil
	}
	for _, m := range ordered(s) {
		comp := component(rc.Cluster.Spec, m)
		r := runtimeResources(comp.Resources)
		if !reflect.DeepEqual(r, m.Resources) {
			s.Phase = "resources"
			stepped, e := prepareDisruption(ctx, rc, s, m)
			if e != nil || stepped {
				return result, e
			}
			if m.Role != agentapi.RoleRouter && r.MaxRAM < m.Resources.MaxRAM {
				cache := max(0.25, 0.5*(r.MaxRAM-0.5))
				if m.CacheGB > cache {
					s.ConfigVersion++
					activateConfig(rc.Cluster, s, m)
					if e = save(rc.Cluster, s); e != nil {
						return result, e
					}
					a, err := agent(ctx, rc, m.Name)
					if err != nil {
						return result, err
					}
					defer a.Close()
					return result, a.Reload(ctx)
				}
				st, err := agentStatus(ctx, rc, m.Name)
				if err != nil || st.ConfigVersion < m.ConfigVersion || !running(st, m.Role) {
					return result, nil
				}
				rs, err := p.replication(ctx, rc, s, m)
				if err != nil || !caught(rs, m.Name) {
					return result, nil
				}
			}
			op, e := rc.Backend.SetResources(ctx, m.Name, r)
			if e != nil {
				v := &pending{Action: "resources", Member: m.Name, Resources: r}
				if wait := s.scheduleCPUModeCooldown(rc, v, e); wait != nil {
					return result, wait
				}
				return result, e
			}
			s.Pending = &pending{Op: op, Action: "resources", Member: m.Name, Resources: r}
			return result, nil
		}
		if m.Volume != "" && (m.Disk.MinDisk != comp.Resources.MinDisk || m.Disk.MaxDisk != comp.Resources.MaxDisk) {
			r := domain.Resources{MinDisk: comp.Resources.MinDisk, MaxDisk: comp.Resources.MaxDisk}
			op, e := rc.Backend.SetResources(ctx, m.Volume, r)
			if e != nil {
				return result, e
			}
			s.Pending = &pending{Op: op, Action: "disk", Member: m.Name, Resources: comp.Resources}
			return result, nil
		}
		if m.Role == agentapi.RoleRouter && m.Containers != comp.Replicas {
			op, e := rc.Backend.SetContainerCount(ctx, m.Name, comp.Replicas, comp.Replicas)
			if e != nil {
				return result, e
			}
			s.Pending = &pending{Op: op, Action: "containers", Member: m.Name, Containers: comp.Replicas}
			return result, nil
		}
	}
	changed, e := reconcileConfig(ctx, rc, s)
	if e != nil || changed {
		return result, e
	}
	for _, m := range ordered(s) {
		st, e := agentStatus(ctx, rc, m.Name)
		if e != nil {
			return result, nil
		}
		pending, err := hasPendingCertificate(ctx, rc, m)
		if err != nil {
			return result, err
		}
		if pending || st.ConfigVersion < s.ConfigVersion || !running(st, m.Role) {
			s.Phase = "reload"
			stepped, e := prepareDisruption(ctx, rc, s, m)
			if e != nil || stepped {
				return result, e
			}
			recordReloadIntent(s, m, st.ConfigVersion)
			// Return before touching active secrets. Only the reconciler can
			// commit the intent; save() merely updates this in-memory cluster.
			return result, nil
		}
		m.ConfigVersion = s.ConfigVersion
	}
	if s.TargetTLS != "" {
		s.TLSMode = nextTLSMode(s.TLSMode, s.TargetTLS)
		if s.TLSMode == s.TargetTLS {
			s.TargetTLS = ""
		}
		s.ConfigVersion++
		return result, nil
	}
	if e = refreshFreshBase(ctx, rc); e != nil {
		return result, e
	}
	storage := rc.Cluster.Spec.Backup.Storage
	if storage == "" {
		storage = s.PBMStorage
	}
	if storage != "" {
		env, e := p.pbmEnvForStorage(ctx, rc, s, rc.Cluster.Ref().Namespace, storage)
		if e != nil {
			return result, e
		}
		h, e := ensurePBM(ctx, env, s.PBMHash)
		if e != nil {
			return result, e
		}
		if h != s.PBMHash {
			s.PBMHash = h
			s.PBMStorage = rc.Cluster.Spec.Backup.Storage
			return result, nil
		}
	}
	for _, m := range ordered(s) {
		ex := rc.Cluster.Spec.Exposure
		if rc.Cluster.Spec.Topology.Type == "sharded" && m.Role != agentapi.RoleRouter {
			ex.Public = false
		}
		h := hash(ex)
		if m.PublicHash == h && (ex.Public || m.PublicPort == 0) {
			continue
		}
		if ex.Public {
			port, err := p.allocatePublicPort(ctx, rc, s, m)
			if err != nil {
				return result, err
			}
			if m.PublicPort != port {
				m.PublicPort = port
				return result, nil
			}
			if e = rc.Backend.EnsurePublicPort(ctx, m.Name, infra.PublicPort{Port: port, TargetPort: 27017, Protocol: "tcp", IPType: ex.IPType, Allowlist: ex.Allowlist}); e != nil {
				return result, e
			}
			m.PublicPort = port
		} else {
			if e = rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
				return result, e
			}
			m.PublicPort = 0
		}
		m.PublicHash = h
		return result, nil
	}
	if d := rc.Cluster.Spec.DataSource; d != nil && !s.CloneQueued {
		name := "initial-restore"
		r := &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, SourceNamespace: d.Namespace, Name: name, Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: d.Backup, PITRTime: d.PITRTime, SourceCluster: d.Cluster}, Phase: domain.PhasePending}
		payload, _ := json.Marshal(map[string]string{"restore": name})
		op := &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, ID: rc.Cluster.Ref().Key() + "-" + s.UID + "-initial-restore", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: payload, Phase: domain.PhasePending}
		if _, e = rc.Store.GetOperation(ctx, op.ID); missing(e) {
			if e = rc.Store.EnqueueRestore(ctx, r, op); e != nil {
				return result, e
			}
		} else if e != nil {
			return result, e
		}
		s.CloneQueued = true
		return result, nil
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return result, e
	}
	for _, v := range retained {
		if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), v.Member); e != nil {
			return result, e
		}
	}
	s.Phase = "ready"
	rc.Cluster.Status.Phase = domain.PhaseReady
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}

func bootstrapSet(ctx context.Context, rc *engine.Context, members []*member, req agentapi.BootstrapRequest) (agentapi.BootstrapResponse, error) {
	name := members[0].Name
	seen := map[string]bool{}
	for !seen[name] {
		seen[name] = true
		a, err := agent(ctx, rc, name)
		if err != nil {
			return agentapi.BootstrapResponse{}, err
		}
		v, err := a.Bootstrap(ctx, req)
		a.Close()
		if err != nil || v.Primary == "" {
			return v, err
		}
		found := false
		for _, m := range members {
			if v.Primary == m.Name+":27017" {
				name, found = m.Name, true
				break
			}
		}
		if !found {
			return v, invalid("bootstrap primary %q is not a planned member", v.Primary)
		}
	}
	return agentapi.BootstrapResponse{}, nil // leadership changed; retry on the next reconciliation
}
func publicHost(ip string, port int) string { return net.JoinHostPort(ip, fmt.Sprint(port)) }

// reloadMember publishes only the member whose disruption was approved.
func reloadMember(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	if err := applyProvidedCertificate(ctx, rc, m); err != nil {
		return err
	}
	if err := publishRenewedCertificates(ctx, rc, m); err != nil {
		return err
	}
	activateConfig(rc.Cluster, s, m)
	m.Stage = "reload-wait"
	if err := save(rc.Cluster, s); err != nil {
		return err
	}
	a, err := agent(ctx, rc, m.Name)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Reload(ctx)
}

func recordReloadIntent(s *internalState, m *member, observedVersion int64) {
	// Staged secrets may survive a crash before their version increment is
	// persisted. Always require an acknowledgement newer than the old config.
	s.ConfigVersion = max(s.ConfigVersion, m.ConfigVersion+1, observedVersion+1)
	m.Stage = "reload-publish"
}

func reconcileConfig(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	materialHash := ""
	if rc.Cluster.Spec.TLS.CA == "userProvided" {
		crt, err := secret(ctx, rc, "tls/user.crt")
		if err != nil && !missing(err) {
			return false, err
		}
		materialHash = hash(string(crt))
	}
	configHash := hash(struct {
		Engine          string
		TLS             domain.TLSConfig
		RAM             domain.Components
		CertificateHash string
		ConfigDB        string
	}{rc.Cluster.Spec.EngineConfig, rc.Cluster.Spec.TLS, rc.Cluster.Spec.Components, materialHash, strings.Join(hosts(s.setMembers("cfg")), ",")})
	if configHash == s.ConfigHash {
		return false, nil
	}
	s.ConfigHash = configHash
	s.TargetTLS = ""
	if s.TLSMode != rc.Cluster.Spec.TLS.Mode {
		s.TargetTLS = rc.Cluster.Spec.TLS.Mode
		s.TLSMode = nextTLSMode(s.TLSMode, s.TargetTLS)
		if s.TLSMode == s.TargetTLS {
			s.TargetTLS = ""
		}
	}
	s.ConfigVersion++
	s.Phase = "reload"
	return true, nil
}

// nextTLSMode advances only between modes that can communicate in both
// directions during a rolling restart. Finish each cluster-wide pass before
// advancing again: disabled <-> allowTLS <-> preferTLS <-> requireTLS.
// allowTLS is an internal transition mode, not a public spec option.
func nextTLSMode(current, target string) string {
	switch current {
	case "disabled":
		if target != "disabled" {
			return "allowTLS"
		}
	case "allowTLS":
		if target == "requireTLS" {
			return "preferTLS"
		}
	case "preferTLS":
		if target == "disabled" {
			return "allowTLS"
		}
	case "requireTLS":
		if target == "disabled" {
			return "preferTLS"
		}
	}
	return target
}
