// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var _ engine.Provider = (*Provider)(nil)
var _ engine.DataPlane = (*Provider)(nil)
var _ engine.AgentConfigSource = (*Provider)(nil)

func connection(c *domain.Cluster, s *internalState) domain.Connection {
	set := "rs0"
	h := hosts(s.setMembers(set))
	if c.Spec.Topology.Type == "sharded" {
		set = ""
		h = []string{c.Name + "r:27017"}
	}
	q := url.Values{"authSource": []string{"admin"}}
	if set != "" {
		q.Set("replicaSet", set)
	}
	if s.TLSMode != "disabled" {
		q.Set("tls", "true")
	}
	return domain.Connection{Hosts: h, ReplicaSet: set, URI: "mongodb://" + strings.Join(h, ",") + "/?" + q.Encode(), PublicHosts: c.Status.Connection.PublicHosts}
}

// Connection reveals databaseAdmin credentials and the verification CA, never application passwords.
func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.ConnectionDetails{}, e
	}
	pw, e := secret(ctx, rc, "users/databaseAdmin")
	if e != nil {
		return domain.ConnectionDetails{}, e
	}
	ca, e := secret(ctx, rc, "tls/ca.crt")
	if e != nil {
		return domain.ConnectionDetails{}, e
	}
	c := connection(rc.Cluster, s)
	uri := strings.Replace(c.URI, "mongodb://", "mongodb://"+url.UserPassword("databaseAdmin", string(pw)).String()+"@", 1)
	return domain.ConnectionDetails{Connection: c, CACertificate: string(ca), Credentials: []domain.ConnectionCredential{{Username: "databaseAdmin", Password: string(pw), Database: "admin", URI: uri}}}, nil
}

// SystemUsers returns provider-managed role assignments without credentials.
func (*Provider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	out := []domain.SystemUser{}
	for _, u := range users.SystemUserDefs() {
		out = append(out, domain.SystemUser{Name: u.Name, Roles: u.Roles})
	}
	return out, nil
}
func (p *Provider) userClient(ctx context.Context, rc *engine.Context) (*mongo.Client, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return nil, e
	}
	set := "rs0"
	if rc.Cluster.Spec.Topology.Type == "sharded" {
		set = "router"
	}
	return p.dial(ctx, rc, s, set, "userAdmin")
}

// ListDatabaseUsers lists application users.
func (p *Provider) ListDatabaseUsers(ctx context.Context, rc *engine.Context) ([]domain.DatabaseUser, error) {
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	defer c.Disconnect(ctx)
	return users.ListUsers(ctx, c)
}

// GetDatabaseUser retrieves one application user.
func (p *Provider) GetDatabaseUser(ctx context.Context, rc *engine.Context, db, name string) (*domain.DatabaseUser, error) {
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	defer c.Disconnect(ctx)
	return users.GetUser(ctx, c, db, name)
}

// CreateDatabaseUser creates a user and returns its supplied or generated password once.
func (p *Provider) CreateDatabaseUser(ctx context.Context, rc *engine.Context, u domain.DatabaseUser, pw string) (string, error) {
	if pw == "" {
		var e error
		pw, e = secrets.Password()
		if e != nil {
			return "", e
		}
	}
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return "", e
	}
	defer c.Disconnect(ctx)
	if e = users.CreateUser(ctx, c, u, pw); e != nil {
		return "", e
	}
	return pw, nil
}

// UpdateDatabaseUser replaces roles and optionally the password.
func (p *Provider) UpdateDatabaseUser(ctx context.Context, rc *engine.Context, u domain.DatabaseUser, pw string) error {
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return users.UpdateUser(ctx, c, u, pw)
}

// DeleteDatabaseUser removes an application user.
func (p *Provider) DeleteDatabaseUser(ctx context.Context, rc *engine.Context, db, name string) error {
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return users.DeleteUser(ctx, c, db, name)
}

// ListDatabaseRoles lists custom roles, excluding provider-managed roles.
func (p *Provider) ListDatabaseRoles(ctx context.Context, rc *engine.Context) ([]domain.DatabaseRole, error) {
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	defer c.Disconnect(ctx)
	roles, e := users.ListRoles(ctx, c)
	if e != nil {
		return nil, e
	}
	out := roles[:0]
	for _, r := range roles {
		if !users.IsSystemRole(r.Database, r.Name) {
			out = append(out, r)
		}
	}
	return out, nil
}

// GetDatabaseRole retrieves a custom role; provider-managed roles are hidden.
func (p *Provider) GetDatabaseRole(ctx context.Context, rc *engine.Context, db, name string) (*domain.DatabaseRole, error) {
	if users.IsSystemRole(db, name) {
		return nil, engine.ErrNotFound
	}
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	defer c.Disconnect(ctx)
	return users.GetRole(ctx, c, db, name)
}

// CreateDatabaseRole creates a custom role.
func (p *Provider) CreateDatabaseRole(ctx context.Context, rc *engine.Context, r domain.DatabaseRole) error {
	if users.IsSystemRole(r.Database, r.Name) {
		return invalid("role name %q is reserved for a system role", r.Name)
	}
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return users.CreateRole(ctx, c, r)
}

// UpdateDatabaseRole replaces a custom role's privileges and inherited roles.
func (p *Provider) UpdateDatabaseRole(ctx context.Context, rc *engine.Context, r domain.DatabaseRole) error {
	if users.IsSystemRole(r.Database, r.Name) {
		return engine.ErrNotFound
	}
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return users.UpdateRole(ctx, c, r)
}

// DeleteDatabaseRole removes a custom role.
func (p *Provider) DeleteDatabaseRole(ctx context.Context, rc *engine.Context, db, name string) error {
	if users.IsSystemRole(db, name) {
		return engine.ErrNotFound
	}
	c, e := p.userClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return users.DeleteRole(ctx, c, db, name)
}

// Balancer reports sharded-cluster balancer state.
func (p *Provider) Balancer(ctx context.Context, rc *engine.Context) (domain.BalancerStatus, error) {
	if rc.Cluster.Spec.Topology.Type != "sharded" {
		return domain.BalancerStatus{}, engine.ErrNotSupported
	}
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.BalancerStatus{}, e
	}
	c, e := p.dial(ctx, rc, s, "router", "clusterAdmin")
	if e != nil {
		return domain.BalancerStatus{}, e
	}
	defer c.Disconnect(ctx)
	mode, running, e := mongoadmin.Balancer(ctx, c)
	return domain.BalancerStatus{Enabled: mode != "off", Running: running, Mode: mode}, e
}

// SetBalancer enables or disables the sharded-cluster balancer.
func (p *Provider) SetBalancer(ctx context.Context, rc *engine.Context, enabled bool) error {
	if rc.Cluster.Spec.Topology.Type != "sharded" {
		return engine.ErrNotSupported
	}
	s, e := load(rc.Cluster)
	if e != nil {
		return e
	}
	c, e := p.dial(ctx, rc, s, "router", "clusterAdmin")
	if e != nil {
		return e
	}
	defer c.Disconnect(ctx)
	return mongoadmin.SetBalancer(ctx, c, enabled)
}

type hooks struct {
	p  *Provider
	rc *engine.Context
}

func (h hooks) SetBalancer(ctx context.Context, enabled bool) error {
	return h.p.SetBalancer(ctx, h.rc, enabled)
}
func (h hooks) ResetSystemUsers(ctx context.Context, recoveryIdentity string) error {
	rc := h.rc
	s, e := load(rc.Cluster)
	if e != nil {
		return e
	}
	// PBM preserves its authenticating account during logical recovery. Its
	// pbmAnyAction role can reset users without knowing snapshot-time passwords.
	// Physical recovery may replace it; use the protected snapshot identity then.
	var recoveryPassword []byte
	if recoveryIdentity != "" {
		recoveryPassword, e = rc.Secrets.GetClusterSecret(ctx, domain.ParseClusterKey(recoveryIdentity), "users/userAdmin")
		if e != nil {
			return e
		}
	}
	ca, e := secret(ctx, rc, "tls/ca.crt")
	if e != nil {
		return e
	}
	pwMap, e := passwords(ctx, rc)
	if e != nil {
		return e
	}
	for set, state := range s.Sets {
		if state.Stage != "ready" {
			continue
		}
		c, e := h.p.dial(ctx, rc, s, set, "backup")
		if e != nil && len(recoveryPassword) > 0 {
			c, e = mongoadmin.Dial(ctx, mongoadmin.DialOptions{Hosts: hosts(s.setMembers(set)), Dialer: rc.Dialer, ReplicaSet: set, Username: "userAdmin", Password: string(recoveryPassword), CAPEM: ca, TLS: s.TLSMode != "disabled"})
		}
		if e != nil {
			// A resumed reset may already have restored the target userAdmin.
			c, e = h.p.dial(ctx, rc, s, set, "userAdmin")
		}
		if e != nil {
			return e
		}
		e = users.EnsureSystemUsers(ctx, c, pwMap)
		_ = c.Disconnect(ctx)
		if e != nil {
			return e
		}
	}
	return nil
}
func (p *Provider) pbmEnv(ctx context.Context, rc *engine.Context, s *internalState) (pbm.Env, error) {
	return p.pbmEnvInNamespace(ctx, rc, s, rc.Cluster.Ref().Namespace)
}

func (p *Provider) pbmEnvInNamespace(ctx context.Context, rc *engine.Context, s *internalState, namespace string) (pbm.Env, error) {
	env := pbm.Env{RC: rc, Sharded: rc.Cluster.Spec.Topology.Type == "sharded", Prefix: s.PBMPrefix, PITR: rc.Cluster.Spec.Backup.PITR, Hooks: hooks{p, rc}}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		if m.Role == agentapi.RoleRouter {
			env.Routers = append(env.Routers, m.Name)
		} else {
			env.Nodes = append(env.Nodes, pbm.Node{Member: m.Name, ReplSet: m.Set})
		}
	}
	name := rc.Cluster.Spec.Backup.Storage
	if name == "" && rc.Cluster.Spec.DataSource != nil {
		d := rc.Cluster.Spec.DataSource
		sourceRef := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
		if sourceRef.Namespace == "" {
			sourceRef.Namespace = namespace
		}
		namespace = sourceRef.Namespace
		if d.Backup != "" {
			b, err := rc.Store.GetBackup(ctx, sourceRef, d.Backup)
			if err != nil {
				return env, err
			}
			name = b.Storage
		} else {
			source, err := rc.Store.GetCluster(ctx, sourceRef)
			if err != nil {
				return env, err
			}
			name = source.Spec.Backup.Storage
		}
	}
	storage, e := rc.Store.GetBackupStorage(ctx, namespace, name)
	if e != nil {
		return env, e
	}
	if namespace == rc.Cluster.Ref().Namespace {
		known := false
		for _, used := range s.UsedStorages {
			known = known || used == name
		}
		if !known {
			s.UsedStorages = append(s.UsedStorages, name)
		}
	}
	env.Storage = *storage
	env.Credentials = storage.Credentials
	if storage.Prefix != "" {
		env.Prefix = strings.TrimRight(storage.Prefix, "/") + "/" + s.PBMPrefix
	}
	var raw map[string]any
	_ = jsonInternal(rc.Cluster, &raw)
	if raw["freshBaseBackupPending"] == true {
		env.PITR.Enabled = false
	}
	return env, nil
}
func jsonInternal(c *domain.Cluster, v any) error { return decodeJSON(c.Internal, v) }
func ensurePBM(ctx context.Context, env pbm.Env, hash string) (string, error) {
	return pbm.EnsureConfig(ctx, env, hash)
}

// PITRWindow retrieves PBM's continuous recoverable windows.
func (p *Provider) PITRWindow(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	if rc.Cluster.Spec.Backup.Storage == "" && rc.Cluster.Spec.DataSource == nil {
		return []domain.TimeWindow{}, nil
	}
	s, e := load(rc.Cluster)
	if e != nil {
		return nil, e
	}
	env, e := p.pbmEnv(ctx, rc, s)
	if e != nil {
		return nil, e
	}
	return pbm.PITRWindows(ctx, env)
}

// Placement returns honest best-effort grouping without anti-affinity promises.
func (p *Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.Placement{}, e
	}
	v := domain.Placement{Groups: map[string][]string{}}
	unknown := false
	for _, m := range ordered(s) {
		st, e := agentStatus(ctx, rc, m.Name)
		if e != nil || st.HostFingerprint == "" {
			unknown = true
			v.Groups["unknown"] = append(v.Groups["unknown"], m.Name)
			continue
		}
		v.Groups[st.HostFingerprint] = append(v.Groups[st.HostFingerprint], m.Name)
	}
	warnings := []string{"Zerops does not guarantee anti-affinity; host fingerprints are best-effort, not proof of host-failure tolerance"}
	for k, g := range v.Groups {
		if k != "unknown" && len(g) > 1 {
			warnings = append(warnings, fmt.Sprintf("members %s appear co-located", strings.Join(g, ", ")))
		}
	}
	if unknown {
		warnings = append(warnings, "physical-host placement is unknown for some members")
	}
	v.Warning = strings.Join(warnings, "; ")
	return v, nil
}

// MemberLogs retrieves bounded node process logs.
func (*Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return nil, e
	}
	m := s.find(name)
	if m == nil {
		return nil, engine.ErrNotFound
	}
	// Before the runtime is deployed there is no agent to ask; report the
	// state conflict instead of an internal error.
	if m.Stage == "planned" || m.Stage == "imported" {
		return nil, fmt.Errorf("%w: member %s is not deployed yet", engine.ErrConflict, name)
	}
	if lines < 1 || lines > 10000 {
		return nil, invalid("log lines must be 1..10000")
	}
	a, e := agent(ctx, rc, name)
	if e != nil {
		return nil, e
	}
	defer a.Close()
	logs, e := a.Logs(ctx, process, lines)
	if e != nil {
		return nil, e
	}
	out := make([]domain.MemberLogLine, 0, len(logs.Lines))
	for _, x := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: x.Timestamp, Member: name, Process: process, Stream: x.Stream, Message: x.Message})
	}
	return out, nil
}
