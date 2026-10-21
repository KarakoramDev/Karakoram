// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var _ Access = (*Core)(nil)

func accessFence(ctx context.Context, rc *engine.Context) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if rc.Cluster.Deleting || s.Suspended || s.Pending != nil || s.GaleraTLS != nil || s.Formation.State != "formed" || s.Formation.reforming() {
		return engine.ErrUnavailable
	}
	switch rc.Cluster.Status.Phase {
	case domain.PhaseRestoring, domain.PhaseUpgrading, domain.PhaseSuspended, domain.PhaseDeleting:
		return engine.ErrUnavailable
	}
	backup, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if backup.RestoreOperation != nil {
		return engine.ErrUnavailable
	}
	for _, m := range s.data() {
		if m.Offline || m.Stage != "joined" {
			return engine.ErrUnavailable
		}
	}
	if rc.Cluster.OperationID != "" && rc.Store != nil {
		op, err := rc.Store.GetOperation(ctx, rc.Cluster.OperationID)
		if err != nil {
			return engine.ErrUnavailable
		}
		if op != nil && !op.Terminal() && op.Phase == domain.PhaseRunning && (op.Kind == domain.OperationBackup || op.Kind == domain.OperationRestore || op.Kind == domain.OperationUpgrade || op.Kind == "initial-restore") {
			return engine.ErrUnavailable
		}
	}
	return nil
}

func (p *Core) accessAction(ctx context.Context, rc *engine.Context, action string, request myspec.AccessRequest, output any) error {
	if err := accessFence(ctx, rc); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if rc.Agents == nil {
		return engine.ErrUnavailable
	}
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil || st.MemberState != "ONLINE" || st.ReadOnly || st.SuperReadOnly {
			continue
		}
		if clustered(s.Topology) && !onlinePrimary(st) {
			continue
		}
		if s.Topology != "single" && !clustered(s.Topology) {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return engine.ErrUnavailable
		}
		defer a.Close()
		if err = a.Engine(ctx, "mysql", action, request, output); err != nil {
			switch {
			case strings.HasSuffix(err.Error(), "HTTP 400"), strings.HasSuffix(err.Error(), "HTTP 403"):
				return engine.Invalid("MySQL rejected the access request")
			case strings.HasSuffix(err.Error(), "HTTP 404"):
				return engine.ErrNotFound
			case strings.HasSuffix(err.Error(), "HTTP 409"):
				return fmt.Errorf("%w: MySQL object conflicts with existing objects or dependencies", engine.ErrConflict)
			default:
				return engine.ErrUnavailable
			}
		}
		return nil
	}
	return engine.ErrUnavailable
}

func managedProxyUsers(ctx context.Context, rc *engine.Context) ([]myspec.ProxyUser, error) {
	raw, err := secret(ctx, rc, "mysql/access/proxy-users")
	if missing(err) {
		return []myspec.ProxyUser{}, nil
	}
	if err != nil {
		return nil, err
	}
	var users []myspec.ProxyUser
	err = json.Unmarshal(raw, &users)
	return users, err
}
func (p *Core) ListUsers(ctx context.Context, rc *engine.Context) ([]User, error) {
	var users []User
	if err := p.accessAction(ctx, rc, myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "list"}, &users); err != nil {
		return nil, err
	}
	managed, err := managedProxyUsers(ctx, rc)
	if err != nil {
		return nil, err
	}
	out := []User{}
	for _, user := range users {
		if !myspec.ManagedUser(user.Name) {
			continue
		}
		user.RoutableThroughProxy = false
		for _, credential := range managed {
			if credential.Username == user.Name {
				user.RoutableThroughProxy = true
				break
			}
		}
		out = append(out, user)
	}
	return out, nil
}
func (p *Core) GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error) {
	if err := ValidateUserName(name); err != nil {
		return nil, err
	}
	users, err := p.ListUsers(ctx, rc)
	if err != nil {
		return nil, err
	}
	for _, user := range users {
		if user.Name == name {
			return &user, nil
		}
	}
	return nil, engine.ErrNotFound
}
func (p *Core) CreateUser(ctx context.Context, rc *engine.Context, user UserWrite) (string, error) {
	if err := ValidateUserWrite(user); err != nil {
		return "", err
	}
	return p.mutateUser(ctx, rc, myspec.AccessRequest{Operation: "create", User: &user})
}
func (p *Core) PatchUser(ctx context.Context, rc *engine.Context, name string, patch UserPatch) error {
	if err := ValidateUserName(name); err != nil {
		return err
	}
	if err := ValidateUserPatch(patch); err != nil {
		return err
	}
	_, err := p.mutateUser(ctx, rc, myspec.AccessRequest{Operation: "patch", Name: name, Patch: &patch})
	return err
}
func (p *Core) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	if err := ValidateUserName(name); err != nil {
		return err
	}
	_, err := p.mutateUser(ctx, rc, myspec.AccessRequest{Operation: "delete", Name: name})
	return err
}
func (p *Core) RotateUserPassword(ctx context.Context, rc *engine.Context, name string, retain bool) (string, error) {
	if err := ValidateUserName(name); err != nil {
		return "", err
	}
	return p.mutateUser(ctx, rc, myspec.AccessRequest{Operation: "password", Name: name, RetainCurrent: retain})
}
func (p *Core) DiscardOldPassword(ctx context.Context, rc *engine.Context, name string) error {
	if err := ValidateUserName(name); err != nil {
		return err
	}
	_, err := p.mutateUser(ctx, rc, myspec.AccessRequest{Operation: "discard-old", Name: name})
	return err
}
func (p *Core) ListDatabases(ctx context.Context, rc *engine.Context) ([]Database, error) {
	var all []Database
	if err := p.accessAction(ctx, rc, myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "list"}, &all); err != nil {
		return nil, err
	}
	out := []Database{}
	for _, db := range all {
		if myspec.ManagedDatabase(db.Name) {
			out = append(out, db)
		}
	}
	return out, nil
}
func (p *Core) GetDatabase(ctx context.Context, rc *engine.Context, name string) (*Database, error) {
	if err := ValidateDatabaseName(name); err != nil {
		return nil, err
	}
	var db Database
	err := p.accessAction(ctx, rc, myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "get", Name: name}, &db)
	return &db, err
}
func (p *Core) plainMutation(ctx context.Context, rc *engine.Context, action string, req myspec.AccessRequest) error {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if err := accessFence(ctx, rc); err != nil {
		return err
	}
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return err
	}
	return p.accessAction(ctx, rc, action, req, nil)
}
func (p *Core) CreateDatabase(ctx context.Context, rc *engine.Context, db Database) error {
	if err := ValidateDatabaseName(db.Name); err != nil {
		return err
	}
	if db.CharacterSet == "" {
		db.CharacterSet = "utf8mb4"
	}
	if db.Collation == "" {
		db.Collation = "utf8mb4_0900_ai_ci"
	}
	return p.plainMutation(ctx, rc, myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "create", Database: &db})
}
func (p *Core) DeleteDatabase(ctx context.Context, rc *engine.Context, name string, force bool) error {
	if err := ValidateDatabaseName(name); err != nil {
		return err
	}
	return p.plainMutation(ctx, rc, myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "delete", Name: name, Force: force})
}
func (p *Core) ListGrants(ctx context.Context, rc *engine.Context, user, database string) ([]Grant, error) {
	if user == "" && database == "" {
		return nil, engine.Invalid("user or database filter required")
	}
	if user != "" {
		if err := ValidateUserName(user); err != nil {
			return nil, err
		}
	}
	if database != "" {
		if err := ValidateDatabaseName(database); err != nil {
			return nil, err
		}
	}
	var grants []Grant
	err := p.accessAction(ctx, rc, myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "list", UserFilter: user, DatabaseFilter: database}, &grants)
	if grants == nil {
		grants = []Grant{}
	}
	return grants, err
}
func (p *Core) Grant(ctx context.Context, rc *engine.Context, grant Grant) error {
	if err := ValidateGrant(grant); err != nil {
		return err
	}
	return p.plainMutation(ctx, rc, myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "grant", Grant: &grant})
}
func (p *Core) Revoke(ctx context.Context, rc *engine.Context, grant Grant) error {
	if err := ValidateGrant(grant); err != nil {
		return err
	}
	return p.plainMutation(ctx, rc, myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "revoke", Grant: &grant})
}
