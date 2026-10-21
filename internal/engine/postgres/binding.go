// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type bindingScope struct{}

// BindingContext grants the trusted in-process binding controller access to the
// reserved kb_ role namespace. Public API handlers never use this scope.
func BindingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, bindingScope{}, true)
}
func bindingAccess(ctx context.Context) bool { v, _ := ctx.Value(bindingScope{}).(bool); return v }

// EnsureBindingRole recovers only roles whose creation was published by our
// credential journal. A matching native name alone is never ownership evidence.
func (p *Core) EnsureBindingRole(ctx context.Context, rc *engine.Context, name string, limit int) error {
	ctx = BindingContext(ctx)
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if !strings.HasPrefix(name, "kb_") || limit < 1 {
		return engine.Invalid("invalid binding role")
	}
	receipts, err := p.credentialReceipts(ctx, rc)
	if err != nil {
		return err
	}
	if !receipts {
		return fmt.Errorf("%w: binding roles require credential receipts", engine.ErrUnavailable)
	}
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return err
	}
	role, err := p.GetRole(ctx, rc, name)
	if err == nil {
		raw, err := secret(ctx, rc, "bindings/roles/"+name)
		if err != nil {
			return fmt.Errorf("%w: binding role has no ownership receipt", engine.ErrConflict)
		}
		var request pgspec.AccessRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return err
		}
		request.RoleOperation, request.Operation = "create", "journal"
		var receipt pgspec.AccessResult
		if err := p.accessAction(ctx, rc, "roles", request, &receipt); err != nil {
			return err
		}
		if receipt.Outcome != "applied" || receipt.RoleOID == 0 {
			return fmt.Errorf("%w: binding role native identity differs from its creation receipt", engine.ErrConflict)
		}
		if !role.Login || role.Superuser || role.Createdb || role.Createrole || role.Replication || role.BypassRLS || len(role.MemberOf) != 0 || role.ConnectionLimit != limit {
			return engine.ErrConflict
		}
		return nil
	}
	if err != engine.ErrNotFound {
		return err
	}
	_, err = p.writeRole(ctx, rc, "create", RoleWrite{Role: Role{Name: name, Login: true, ConnectionLimit: limit}})
	return err
}

// bindingRoleOID verifies the sealed creation receipt against the current native
// role. The returned OID must also fence the subsequent mutation on the agent.
func (p *Core) bindingRoleOID(ctx context.Context, rc *engine.Context, name string) (uint32, error) {
	raw, err := secret(ctx, rc, "bindings/roles/"+name)
	if err != nil {
		if missing(err) {
			return 0, fmt.Errorf("%w: binding role has no ownership receipt", engine.ErrConflict)
		}
		return 0, err
	}
	var request pgspec.AccessRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return 0, err
	}
	if request.Role == nil || request.Role.Name != name || request.Operation != "create" || !request.Binding || request.RequestID == "" {
		return 0, fmt.Errorf("%w: invalid binding role creation receipt", engine.ErrConflict)
	}
	request.RoleOperation, request.Operation = "create", "journal"
	var receipt pgspec.AccessResult
	if err := p.accessAction(ctx, rc, "roles", request, &receipt); err != nil {
		return 0, err
	}
	if receipt.Outcome != "applied" || receipt.RoleOID == 0 {
		return 0, fmt.Errorf("%w: binding role identity differs from its creation receipt", engine.ErrConflict)
	}
	return receipt.RoleOID, nil
}

// VerifyBindingRole checks ownership before cleanup touches dependent objects.
func (p *Core) VerifyBindingRole(ctx context.Context, rc *engine.Context, name string) (uint32, error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return 0, err
	}
	if _, err := p.GetRole(ctx, rc, name); err != nil {
		return 0, err
	}
	return p.bindingRoleOID(ctx, rc, name)
}

// OwnBindingDatabase is deliberately limited to a binding-owned database's
// public schema; it cannot alter arbitrary schemas or database ownership.
func (p *Core) OwnBindingDatabase(ctx context.Context, rc *engine.Context, db Database) error {
	if !bindingAccess(ctx) || !strings.HasPrefix(db.Owner, "kb_") {
		return engine.ErrConflict
	}
	return p.accessAction(ctx, rc, "databases", pgspec.AccessRequest{Operation: "public-owner", Database: &db}, nil)
}

// BindingEndpoint resolves private writer discovery without revealing system credentials.
func (p *Core) BindingEndpoint(ctx context.Context, rc *engine.Context) (string, []byte, error) {
	if rc.Cluster.Spec.TLS.Mode == "disabled" {
		return "", nil, engine.Invalid("bindings require source TLS")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return "", nil, err
	}
	if rc.Cluster.Spec.Components.Extra["proxy"].Replicas == 0 && len(s.data()) != 1 {
		return "", nil, engine.Invalid("HA bindings require a writer proxy")
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	return connectionMember(rc, s), ca, err
}
