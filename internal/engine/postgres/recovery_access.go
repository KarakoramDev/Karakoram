// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// The manifest lives only in the sealed recovery scope, never backup metadata.
type managedRoleManifest map[string]string

func (p *Core) captureManagedRoles(ctx context.Context, rc *engine.Context, backup *domain.Backup) error {
	if err := p.accessMu.Lock(ctx); err != nil {
		return err
	}
	defer p.accessMu.Unlock()
	names, err := p.managedRoleNames(ctx, rc)
	if err != nil {
		return err
	}
	manifest := managedRoleManifest{}
	for _, name := range names {
		password, err := secret(ctx, rc, rolePasswordKey(name))
		if err != nil {
			return fmt.Errorf("read managed recovery password: %w", err)
		}
		manifest[name] = string(password)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, domain.ClusterRef{Namespace: backup.Namespace, Name: backup.RecoveryIdentity}, "access/manifest", data)
}

func (p *Core) restoreManagedRoles(ctx context.Context, rc *engine.Context, backup *domain.Backup) error {
	if err := p.accessMu.Lock(ctx); err != nil {
		return err
	}
	defer p.accessMu.Unlock()
	data, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: backup.Namespace, Name: backup.RecoveryIdentity}, "access/manifest")
	if err != nil {
		return fmt.Errorf("read sealed managed-role recovery manifest: %w", err)
	}
	var manifest managedRoleManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("decode sealed managed-role recovery manifest: %w", err)
	}
	names := make([]string, 0, len(manifest))
	for name, password := range manifest {
		if err = p.writableRole(BindingContext(ctx), name); err != nil {
			return err
		}
		if err = p.ValidateDatabasePassword(password); err != nil {
			return err
		}
		if err = putSecret(ctx, rc, rolePasswordKey(name), []byte(password)); err != nil {
			return err
		}
		names = append(names, name)
	}
	slices.Sort(names)
	data, err = json.Marshal(names)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "access/roles", data); err != nil {
		return err
	}
	return p.bumpAccessRevision(ctx, rc)
}

// Preserve the target's current passwords for in-place recovery; clones first
// import the backup manifest. Catalog attributes/memberships remain recovered.
func (p *Core) reconcileRecoveredRoles(ctx context.Context, rc *engine.Context) error {
	if err := p.accessMu.Lock(ctx); err != nil {
		return err
	}
	defer p.accessMu.Unlock()
	names, err := p.managedRoleNames(ctx, rc)
	if err != nil || len(names) == 0 {
		return err
	}
	roles, err := p.ListRoles(ctx, rc)
	if err != nil {
		return err
	}
	present := make(map[string]bool, len(roles))
	for _, role := range roles {
		present[role.Name] = role.Login
	}
	kept := make([]string, 0, len(names))
	for _, name := range names {
		if !present[name] {
			if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), rolePasswordKey(name)); err != nil && !missing(err) {
				return err
			}
			continue
		}
		password, err := secret(ctx, rc, rolePasswordKey(name))
		if err != nil {
			return err
		}
		verifier, err := secrets.SCRAMVerifier(string(password))
		if err != nil {
			return err
		}
		// Ordinary roles use the original password-only protocol, including
		// during recovery of an older member agent. Binding identities require
		// the restore-only action; ordinary binding mutation fences stay intact.
		request := pgspec.AccessRequest{Operation: "password", Role: &RoleWrite{Role: Role{Name: name}, Password: &verifier}}
		if strings.HasPrefix(name, "kb_") {
			request.Operation, request.Binding = "recover-password", true
		}
		if err = p.recoveredRolePassword(ctx, rc, request); err != nil {
			return err
		}
		kept = append(kept, name)
	}
	data, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "access/roles", data); err != nil {
		return err
	}
	return p.bumpAccessRevision(ctx, rc)
}

func (p *Core) recoveredRolePassword(ctx context.Context, rc *engine.Context, request pgspec.AccessRequest) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if s.Topology != "distributed" {
		return p.accessAction(ctx, rc, pgspec.ActionRoles, request, nil)
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if !bs.ManagedRolesHeld {
		return engine.Invalid("distributed role recovery requires client holds")
	}
	// One verifier, all recovered group primaries, before the access-release
	// checkpoint. Retry is idempotent, including a lost acknowledgement. Physical
	// replication supplies each group's standbys; no remote DDL is required.
	for _, group := range s.groups() {
		m, _, err := group.primary(ctx, rc)
		if err != nil {
			return err
		}
		if err = pgAction(ctx, rc, m.Name, pgspec.ActionRoles, request, nil); err != nil {
			return err
		}
	}
	return nil
}
