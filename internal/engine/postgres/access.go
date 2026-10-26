// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/xdg-go/stringprep"
)

type Role = pgspec.Role
type RoleWrite = pgspec.RoleWrite
type Database = pgspec.Database
type Grant = pgspec.Grant

// Access is shared by PostgreSQL and providers embedding its core.
type Access interface {
	ListRoles(context.Context, *engine.Context) ([]Role, error)
	GetRole(context.Context, *engine.Context, string) (*Role, error)
	CreateRole(context.Context, *engine.Context, RoleWrite) (string, error)
	UpdateRole(context.Context, *engine.Context, RoleWrite) error
	DeleteRole(context.Context, *engine.Context, string) error
	RotateRolePassword(context.Context, *engine.Context, string) (string, error)
	ListDatabases(context.Context, *engine.Context) ([]Database, error)
	GetDatabase(context.Context, *engine.Context, string) (*Database, error)
	CreateDatabase(context.Context, *engine.Context, Database) error
	DeleteDatabase(context.Context, *engine.Context, string, bool) error
	ListGrants(context.Context, *engine.Context, string) ([]Grant, error)
	Grant(context.Context, *engine.Context, Grant) error
	Revoke(context.Context, *engine.Context, Grant) error
}

var _ Access = (*Core)(nil)
var _ engine.DatabasePasswordValidator = (*Core)(nil)

func identifier(v string) error {
	if v == "" || len(v) > 63 || !utf8.ValidString(v) || strings.IndexFunc(v, unicode.IsControl) >= 0 {
		return engine.Invalid("identifiers must contain 1..63 UTF-8 bytes without control characters")
	}
	return nil
}

func (p *Core) reservedRole(v string) bool {
	return v == "postgres" || strings.HasPrefix(v, "pg_") || strings.HasPrefix(v, "kk_") || p.options.ReservedRole != nil && p.options.ReservedRole(v)
}

func (p *Core) writableRole(ctx context.Context, v string) error {
	if err := identifier(v); err != nil {
		return err
	}
	if p.reservedRole(v) || strings.HasPrefix(v, "kb_") && !bindingAccess(ctx) {
		return fmt.Errorf("%w: role is reserved and managed by the engine", engine.ErrConflict)
	}
	return nil
}

func (*Core) ValidateDatabasePassword(v string) error {
	if len(v) < 12 || len(v) > 1024 || !utf8.ValidString(v) || strings.IndexFunc(v, unicode.IsControl) >= 0 {
		return engine.Invalid("password must contain 12..1024 UTF-8 bytes without control characters")
	}
	if _, err := stringprep.SASLprep.Prepare(v); err != nil {
		return engine.Invalid("password must be valid SASLprep text")
	}
	return nil
}

var predefinedMemberships = []string{"pg_read_all_data", "pg_write_all_data", "pg_monitor", "pg_read_all_stats", "pg_signal_backend"}

func (p *Core) validateRole(ctx context.Context, v RoleWrite) error {
	if err := p.writableRole(ctx, v.Name); err != nil {
		return err
	}
	if v.ConnectionLimit < -1 {
		return engine.Invalid("connectionLimit must be -1 or nonnegative")
	}
	for _, parent := range v.MemberOf {
		if parent == v.Name {
			return engine.Invalid("a role cannot be a member of itself")
		}
		if !slices.Contains(predefinedMemberships, parent) {
			if err := p.writableRole(ctx, parent); err != nil {
				return engine.Invalid("memberOf contains a reserved or invalid role")
			}
		}
	}
	for _, parent := range v.MemberOfAdmin {
		if !slices.Contains(v.MemberOf, parent) {
			return engine.Invalid("memberOfAdmin must be a subset of memberOf")
		}
	}
	if v.ValidUntil != nil {
		if _, err := time.Parse(time.RFC3339, *v.ValidUntil); err != nil {
			return engine.Invalid("validUntil must be an RFC3339 timestamp")
		}
	}
	if v.Password != nil {
		return p.ValidateDatabasePassword(*v.Password)
	}
	return nil
}

// accessAction re-observes leadership for every call; stale cluster status is not
// used as an authority. The agent checks recovery/leader lock again before SQL.
func (p *Core) accessAction(ctx context.Context, rc *engine.Context, action string, request pgspec.AccessRequest, output any) error {
	request.Binding = request.Binding || bindingAccess(ctx)
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if rc.Agents == nil {
		return engine.ErrUnavailable
	}
	for _, m := range s.group(0).data() {
		if m.Removing {
			continue
		}
		status, err := live(ctx, rc, m.Name)
		if err != nil || !status.Running || status.Role != "primary" && status.Role != "standalone" {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return engine.ErrUnavailable
		}
		defer a.Close()
		if err = a.Engine(ctx, "postgres", action, request, output); err != nil {
			switch {
			case strings.HasSuffix(err.Error(), "HTTP 400"):
				return engine.Invalid("PostgreSQL rejected the access request")
			case strings.HasSuffix(err.Error(), "HTTP 403"):
				return engine.Invalid("PostgreSQL rejected the requested privileges")
			case strings.HasSuffix(err.Error(), "HTTP 404"):
				return engine.ErrNotFound
			case strings.HasSuffix(err.Error(), "HTTP 409"):
				return fmt.Errorf("%w: PostgreSQL object conflicts with existing objects or dependencies", engine.ErrConflict)
			default:
				return engine.ErrUnavailable
			}
		}
		return nil
	}
	return engine.ErrUnavailable
}

func (p *Core) ListRoles(ctx context.Context, rc *engine.Context) ([]Role, error) {
	var roles []Role
	if err := p.accessAction(ctx, rc, "roles", pgspec.AccessRequest{Operation: "list"}, &roles); err != nil {
		return nil, err
	}
	out := make([]Role, 0, len(roles))
	for _, role := range roles {
		if !p.reservedRole(role.Name) {
			if role.MemberOf == nil {
				role.MemberOf = []string{}
			}
			out = append(out, role)
		}
	}
	return out, nil
}

func (p *Core) GetRole(ctx context.Context, rc *engine.Context, name string) (*Role, error) {
	if err := identifier(name); err != nil {
		return nil, err
	}
	if p.reservedRole(name) {
		return nil, engine.ErrNotFound
	}
	roles, err := p.ListRoles(ctx, rc)
	if err != nil {
		return nil, err
	}
	for _, role := range roles {
		if role.Name == name {
			return &role, nil
		}
	}
	return nil, engine.ErrNotFound
}

func (p *Core) managedRoleNames(ctx context.Context, rc *engine.Context) ([]string, error) {
	b, err := secret(ctx, rc, "access/roles")
	if missing(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading managed role index: %w", err)
	}
	var names []string
	if err := json.Unmarshal(b, &names); err != nil {
		return nil, fmt.Errorf("decoding managed role index: %w", err)
	}
	return names, nil
}

func rolePasswordKey(name string) string {
	return "access/passwords/" + base64.RawURLEncoding.EncodeToString([]byte(name))
}

func (p *Core) accessRevision(ctx context.Context, rc *engine.Context) (string, error) {
	b, err := secret(ctx, rc, "access/revision")
	if missing(err) {
		return "", nil
	}
	return string(b), err
}

func (p *Core) bumpAccessRevision(ctx context.Context, rc *engine.Context) error {
	revision, err := secrets.Token()
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, "access/revision", []byte(revision))
}

func (p *Core) proxyUsers(ctx context.Context, rc *engine.Context) ([]pgspec.ProxyUser, error) {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return nil, err
	}
	if bs.ManagedRolesHeld {
		return nil, nil
	}
	names, err := p.managedRoleNames(ctx, rc)
	if err != nil {
		return nil, err
	}
	out := make([]pgspec.ProxyUser, 0, len(names))
	for _, name := range names {
		if p.reservedRole(name) {
			continue
		}
		password, err := secret(ctx, rc, rolePasswordKey(name))
		if missing(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading managed role password: %w", err)
		}
		out = append(out, pgspec.ProxyUser{Username: name, Password: string(password), RequireTLS: rc.Cluster.Spec.TLS.Mode == "requireTLS"})
	}
	return out, nil
}

// writeRole sends only a verifier to PostgreSQL and seals the applied password
// before publishing the matching ProxySQL configuration revision.
func (p *Core) writeRole(ctx context.Context, rc *engine.Context, op string, v RoleWrite) (string, error) {
	v.Binding = bindingAccess(ctx)
	if op != "delete" {
		if err := p.validateRole(ctx, v); err != nil {
			return "", err
		}
	}
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return "", err
	}
	receipts, err := p.credentialReceipts(ctx, rc)
	if err != nil {
		return "", err
	}
	var expectedOID uint32
	if bindingAccess(ctx) {
		if !receipts {
			return "", fmt.Errorf("%w: binding role mutations require credential receipts", engine.ErrUnavailable)
		}
		if op != "create" {
			expectedOID, err = p.bindingRoleOID(ctx, rc, v.Name)
			if err != nil {
				return "", err
			}
		}
	}
	names, err := p.managedRoleNames(ctx, rc)
	if err != nil {
		return "", err
	}
	generated := ""
	if op == "create" && v.Login && v.Password == nil {
		generated, err = secrets.Password()
		if err != nil {
			return "", err
		}
		v.Password = &generated
	}
	password := ""
	if op != "delete" && v.Login && v.Password == nil {
		if _, err := secret(ctx, rc, rolePasswordKey(v.Name)); missing(err) {
			return "", engine.Invalid("a password is required when enabling login for a role without a managed password")
		} else if err != nil {
			return "", err
		}
	}
	if v.Password != nil {
		password = *v.Password
		verifier, err := secrets.SCRAMVerifier(password)
		if err != nil {
			return "", err
		}
		v.Password = &verifier
	}
	if op != "delete" && v.Login {
		if !slices.Contains(names, v.Name) {
			names = append(names, v.Name)
		}
	} else {
		names = slices.DeleteFunc(names, func(name string) bool { return name == v.Name })
	}
	slices.Sort(names)
	revision, err := secrets.Token()
	if err != nil {
		return "", err
	}
	intent := &accessIntent{
		// The trusted binding scope survives credential-journal recovery.
		Request: pgspec.AccessRequest{Operation: op, Role: &v, Binding: bindingAccess(ctx), ExpectedRoleOID: expectedOID},
		Names:   names, Revision: revision, Generated: generated != "",
	}
	if v.Password != nil {
		intent.Password = &password
	}
	if !receipts {
		// Preserve pre-receipt agent behavior during a mixed-version rollout:
		// direct SQL followed by metadata publication, without promising
		// crash-safe recovery that the old agent cannot provide.
		if err := p.accessAction(ctx, rc, "roles", intent.Request, nil); err != nil {
			return "", err
		}
		return generated, p.publishAccessIntent(ctx, rc, intent)
	}
	intent.Request.RequestID = revision
	if intent.Request.Binding {
		// The role name hashes binding ID + incarnation; never reuse another
		// incarnation's native receipt identity.
		intent.Request.RequestID = v.Name + "/" + revision
	}
	intent.Previous, err = accessSnapshot(ctx, rc, v.Name)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		return "", err
	}
	if err = putSecret(ctx, rc, "access/pending", raw); err != nil {
		return "", fmt.Errorf("sealing credential intent before SQL: %w", err)
	}
	if err = p.completeAccessIntent(ctx, rc, intent); err != nil {
		return "", err
	}
	return generated, nil
}

func (p *Core) CreateRole(ctx context.Context, rc *engine.Context, v RoleWrite) (string, error) {
	if err := p.accessMu.Lock(ctx); err != nil {
		return "", err
	}
	defer p.accessMu.Unlock()
	if err := p.validateRole(ctx, v); err != nil {
		return "", err
	}
	if intent, err := loadAccessIntent(ctx, rc); err != nil {
		return "", err
	} else if intent != nil {
		err := p.completeAccessIntent(ctx, rc, intent)
		if err != nil && !definitiveAccessRejection(err) {
			return "", err
		}
		if err == nil && intent.Request.Operation == "create" && intent.Request.Role.Name == v.Name {
			if intent.Generated && intent.Password != nil {
				return *intent.Password, nil
			}
			return "", nil
		}
	}
	if _, err := p.GetRole(ctx, rc, v.Name); err == nil {
		return "", engine.ErrConflict
	} else if err != engine.ErrNotFound {
		return "", err
	}
	return p.writeRole(ctx, rc, "create", v)
}

func (p *Core) UpdateRole(ctx context.Context, rc *engine.Context, v RoleWrite) error {
	if err := p.accessMu.Lock(ctx); err != nil {
		return err
	}
	defer p.accessMu.Unlock()
	if err := p.validateRole(ctx, v); err != nil {
		return err
	}
	if _, err := p.GetRole(ctx, rc, v.Name); err != nil {
		return err
	}
	_, err := p.writeRole(ctx, rc, "update", v)
	return err
}

func (p *Core) DeleteRole(ctx context.Context, rc *engine.Context, name string) error {
	if err := p.accessMu.Lock(ctx); err != nil {
		return err
	}
	defer p.accessMu.Unlock()
	if err := p.writableRole(ctx, name); err != nil {
		return err
	}
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return err
	}
	if bindingAccess(ctx) {
		if _, err := p.GetRole(ctx, rc, name); err == nil {
			if _, err := p.writeRole(ctx, rc, "delete", RoleWrite{Role: Role{Name: name}}); err != nil {
				return err
			}
		} else if !missing(err) {
			return err
		}
	} else if err := p.accessAction(ctx, rc, "roles", pgspec.AccessRequest{Operation: "delete", Name: name}, nil); err != nil {
		return err
	}
	names, err := p.managedRoleNames(ctx, rc)
	if err != nil {
		return err
	}
	names = slices.DeleteFunc(names, func(v string) bool { return v == name })
	b, err := json.Marshal(names)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "access/roles", b); err != nil {
		return err
	}
	if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), rolePasswordKey(name)); err != nil && !missing(err) {
		return err
	}
	if bindingAccess(ctx) {
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "bindings/roles/"+name); err != nil && !missing(err) {
			return err
		}
	}
	return p.bumpAccessRevision(ctx, rc)
}

func (p *Core) RotateRolePassword(ctx context.Context, rc *engine.Context, name string) (string, error) {
	if err := p.accessMu.Lock(ctx); err != nil {
		return "", err
	}
	defer p.accessMu.Unlock()
	if err := p.writableRole(ctx, name); err != nil {
		return "", err
	}
	if intent, err := loadAccessIntent(ctx, rc); err != nil {
		return "", err
	} else if intent != nil && intent.Request.Role.Name == name && intent.Password != nil {
		err := p.completeAccessIntent(ctx, rc, intent)
		if err != nil && !definitiveAccessRejection(err) {
			return "", err
		}
		if err == nil {
			return *intent.Password, nil
		}
	} else if intent != nil {
		if err := p.completeAccessIntent(ctx, rc, intent); err != nil && !definitiveAccessRejection(err) {
			return "", err
		}
	}
	role, err := p.GetRole(ctx, rc, name)
	if err != nil {
		return "", err
	}
	if !role.Login {
		return "", engine.Invalid("password rotation requires a login role")
	}
	password, err := secrets.Password()
	if err != nil {
		return "", err
	}
	if _, err = p.writeRole(ctx, rc, "update", RoleWrite{Role: *role, Password: &password}); err != nil {
		return "", err
	}
	return password, nil
}

func reservedDatabase(name string) bool {
	return name == "postgres" || name == "template0" || name == "template1" || strings.HasPrefix(name, "_karakoram")
}

func (p *Core) ListDatabases(ctx context.Context, rc *engine.Context) ([]Database, error) {
	var databases []Database
	if err := p.accessAction(ctx, rc, "databases", pgspec.AccessRequest{Operation: "list"}, &databases); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(databases, func(v Database) bool { return reservedDatabase(v.Name) }), nil
}

func (p *Core) GetDatabase(ctx context.Context, rc *engine.Context, name string) (*Database, error) {
	if err := identifier(name); err != nil {
		return nil, err
	}
	if reservedDatabase(name) {
		return nil, engine.ErrNotFound
	}
	var database Database
	if err := p.accessAction(ctx, rc, "databases", pgspec.AccessRequest{Operation: "get", Name: name}, &database); err != nil {
		return nil, err
	}
	return &database, nil
}

func (p *Core) CreateDatabase(ctx context.Context, rc *engine.Context, v Database) error {
	for _, name := range []string{v.Name, v.Owner} {
		if err := identifier(name); err != nil {
			return err
		}
	}
	if reservedDatabase(v.Name) {
		return fmt.Errorf("%w: database is reserved", engine.ErrConflict)
	}
	if v.Encoding == "" {
		v.Encoding = "UTF8"
	}
	if v.Locale == "" {
		v.Locale = "C.UTF-8"
	}
	available, err := p.availableExtensions(ctx, rc)
	if err != nil {
		return err
	}
	if err := validateExtensions(v.Extensions, available); err != nil {
		return err
	}
	if len(v.Extensions) != 0 && !v.AllowConnections {
		return engine.Invalid("database extensions require allowConnections")
	}
	return p.accessAction(ctx, rc, "databases", pgspec.AccessRequest{Operation: "create", Database: &v, AvailableExtensions: available}, nil)
}

func (p *Core) DeleteDatabase(ctx context.Context, rc *engine.Context, name string, force bool) error {
	if err := identifier(name); err != nil {
		return err
	}
	if reservedDatabase(name) {
		return fmt.Errorf("%w: database is reserved", engine.ErrConflict)
	}
	return p.accessAction(ctx, rc, "databases", pgspec.AccessRequest{Operation: "delete", Name: name, Force: force}, nil)
}

func (p *Core) ListGrants(ctx context.Context, rc *engine.Context, database string) ([]Grant, error) {
	if err := identifier(database); err != nil {
		return nil, err
	}
	var grants []Grant
	if err := p.accessAction(ctx, rc, "grants", pgspec.AccessRequest{Operation: "list", Name: database}, &grants); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(grants, func(v Grant) bool { return p.reservedRole(v.Role) }), nil
}

func (p *Core) writeGrant(ctx context.Context, rc *engine.Context, op string, v Grant) error {
	if err := p.writableRole(ctx, v.Role); err != nil {
		return err
	}
	if err := identifier(v.Database); err != nil {
		return err
	}
	allowed := []string{"CONNECT", "CREATE", "TEMP", "TEMPORARY"}
	if v.Schema != "" {
		if err := identifier(v.Schema); err != nil {
			return err
		}
		allowed = []string{"USAGE", "CREATE"}
	}
	if len(v.Privileges) == 0 {
		return engine.Invalid("privileges must not be empty")
	}
	for _, privilege := range v.Privileges {
		if !slices.Contains(allowed, privilege) {
			return engine.Invalid("unsupported privilege for grant target")
		}
		// Revocation must remain available to remove previously issued grants.
		if op == "grant" && pgspec.ProtectedSchema(v.Schema) && privilege != "USAGE" {
			return engine.Invalid("write privileges on system or engine-managed schemas are forbidden")
		}
	}
	return p.accessAction(ctx, rc, "grants", pgspec.AccessRequest{Operation: op, Grant: &v}, nil)
}

func (p *Core) Grant(ctx context.Context, rc *engine.Context, v Grant) error {
	return p.writeGrant(ctx, rc, "grant", v)
}

func (p *Core) Revoke(ctx context.Context, rc *engine.Context, v Grant) error {
	return p.writeGrant(ctx, rc, "revoke", v)
}
