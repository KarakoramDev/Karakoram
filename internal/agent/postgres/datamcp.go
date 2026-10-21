package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/jackc/pgx/v5"
)

func dataRoleStatements(spec *pgspec.DataAccessRoles) ([]string, error) {
	if spec == nil {
		return nil, nil
	}
	var statements []string
	for _, r := range spec.Roles {
		if r.Username != "kk_mcp_ro" && r.Username != "kk_mcp_rw" {
			return nil, fmt.Errorf("invalid managed data role")
		}
		role := sqlIdent(r.Username)
		if !r.Enabled {
			statements = append(statements, "DO $kk$ BEGIN IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = "+sqlQuote(r.Username)+") THEN ALTER ROLE "+role+" NOLOGIN; END IF; END $kk$")
			continue
		}
		verifier, err := secrets.SCRAMVerifier(r.Password)
		if err != nil {
			return nil, err
		}
		statements = append(statements, "DO $kk$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = "+sqlQuote(r.Username)+") THEN CREATE ROLE "+role+"; END IF; END $kk$", "ALTER ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 10 PASSWORD "+sqlQuote(verifier), "ALTER ROLE "+role+" RESET ALL")
		for _, key := range sortedKeys(r.Settings) {
			switch key {
			case "default_transaction_read_only", "statement_timeout", "idle_in_transaction_session_timeout":
			default:
				return nil, fmt.Errorf("unsupported data role GUC")
			}
			statements = append(statements, "ALTER ROLE "+role+" SET "+sqlIdent(key)+" = "+sqlQuote(r.Settings[key]))
		}
		// Revoke the global membership when an installation narrows its scope.
		statements = append(statements, "REVOKE pg_read_all_data FROM "+role)
		if !r.Write && spec.GlobalRead {
			statements = append(statements, "GRANT pg_read_all_data TO "+role)
		}
	}
	return statements, nil
}

func dataDropStatement(name string) string { return "DROP ROLE IF EXISTS " + sqlIdent(name) }

// Reconciliation runs on the writable member only. Every database is visited so
// old schema grants/default ACLs are removed when scope narrows or is disabled.
// PUBLIC grants are not revoked: doing so would change application privileges.
func reconcileDataGrants(ctx context.Context, c *pgx.Conn, spec *pgspec.DataAccessRoles) error {
	return reconcileDataGrantsMode(ctx, c, spec, true)
}

// A periodic refresh is additive only: it never drops ACLs, roles, or generates
// password verifiers. Destructive scope cutovers use reset=true.
func reconcileDataGrantsMode(ctx context.Context, c *pgx.Conn, spec *pgspec.DataAccessRoles, reset bool) error {
	if spec == nil {
		return nil
	}
	rows, err := c.Query(ctx, "SELECT datname FROM pg_catalog.pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname")
	if err != nil {
		return err
	}
	var databases []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		databases = append(databases, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if slices.ContainsFunc(spec.Roles, func(r pgspec.DataAccessRole) bool { return r.Enabled }) {
		for _, database := range spec.Databases {
			if !slices.Contains(databases, database) {
				return fmt.Errorf("scoped data access database does not exist")
			}
		}
	}
	for _, database := range databases {
		if !reset && !slices.Contains(spec.Databases, database) {
			continue
		}
		cfg := c.Config().Copy()
		cfg.Database = database
		db, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return err
		}
		err = applyDataDatabase(ctx, db, database, spec, reset)
		db.Close(ctx)
		if err != nil {
			return err
		}
	}
	for _, r := range spec.Roles {
		if !r.Enabled && reset {
			if _, err = c.Exec(ctx, dataDropStatement(r.Username)); err != nil {
				return err
			}
		} else if r.Enabled {
			for _, database := range spec.Databases {
				if _, err = c.Exec(ctx, "GRANT CONNECT ON DATABASE "+sqlIdent(database)+" TO "+sqlIdent(r.Username)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func applyDataDatabase(ctx context.Context, c *pgx.Conn, database string, spec *pgspec.DataAccessRoles, reset bool) error {
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, r := range spec.Roles {
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", r.Username).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		role := sqlIdent(r.Username)
		// These reserved roles never own application objects. DROP OWNED clears ACLs,
		// including ALTER DEFAULT PRIVILEGES entries from previous scopes.
		if reset {
			if _, err = tx.Exec(ctx, "DROP OWNED BY "+role); err != nil {
				return err
			}
		}
		if !r.Enabled || !slices.Contains(spec.Databases, database) {
			continue
		}
		rows, err := tx.Query(ctx, "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname <> 'information_schema' AND nspname !~ '^pg_' ORDER BY nspname")
		if err != nil {
			return err
		}
		var schemas []string
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			if len(spec.Schemas) == 0 || slices.Contains(spec.Schemas, name) {
				schemas = append(schemas, name)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, schema := range spec.Schemas {
			if !slices.Contains(schemas, schema) {
				return fmt.Errorf("scoped data access schema does not exist")
			}
		}
		// Grant defaults for every existing application owner, including roles that
		// have not created a table yet. New owner roles require a reconciliation.
		rows, err = tx.Query(ctx, "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname !~ '^(pg_|kk_)' ORDER BY rolname")
		if err != nil {
			return err
		}
		var owners []string
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			owners = append(owners, name)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, schema := range schemas {
			for _, sql := range dataSchemaStatements(r, schema, owners) {
				if _, err = tx.Exec(ctx, sql); err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit(ctx)
}

func dataSchemaStatements(r pgspec.DataAccessRole, schema string, owners []string) []string {
	role, ns := sqlIdent(r.Username), sqlIdent(schema)
	privileges := "SELECT"
	if r.Write {
		privileges = "SELECT, INSERT, UPDATE, DELETE"
	}
	out := []string{"GRANT USAGE ON SCHEMA " + ns + " TO " + role, "GRANT " + privileges + " ON ALL TABLES IN SCHEMA " + ns + " TO " + role}
	if r.Write {
		out = append(out, "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "+ns+" TO "+role)
	}
	for _, owner := range owners {
		out = append(out, "ALTER DEFAULT PRIVILEGES FOR ROLE "+sqlIdent(owner)+" IN SCHEMA "+ns+" GRANT "+privileges+" ON TABLES TO "+role)
		if r.Write {
			out = append(out, "ALTER DEFAULT PRIVILEGES FOR ROLE "+sqlIdent(owner)+" IN SCHEMA "+ns+" GRANT USAGE, SELECT ON SEQUENCES TO "+role)
		}
	}
	return out
}

const dataGrantRefreshInterval = 10 * time.Minute

// Access catalog revisions invalidate ACL refreshes, not salted password SQL.
func dataAccessDefinition(spec *pgspec.DataAccessRoles) pgspec.DataAccessRoles {
	if spec == nil {
		return pgspec.DataAccessRoles{}
	}
	definition := *spec
	definition.Revision = ""
	return definition
}

type dataGrantCache struct {
	digest    [32]byte
	appliedAt time.Time
}

func (cache dataGrantCache) due(spec *pgspec.DataAccessRoles, now time.Time) bool {
	return cache.appliedAt.IsZero() || cache.digest != hash(spec) ||
		now.Before(cache.appliedAt) || now.Sub(cache.appliedAt) >= dataGrantRefreshInterval
}

// Called only after observing a writable local member. Failed refreshes never
// advance the cache, so the next readiness observation retries them.
func (e *Runner) refreshDataAccess(ctx context.Context, c *pgx.Conn, s pgspec.Spec, now time.Time) error {
	if s.DataAccess == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	digest := hash(dataAccessDefinition(s.DataAccess))
	if !e.dataAccessApplied || e.dataAccessHash != digest {
		if err := bootstrapRoles(ctx, c, s); err != nil {
			return err
		}
		e.dataAccessApplied, e.dataAccessHash = true, digest
		e.dataGrants = dataGrantCache{digest: hash(s.DataAccess), appliedAt: now}
		return nil
	}
	if !slices.ContainsFunc(s.DataAccess.Roles, func(role pgspec.DataAccessRole) bool { return role.Enabled }) {
		return nil
	}
	if !e.dataGrants.due(s.DataAccess, now) {
		return nil
	}
	if err := reconcileDataGrantsMode(ctx, c, s.DataAccess, false); err != nil {
		return err
	}
	e.dataGrants = dataGrantCache{digest: hash(s.DataAccess), appliedAt: now}
	return nil
}
