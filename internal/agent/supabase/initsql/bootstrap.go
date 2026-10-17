// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package initsql owns the curated Supabase prerequisites on PostgreSQL 17.
// Service table migrations remain owned by GoTrue, Storage, Realtime, Logflare
// and Supavisor. It never demotes the engine's postgres superuser or grants the
// application migration superuser to PostgREST's authenticator.
//
// SQL is curated from supabase/postgres d156ba65c14694c12cc5e782bc15b9b8ed2d1376
// and supabase/supabase bc27d2661edd09a5d0b8d76ce33da6d4071a756e. Plan is additive:
// disabling a component does not destroy its existing schema or user data.
// Migrations are immutable: change an applied migration by adding a new ID.
package initsql

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn is satisfied by *pgx.Conn. Apply requires a dedicated, idle connection
// to the project database on the primary, using the engine migration superuser.
// Config is copied only to connect to the optional private _supabase database.
// Connections and their tracer/logger must not log sensitive SQL or parameters.
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Config() *pgx.ConnConfig
}

// Step is one immutable embedded migration. Empty Database means the supplied
// connection's database; _supabase is the only permitted alternate database.
// SQL is included for inspection, not as an arbitrary-SQL execution interface:
// Apply rejects any step that differs from the curated catalog.
type Step struct{ ID, Database, SQL string }

//go:embed sql/*.sql
var sqlFiles embed.FS

const lockID int64 = 0x4b4b535542415345
const ledgerSQL = `CREATE SCHEMA IF NOT EXISTS _karakoram;
REVOKE ALL ON SCHEMA _karakoram FROM PUBLIC;
CREATE TABLE IF NOT EXISTS _karakoram.supabase_migrations (
 id text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp());
REVOKE ALL ON TABLE _karakoram.supabase_migrations FROM PUBLIC;`

var componentNames = strings.Fields("gateway auth rest realtime storage imgproxy meta studio functions pooler analytics vector")
var extensionNames = strings.Fields("pgcrypto uuid-ossp pg_stat_statements pg_cron pgaudit vector postgis pg_repack pg_stat_monitor set_user pg_net supabase_vault pgsodium pgjwt pg_graphql pg_jsonschema hypopg index_advisor plpgsql_check pg_hashids hstore citext btree_gist btree_gin unaccent fuzzystrmatch")
var passwordRoles = strings.Fields("supabase_admin authenticator supabase_auth_admin supabase_storage_admin supabase_functions_admin pgbouncer supabase_replication_admin supabase_read_only_user")

// Validate admits only curated component/extension selections and managed login
// password names. Passwords are optional overlays: omitted roles retain their
// existing password; passwords for currently disabled roles are not applied.
// It never includes a password value in an error.
func Validate(components, extensions []string, passwords map[string]string) error {
	for _, item := range components {
		if !includes(componentNames, item) {
			return errors.New("unknown Supabase bootstrap component")
		}
	}
	for _, item := range extensions {
		if !includes(extensionNames, item) {
			return errors.New("unsupported Supabase bootstrap extension")
		}
	}
	for role, password := range passwords {
		if !includes(passwordRoles, role) {
			return errors.New("unmanaged Supabase password role")
		}
		if password == "" || strings.ContainsRune(password, 0) {
			return errors.New("invalid Supabase role password")
		}
	}
	return nil
}

// Plan produces a stable dependency-ordered plan independent of input order and
// duplicates. Call Validate before Plan: invalid selections return nil. The base
// installs pgcrypto and uuid-ossp, API roles and modern auth helpers. Components
// add only their prerequisites; Edge HTTP functions alone never enable pg_net.
func Plan(components, extensions []string) []Step {
	if Validate(components, extensions, nil) != nil {
		return nil
	}
	steps := []Step{
		embedded("001-base-v1", "", "001-base.sql"),
		embedded("002-api-schemas-v1", "", "002-api-schemas.sql"),
	}
	selected := map[string]bool{"pgcrypto": true, "uuid-ossp": true}
	for _, extension := range extensions {
		selected[extension] = true
	}
	// Explicit dependency order is stable and ensures prerequisite extensions are
	// installed first (not implicitly pulled in by CASCADE).
	order := append([]string{"pgcrypto", "uuid-ossp", "pgsodium"}, extensionNames...)
	for _, extension := range order {
		if !selected[extension] {
			continue
		}
		delete(selected, extension)
		steps = append(steps, extensionStep(extension))
	}
	steps = append(steps, embedded("010-auth-v1", "", "010-auth.sql"))
	if includes(components, "storage") {
		steps = append(steps, embedded("020-storage-v1", "", "020-storage.sql"))
	}
	if includes(components, "realtime") {
		steps = append(steps, embedded("030-realtime-v1", "", "030-realtime.sql"))
	}
	if includes(components, "pooler") {
		steps = append(steps, embedded("040-pooler-v1", "", "040-pooler.sql"), embedded("041-supavisor-v1", "_supabase", "041-supavisor.sql"))
	}
	if includes(components, "analytics") {
		steps = append(steps, embedded("042-analytics-v1", "_supabase", "042-analytics.sql"))
	}
	if includes(extensions, "pg_graphql") {
		steps = append(steps, embedded("050-graphql-v1", "", "050-graphql.sql"))
	}
	if includes(extensions, "pg_net") {
		steps = append(steps, embedded("060-webhooks-v1", "", "060-webhooks.sql"))
	}
	return steps
}
func embedded(id, database, name string) Step {
	content, err := sqlFiles.ReadFile("sql/" + name)
	if err != nil {
		panic("missing embedded Supabase migration")
	}
	return Step{ID: id, Database: database, SQL: string(content)}
}
func extensionStep(name string) Step {
	sql := "CREATE EXTENSION IF NOT EXISTS " + pgx.Identifier{name}.Sanitize()
	if !includes([]string{"pg_graphql", "pgsodium", "supabase_vault", "pg_cron"}, name) {
		sql += " WITH SCHEMA extensions"
	}
	return Step{ID: "005-extension-" + name + "-v1", SQL: sql + ";\n"}
}
func includes(items []string, item string) bool {
	for _, candidate := range items {
		if candidate == item {
			return true
		}
	}
	return false
}

// Apply serializes bootstrap with a session advisory lock, records a checksum
// ledger in every touched database, and commits each migration together with its
// receipt. Interrupted runs resume safely. _supabase is created only if the plan
// contains a step for it. Role password overlays run on every invocation, outside
// the migration ledger, so retries and later credential rotation remain correct.
// Errors expose migration IDs/SQLSTATE only, never SQL, DSNs or password values.
func Apply(ctx context.Context, conn Conn, steps []Step, passwords map[string]string) (result error) {
	if conn == nil {
		return errors.New("a connection is required for Supabase bootstrap")
	}
	if err := Validate(nil, nil, passwords); err != nil {
		return err
	}
	catalog := Plan(componentNames, extensionNames)
	allowed := make(map[string]Step, len(catalog))
	for _, step := range catalog {
		allowed[step.ID] = step
	}
	seen := map[string]bool{}
	for _, step := range steps {
		expected, ok := allowed[step.ID]
		if !ok || step != expected || seen[step.ID] {
			return errors.New("untrusted or duplicate Supabase migration")
		}
		seen[step.ID] = true
	}
	if len(steps) == 0 {
		return errors.New("empty Supabase bootstrap plan")
	}
	if _, err := conn.Exec(ctx, "SET search_path = pg_catalog"); err != nil {
		return safeError("protecting Supabase management search path", err)
	}
	var recovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
		return safeError("checking bootstrap primary", err)
	}
	if recovery {
		return errors.New("the primary is required for Supabase bootstrap")
	}
	if _, err := conn.Exec(ctx, "SELECT pg_catalog.pg_advisory_lock($1)", lockID); err != nil {
		return safeError("locking Supabase bootstrap", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanup, "SELECT pg_catalog.pg_advisory_unlock($1)", lockID); err != nil && result == nil {
			result = safeError("unlocking Supabase bootstrap", err)
		}
	}()
	connections := map[string]Conn{"": conn}
	ledgers := map[string]bool{}
	var private *pgx.Conn
	defer func() {
		if private != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			private.Close(cleanup)
		}
	}()
	for _, step := range steps {
		target := connections[step.Database]
		if target == nil {
			var exists bool
			if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = '_supabase')").Scan(&exists); err != nil {
				return safeError("checking private Supabase database", err)
			}
			if !exists {
				if _, err := conn.Exec(ctx, "CREATE DATABASE _supabase OWNER supabase_admin"); err != nil {
					return safeError("creating private Supabase database", err)
				}
			}
			config := conn.Config()
			if config == nil {
				return errors.New("connection config for Supabase bootstrap is unavailable")
			}
			config = config.Copy()
			config.Database = "_supabase"
			if config.RuntimeParams == nil {
				config.RuntimeParams = map[string]string{}
			}
			config.RuntimeParams["search_path"] = "pg_catalog"
			var err error
			private, err = pgx.ConnectConfig(ctx, config)
			if err != nil {
				return safeError("connecting private Supabase database", err)
			}
			target = private
			connections[step.Database] = target
		}
		if !ledgers[step.Database] {
			if _, err := target.Exec(ctx, ledgerSQL); err != nil {
				return safeError("initializing Supabase migration ledger", err)
			}
			ledgers[step.Database] = true
		}
		sum := sha256.Sum256([]byte(step.SQL))
		checksum := hex.EncodeToString(sum[:])
		var previous string
		err := target.QueryRow(ctx, "SELECT checksum FROM _karakoram.supabase_migrations WHERE id=$1", step.ID).Scan(&previous)
		if err == nil {
			if previous != checksum {
				return fmt.Errorf("checksum differs for Supabase migration %s", step.ID)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return safeError("reading Supabase migration ledger", err)
		}
		if err := applyStep(ctx, target, step, checksum); err != nil {
			return err
		}
	}
	return applyPasswords(ctx, conn, passwords)
}
func applyStep(ctx context.Context, conn Conn, step Step, checksum string) error {
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return safeError("starting Supabase migration", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()
	if _, err := conn.Exec(ctx, step.SQL); err != nil {
		return safeError("Supabase migration "+step.ID, err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO _karakoram.supabase_migrations(id,checksum) VALUES($1,$2)", step.ID, checksum); err != nil {
		return safeError("recording Supabase migration "+step.ID, err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return safeError("committing Supabase migration "+step.ID, err)
	}
	committed = true
	return nil
}
func applyPasswords(ctx context.Context, conn Conn, passwords map[string]string) error {
	if len(passwords) == 0 {
		return nil
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return safeError("starting Supabase password overlay", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()
	// Suppress PostgreSQL statement/parameter logging for this secret-only phase.
	// Simple protocol delegates literal escaping to pgx; DDL cannot bind PASSWORD.
	if _, err := conn.Exec(ctx, "SET LOCAL log_statement = 'none'; SET LOCAL log_min_duration_statement = -1; SET LOCAL log_min_duration_sample = -1; SET LOCAL log_min_error_statement = 'panic'; SET LOCAL log_parameter_max_length = 0; SET LOCAL log_parameter_max_length_on_error = 0"); err != nil {
		return safeError("protecting Supabase password overlay", err)
	}
	names := make([]string, 0, len(passwords))
	for name := range passwords {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, role := range names {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
			return safeError("checking managed Supabase role", err)
		}
		if !exists {
			continue
		}
		verifier, err := secrets.SCRAMVerifier(passwords[role])
		if err != nil {
			return safeError("deriving managed Supabase password", err)
		}
		if _, err := conn.Exec(ctx, "ALTER ROLE "+pgx.Identifier{role}.Sanitize()+" PASSWORD $1", pgx.QueryExecModeSimpleProtocol, verifier); err != nil {
			return safeError("setting managed Supabase password", err)
		}
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return safeError("committing Supabase passwords", err)
	}
	committed = true
	return nil
}
func rollback(conn Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn.Exec(ctx, "ROLLBACK")
}
func safeError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	var databaseError *pgconn.PgError
	if errors.As(err, &databaseError) {
		return fmt.Errorf("%s failed (SQLSTATE %s)", operation, databaseError.Code)
	}
	return fmt.Errorf("%s failed", operation)
}
