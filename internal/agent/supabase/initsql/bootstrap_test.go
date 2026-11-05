// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package initsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPlan(t *testing.T) {
	minimal := Plan([]string{"gateway", "auth", "rest"}, nil)
	if len(minimal) != 5 {
		t.Fatalf("minimal step count %d", len(minimal))
	}
	if minimal[0].ID != "001-base-v1" || minimal[1].ID != "002-api-schemas-v1" || minimal[4].ID != "010-auth-v1" {
		t.Fatal("base dependency order")
	}
	if !reflect.DeepEqual(minimal, Plan([]string{"rest", "auth", "gateway", "auth"}, []string{"pgcrypto", "uuid-ossp", "pgcrypto"})) {
		t.Fatal("selection ordering or duplicates changed plan")
	}
	cases := []struct {
		components, extensions []string
		required, forbidden    string
	}{{[]string{"functions"}, nil, "010-auth-v1", "060-webhooks-v1"}, {nil, []string{"pg_net"}, "060-webhooks-v1", "050-graphql-v1"}, {nil, []string{"pg_graphql"}, "050-graphql-v1", "060-webhooks-v1"}, {[]string{"pooler"}, nil, "041-supavisor-v1", "042-analytics-v1"}, {[]string{"analytics"}, nil, "042-analytics-v1", "041-supavisor-v1"}, {[]string{"storage"}, nil, "020-storage-v1", "030-realtime-v1"}, {[]string{"realtime"}, nil, "030-realtime-v1", "020-storage-v1"}}
	for _, tc := range cases {
		steps := Plan(tc.components, tc.extensions)
		found := false
		for _, step := range steps {
			if step.ID == tc.required {
				found = true
			}
			if step.ID == tc.forbidden {
				t.Fatalf("unexpected migration %s", tc.forbidden)
			}
			if strings.Contains(step.SQL, "GRANT supabase_admin TO authenticator") || strings.Contains(step.SQL, "ALTER ROLE postgres") {
				t.Fatal("engine/application privilege boundary")
			}
		}
		if !found {
			t.Fatalf("missing migration %s", tc.required)
		}
	}
	for _, step := range minimal {
		if step.Database != "" {
			t.Fatal("minimal plan creates optional database")
		}
	}
	if Plan([]string{"unknown"}, nil) != nil || Plan(nil, []string{"invalid"}) != nil {
		t.Fatal("invalid plan selection")
	}
}
func TestBaselineGraphQLSchemaWithoutExtensionAndExistingLedger(t *testing.T) {
	steps := Plan([]string{"gateway", "auth", "rest"}, nil)
	var existing []Step
	var schema Step
	for _, step := range steps {
		if step.ID == "002-api-schemas-v1" {
			schema = step
		} else {
			existing = append(existing, step)
		}
		if strings.Contains(step.SQL, "CREATE EXTENSION IF NOT EXISTS \"pg_graphql\"") || strings.Contains(step.SQL, "CREATE OR REPLACE FUNCTION graphql_public.graphql") {
			t.Fatal("default exposed schema must not enable the optional GraphQL extension or wrapper")
		}
	}
	if !strings.Contains(schema.SQL, "CREATE SCHEMA IF NOT EXISTS graphql_public AUTHORIZATION supabase_admin") ||
		!strings.Contains(schema.SQL, "REVOKE ALL ON SCHEMA graphql_public FROM PUBLIC") ||
		!strings.Contains(schema.SQL, "GRANT USAGE ON SCHEMA graphql_public TO anon, authenticated, service_role") {
		t.Fatal("baseline schema is missing or has incorrect API role grants")
	}
	conn := newFake()
	if err := Apply(t.Context(), conn, existing, nil); err != nil {
		t.Fatal(err)
	}
	before := len(conn.calls)
	if err := Apply(t.Context(), conn, steps, nil); err != nil {
		t.Fatal("existing bootstrap ledger rejected additive schema migration", err)
	}
	applied := 0
	for _, call := range conn.calls[before:] {
		for _, step := range steps {
			if call.sql == step.SQL {
				if step.ID != schema.ID {
					t.Fatal("already applied migration was rerun")
				}
				applied++
			}
		}
	}
	if applied != 1 || len(conn.ledger) != len(steps) {
		t.Fatal("existing database did not apply exactly the new schema migration")
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		components, extensions []string
		passwords              map[string]string
	}{{[]string{"not-a-component"}, nil, nil}, {nil, []string{"pg_net; DROP ROLE postgres"}, nil}, {nil, nil, map[string]string{"postgres": "secret"}}, {nil, nil, map[string]string{"authenticator": ""}}, {nil, nil, map[string]string{"authenticator": "a\x00b"}}} {
		if Validate(tc.components, tc.extensions, tc.passwords) == nil {
			t.Fatal("invalid bootstrap admitted")
		}
	}
}

type call struct {
	sql  string
	args []any
}
type fakeConn struct {
	ledger, pending map[string]string
	calls           []call
	failSQL         string
	failure         error
	recovery        bool
	rolledBack      int
	extensions      map[string][2]string
	privateExists   bool
}

func newFake() *fakeConn {
	return &fakeConn{ledger: map[string]string{}, failure: errors.New("injected database failure")}
}
func (f *fakeConn) Config() *pgx.ConnConfig { return nil }
func (f *fakeConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, call{sql, args})
	if sql == f.failSQL {
		return pgconn.CommandTag{}, f.failure
	}
	if strings.HasPrefix(sql, "ALTER EXTENSION ") {
		name := strings.TrimSuffix(strings.TrimPrefix(sql, "ALTER EXTENSION \""), "\" UPDATE")
		versions := f.extensions[name]
		f.extensions[name] = [2]string{versions[1], versions[1]}
	}
	switch {
	case sql == "BEGIN":
		f.pending = map[string]string{}
	case sql == "COMMIT":
		for id, sum := range f.pending {
			f.ledger[id] = sum
		}
		f.pending = nil
	case sql == "ROLLBACK":
		f.pending = nil
		f.rolledBack++
	case strings.HasPrefix(sql, "INSERT INTO _karakoram.supabase_migrations"):
		f.pending[args[0].(string)] = args[1].(string)
	}
	return pgconn.NewCommandTag("OK"), nil
}
func (f *fakeConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	switch {
	case sql == "SELECT pg_catalog.pg_is_in_recovery()":
		return fakeRow{value: f.recovery}
	case strings.Contains(sql, "pg_available_extensions"):
		versions, ok := f.extensions[args[0].(string)]
		if !ok {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeRow{value: versions}
	case strings.Contains(sql, "pg_database"):
		return fakeRow{value: f.privateExists}
	case strings.HasPrefix(sql, "SELECT checksum FROM"):
		value, ok := f.ledger[args[0].(string)]
		if !ok {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeRow{value: value}
	case strings.Contains(sql, "pg_roles"):
		return fakeRow{value: true}
	default:
		return fakeRow{err: errors.New("unexpected query")}
	}
}

type fakeRow struct {
	value any
	err   error
}

func (r fakeRow) Scan(values ...any) error {
	if r.err != nil {
		return r.err
	}
	switch target := values[0].(type) {
	case *bool:
		*target = r.value.(bool)
	case *string:
		if len(values) == 2 {
			versions := r.value.([2]string)
			*target = versions[0]
			*values[1].(*string) = versions[1]
		} else {
			*target = r.value.(string)
		}
	default:
		return errors.New("unexpected scan destination")
	}
	return nil
}
func TestApplyIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := newFake()
	steps := Plan([]string{"auth", "rest"}, nil)
	passwords := map[string]string{"authenticator": "quote'\\; arbitrary password"}
	if err := Apply(ctx, conn, steps, passwords); err != nil {
		t.Fatal(err)
	}
	if len(conn.ledger) != len(steps) {
		t.Fatal("missing receipts")
	}
	applied := len(conn.calls)
	if err := Apply(ctx, conn, steps, passwords); err != nil {
		t.Fatal(err)
	}
	for _, c := range conn.calls[applied:] {
		for _, step := range steps {
			if c.sql == step.SQL {
				t.Fatal("migration rerun despite committed receipt")
			}
		}
	}
	passwordCalls := 0
	locks := 0
	unlocks := 0
	for _, c := range conn.calls {
		if strings.HasPrefix(c.sql, "ALTER ROLE") {
			passwordCalls++
			if len(c.args) != 2 {
				t.Fatal("unexpected password arguments")
			}
			verifier, ok := c.args[1].(string)
			if strings.Contains(c.sql, passwords["authenticator"]) || c.args[0] != pgx.QueryExecModeSimpleProtocol || !ok || !strings.HasPrefix(verifier, "SCRAM-SHA-256$4096:") || strings.Contains(verifier, passwords["authenticator"]) {
				t.Fatal("password overlay must send only a SCRAM verifier")
			}
		}
		if strings.Contains(c.sql, "pg_advisory_lock(") {
			locks++
		}
		if strings.Contains(c.sql, "pg_advisory_unlock(") {
			unlocks++
		}
	}
	if passwordCalls != 2 || locks != 2 || unlocks != 2 {
		t.Fatal("idempotent password overlay or lock lifecycle")
	}
}

func TestPasswordOverlayDisablesDurationLogging(t *testing.T) {
	conn := newFake()
	if err := applyPasswords(t.Context(), conn, map[string]string{"authenticator": "strong-password"}); err != nil {
		t.Fatal(err)
	}
	protected := false
	for _, call := range conn.calls {
		if strings.HasPrefix(call.sql, "SET LOCAL") {
			protected = strings.Contains(call.sql, "log_statement = 'none'") &&
				strings.Contains(call.sql, "log_min_duration_statement = -1") &&
				strings.Contains(call.sql, "log_min_error_statement = 'panic'") &&
				strings.Contains(call.sql, "log_parameter_max_length_on_error = 0")
		}
		if strings.HasPrefix(call.sql, "ALTER ROLE") && !protected {
			t.Fatal("credential DDL ran before logging protection")
		}
	}
}
func TestApplyResumeAndRejectDrift(t *testing.T) {
	conn := newFake()
	steps := Plan(nil, nil)
	conn.failSQL = steps[len(steps)-1].SQL
	if err := Apply(context.Background(), conn, steps, nil); err == nil {
		t.Fatal("injected migration succeeded")
	}
	if conn.rolledBack != 1 || len(conn.ledger) != len(steps)-1 {
		t.Fatal("failed transaction receipt persisted")
	}
	conn.failSQL = ""
	if err := Apply(context.Background(), conn, steps, nil); err != nil {
		t.Fatal(err)
	}
	conn.ledger[steps[0].ID] = "incorrect"
	if err := Apply(context.Background(), conn, steps, nil); err == nil || !strings.Contains(err.Error(), "checksum differs") {
		t.Fatal("migration drift accepted")
	}
}
func TestApplyRejectsUntrustedSQL(t *testing.T) {
	steps := Plan(nil, nil)
	steps[0].SQL += "SELECT 1;"
	conn := newFake()
	if Apply(context.Background(), conn, steps, nil) == nil || len(conn.calls) != 0 {
		t.Fatal("untrusted migration reached database")
	}
	steps = Plan(nil, nil)
	steps = append(steps, steps[0])
	if Apply(context.Background(), conn, steps, nil) == nil {
		t.Fatal("duplicate migration admitted")
	}
	conn.recovery = true
	if Apply(context.Background(), conn, Plan(nil, nil), nil) == nil {
		t.Fatal("standby accepted")
	}
}
func TestSafeErrors(t *testing.T) {
	const sensitive = "password-must-not-escape"
	conn := newFake()
	conn.failSQL = "ALTER ROLE \"authenticator\" PASSWORD $1"
	conn.failure = &pgconn.PgError{Code: "22000", Message: sensitive, Detail: sensitive, InternalQuery: sensitive}
	err := Apply(context.Background(), conn, Plan(nil, nil), map[string]string{"authenticator": sensitive})
	if err == nil || strings.Contains(err.Error(), sensitive) || !strings.Contains(err.Error(), "22000") {
		t.Fatal("unsafe database error")
	}
	if !errors.Is(safeError("bootstrap", context.Canceled), context.Canceled) {
		t.Fatal("cancellation identity lost")
	}
}
func TestPlanChecksums(t *testing.T) {
	for _, step := range Plan(componentNames, extensionNames) {
		sum := sha256.Sum256([]byte(step.SQL))
		if len(hex.EncodeToString(sum[:])) != 64 || step.SQL == "" {
			t.Fatal("empty migration")
		}
	}
}

func TestInstalledExtensionUpdatesOutsideLedger(t *testing.T) {
	conn := newFake()
	conn.extensions = map[string][2]string{
		"vector":         {"0.8.6", "0.8.7"},
		"set_user":       {"4.2.0", "4.3.0"},
		"pg_net":         {"0.20.3", "0.20.5"},
		"hypopg":         {"1.4.1", "1.4.5"},
		"index_advisor":  {"0.2.0", "0.2.1"},
		"pgsodium":       {"3.1.8", "3.1.11"},
		"supabase_vault": {"0.3.1", "0.3.2"},
	}
	// None are currently selected; previously installed extensions still update.
	steps := Plan(nil, nil)
	if err := Apply(t.Context(), conn, steps, nil); err != nil {
		t.Fatal(err)
	}
	updates := []string{}
	for _, call := range conn.calls {
		if strings.HasPrefix(call.sql, "ALTER EXTENSION") {
			updates = append(updates, call.sql)
			if strings.Contains(call.sql, "SCHEMA") || strings.Contains(call.sql, "CASCADE") {
				t.Fatal("update must preserve schemas and dependent objects")
			}
		}
	}
	if len(updates) != len(conn.extensions) || updates[0] != "ALTER EXTENSION \"pgsodium\" UPDATE" || updates[1] != "ALTER EXTENSION \"hypopg\" UPDATE" {
		t.Fatal("missing managed extension updates or dependency ordering", updates)
	}
	before := len(conn.calls)
	if err := Apply(t.Context(), conn, steps, nil); err != nil {
		t.Fatal(err)
	}
	for _, call := range conn.calls[before:] {
		if strings.HasPrefix(call.sql, "ALTER EXTENSION") {
			t.Fatal("up-to-date extension was updated again")
		}
	}
}

func TestExtensionUpdateFailureAndResume(t *testing.T) {
	conn := newFake()
	conn.extensions = map[string][2]string{"vector": {"0.8.6", "0.8.7"}}
	conn.failSQL = "ALTER EXTENSION \"vector\" UPDATE"
	conn.failure = &pgconn.PgError{Code: "42501", Message: "private SQL detail"}
	err := Apply(t.Context(), conn, Plan(nil, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "vector failed (SQLSTATE 42501)") || strings.Contains(err.Error(), "private SQL") {
		t.Fatal("extension update failure is not safely reported", err)
	}
	conn.failSQL = ""
	if err := Apply(t.Context(), conn, Plan(nil, nil), nil); err != nil {
		t.Fatal("committed migration ledger prevented upgrade retry", err)
	}
	conn.extensions["vector"] = [2]string{"0.8.7", ""}
	if err := updateExtensions(t.Context(), conn); err == nil || !strings.Contains(err.Error(), "no installed default") {
		t.Fatal("missing installed binaries silently accepted", err)
	}
}

func TestExistingPrivateDatabaseRequiresUpdateConnection(t *testing.T) {
	conn := newFake()
	conn.privateExists = true
	err := Apply(t.Context(), conn, Plan(nil, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "connection config") {
		t.Fatal("existing private database was skipped when component deselected", err)
	}
	for _, call := range conn.calls {
		if strings.HasPrefix(call.sql, "CREATE DATABASE") {
			t.Fatal("deselected private component must not create a database")
		}
	}
}
