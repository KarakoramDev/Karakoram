// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNativeCredentialReceiptRollbackAndIdentity(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native credential receipts")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	req := journalTestRequest()
	req.Role.Name = "receipt_child"
	req.Role.MemberOf = []string{"receipt_parent"}
	var result pgspec.AccessResult
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "rejected" || result.Code != "role_or_membership_missing" {
		t.Fatalf("SQL rollback not acknowledged: %+v", result)
	}
	var count int
	if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname=$1", req.Role.Name).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected CREATE survived rollback", count, err)
	}
	parent := pgspec.RoleWrite{Role: pgspec.Role{Name: "receipt_parent", ConnectionLimit: -1}}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &parent}, nil)
	req.RequestID = "valid-create-after-rejection"
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "applied" || result.RoleOID == 0 {
		t.Fatalf("later transaction blocked: %+v", result)
	}
	var receiptOID uint32
	if err = c.QueryRow(ctx, "SELECT role_oid FROM _karakoram.access_receipts WHERE operation_id=$1", req.RequestID).Scan(&receiptOID); err != nil || receiptOID != result.RoleOID {
		t.Fatal("native OID receipt missing", err)
	}
	// Simulate a lost success response: exact replay observes the committed
	// receipt without reapplying any role options or membership writes.
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "applied" || result.RoleOID != receiptOID {
		t.Fatal("receipt replay failed", result)
	}
	if _, err = c.Exec(ctx, "DROP ROLE receipt_child"); err != nil {
		t.Fatal(err)
	}
	foreignVerifier := nativeVerifier(t, "foreign-administrator-password")
	if _, err = c.Exec(ctx, "CREATE ROLE receipt_child WITH LOGIN CREATEDB PASSWORD $1; GRANT pg_monitor TO receipt_child", pgx.QueryExecModeSimpleProtocol, foreignVerifier); err != nil {
		t.Fatal(err)
	}
	assertForeign := func() {
		t.Helper()
		var oid uint32
		var verifier string
		var createdb, monitor bool
		if err := c.QueryRow(ctx, "SELECT oid,rolpassword,rolcreatedb,pg_catalog.pg_has_role(oid,'pg_monitor','MEMBER') FROM pg_catalog.pg_authid WHERE rolname='receipt_child'").Scan(&oid, &verifier, &createdb, &monitor); err != nil {
			t.Fatal(err)
		}
		if oid == receiptOID || verifier != foreignVerifier || !createdb || !monitor {
			t.Fatal("foreign role identity, verifier, privileges or memberships were overwritten")
		}
	}
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "rejected" || result.Code != "role_identity_changed" {
		t.Fatal("drop/recreate was adopted", result)
	}
	assertForeign()
	// Also cover the original absence-to-CREATE race: no receipt exists for
	// this operation and a same-name role must receive duplicate-object refusal.
	req.RequestID = "racing-first-create"
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "rejected" || result.Code != "foreign_role_exists" {
		t.Fatal("unrelated existing role was adopted", result)
	}
	assertForeign()
}

func TestNativeBindingReceiptOIDFence(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native binding receipts")
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	req := bindingJournalRequest("create", 0)
	var result pgspec.AccessResult
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "applied" || result.RoleOID == 0 {
		t.Fatal("binding create failed", result)
	}
	originalOID := result.RoleOID
	// Prove the catalog lock conflicts with native role DDL, not just
	// cooperating journal writers or advisory-lock participants.
	lockTx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(context.Background())
	if _, err = lockTx.Exec(ctx, "LOCK TABLE pg_catalog.pg_authid IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	other, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	if _, err = other.Exec(ctx, "SET lock_timeout='200ms'"); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"DROP ROLE " + pgx.Identifier{req.Role.Name}.Sanitize(), "CREATE ROLE catalog_lock_probe"} {
		_, err = other.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("catalog fence did not block native DDL %q: %v", sql, err)
		}
	}
	if err = lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A real matching OID authorizes rotation and keep-data NOLOGIN.
	for i, login := range []bool{true, false} {
		req.RequestID = []string{"binding-rotate", "binding-disable"}[i]
		req.RoleOperation, req.ExpectedRoleOID, req.Role.Login = "update", originalOID, login
		nativeRequest(t, h, pgspec.ActionRoles, req, &result)
		if result.Outcome != "applied" || result.RoleOID != originalOID {
			t.Fatal("matching OID update failed", result)
		}
	}
	if _, err = c.Exec(ctx, "DROP ROLE "+pgx.Identifier{req.Role.Name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	foreignVerifier := nativeVerifier(t, "binding-foreign-password")
	if _, err = c.Exec(ctx, "CREATE ROLE "+pgx.Identifier{req.Role.Name}.Sanitize()+" WITH LOGIN CREATEDB PASSWORD $1", pgx.QueryExecModeSimpleProtocol, foreignVerifier); err != nil {
		t.Fatal(err)
	}
	var foreignOID uint32
	if err = c.QueryRow(ctx, "SELECT oid FROM pg_roles WHERE rolname=$1", req.Role.Name).Scan(&foreignOID); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"update", "delete"} {
		req.RequestID, req.RoleOperation = "foreign-"+operation, operation
		nativeRequest(t, h, pgspec.ActionRoles, req, &result)
		if result.Outcome != "rejected" || result.Code != "role_identity_changed" {
			t.Fatalf("foreign %s not rejected: %+v", operation, result)
		}
		var oid uint32
		var verifier string
		var login, createdb bool
		if err = c.QueryRow(ctx, "SELECT oid,rolpassword,rolcanlogin,rolcreatedb FROM pg_authid WHERE rolname=$1", req.Role.Name).Scan(&oid, &verifier, &login, &createdb); err != nil || oid != foreignOID || verifier != foreignVerifier || !login || !createdb {
			t.Fatalf("foreign role changed: oid=%d login=%v createdb=%v err=%v", oid, login, createdb, err)
		}
	}
	// Only a committed DROP receipt makes absence an applied outcome.
	req.RequestID, req.ExpectedRoleOID = "owned-drop", foreignOID
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "applied" || result.RoleOID != foreignOID {
		t.Fatal("owned drop failed", result)
	}
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "applied" || result.RoleOID != foreignOID {
		t.Fatal("committed drop replay failed", result)
	}
	req.RequestID = "unproven-missing-drop"
	nativeRequest(t, h, pgspec.ActionRoles, req, &result)
	if result.Outcome != "rejected" || result.Code != "role_identity_changed" {
		t.Fatal("missing role accepted without drop receipt", result)
	}
}
