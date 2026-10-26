// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Receipts are in the management database and replicate with native SQL. The
// role and its OID receipt commit in one transaction; a name alone is never
// authority to adopt a CREATE. No password/verifier is stored in the receipt.
const accessReceiptDDL = `CREATE SCHEMA IF NOT EXISTS _karakoram;
REVOKE ALL ON SCHEMA _karakoram FROM PUBLIC;
CREATE TABLE IF NOT EXISTS _karakoram.access_receipts (
 operation_id text PRIMARY KEY, request_hash text NOT NULL, role_oid oid NOT NULL
);
REVOKE ALL ON TABLE _karakoram.access_receipts FROM PUBLIC`

type accessBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

func accessRejected(code, message string, status int) pgspec.AccessResult {
	return pgspec.AccessResult{Outcome: "rejected", Code: code, Message: message, Status: status}
}

func journalRole(ctx context.Context, c accessBeginner, req pgspec.AccessRequest) pgspec.AccessResult {
	unknown := pgspec.AccessResult{Outcome: "unknown", Code: "transaction_unconfirmed", Message: "PostgreSQL credential transaction outcome is unknown; retry the same sealed intent"}
	if err := validateAccessRequest(pgspec.ActionRoles, req); err != nil {
		return accessRejected("invalid_request", "PostgreSQL rejected the journaled role definition", http.StatusBadRequest)
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return unknown
	}
	sum := sha256.Sum256(encoded)
	digest := hex.EncodeToString(sum[:])
	tx, err := c.Begin(ctx)
	if err != nil {
		return unknown
	}
	defer tx.Rollback(ctx)
	// Serialize receipt creation and lookup before the first schema/table DDL,
	// including separate requests racing on the same otherwise absent role.
	if _, err = tx.Exec(ctx, "SELECT pg_catalog.pg_advisory_xact_lock(68474219400333)"); err != nil {
		return unknown
	}
	if _, err = tx.Exec(ctx, accessReceiptDDL); err != nil {
		return unknown
	}
	var savedDigest string
	var savedOID uint32
	err = tx.QueryRow(ctx, `SELECT request_hash,role_oid FROM _karakoram.access_receipts WHERE operation_id=$1`, req.RequestID).Scan(&savedDigest, &savedOID)
	if err == nil {
		if savedDigest != digest {
			return accessRejected("receipt_conflict", "Credential operation identity belongs to a different request", http.StatusConflict)
		}
		var currentOID uint32
		err = tx.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$1`, req.Role.Name).Scan(&currentOID)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && currentOID != savedOID {
			return accessRejected("role_identity_changed", "Credential recovery refused: the original role was removed or replaced by a different native role", http.StatusConflict)
		}
		if err != nil {
			return unknown
		}
		// Do not replay ALTER after a successful receipt: even matching role
		// identity is not permission to erase subsequent native admin changes.
		return pgspec.AccessResult{Outcome: "applied", RoleOID: savedOID}
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return unknown
	}
	// Only after positively establishing receipt absence can a rejected SQL
	// transaction prove this intent never committed. Transport errors and
	// failures before lookup remain unknown regardless of HTTP status.
	if err = writeRoleTx(ctx, tx, req.RoleOperation, *req.Role); err != nil {
		return rejectedRoleTransaction(ctx, tx, err, req.RoleOperation)
	}
	var oid uint32
	if err = tx.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$1`, req.Role.Name).Scan(&oid); err != nil {
		return unknown
	}
	if _, err = tx.Exec(ctx, `INSERT INTO _karakoram.access_receipts(operation_id,request_hash,role_oid) VALUES($1,$2,$3)`, req.RequestID, digest, oid); err != nil {
		return unknown
	}
	if err = tx.Commit(ctx); err != nil {
		// Only an explicit rollback class is definitive. In particular,
		// statement/transaction-resolution-unknown errors are not rollback
		// acknowledgements even when PostgreSQL sent an ErrorResponse.
		if definitiveRoleCommitRejection(err) {
			return accessRejected("transaction_rejected", "PostgreSQL rolled back the credential transaction", http.StatusConflict)
		}
		return unknown
	}
	return pgspec.AccessResult{Outcome: "applied", RoleOID: oid}
}

func definitiveRoleCommitRejection(err error) bool {
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return true
	}
	var sqlError *pgconn.PgError
	if !errors.As(err, &sqlError) {
		return false
	}
	return strings.HasPrefix(sqlError.Code, "23") ||
		strings.HasPrefix(sqlError.Code, "40") && sqlError.Code != "40003"
}

func rejectedRoleTransaction(ctx context.Context, tx pgx.Tx, err error, operation string) pgspec.AccessResult {
	var sqlError *pgconn.PgError
	if !errors.As(err, &sqlError) || tx.Rollback(ctx) != nil {
		return pgspec.AccessResult{Outcome: "unknown", Code: "transaction_unconfirmed", Message: "PostgreSQL credential transaction rollback is unconfirmed; retry the same sealed intent"}
	}
	safe := accessFailure(err).(*accessError)
	code, message := "transaction_rejected", safe.message
	switch sqlError.Code {
	case "42704":
		code, message = "role_or_membership_missing", "PostgreSQL rejected the role change: the role or a requested membership target does not exist"
	case "42710":
		if operation == "create" {
			code, message = "foreign_role_exists", "Role creation refused: a native role with this name already exists without this operation's receipt"
		}
	}
	return accessRejected(code, message, safe.status)
}
