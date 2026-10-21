// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// MySQL account DDL implicitly commits, so wrapping it and an INSERT in a
// transaction is NOT a receipt. The operation marker is an ATTRIBUTE on the
// very same atomic CREATE/ALTER USER statement. The table is a durable replay
// index; a matching native marker repairs a crash between DDL and receipt update.
// Neither location contains a password or authentication verifier. ALTER first
// writes a credential-free prepared marker, then the pending receipt. Its DDL
// atomically changes prepared to applied along with the credential. Recovery can
// execute a prepared operation, repair an applied receipt, or fail closed on a
// replaced account; it never blindly repeats a dual-password rotation.
const accessReceiptDDL = `CREATE TABLE IF NOT EXISTS _karakoram.access_receipts (
 operation_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 outcome VARCHAR(16) CHARACTER SET ascii NOT NULL,
 code VARCHAR(64) CHARACTER SET ascii NOT NULL DEFAULT '',
 status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
 identity_marker VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
) ENGINE=InnoDB`

func accessRequestDigest(req myspec.AccessRequest) (string, error) {
	encoded, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
func accessRejected(code string, status int) myspec.AccessResult {
	return myspec.AccessResult{Outcome: "rejected", Code: code, Message: "MySQL rejected the account operation", Status: status}
}
func accessUnknown() myspec.AccessResult {
	return myspec.AccessResult{Outcome: "unknown", Code: "execution_unconfirmed", Message: "MySQL account operation outcome is unknown; preserve the sealed intent"}
}
func nativeMarker(ctx context.Context, c *sql.Conn, name string) (operation, digest, phase string, err error) {
	err = c.QueryRowContext(ctx, `SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.metadata._karakoram_access.operation')), ''), COALESCE(JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.metadata._karakoram_access.requestHash')), ''), COALESCE(JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.metadata._karakoram_access.phase')), '') FROM mysql.user WHERE User = ? AND Host = '%'`, name).Scan(&operation, &digest, &phase)
	return
}
func journalUser(ctx context.Context, c *sql.Conn, req myspec.AccessRequest) myspec.AccessResult {
	digest, err := accessRequestDigest(req)
	if err != nil {
		return accessUnknown()
	}
	stmt, err := userStatement(req, digest)
	if err != nil {
		return accessRejected("invalid_request", 400)
	}
	name := req.Name
	if req.Operation == "create" {
		name = req.User.Name
	}
	if _, err = c.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS _karakoram"); err != nil {
		return accessUnknown()
	}
	if _, err = c.ExecContext(ctx, accessReceiptDDL); err != nil {
		return accessUnknown()
	}
	var savedHash, outcome, code string
	var status int
	err = c.QueryRowContext(ctx, "SELECT request_hash, outcome, code, status FROM _karakoram.access_receipts WHERE operation_id = ?", req.RequestID).Scan(&savedHash, &outcome, &code, &status)
	if err == nil {
		if savedHash != digest {
			return accessRejected("receipt_conflict", 409)
		}
		if outcome == "rejected" {
			return accessRejected(code, status)
		}
		operation, markerHash, phase, markerErr := nativeMarker(ctx, c, name)
		if markerErr == nil && operation == req.RequestID && markerHash == digest && phase == "applied" {
			if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome = 'applied' WHERE operation_id = ?", req.RequestID); err != nil {
				return accessUnknown()
			}
			return myspec.AccessResult{Outcome: "applied"}
		}
		if outcome == "applied" {
			return accessRejected("account_identity_changed", 409)
		}
		if !pendingCanExecute(req, digest, operation, markerHash, phase, markerErr) {
			return accessUnknown()
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if req.Operation != "create" {
			// This preparation changes metadata only, never credentials. A crash
			// here can safely repeat preparation before inserting the receipt.
			_, err = c.ExecContext(ctx, "ALTER USER "+account(name)+markerAttribute(req.RequestID, digest, "prepared"))
			if err != nil {
				var native *mysqldriver.MySQLError
				if errors.As(err, &native) {
					return accessRejected("native_rejection", accessFailure(err).status)
				}
				return accessUnknown()
			}
		}
		if _, err = c.ExecContext(ctx, "INSERT INTO _karakoram.access_receipts(operation_id, request_hash, outcome) VALUES (?, ?, 'pending')", req.RequestID, digest); err != nil {
			return accessUnknown()
		}
	} else {
		return accessUnknown()
	}
	if err = executeUserStatement(ctx, c, stmt); err != nil {
		var native *mysqldriver.MySQLError
		// Only a server error packet is definitive rejection. Network errors leave
		// the receipt pending even if the client believes EXECUTE failed.
		if !errors.As(err, &native) {
			return accessUnknown()
		}
		safe := accessFailure(err)
		if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome = 'rejected', code = 'native_rejection', status = ? WHERE operation_id = ?", safe.status, req.RequestID); err != nil {
			return accessUnknown()
		}
		return accessRejected("native_rejection", safe.status)
	}
	if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome = 'applied' WHERE operation_id = ?", req.RequestID); err != nil {
		return accessUnknown()
	}
	return myspec.AccessResult{Outcome: "applied"}
}

func pendingCanExecute(req myspec.AccessRequest, digest, operation, markerHash, phase string, markerErr error) bool {
	if req.Operation == "create" {
		return errors.Is(markerErr, sql.ErrNoRows)
	}
	return markerErr == nil && operation == req.RequestID && markerHash == digest && phase == "prepared"
}
func executeUserStatement(ctx context.Context, c *sql.Conn, stmt accessStatement) error {
	if stmt.password == nil {
		_, err := c.ExecContext(ctx, stmt.prefix)
		return err
	}
	// Only SET contains a bound plaintext value. QUOTE operates inside the server
	// and account DDL is prepared there. No client-built SQL embeds the password.
	if _, err := c.ExecContext(ctx, "SET @kk_access_pw = ?", *stmt.password); err != nil {
		return err
	}
	if _, err := c.ExecContext(ctx, "SET @kk_access_sql = CONCAT(?, QUOTE(@kk_access_pw), ?)", stmt.prefix, stmt.suffix); err != nil {
		return err
	}
	if _, err := c.ExecContext(ctx, "PREPARE kk_access_statement FROM @kk_access_sql"); err != nil {
		return err
	}
	if _, err := c.ExecContext(ctx, "EXECUTE kk_access_statement"); err != nil {
		return err
	}
	_, _ = c.ExecContext(ctx, "DEALLOCATE PREPARE kk_access_statement")
	_, _ = c.ExecContext(ctx, "SET @kk_access_pw = NULL, @kk_access_sql = NULL")
	return nil
}
