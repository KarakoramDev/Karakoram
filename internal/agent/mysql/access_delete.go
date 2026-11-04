// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// A deletion snapshots a unique, credential-free marker before DROP. Account
// DDL implicitly commits: after an interrupted DROP, native absence establishes
// completion, but the same name with a different marker is a foreign account.
// An already-applied tombstone must never inspect or remove a recreated account.
func journalDeleteUser(ctx context.Context, c *sql.Conn, req myspec.AccessRequest) myspec.AccessResult {
	if req.Operation != "delete" || !myspec.ManagedUser(req.Name) || !requestIDPattern.MatchString(req.RequestID) {
		return accessRejected("invalid_request", 400)
	}
	digest, err := accessRequestDigest(req)
	if err != nil {
		return accessUnknown()
	}
	if _, err = c.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS _karakoram"); err != nil {
		return accessUnknown()
	}
	if _, err = c.ExecContext(ctx, accessReceiptDDL); err != nil {
		return accessUnknown()
	}
	var savedHash, outcome, code, savedMarker string
	var status int
	err = c.QueryRowContext(ctx, "SELECT request_hash, outcome, code, status, identity_marker FROM _karakoram.access_receipts WHERE operation_id = ?", req.RequestID).Scan(&savedHash, &outcome, &code, &status, &savedMarker)
	if err == nil {
		if savedHash != digest {
			return accessRejected("receipt_conflict", 409)
		}
		if outcome == "applied" {
			return myspec.AccessResult{Outcome: "applied"}
		}
		if outcome == "rejected" {
			return accessRejected(code, status)
		}
		if outcome != "pending" {
			return accessUnknown()
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		_, _, _, markerErr := nativeMarker(ctx, c, req.Name)
		if markerErr != nil && !errors.Is(markerErr, sql.ErrNoRows) {
			return accessUnknown()
		}
		if markerErr == nil {
			// Even SQL-created accounts need a nonempty identity for safe deletion.
			// Preparation can repeat before receipt insertion: it changes no password.
			_, err = c.ExecContext(ctx, "ALTER USER "+account(req.Name)+markerAttribute(req.RequestID, digest, "delete-prepared"))
			if err != nil {
				return deleteNativeFailure(err)
			}
			savedMarker = encodeDeletionMarker(req.RequestID, digest, "delete-prepared")
		}
		if _, err = c.ExecContext(ctx, "INSERT INTO _karakoram.access_receipts(operation_id, request_hash, outcome, identity_marker) VALUES (?, ?, 'pending', ?)", req.RequestID, digest, savedMarker); err != nil {
			return accessUnknown()
		}
	} else {
		return accessUnknown()
	}
	operation, markerHash, phase, markerErr := nativeMarker(ctx, c, req.Name)
	decision := deletionDecision(savedMarker, encodeDeletionMarker(operation, markerHash, phase), markerErr)
	switch decision {
	case "unknown":
		return accessUnknown()
	case "conflict":
		if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome='rejected', code='account_identity_changed', status=409 WHERE operation_id=?", req.RequestID); err != nil {
			return accessUnknown()
		}
		return accessRejected("account_identity_changed", 409)
	case "drop":
		if _, err = c.ExecContext(ctx, "DROP USER "+account(req.Name)); err != nil {
			result := deleteNativeFailure(err)
			if result.Outcome == "rejected" {
				if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome='rejected', code=?, status=? WHERE operation_id=?", result.Code, result.Status, req.RequestID); err != nil {
					return accessUnknown()
				}
			}
			return result
		}
	}
	if _, err = c.ExecContext(ctx, "UPDATE _karakoram.access_receipts SET outcome='applied' WHERE operation_id=?", req.RequestID); err != nil {
		return accessUnknown()
	}
	return myspec.AccessResult{Outcome: "applied"}
}
func encodeDeletionMarker(operation, digest, phase string) string {
	marker, _ := json.Marshal([3]string{operation, digest, phase})
	return string(marker)
}
func deletionDecision(savedMarker, currentMarker string, err error) string {
	if errors.Is(err, sql.ErrNoRows) {
		return "applied"
	}
	if err != nil {
		return "unknown"
	}
	if savedMarker == "" || savedMarker != currentMarker {
		return "conflict"
	}
	return "drop"
}
func deleteNativeFailure(err error) myspec.AccessResult {
	var native *mysqldriver.MySQLError
	// A missing account after an attempted DROP is resolved by observing absence
	// on replay; do not mistake MySQL's shared 1396 error for a transport receipt.
	if !errors.As(err, &native) || native.Number == 1396 {
		return accessUnknown()
	}
	return accessRejected("native_rejection", accessFailure(err).status)
}
