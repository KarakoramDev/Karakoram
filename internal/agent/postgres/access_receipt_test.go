// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type receiptSQL struct {
	oid              uint32
	digest           string
	receiptOID       uint32
	mutations        int
	rejectMembership bool
	loseCommit       bool
	beginError       error
}
type receiptTx struct {
	pgx.Tx
	db         *receiptSQL
	oid        uint32
	digest     string
	receiptOID uint32
	rolledBack bool
}

func (s *receiptSQL) Begin(context.Context) (pgx.Tx, error) {
	if s.beginError != nil {
		return nil, s.beginError
	}
	return &receiptTx{db: s, oid: s.oid, digest: s.digest, receiptOID: s.receiptOID}, nil
}
func (tx *receiptTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.HasPrefix(sql, "CREATE ROLE"):
		if tx.oid != 0 {
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "42710"}
		}
		tx.oid = 17001
		tx.db.mutations++
	case strings.HasPrefix(sql, "ALTER ROLE"):
		tx.db.mutations++
	case strings.HasPrefix(sql, "GRANT "):
		if tx.db.rejectMembership {
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "42704", Message: "private SQL and credential material"}
		}
	case strings.HasPrefix(sql, "INSERT INTO _karakoram.access_receipts"):
		tx.digest, tx.receiptOID = args[1].(string), args[2].(uint32)
	}
	return pgconn.NewCommandTag("OK"), nil
}

type receiptRow struct{ scan func(...any) error }

func (r receiptRow) Scan(dest ...any) error { return r.scan(dest...) }
func (tx *receiptTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	return receiptRow{scan: func(dest ...any) error {
		if strings.Contains(sql, "SELECT request_hash") {
			if tx.digest == "" {
				return pgx.ErrNoRows
			}
			*dest[0].(*string), *dest[1].(*uint32) = tx.digest, tx.receiptOID
		} else {
			if tx.oid == 0 {
				return pgx.ErrNoRows
			}
			*dest[0].(*uint32) = tx.oid
		}
		return nil
	}}
}

type noMemberships struct{ pgx.Rows }

func (noMemberships) Next() bool { return false }
func (noMemberships) Err() error { return nil }
func (noMemberships) Close()     {}
func (tx *receiptTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return noMemberships{}, nil
}
func (tx *receiptTx) Rollback(context.Context) error { tx.rolledBack = true; return nil }
func (tx *receiptTx) Commit(context.Context) error {
	tx.db.oid, tx.db.digest, tx.db.receiptOID = tx.oid, tx.digest, tx.receiptOID
	if tx.db.loseCommit {
		tx.db.loseCommit = false
		return errors.New("connection lost after commit")
	}
	return nil
}
func journalTestRequest() pgspec.AccessRequest {
	verifier := testVerifier()
	return pgspec.AccessRequest{Operation: "journal", RoleOperation: "create", RequestID: "sealed-operation-1", Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "app", Login: true, ConnectionLimit: -1}, Password: &verifier}}
}

func TestCredentialReceiptDefinitiveRejection(t *testing.T) {
	db := &receiptSQL{rejectMembership: true}
	req := journalTestRequest()
	req.Role.MemberOf = []string{"missing_parent"}
	result := journalRole(t.Context(), db, req)
	if result.Outcome != "rejected" || result.Code != "role_or_membership_missing" || result.Status != 404 {
		t.Fatalf("not a precise definitive rejection: %+v", result)
	}
	if db.oid != 0 || db.digest != "" {
		t.Fatal("rejected role or receipt committed")
	}
	if strings.Contains(result.Message, "private SQL") {
		t.Fatal("SQL diagnostic leaked")
	}
	db.rejectMembership = false
	req.RequestID = "different-valid-request"
	if result = journalRole(t.Context(), db, req); result.Outcome != "applied" || result.RoleOID == 0 {
		t.Fatalf("later write fenced: %+v", result)
	}
}

func TestCredentialReceiptCreateIdentity(t *testing.T) {
	for _, replacement := range []string{"preexisting", "recreated", "dropped"} {
		t.Run(replacement, func(t *testing.T) {
			db, req := &receiptSQL{}, journalTestRequest()
			if replacement == "preexisting" {
				db.oid = 19001
			} else {
				db.loseCommit = true
				if result := journalRole(t.Context(), db, req); result.Outcome != "unknown" {
					t.Fatalf("lost commit guessed: %+v", result)
				}
				if db.receiptOID != 17001 || db.oid != 17001 {
					t.Fatal("role OID receipt did not commit atomically")
				}
				if result := journalRole(t.Context(), db, req); result.Outcome != "applied" || result.RoleOID != 17001 || db.mutations != 1 {
					t.Fatalf("committed replay mutated SQL: %+v calls=%d", result, db.mutations)
				}
				db.oid = 19001
				if replacement == "dropped" {
					db.oid = 0
				}
			}
			before, mutations := db.oid, db.mutations
			result := journalRole(t.Context(), db, req)
			want := "role_identity_changed"
			if replacement == "preexisting" {
				want = "foreign_role_exists"
			}
			if result.Outcome != "rejected" || result.Code != want || db.oid != before || db.mutations != mutations {
				t.Fatalf("foreign role adopted: %+v oid=%d writes=%d", result, db.oid, db.mutations)
			}
		})
	}
}

func TestCredentialReceiptRequestAndUnknownBoundaries(t *testing.T) {
	req := journalTestRequest()
	db := &receiptSQL{beginError: errors.New("temporary connection failure")}
	if result := journalRole(t.Context(), db, req); result.Outcome != "unknown" {
		t.Fatalf("pre-lookup error guessed: %+v", result)
	}
	db.beginError = nil
	if result := journalRole(t.Context(), db, req); result.Outcome != "applied" {
		t.Fatal(result)
	}
	req.Role.Createdb = true
	if result := journalRole(t.Context(), db, req); result.Code != "receipt_conflict" || result.Outcome != "rejected" || db.mutations != 1 {
		t.Fatalf("request identity not enforced: %+v", result)
	}
}

func TestCredentialReceiptCommitClassification(t *testing.T) {
	for _, code := range []string{"40003", "08007", "08006", "57P01", "XX000"} {
		if definitiveRoleCommitRejection(&pgconn.PgError{Code: code}) {
			t.Fatalf("ambiguous COMMIT SQLSTATE %s treated as rollback", code)
		}
	}
	for _, code := range []string{"23505", "23514", "40001", "40P01"} {
		if !definitiveRoleCommitRejection(&pgconn.PgError{Code: code}) {
			t.Fatalf("explicit rollback SQLSTATE %s not recognized", code)
		}
	}
	if !definitiveRoleCommitRejection(pgx.ErrTxCommitRollback) {
		t.Fatal("COMMIT acknowledged as ROLLBACK not recognized")
	}
	if definitiveRoleCommitRejection(errors.New("connection lost")) {
		t.Fatal("transport ambiguity treated as rollback")
	}
}
