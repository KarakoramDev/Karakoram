// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"strconv"
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
	requireRoleLock  bool
}
type receiptTx struct {
	pgx.Tx
	db         *receiptSQL
	oid        uint32
	digest     string
	receiptOID uint32
	rolledBack bool
	roleLocked bool
}

func (s *receiptSQL) Begin(context.Context) (pgx.Tx, error) {
	if s.beginError != nil {
		return nil, s.beginError
	}
	return &receiptTx{db: s, oid: s.oid, digest: s.digest, receiptOID: s.receiptOID}, nil
}
func (tx *receiptTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case sql == "LOCK TABLE pg_catalog.pg_authid IN SHARE ROW EXCLUSIVE MODE":
		tx.roleLocked = true
	case strings.HasPrefix(sql, "DROP ROLE"):
		if tx.db.requireRoleLock && !tx.roleLocked {
			panic("binding drop without role catalog lock")
		}
		tx.oid = 0
		tx.db.mutations++
	case strings.HasPrefix(sql, "CREATE ROLE"):
		if tx.oid != 0 {
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "42710"}
		}
		tx.oid = 17001
		tx.db.mutations++
	case strings.HasPrefix(sql, "ALTER ROLE"):
		if tx.db.requireRoleLock && !tx.roleLocked {
			panic("binding update without role catalog lock")
		}
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
			if tx.db.requireRoleLock && !tx.roleLocked {
				panic("binding OID comparison without role catalog lock")
			}
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

func bindingJournalRequest(operation string, oid uint32) pgspec.AccessRequest {
	req := journalTestRequest()
	req.Binding = true
	req.Role.Binding = true
	req.Role.Name = "kb_0123456789abcdef0123456789abcdef"
	req.Role.ConnectionLimit = 10
	req.RoleOperation = operation
	req.ExpectedRoleOID = oid
	return req
}

func TestBindingReceiptMutationIdentity(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		for _, oid := range []uint32{0, 19001, 17001} {
			t.Run(operation+"/"+strconv.FormatUint(uint64(oid), 10), func(t *testing.T) {
				db := &receiptSQL{oid: oid, requireRoleLock: true}
				req := bindingJournalRequest(operation, 17001)
				result := journalRole(t.Context(), db, req)
				if oid != 17001 {
					if result.Outcome != "rejected" || result.Code != "role_identity_changed" || db.mutations != 0 || db.oid != oid {
						t.Fatalf("foreign or missing role mutated: %+v db=%+v", result, db)
					}
					return
				}
				if result.Outcome != "applied" || result.RoleOID != oid || db.mutations != 1 {
					t.Fatalf("original role mutation failed: %+v db=%+v", result, db)
				}
			})
		}
	}
}

func TestBindingReceiptDropReplay(t *testing.T) {
	db := &receiptSQL{oid: 17001, loseCommit: true, requireRoleLock: true}
	req := bindingJournalRequest("delete", 17001)
	if result := journalRole(t.Context(), db, req); result.Outcome != "unknown" || db.oid != 0 || db.receiptOID != 17001 {
		t.Fatalf("drop commit ambiguity guessed: %+v db=%+v", result, db)
	}
	if result := journalRole(t.Context(), db, req); result.Outcome != "applied" || result.RoleOID != 17001 || db.mutations != 1 {
		t.Fatalf("drop receipt not replayed: %+v db=%+v", result, db)
	}
	db.oid = 19001
	if result := journalRole(t.Context(), db, req); result.Outcome != "rejected" || result.Code != "role_identity_changed" || db.oid != 19001 || db.mutations != 1 {
		t.Fatalf("drop replay touched replacement: %+v db=%+v", result, db)
	}
}

func TestBindingAccessMutationValidation(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		req := bindingJournalRequest(operation, 0)
		if err := validateAccessRequest(pgspec.ActionRoles, req); err == nil {
			t.Fatalf("%s accepted zero expected OID", operation)
		}
		req.ExpectedRoleOID = 17001
		if err := validateAccessRequest(pgspec.ActionRoles, req); err != nil {
			t.Fatalf("%s refused fenced journal: %v", operation, err)
		}
	}
	for _, operation := range []string{"update", "password", "delete"} {
		req := bindingJournalRequest("update", 17001)
		req.Operation, req.Name = operation, req.Role.Name
		if err := validateAccessRequest(pgspec.ActionRoles, req); err == nil {
			t.Fatalf("legacy binding %s bypassed journal fence", operation)
		}
	}
	req := bindingJournalRequest("update", 17001)
	req.Role.Login = false
	sql, _, err := roleStatement("update", *req.Role)
	if err != nil || !strings.HasPrefix(sql, `ALTER ROLE "kb_0123456789abcdef0123456789abcdef" WITH NOLOGIN`) {
		t.Fatalf("keep-data release rendering: %q %v", sql, err)
	}
}

func TestBindingReceiptUpdateReplay(t *testing.T) {
	db := &receiptSQL{oid: 17001, loseCommit: true, requireRoleLock: true}
	req := bindingJournalRequest("update", 17001)
	if result := journalRole(t.Context(), db, req); result.Outcome != "unknown" {
		t.Fatalf("update commit ambiguity guessed: %+v", result)
	}
	if result := journalRole(t.Context(), db, req); result.Outcome != "applied" || result.RoleOID != 17001 || db.mutations != 1 {
		t.Fatalf("update receipt replay mutated role: %+v db=%+v", result, db)
	}
	for _, oid := range []uint32{0, 19001} {
		db.oid = oid
		if result := journalRole(t.Context(), db, req); result.Outcome != "rejected" || result.Code != "role_identity_changed" || db.mutations != 1 || db.oid != oid {
			t.Fatalf("update receipt adopted missing/replacement role: %+v db=%+v", result, db)
		}
	}
}
