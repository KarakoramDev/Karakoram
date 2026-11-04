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
)

func TestAdministratorTransitionLocalAuthentication(t *testing.T) {
	s := testSpec()
	s.Superuser = pgspec.Credentials{Username: "postgres", Password: "desired-synthetic"}
	s.AlternateSuperuser = &pgspec.Credentials{Username: "postgres", Password: "alternate-synthetic"}
	for _, desiredWorks := range []bool{true, false} {
		var attempts []string
		_, err := localConnectWith(t.Context(), s, dataPaths(t.TempDir()), func(_ context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
			attempts = append(attempts, cfg.Password)
			if desiredWorks || cfg.Password == s.AlternateSuperuser.Password {
				return &pgx.Conn{}, nil
			}
			return nil, errors.New("authentication rejected")
		})
		if err != nil || len(attempts) != map[bool]int{true: 1, false: 2}[desiredWorks] || attempts[0] != s.Superuser.Password {
			t.Fatal("incorrect credential fallback order")
		}
	}
	statements, err := roleStatements(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range statements {
		if strings.Contains(sql, "ALTER ROLE \"postgres\"") {
			t.Fatal("bootstrap rewrote administrator during receipt-backed transition")
		}
	}
}

func TestAdministratorReceiptSurvivesLostCommit(t *testing.T) {
	db := &receiptSQL{oid: 10, loseCommit: true}
	verifier := testVerifier()
	req := pgspec.AccessRequest{Operation: "admin-rotation", RequestID: "admin-rotation-test", Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "postgres"}, Password: &verifier}}
	result := journalRole(t.Context(), db, req)
	if result.Outcome != "unknown" || db.mutations != 1 {
		t.Fatalf("lost commit guessed: %+v", result)
	}
	result = journalRole(t.Context(), db, req)
	if result.Outcome != "applied" || db.mutations != 1 {
		t.Fatalf("receipt replay repeated ALTER: %+v", result)
	}
	propagationDisabled := false
	for _, statement := range db.statements {
		if strings.Contains(statement, "citus.enable_ddl_propagation") && strings.Contains(statement, "'off',true") {
			propagationDisabled = true
		}
		if strings.HasPrefix(statement, "ALTER ROLE postgres") && !propagationDisabled {
			t.Fatal("administrator DDL preceded node-local Citus propagation fencing")
		}
	}
	if !propagationDisabled {
		t.Fatal("Citus transaction propagation was not fenced")
	}
	// A fresh password must not be adopted under the same operation identity.
	changed := strings.Replace(verifier, "$4096:", "$8192:", 1)
	req.Role.Password = &changed
	if result = journalRole(t.Context(), db, req); result.Outcome != "rejected" || strings.Contains(result.Message, verifier) {
		t.Fatal("receipt conflict accepted or disclosed credential")
	}
}
