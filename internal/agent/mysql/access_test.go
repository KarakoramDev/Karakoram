// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestUserStatements(t *testing.T) {
	password := "secret'\\你好-password"
	req := myspec.AccessRequest{Operation: "create", RequestID: "operation_1", User: &myspec.UserWrite{Name: "app", MaxUserConnections: 7, Password: &password}}
	digest, err := accessRequestDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := userStatement(req, digest)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stmt.prefix+stmt.suffix, password) || stmt.password == nil || *stmt.password != password {
		t.Fatal("password escaped into SQL")
	}
	if stmt.prefix != "CREATE USER 'app'@'%' IDENTIFIED BY " || !strings.Contains(stmt.suffix, "REQUIRE SSL WITH MAX_USER_CONNECTIONS 7 ACCOUNT UNLOCK ATTRIBUTE '") {
		t.Fatalf("unexpected statement: %#v", stmt)
	}
	req = myspec.AccessRequest{Operation: "password", RequestID: "operation_2", Name: "app", Password: &password, RetainCurrent: true}
	stmt, err = userStatement(req, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stmt.suffix, "RETAIN CURRENT PASSWORD REQUIRE SSL ATTRIBUTE") {
		t.Fatal(stmt.suffix)
	}
	req.Operation = "discard-old"
	req.Password = nil
	stmt, err = userStatement(req, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stmt.prefix, "ALTER USER 'app'@'%' DISCARD OLD PASSWORD REQUIRE SSL ATTRIBUTE") {
		t.Fatal(stmt.prefix)
	}
	for _, name := range []string{"root", "kk_agent", "mysql", "sys", "performance_schema", "information_schema", "a' OR 1=1", "a@b", strings.Repeat("a", 33)} {
		req.Name = name
		if _, err = userStatement(req, digest); err == nil {
			t.Errorf("accepted protected/invalid account %q", name)
		}
	}
	req.Name = "app"
	req.RequestID = "x'"
	if _, err = userStatement(req, digest); err == nil {
		t.Fatal("accepted invalid receipt identity")
	}
}
func TestDatabaseStatements(t *testing.T) {
	stmt, err := databaseStatement(myspec.Database{Name: "app_db$"})
	if err != nil {
		t.Fatal(err)
	}
	if stmt != "CREATE DATABASE `app_db$` CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci`" {
		t.Fatal(stmt)
	}
	for _, database := range []myspec.Database{{Name: "mysql"}, {Name: "_karakoram"}, {Name: "app", CharacterSet: "utf8mb4;DROP"}, {Name: "app", Collation: "a`b"}} {
		if _, err := databaseStatement(database); err == nil {
			t.Fatalf("accepted unsafe database %#v", database)
		}
	}
	if quoteIdentifier("a`b") != "`a``b`" {
		t.Fatal("identifier quoting")
	}
}
func TestGrantStatements(t *testing.T) {
	grant := myspec.Grant{User: "app", Database: "app_db", Privileges: []string{"SELECT", "CREATE_VIEW"}}
	stmt, err := grantStatement("grant", grant, false)
	if err != nil {
		t.Fatal(err)
	}
	if stmt != "GRANT SELECT, CREATE VIEW ON `app\\_db`.* TO 'app'@'%'" {
		t.Fatal(stmt)
	}
	stmt, err = grantStatement("revoke", grant, true)
	if err != nil {
		t.Fatal(err)
	}
	if stmt != "REVOKE SELECT, CREATE VIEW ON `app_db`.* FROM 'app'@'%'" {
		t.Fatal(stmt)
	}
	for _, privileges := range [][]string{nil, {"SUPER"}, {"GRANT_OPTION"}, {"ALL", "SELECT"}, {"SELECT", "SELECT"}, {"SELECT; DROP USER root"}} {
		grant.Privileges = privileges
		if _, err = grantStatement("grant", grant, false); err == nil {
			t.Fatalf("accepted invalid privileges %#v", privileges)
		}
	}
}
func TestRequestDigest(t *testing.T) {
	password := "a-secret-password"
	req := myspec.AccessRequest{Operation: "password", RequestID: "one", Name: "app", Password: &password}
	a, _ := accessRequestDigest(req)
	b, _ := accessRequestDigest(req)
	if a != b || len(a) != 64 {
		t.Fatal("unstable digest")
	}
	req.RetainCurrent = true
	b, _ = accessRequestDigest(req)
	if a == b {
		t.Fatal("retain flag missing from digest")
	}
	password = "a-different-password"
	c, _ := accessRequestDigest(req)
	if b == c {
		t.Fatal("password missing from digest")
	}
}
func TestAccessErrorsClosedVocabulary(t *testing.T) {
	secret := "sensitive-native-error"
	for _, err := range []error{errors.New(secret), sql.ErrNoRows, &mysqldriver.MySQLError{Number: 1064, Message: secret}, &mysqldriver.MySQLError{Number: 1045, Message: secret}, &mysqldriver.MySQLError{Number: 1396, Message: secret}} {
		safe := accessFailure(err)
		if strings.Contains(safe.message, secret) || safe.status < 400 {
			t.Fatalf("unsafe failure: %#v", safe)
		}
	}
}

type failingAccessDeps struct{}

func (failingAccessDeps) GaleraAccess() bool { return false }

func (failingAccessDeps) LocalAccessDB(context.Context) (*sql.DB, error) {
	return nil, errors.New("private failure")
}
func TestAccessRoutes(t *testing.T) {
	mux := http.NewServeMux()
	RegisterAccessRoutes(mux, failingAccessDeps{})
	for _, test := range []struct {
		path, body string
		status     int
	}{{myspec.ActionAccessCapabilities, "{}", 200}, {myspec.ActionAccessUsers, "{}", 503}, {myspec.ActionAccessDatabases, "{}", 503}, {myspec.ActionAccessGrants, "{}", 503}, {myspec.ActionAccessUsers, `{"unexpected":true}`, 400}, {myspec.ActionAccessUsers, "{} {}", 400}} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/mysql/"+test.path, strings.NewReader(test.body)))
		if w.Code != test.status {
			t.Fatalf("%s: %d %s", test.path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private failure") {
			t.Fatal("leaked dependency error")
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store")
		}
	}
}

func TestPendingReceiptRecovery(t *testing.T) {
	req := myspec.AccessRequest{Operation: "password", RequestID: "op"}
	for _, test := range []struct {
		operation, digest, phase string
		err                      error
		want                     bool
	}{
		{"op", "hash", "prepared", nil, true},
		{"op", "hash", "applied", nil, false},
		{"different", "hash", "prepared", nil, false},
		{"op", "different", "prepared", nil, false},
		{"", "", "", sql.ErrNoRows, false},
		{"op", "hash", "prepared", errors.New("transport"), false},
	} {
		if got := pendingCanExecute(req, "hash", test.operation, test.digest, test.phase, test.err); got != test.want {
			t.Fatalf("pending recovery %+v: %v", test, got)
		}
	}
	req.Operation = "create"
	if !pendingCanExecute(req, "hash", "", "", "", sql.ErrNoRows) || pendingCanExecute(req, "hash", "", "", "", nil) {
		t.Fatal("creation replay requires positively observed native absence")
	}
}
