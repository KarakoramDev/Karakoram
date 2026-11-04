// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type accessLockConnector struct {
	groupTestConnector
	statements []string
}

func (c *accessLockConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *accessLockConnector) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.statements = append(c.statements, query)
	return driver.RowsAffected(1), nil
}

func TestAccessPXCStrictModeAndMySQLLocking(t *testing.T) {
	for _, galera := range []bool{false, true} {
		for _, action := range []string{myspec.ActionAccessUsers, myspec.ActionAccessDatabases, myspec.ActionAccessGrants} {
			t.Run(action+map[bool]string{false: "/mysql", true: "/pxc"}[galera], func(t *testing.T) {
				c := &accessLockConnector{groupTestConnector: groupTestConnector{t: t, queries: []groupQuery{{contains: "SELECT CURRENT_USER()", values: []driver.Value{myspec.UserAgent + "@localhost", false, false}}}}}
				if !galera {
					c.queries = append(c.queries, groupQuery{contains: "GET_LOCK", values: []driver.Value{int64(1)}})
				}
				req := myspec.AccessRequest{Operation: "capabilities"}
				switch action {
				case myspec.ActionAccessDatabases:
					req.Operation, req.Database = "create", &myspec.Database{Name: "app_db"}
				case myspec.ActionAccessGrants:
					req.Operation, req.Grant = "grant", &myspec.Grant{User: "app", Database: "app_db", Privileges: []string{"SELECT"}}
					c.queries = append(c.queries, groupQuery{contains: "partial_revokes", values: []driver.Value{false}}, groupQuery{contains: "SELECT 1", values: []driver.Value{int64(1)}})
				}
				db := sql.OpenDB(c)
				defer db.Close()
				var lock chan struct{}
				if galera {
					lock = make(chan struct{}, 1)
				}
				if _, err := executeAccess(t.Context(), db, action, req, lock); err != nil {
					t.Fatal(err)
				}
				if len(c.queries) != 0 {
					t.Fatal("missing expected SQL", c.queries)
				}
				if len(c.statements) == 0 || !strings.HasPrefix(c.statements[0], "SET SESSION") {
					t.Fatal("missing session setup", c.statements)
				}
				if galera && len(lock) != 0 {
					t.Fatal("agent lock leaked")
				}
			})
		}
	}
}

func TestGaleraAccessLockCancellation(t *testing.T) {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A blocked call must stop before acquiring a connection or issuing SQL.
	if _, err := executeAccess(ctx, nil, myspec.ActionAccessUsers, myspec.AccessRequest{}, lock); err == nil {
		t.Fatal("cancelled waiter entered access")
	}
	if len(lock) != 1 {
		t.Fatal("cancelled waiter released another request's lock")
	}
	<-lock
	c := &accessLockConnector{groupTestConnector: groupTestConnector{t: t, queries: []groupQuery{{contains: "SELECT CURRENT_USER()", values: []driver.Value{"wrong@localhost", false, false}}}}}
	db := sql.OpenDB(c)
	defer db.Close()
	_, err := executeAccess(t.Context(), db, myspec.ActionAccessUsers, myspec.AccessRequest{}, lock)
	var safe *accessError
	if !errors.As(err, &safe) || safe.status != 503 {
		t.Fatal("missing local account fence", err)
	}
	if len(lock) != 0 {
		t.Fatal("failed request leaked agent lock")
	}
}
