// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type switchoverConnector struct {
	groupTestConnector
	calls int
	err   error
}

func (c *switchoverConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *switchoverConnector) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.calls++
	if query != "SELECT group_replication_set_as_primary('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')" || len(args) != 0 {
		c.t.Fatal("switchover must supply a validated literal UUID at UDF initialization", query, args)
	}
	return driver.RowsAffected(0), c.err
}

func TestSwitchoverUsesLiteralUUID(t *testing.T) {
	for _, failure := range []error{nil, errors.New("native primary election rejected")} {
		c := &switchoverConnector{groupTestConnector: groupTestConnector{t: t}, err: failure}
		d := &cloneTestDeps{db: sql.OpenDB(c)}
		t.Cleanup(func() { _ = d.db.Close() })
		err := SwitchoverGroup(t.Context(), d, myspec.SwitchoverRequest{TargetUUID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
		if !errors.Is(err, failure) || c.calls != 1 {
			t.Fatalf("calls=%d error=%v, want %v", c.calls, err, failure)
		}
	}
}

func TestSwitchoverRejectsInvalidUUIDBeforeSQL(t *testing.T) {
	for _, target := range []string{"", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'); SELECT 1; --", "not-a-uuid"} {
		if err := SwitchoverGroup(t.Context(), &rejectedGroupDeps{}, myspec.SwitchoverRequest{TargetUUID: target}); err == nil {
			t.Fatal("invalid target accepted")
		}
	}
}
