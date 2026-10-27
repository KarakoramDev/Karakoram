// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type nativeStatusConnector struct {
	groupTestConnector
	galeraRows [][]driver.Value
	memberErr  error
	countErr   error
}

func (c *nativeStatusConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *nativeStatusConnector) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "WHERE MEMBER_ID=@@server_uuid") && c.memberErr != nil {
		return nil, c.memberErr
	}
	if strings.HasPrefix(query, "SELECT COUNT(*)") && c.countErr != nil {
		return nil, c.countErr
	}
	if strings.HasPrefix(query, "SHOW GLOBAL STATUS") {
		rows := c.galeraRows
		c.galeraRows = nil
		return &nativeStatusRows{rows: rows}, nil
	}
	return c.groupTestConnector.QueryContext(ctx, query, args)
}

type nativeStatusRows struct{ rows [][]driver.Value }

func (*nativeStatusRows) Columns() []string { return []string{"Variable_name", "Value"} }
func (*nativeStatusRows) Close() error      { return nil }
func (r *nativeStatusRows) Next(dst []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dst, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

func TestNativeStatusBeforeAndAfterGroupMembership(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	for _, mode := range []string{"group absent", "group offline", "group online", "group query error", "group count error", "single", "pxc"} {
		t.Run(mode, func(t *testing.T) {
			p := dataPaths(t.TempDir())
			for _, dir := range []string{p.root, p.data} {
				if err := privateDir(dir); err != nil {
					t.Fatal(err)
				}
			}
			s := myspec.Spec{Role: myspec.RoleData, Topology: myspec.TopologyGroup}
			c := &nativeStatusConnector{groupTestConnector: groupTestConnector{t: t, queries: []groupQuery{{contains: "SELECT @@server_uuid", values: []driver.Value{uuid, "", true, true, "8.4.10-10"}}}}}
			state, role := "OFFLINE", ""
			switch mode {
			case "group absent":
				// The native pre-bootstrap row has MEMBER_ID='', so the UUID
				// predicate returns no row even though mysqld is SQL-ready.
				c.queries = append(c.queries, groupQuery{contains: "WHERE MEMBER_ID=@@server_uuid"})
			case "group offline":
				c.queries = append(c.queries, groupQuery{contains: "WHERE MEMBER_ID=@@server_uuid", values: []driver.Value{"OFFLINE", ""}})
			case "group online":
				state, role = "ONLINE", "PRIMARY"
				c.queries = append(c.queries, groupQuery{contains: "WHERE MEMBER_ID=@@server_uuid", values: []driver.Value{state, role}})
			case "group query error":
				c.memberErr = errors.New("membership query unavailable")
			case "group count error":
				c.queries = append(c.queries, groupQuery{contains: "WHERE MEMBER_ID=@@server_uuid"})
				c.countErr = errors.New("membership count unavailable")
			case "single":
				s.Topology = myspec.TopologySingle
				state, role = "ONLINE", "PRIMARY"
			case "pxc":
				s.Topology = "cluster"
				s.Galera = &myspec.Galera{}
				state, role = "Synced", "PRIMARY"
				c.galeraRows = [][]driver.Value{{"wsrep_cluster_status", "Primary"}, {"wsrep_local_state_comment", "Synced"}, {"wsrep_ready", "ON"}, {"wsrep_connected", "ON"}, {"wsrep_cluster_size", "3"}, {"wsrep_cluster_state_uuid", uuid}, {"wsrep_last_committed", "10"}}
				c.queries = append(c.queries, groupQuery{contains: "SELECT @@GLOBAL.gtid_executed", values: []driver.Value{uuid + ":1-10"}})
			}
			count := int64(0)
			if mode == "group online" {
				count = 3
			}
			if s.Galera == nil && s.Topology == myspec.TopologyGroup && c.memberErr == nil && c.countErr == nil {
				c.queries = append(c.queries, groupQuery{contains: "SELECT COUNT(*)", values: []driver.Value{count}})
			}
			db := sql.OpenDB(c)
			defer db.Close()
			out := myspec.StatusResponse{Role: s.Role}
			err := observeNativeStatus(t.Context(), db, s, p, &out)
			if mode == "group query error" || mode == "group count error" {
				if err == nil {
					t.Fatal("SQL failure was hidden as absent membership")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if out.MemberState != state || out.MemberRole != role || out.GroupMembers != int(count) {
				t.Fatalf("membership status=%+v", out)
			}
			if out.ServerUUID != uuid || out.Version != "8.4.10-10" || out.PhysicalIdentity == "" {
				t.Fatalf("native identity/version was lost: %+v", out)
			}
			if len(c.queries) != 0 || len(c.galeraRows) != 0 {
				t.Fatal("native observations were skipped")
			}
		})
	}
}
