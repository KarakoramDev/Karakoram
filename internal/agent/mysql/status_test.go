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
	viewRows   [][]driver.Value
	viewErr    error
	viewIDErr  error
}

func (c *nativeStatusConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *nativeStatusConnector) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.HasPrefix(query, "SELECT MEMBER_ID,MEMBER_HOST") {
		if c.viewErr != nil {
			return nil, c.viewErr
		}
		return &groupViewRows{rows: c.viewRows}, nil
	}
	if strings.HasPrefix(query, "SELECT VIEW_ID") {
		if c.viewIDErr != nil {
			return nil, c.viewIDErr
		}
		return &groupViewRows{rows: [][]driver.Value{{"view:1"}}, columns: []string{"VIEW_ID"}}, nil
	}
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

type groupViewRows struct {
	rows    [][]driver.Value
	columns []string
}

func (r *groupViewRows) Columns() []string {
	if r.columns != nil {
		return r.columns
	}
	return []string{"MEMBER_ID", "MEMBER_HOST", "MEMBER_PORT", "MEMBER_STATE", "MEMBER_ROLE"}
}
func (*groupViewRows) Close() error { return nil }
func (r *groupViewRows) Next(dst []driver.Value) error {
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
			c.viewRows = [][]driver.Value{{uuid, "member0", int64(3306), "ONLINE", "PRIMARY"}, {"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "old", int64(3306), "UNREACHABLE", "SECONDARY"}}
			if strings.HasPrefix(mode, "group ") {
				c.queries = append(c.queries, groupQuery{contains: "SELECT @@group_replication_group_name", values: []driver.Value{uuid}})
			}
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
				c.queries = append(c.queries, groupQuery{contains: "SELECT @@GLOBAL.gtid_executed", values: []driver.Value{uuid + ":1-10", uuid, false}})
			}
			count := int64(0)
			if mode == "group online" || mode == "pxc" {
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
			if strings.HasPrefix(mode, "group ") && out.GroupName != uuid {
				t.Fatalf("group identity missing: %+v", out)
			}
			if strings.HasPrefix(mode, "group ") {
				if out.GroupView == nil || len(out.GroupView.Members) != 2 || out.GroupView.Members[1].State != "UNREACHABLE" || out.GroupView.ViewID != "view:1" {
					t.Fatalf("full native membership view lost: %+v", out.GroupView)
				}
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

func TestObserveGroupViewCapability(t *testing.T) {
	for _, mode := range []string{"complete", "view-id-unavailable", "membership-unavailable", "offline-null-port"} {
		t.Run(mode, func(t *testing.T) {
			c := &nativeStatusConnector{groupTestConnector: groupTestConnector{t: t}, viewRows: [][]driver.Value{
				{"11111111-2222-3333-4444-555555555555", "member0", int64(3306), "ONLINE", "PRIMARY"},
				{"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "old", int64(3306), "UNREACHABLE", "SECONDARY"},
			}}
			switch mode {
			case "view-id-unavailable":
				c.viewIDErr = errors.New("stats unavailable")
			case "membership-unavailable":
				c.viewErr = errors.New("members unavailable")
			case "offline-null-port":
				c.viewRows = [][]driver.Value{{"", "", nil, "OFFLINE", ""}}
			}
			db := sql.OpenDB(c)
			defer db.Close()
			view, err := observeGroupView(t.Context(), db)
			if mode == "membership-unavailable" {
				if err == nil || view != nil {
					t.Fatal("failed full observation advertised capability")
				}
				return
			}
			if err != nil || view == nil {
				t.Fatalf("view=%+v err=%v", view, err)
			}
			if mode == "offline-null-port" {
				if len(view.Members) != 1 || view.Members[0].State != "OFFLINE" || view.Members[0].Port != 0 {
					t.Fatalf("offline view=%+v", view)
				}
			} else if len(view.Members) != 2 || view.Members[1].State != "UNREACHABLE" {
				t.Fatalf("full native view was filtered: %+v", view)
			}
			if mode == "view-id-unavailable" && view.ViewID != "" {
				t.Fatalf("fabricated view id %q", view.ViewID)
			}
		})
	}
}
