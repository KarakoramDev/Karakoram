// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type fencedCloneConnector struct {
	groupTestConnector
	offline, superReadOnly bool
	failure                error
	statements             []string
}

func (c *fencedCloneConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *fencedCloneConnector) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.statements = append(c.statements, q)
	switch q {
	case "SET GLOBAL offline_mode=ON":
		c.offline = true
	case "SET GLOBAL offline_mode=OFF":
		if !c.superReadOnly {
			c.t.Fatal("admitted clients without write fence")
		}
		c.offline = false
	case "SET GLOBAL super_read_only=OFF":
		if !c.offline {
			c.t.Fatal("lifted write fence before fencing connections")
		}
		c.superReadOnly = false
	case "SET GLOBAL super_read_only=ON":
		c.superReadOnly = true
	default:
		if !strings.HasPrefix(q, "CLONE INSTANCE FROM ") {
			c.t.Fatalf("unexpected mutation: %s", q)
		}
		// Native clone issues DROP TABLE/SCHEMA for a populated recipient.
		if c.superReadOnly {
			return nil, &mysqldriver.MySQLError{Number: 1290, Message: "recipient super_read_only"}
		}
		if !c.offline {
			c.t.Fatal("clone recipient accepts client connections")
		}
		return nil, c.failure
	}
	return driver.RowsAffected(0), nil
}

func TestClonePopulatedRecipientLiftsWriteFenceOnlyWhileOffline(t *testing.T) {
	for _, mode := range []string{"success", "rejected start", "lost reply", "canceled reply", "network error", "restart required", "already offline"} {
		t.Run(mode, func(t *testing.T) {
			c := &fencedCloneConnector{groupTestConnector: groupTestConnector{t: t}, superReadOnly: true, offline: mode == "already offline"}
			switch mode {
			case "rejected start", "already offline":
				c.failure = &mysqldriver.MySQLError{Number: 1290}
			case "lost reply":
				c.failure = mysqldriver.ErrInvalidConn
			case "canceled reply":
				c.failure = context.Canceled
			case "network error":
				c.failure = errors.New("connection lost")
			case "restart required":
				c.failure = &mysqldriver.MySQLError{Number: 3707}
			}
			db := sql.OpenDB(c)
			defer db.Close()
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "recovery", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			// Demonstrate the pre-fix recipient rejection, independently of the helper.
			if _, err := db.ExecContext(t.Context(), "CLONE INSTANCE FROM 'test'@'donor':3306 IDENTIFIED BY 'test' REQUIRE SSL"); err == nil {
				t.Fatal("populated super-read-only recipient did not reject clone")
			}
			c.statements = nil
			c.queries = []groupQuery{{contains: "SELECT @@GLOBAL.offline_mode", values: []driver.Value{c.offline}}}
			_, err := executeNativeClone(t.Context(), db, s, req)
			if !errors.Is(err, c.failure) {
				t.Fatalf("lost native error: %v", err)
			}
			want := []string{"SET GLOBAL offline_mode=ON", "SET GLOBAL super_read_only=OFF"}
			if !reflect.DeepEqual(c.statements[:2], want) {
				t.Fatalf("unsafe fence order: %v", c.statements)
			}
			rejected := mode == "rejected start" || mode == "already offline"
			if rejected {
				if !c.superReadOnly || c.offline != (mode == "already offline") {
					t.Fatalf("failed clone did not restore fences: %+v", c)
				}
			} else if !c.offline {
				t.Fatal("ambiguous/successful copy lost offline fence before restart")
			}
			if len(c.queries) != 0 {
				t.Fatal("missing offline observation")
			}
		})
	}
}

func TestCloneReceiptRetriesOnlyFailedStartWithoutCopy(t *testing.T) {
	for _, mode := range []string{"before copy", "copy started", "missing progress", "stale completion", "wrong donor"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "restore-follower", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			s.GroupReplication.Reclone = true
			s.GroupReplication.RecloneReceipt = req.Receipt
			baseline := cloneNativeStatus{ID: 1, BeginTime: "2026-10-20 00:00:00.000", Source: "127.0.0.2:24850", State: "Completed"}
			current := cloneNativeStatus{ID: 2, BeginTime: "2026-10-21 00:00:00.000", Source: baseline.Source, State: "Failed"}
			if mode == "stale completion" {
				current = baseline
			}
			if mode == "wrong donor" {
				current.Source = "other:3306"
			}
			queries := []groupQuery{{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(current.ID), current.BeginTime, current.Source, current.State}}}
			if current.State == "Failed" && mode != "wrong donor" {
				queries = append(queries, groupQuery{contains: "STAGE='FILE COPY' AND STATE='Not Started'", values: []driver.Value{mode == "before copy"}})
			}
			if mode == "before copy" {
				// A retry must repeat recipient preflight, not accept the Failed row
				// as completion. A version mismatch stops before any donor connection.
				queries = append(queries, groupQuery{contains: "SELECT VERSION()", values: []driver.Value{"wrong-version"}})
			}
			c := &groupTestConnector{t: t, queries: queries}
			db := sql.OpenDB(c)
			defer db.Close()
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: myspec.DataIdentity{Initialized: true}}, db: db}
			record := cloneReceipt{Request: req, Baseline: &baseline}
			b, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err = durableGroupFile(filepath.Join(d.dir, "clone-"+req.Receipt), b); err != nil {
				t.Fatal(err)
			}
			err = CloneGroup(t.Context(), d, req)
			if err == nil || d.restarts != 0 {
				t.Fatalf("failed/stale clone was finalized: %v", err)
			}
			var action *cloneActionError
			if !errors.As(err, &action) {
				t.Fatal(err)
			}
			wantStage := "receipt-recovery"
			if mode == "before copy" {
				wantStage = "recipient-preflight"
			}
			if action.stage != wantStage {
				t.Fatalf("stage=%s, want %s", action.stage, wantStage)
			}
			if len(c.queries) != 0 {
				t.Fatalf("missing recovery observations: %d", len(c.queries))
			}
		})
	}
}
