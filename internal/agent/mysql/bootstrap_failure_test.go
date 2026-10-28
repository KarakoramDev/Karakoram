// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type bootstrapFailureConnector struct {
	groupTestConnector
	exec func(string) error
}

func (c *bootstrapFailureConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }

func (c *bootstrapFailureConnector) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.exec(query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

// A non-2xx response is not a negative bootstrap receipt: it can follow START
// succeeding, loss of its reply, or failure to record an ONLINE observation.
func TestBootstrapHTTPFailureEpochBoundaries(t *testing.T) {
	for _, stage := range []string{"database", "enable", "start rejected", "start ambiguous", "disable", "offline", "online"} {
		t.Run(stage, func(t *testing.T) {
			s := testGroupSpec()
			s.GroupReplication.Bootstrap = true
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: myspec.DataIdentity{Initialized: true}}}
			connector := &bootstrapFailureConnector{groupTestConnector: groupTestConnector{t: t}}
			d.db = sql.OpenDB(connector)
			t.Cleanup(func() { _ = d.db.Close() })
			enables, starts, disables := 0, 0, 0
			connector.exec = func(query string) error {
				// The durable epoch must precede any native bootstrap permission.
				if d.identity.FormationEpoch != 1 {
					t.Fatal("bootstrap SQL preceded durable epoch")
				}
				switch {
				case query == "SET GLOBAL group_replication_bootstrap_group=ON":
					enables++
					if stage == "enable" {
						return &mysqldriver.MySQLError{Number: 1227, Message: "denied"}
					}
				case strings.HasPrefix(query, "START GROUP_REPLICATION USER="):
					starts++
					if stage == "start rejected" {
						return &mysqldriver.MySQLError{Number: 3092, Message: "not configured"}
					}
					if stage == "start ambiguous" {
						return context.DeadlineExceeded
					}
				case query == "SET GLOBAL group_replication_bootstrap_group=OFF":
					disables++
					if stage == "disable" && disables == 1 {
						return errors.New("lost disable reply")
					}
				default:
					t.Fatal("unexpected bootstrap SQL")
				}
				return nil
			}
			if stage == "database" {
				d.localErr = errors.New("local administration unavailable")
			}
			if stage == "offline" || stage == "online" {
				state := "OFFLINE"
				if stage == "online" {
					state = "ONLINE"
				}
				connector.queries = []groupQuery{{contains: "FROM performance_schema.replication_group_members", values: []driver.Value{state}}}
				if stage == "online" {
					connector.queries = append(connector.queries, connector.queries[0])
				}
			}
			mux := http.NewServeMux()
			RegisterGroupRoutes(mux, d)
			body, err := json.Marshal(myspec.GroupRequest{GroupName: s.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true})
			if err != nil {
				t.Fatal(err)
			}
			invoke := func() *httptest.ResponseRecorder {
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/mysql/bootstrap", strings.NewReader(string(body))))
				return response
			}
			response := invoke()
			wantStatus, wantEpoch := http.StatusServiceUnavailable, uint64(1)
			if stage == "database" {
				wantEpoch = 0
			}
			if stage == "online" {
				wantStatus = http.StatusOK
			}
			if response.Code != wantStatus || d.identity.FormationEpoch != wantEpoch {
				t.Fatalf("status=%d epoch=%d, want %d/%d", response.Code, d.identity.FormationEpoch, wantStatus, wantEpoch)
			}
			wantStarts, wantDisables := 1, 1
			if stage == "enable" || stage == "database" {
				wantStarts = 0
			}
			if stage == "database" {
				wantDisables = 0
			}
			if stage == "disable" {
				wantDisables = 2 // Fresh-connection cleanup after the failed OFF.
			}
			if starts != wantStarts || disables != wantDisables || len(connector.queries) != 0 {
				t.Fatalf("starts=%d disables=%d pending queries=%d", starts, disables, len(connector.queries))
			}
			if stage != "database" {
				if enables != 1 || GroupJoined(d.dir, s) != (stage == "online") {
					t.Fatal("unexpected bootstrap permission or join receipt")
				}
				if replay := invoke(); replay.Code != http.StatusServiceUnavailable || starts != wantStarts || disables != wantDisables || enables != 1 {
					t.Fatal("consumed epoch replay touched native SQL")
				}
			}
		})
	}
}
