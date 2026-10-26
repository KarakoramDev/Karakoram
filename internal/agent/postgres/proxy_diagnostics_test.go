// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	_ "modernc.org/sqlite"
)

func TestProxyDiagnosticsAreBoundedAndSanitized(t *testing.T) {
	backends := []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "SHUNNED"}, {Host: "reader", Hostgroup: 20, Status: "ONLINE"}}
	calls := 0
	proxyDiagnostics(t.Context(), backends, func(ctx context.Context, sql string) ([]tableRow, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 250*time.Millisecond {
			t.Fatal("unbounded diagnostics")
		}
		if !strings.HasSuffix(sql, "LIMIT 32") || !strings.Contains(sql, "5432") {
			t.Fatal("unbounded/native port query", sql)
		}
		switch {
		case strings.Contains(sql, "connect_log"):
			return []tableRow{{"hostname": "db", "time_start_us": "1792300090000000", "error": "password authentication failed for user secret-user with password=secret-value"}, {"hostname": "unconfigured", "time_start_us": "1792300090000000", "error": "private"}}, nil
		case strings.Contains(sql, "ping_log"):
			return []tableRow{{"hostname": "db", "time_start_us": "1792300091000000", "error": "ping timeout SQL SELECT secret-value"}}, nil
		case strings.Contains(sql, "read_only_log"):
			return []tableRow{{"hostname": "db", "time_start_us": "1792300092000000", "error": "certificate verify failed secret-value"}}, nil
		default:
			return []tableRow{{"hostname": "db", "conn_errors": "17"}}, nil
		}
	})
	d := backends[0].Diagnostics
	if calls != 4 || d == nil || d.Connect.Class != "authentication" || d.Ping.Class != "timeout" || d.ReadOnly.Class != "tls" || d.ConnectionErrors != 17 || !d.PoolObserved {
		t.Fatalf("diagnostics=%+v calls=%d", d, calls)
	}
	text, _ := json.Marshal(backends)
	for _, forbidden := range []string{"secret-value", "secret-user", "SELECT", "unconfigured"} {
		if strings.Contains(string(text), forbidden) {
			t.Fatal("native message escaped", string(text))
		}
	}
	if backends[0].Status != "SHUNNED" {
		t.Fatal("diagnostics overrode native routing")
	}
}

func TestProxyDiagnosticsMissingTablesDoNotChangeHealth(t *testing.T) {
	backends := []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "ONLINE"}}
	proxyDiagnostics(t.Context(), backends, func(context.Context, string) ([]tableRow, error) { return nil, errors.New("old native version") })
	if backends[0].Diagnostics != nil || !proxyWriterReady(&pgspec.Proxy{Servers: []string{"db"}}, backends) {
		t.Fatal("optional diagnostics changed readiness")
	}
}

func TestProxyMonitorErrorClasses(t *testing.T) {
	for input, want := range map[string]string{"timed out password=private": "timeout", "no pg_hba.conf entry for private": "authentication", "SSL handshake failed private": "tls", "could not resolve private": "dns", "too many clients private": "capacity", "connection refused private": "refused", "connection reset private": "connection", "private unknown error": "other"} {
		if got := proxyErrorClass(input); got != want {
			t.Errorf("class=%q want %q", got, want)
		}
	}
}

func TestProxyDiagnosticsMonitorQueriesScanHistoryOnce(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("ATTACH DATABASE ':memory:' AS monitor"); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct{ table, column string }{
		{"pgsql_server_connect_log", "connect_error"},
		{"pgsql_server_ping_log", "ping_error"},
		{"pgsql_server_read_only_log", "error"},
	} {
		// Match the pinned ProxySQL monitor's composite primary key.
		if _, err := db.Exec("CREATE TABLE monitor." + check.table + " (hostname TEXT, port INT, time_start_us INT, " + check.column + " TEXT, PRIMARY KEY (hostname,port,time_start_us))"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO monitor." + check.table + " VALUES ('db',5432,100,'connection refused'),('db',5432,200,'ping timeout'),('db',5432,300,NULL),('db',5433,400,'wrong port'),('other',5432,500,'other host')"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE stats_pgsql_connection_pool (srv_host TEXT,srv_port INT,ConnERR INT); INSERT INTO stats_pgsql_connection_pool VALUES ('db',5432,4),('db',5432,6),('db',5433,100)"); err != nil {
		t.Fatal(err)
	}
	backends := []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10}}
	proxyDiagnostics(t.Context(), backends, func(ctx context.Context, query string) ([]tableRow, error) {
		if strings.Contains(query, "monitor.") {
			plan, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query)
			if err != nil {
				t.Fatal(err)
			}
			for plan.Next() {
				var id, parent, unused int
				var detail string
				if err := plan.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(detail, "CORRELATED") {
					t.Errorf("monitor history is rescanned per outer row: %s", detail)
				}
			}
			if err := plan.Err(); err != nil {
				t.Fatal(err)
			}
			plan.Close()
		}
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		var result []tableRow
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				return nil, err
			}
			row := tableRow{}
			for i, column := range columns {
				row[column] = fmt.Sprint(values[i])
			}
			result = append(result, row)
		}
		return result, rows.Err()
	})
	d := backends[0].Diagnostics
	if d == nil || d.Connect == nil || d.Ping == nil || d.ReadOnly == nil {
		t.Fatal("latest monitor failures missing")
	}
	for _, sample := range []*pgspec.ProxyMonitorError{d.Connect, d.Ping, d.ReadOnly} {
		if sample.Class != "timeout" || sample.TimeUnixMicro != 200 {
			t.Fatalf("latest failure/port/host selection changed: %+v", sample)
		}
	}
	if !d.PoolObserved || d.ConnectionErrors != 10 {
		t.Fatalf("pool aggregation changed: %+v", d)
	}
}
