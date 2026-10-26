// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func proxyErrorClass(message string) string {
	message = strings.ToLower(message)
	switch {
	case strings.Contains(message, "timeout"), strings.Contains(message, "timed out"):
		return "timeout"
	case strings.Contains(message, "password authentication failed"), strings.Contains(message, "authentication"), strings.Contains(message, "pg_hba"):
		return "authentication"
	case strings.Contains(message, "certificate"), strings.Contains(message, "ssl"), strings.Contains(message, "tls"):
		return "tls"
	case strings.Contains(message, "resolve"), strings.Contains(message, "name or service"), strings.Contains(message, "host name"):
		return "dns"
	case strings.Contains(message, "too many"), strings.Contains(message, "remaining connection slots"):
		return "capacity"
	case strings.Contains(message, "refused"):
		return "refused"
	case strings.Contains(message, "reset"), strings.Contains(message, "closed"), strings.Contains(message, "unreachable"):
		return "connection"
	default:
		return "other"
	}
}

// Optional diagnostics never change readiness and never write to ProxySQL.
// Queries return at most one last failure per configured backend/check, and the
// shared deadline bounds missing tables/older native versions. Native messages
// are immediately reduced to a closed vocabulary, not returned to the CP.
func proxyDiagnostics(ctx context.Context, backends []pgspec.ProxyBackend, query func(context.Context, string) ([]tableRow, error)) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	hosts := make([]string, 0, min(len(backends), 32))
	known := map[string]*pgspec.ProxyDiagnostics{}
	for _, b := range backends {
		if _, ok := known[b.Host]; ok {
			continue
		}
		if len(hosts) == 32 {
			break
		}
		known[b.Host] = &pgspec.ProxyDiagnostics{}
		hosts = append(hosts, sqlQuote(b.Host))
	}
	if len(hosts) == 0 {
		return
	}
	filter := strings.Join(hosts, ",")
	for _, check := range []struct{ table, column string }{
		{"pgsql_server_connect_log", "connect_error"},
		{"pgsql_server_ping_log", "ping_error"},
		{"pgsql_server_read_only_log", "error"},
	} {
		table := "monitor." + check.table
		// Aggregate each history once, then fetch at most one row per backend
		// through the native (hostname,port,time_start_us) primary key. A
		// correlated MAX would repeat the history walk for every failure row;
		// LIMIT bounds the result, not that native SQLite work.
		sql := "SELECT l.hostname,l.time_start_us,l." + check.column + " AS error FROM " + table + " l JOIN (SELECT hostname,port,MAX(time_start_us) AS time_start_us FROM " + table + " WHERE port=5432 AND hostname IN (" + filter + ") AND " + check.column + " IS NOT NULL AND " + check.column + "<>'' GROUP BY hostname,port) latest ON latest.hostname=l.hostname AND latest.port=l.port AND latest.time_start_us=l.time_start_us LIMIT 32"
		rows, err := query(ctx, sql)
		if err != nil {
			continue
		}
		for _, row := range rows {
			d := known[row["hostname"]]
			if d == nil || row["error"] == "" {
				continue
			}
			stamp, err := strconv.ParseInt(row["time_start_us"], 10, 64)
			if err != nil || stamp <= 0 {
				continue
			}
			sample := &pgspec.ProxyMonitorError{Class: proxyErrorClass(row["error"]), TimeUnixMicro: stamp}
			switch check.column {
			case "connect_error":
				d.Connect = sample
			case "ping_error":
				d.Ping = sample
			default:
				d.ReadOnly = sample
			}
		}
	}
	rows, err := query(ctx, "SELECT srv_host AS hostname,SUM(ConnERR) AS conn_errors FROM stats_pgsql_connection_pool WHERE srv_port=5432 AND srv_host IN ("+filter+") GROUP BY srv_host LIMIT 32")
	if err == nil {
		for _, row := range rows {
			d := known[row["hostname"]]
			if d == nil {
				continue
			}
			count, err := strconv.ParseUint(row["conn_errors"], 10, 64)
			if err != nil {
				continue
			}
			d.ConnectionErrors, d.PoolObserved = count, true
		}
	}
	for i := range backends {
		d := known[backends[i].Host]
		if d != nil && (d.Connect != nil || d.Ping != nil || d.ReadOnly != nil || d.PoolObserved) {
			backends[i].Diagnostics = d
		}
	}
}
