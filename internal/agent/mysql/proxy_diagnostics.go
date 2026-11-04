// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"strings"
	"time"
)

func proxyErrorClass(message string) string {
	s := strings.ToLower(message)
	switch {
	case strings.Contains(s, "timeout"), strings.Contains(s, "timed out"):
		return "timeout"
	case strings.Contains(s, "access denied"), strings.Contains(s, "password"), strings.Contains(s, "authentication"):
		return "authentication"
	case strings.Contains(s, "certificate"), strings.Contains(s, "ssl"), strings.Contains(s, "tls"):
		return "tls"
	case strings.Contains(s, "resolve"), strings.Contains(s, "name or service"), strings.Contains(s, "unknown host"):
		return "dns"
	case strings.Contains(s, "too many"), strings.Contains(s, "max_connections"):
		return "capacity"
	case strings.Contains(s, "refused"):
		return "refused"
	case strings.Contains(s, "reset"), strings.Contains(s, "closed"), strings.Contains(s, "unreachable"):
		return "connection"
	default:
		return "other"
	}
}
func proxyDiagnostics(ctx context.Context, db *sql.DB, backends []myspec.ProxyBackend) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	for _, check := range []struct{ table, column, kind string }{{"mysql_server_connect_log", "connect_error", "connect"}, {"mysql_server_ping_log", "ping_error", "ping"}, {"mysql_server_group_replication_log", "error", "group"}} {
		// Aggregate once, fetching only the most recent check per backend. Historical
		// failures are not promoted into current errors after a subsequent success.
		q := "SELECT l.hostname,l.port,l.time_start_us,l." + check.column + " FROM monitor." + check.table + " l JOIN (SELECT hostname,port,MAX(time_start_us) stamp FROM monitor." + check.table + " GROUP BY hostname,port) t ON l.hostname=t.hostname AND l.port=t.port AND l.time_start_us=t.stamp WHERE l." + check.column + " IS NOT NULL AND l." + check.column + "<>'' LIMIT 64"
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			continue
		}
		for rows.Next() {
			var host, message string
			var port int
			var stamp int64
			if rows.Scan(&host, &port, &stamp, &message) != nil {
				continue
			}
			for i := range backends {
				if backends[i].Host == host && backends[i].Port == port {
					backends[i].Diagnostics = append(backends[i].Diagnostics, myspec.ProxyMonitorError{Check: check.kind, Class: proxyErrorClass(message), TimeUnixMicro: stamp})
				}
			}
		}
		rows.Close()
	}
}
