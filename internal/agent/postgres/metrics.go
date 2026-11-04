// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func metric(name, help string, value float64, counter bool, labels map[string]string) agent.Metric {
	return agent.Metric{Name: "karakoram_postgres_" + name, Help: help, Value: value, Counter: counter, Labels: labels}
}
func databaseMetrics(ctx context.Context, c *pgx.Conn) []agent.Metric {
	result := []agent.Metric{}
	rows, err := c.Query(ctx, `SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, numbackends, pg_catalog.pg_database_size(datid) FROM pg_catalog.pg_stat_database WHERE datname IS NOT NULL`)
	if err != nil {
		return result
	}
	names := []string{"transactions_committed_total", "transactions_rolled_back_total", "blocks_read_total", "blocks_hit_total", "tuples_returned_total", "tuples_fetched_total", "tuples_inserted_total", "tuples_updated_total", "tuples_deleted_total", "connections", "database_size_bytes"}
	for rows.Next() {
		var database string
		v := make([]int64, len(names))
		args := []any{&database}
		for i := range v {
			args = append(args, &v[i])
		}
		if rows.Scan(args...) != nil {
			continue
		}
		for i, name := range names {
			result = append(result, metric(name, "PostgreSQL database "+name+".", float64(v[i]), i < 9, map[string]string{"database": database}))
		}
	}
	rows.Close()
	var maxConnections float64
	if c.QueryRow(ctx, "SELECT pg_catalog.current_setting('max_connections')::double precision").Scan(&maxConnections) == nil {
		result = append(result, metric("max_connections", "Configured PostgreSQL connection limit.", maxConnections, false, nil))
	}
	var failed, age float64
	if c.QueryRow(ctx, "SELECT failed_count::double precision, COALESCE(GREATEST(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()-last_archived_time),0),0)::double precision FROM pg_catalog.pg_stat_archiver").Scan(&failed, &age) == nil {
		result = append(result, metric("archiver_failed_total", "Failed WAL archive attempts.", failed, true, nil), metric("last_archived_age_seconds", "Seconds since the last successfully archived WAL; zero before the first archive.", age, false, nil))
	}
	var recovery bool
	if c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery) == nil {
		var lag float64
		name := "replication_lag_bytes"
		help := "Maximum primary-observed WAL distance to replica replay, including transport and replay lag, in bytes."
		sql := "SELECT COALESCE(pg_catalog.max(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),replay_lsn)),0)::double precision FROM pg_catalog.pg_stat_replication"
		if recovery {
			name = "replay_backlog_bytes"
			help = "Replica-local received-but-not-replayed WAL in bytes; excludes transport lag."
			sql = "SELECT COALESCE(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_receive_lsn(),pg_catalog.pg_last_wal_replay_lsn()),0)::double precision"
		}
		if c.QueryRow(ctx, sql).Scan(&lag) == nil {
			result = append(result, metric(name, help, lag, false, nil))
		}
		if !recovery {
			var slots []pgspec.ReplicationSlot
			if c.QueryRow(ctx, slotRetentionSQL).Scan(&slots) == nil {
				for _, slot := range slots {
					labels := map[string]string{"slot": slot.Name, "type": slot.Type}
					active, lost := 0.0, 0.0
					if slot.Active {
						active = 1
					}
					if slot.WALStatus == "lost" || slot.WALStatus == "unreserved" {
						lost = 1
					}
					result = append(result,
						metric("slot_retained_wal_bytes", "WAL retained by a replication slot.", float64(slot.RetainedBytes), false, labels),
						metric("slot_active", "Replication slot currently has an active consumer.", active, false, labels),
						metric("slot_wal_unavailable", "Required slot WAL is lost or unreserved.", lost, false, labels))
				}
			}
		}
	}
	var walBytes, pending int64
	if c.QueryRow(ctx, `SELECT COALESCE((SELECT pg_catalog.sum(size) FROM pg_catalog.pg_ls_waldir()),0)::bigint, (SELECT pg_catalog.count(*) FROM pg_catalog.pg_ls_archive_statusdir() WHERE name LIKE '%.ready')`).Scan(&walBytes, &pending) == nil {
		result = append(result, metric("wal_size_bytes", "Size of the PostgreSQL WAL directory.", float64(walBytes), false, nil), metric("archive_pending_segments", "WAL segments awaiting successful archiving.", float64(pending), false, nil))
	}
	return result
}
func proxyMetrics(ctx context.Context, p *pgspec.Proxy) []agent.Metric {
	c, err := proxyConnect(ctx, p)
	if err != nil {
		return nil
	}
	defer c.Close(ctx)
	rows, err := queryTable(ctx, c, "SELECT hostgroup,srv_host,ConnUsed,ConnFree,Queries FROM stats_pgsql_connection_pool")
	if err != nil {
		return nil
	}
	result := []agent.Metric{}
	for _, row := range rows {
		labels := map[string]string{"host": row["srv_host"], "hostgroup": row["hostgroup"]}
		for _, pair := range [][2]string{{"ConnUsed", "proxysql_connections_used"}, {"ConnFree", "proxysql_connections_free"}, {"Queries", "proxysql_queries_total"}} {
			v, err := strconv.ParseFloat(row[pair[0]], 64)
			if err == nil {
				result = append(result, metric(pair[1], "ProxySQL PostgreSQL connection pool "+pair[0]+".", v, pair[0] == "Queries", labels))
			}
		}
	}
	return result
}
func (e *Runner) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	s, err := decodeSpec(n.Config())
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if s.Role == pgspec.RoleProxy {
		return proxyMetrics(ctx, s.Proxy)
	}
	c, err := localConnect(ctx, s, dataPaths(n.DataDir()))
	if err != nil {
		return nil
	}
	defer c.Close(ctx)
	result := databaseMetrics(ctx, c)
	if usesPatroni(s) {
		var state struct {
			Role     string  `json:"role"`
			Timeline float64 `json:"timeline"`
		}
		if patroniRequest(ctx, s, "GET", "/patroni", nil, &state) == nil {
			primary := 0.0
			if state.Role == "primary" || state.Role == "master" {
				primary = 1
			}
			result = append(result, metric("patroni_primary", "Patroni reports the local PostgreSQL member as primary.", primary, false, nil), metric("patroni_timeline", "PostgreSQL timeline reported by Patroni.", state.Timeline, false, nil))
		}
	}
	return result
}
