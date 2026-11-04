// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func (e *Runner) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	s, _ := e.state()
	var out []agent.Metric
	add := func(name string, value float64, counter bool, labels map[string]string) {
		out = append(out, agent.Metric{Name: name, Help: name, Value: value, Counter: counter, Labels: labels})
	}
	if s.Role == myspec.RoleProxy {
		status, err := ProxyStatus(ctx, s)
		if err != nil {
			return nil
		}
		for _, b := range status.Backends {
			labels := map[string]string{"host": b.Host, "port": strconv.Itoa(b.Port), "hostgroup": strconv.Itoa(b.Hostgroup)}
			online := 0.
			if b.Status == "ONLINE" {
				online = 1
			}
			add("karakoram_mysql_proxysql_backend_state", online, false, labels)
			add("karakoram_mysql_proxysql_connections_used", float64(b.ConnectionsUsed), false, labels)
			add("karakoram_mysql_proxysql_connections_free", float64(b.ConnectionsFree), false, labels)
		}
		return out
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return nil
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Connections','Questions','Bytes_received','Bytes_sent')")
	if err != nil {
		return nil
	}
	values := map[string]float64{}
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) != nil {
			continue
		}
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			values[k] = f
		}
	}
	_ = rows.Close()
	for k, name := range map[string]string{"Threads_connected": "connections", "Threads_running": "threads_running", "Connections": "connections_total", "Questions": "questions_total", "Bytes_received": "bytes_received_total", "Bytes_sent": "bytes_sent_total"} {
		if v, ok := values[k]; ok {
			add("karakoram_mysql_"+name, v, k != "Threads_connected" && k != "Threads_running", nil)
		}
	}
	var max float64
	if db.QueryRowContext(ctx, "SELECT @@max_connections").Scan(&max) == nil {
		add("karakoram_mysql_max_connections", max, false, nil)
	}
	if q, ok := values["Questions"]; ok {
		now := time.Now()
		e.metricMu.Lock()
		if !e.questionTime.IsZero() && q >= e.questions {
			add("karakoram_mysql_questions_per_second", (q-e.questions)/now.Sub(e.questionTime).Seconds(), false, nil)
		}
		e.questions = q
		e.questionTime = now
		e.metricMu.Unlock()
	}
	if s.Topology == myspec.TopologyGroup {
		var state string
		if db.QueryRowContext(ctx, "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state) == nil {
			value := 0.
			if state == "ONLINE" {
				value = 1
			}
			add("karakoram_mysql_gr_member_state", value, false, map[string]string{"state": state})
		}
		var queue float64
		queueKnown := db.QueryRowContext(ctx, "SELECT COUNT_TRANSACTIONS_IN_QUEUE FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID=@@server_uuid").Scan(&queue) == nil
		if queueKnown {
			add("karakoram_mysql_replication_applier_queue", queue, false, nil)
		}
		// Commit timestamps describe backlog, not just receiver liveness. A fully
		// idle caught-up applier has zero queued work; absent evidence is omitted.
		var lag sql.NullFloat64
		if db.QueryRowContext(ctx, "SELECT MAX(CASE WHEN APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP > '1970-01-01' THEN GREATEST(0,TIMESTAMPDIFF(MICROSECOND,APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,NOW(6))/1000000) ELSE NULL END) FROM performance_schema.replication_applier_status_by_worker WHERE CHANNEL_NAME='group_replication_applier'").Scan(&lag) == nil && state == "ONLINE" {
			if lag.Valid {
				add("karakoram_mysql_replication_lag_seconds", lag.Float64, false, nil)
			} else if queueKnown && queue == 0 {
				add("karakoram_mysql_replication_lag_seconds", 0, false, nil)
			}
		}
	}
	return out
}
