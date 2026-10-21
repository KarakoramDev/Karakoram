// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

// MetricNames is the stable exposition vocabulary. Questions is the QPS source counter.
var MetricNames = []string{
	"karakoram_mysql_connections", "karakoram_mysql_max_connections",
	"karakoram_mysql_connections_total", "karakoram_mysql_threads_running",
	"karakoram_mysql_questions_total", "karakoram_mysql_bytes_received_total", "karakoram_mysql_bytes_sent_total",
	"karakoram_mysql_questions_per_second",
	"karakoram_mysql_gr_member_state", "karakoram_mysql_replication_applier_queue",
	"karakoram_mysql_replication_lag_seconds", "karakoram_mysql_binlog_archive_lag_seconds",
	"karakoram_mysql_proxysql_backend_state", "karakoram_mysql_proxysql_connections_used", "karakoram_mysql_proxysql_connections_free",
}
