// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pgspec

// MetricNames is the stable exposition vocabulary shared with the provider.
var MetricNames = []string{
	"karakoram_postgres_transactions_committed_total",
	"karakoram_postgres_transactions_rolled_back_total",
	"karakoram_postgres_blocks_read_total",
	"karakoram_postgres_blocks_hit_total",
	"karakoram_postgres_tuples_returned_total",
	"karakoram_postgres_tuples_fetched_total",
	"karakoram_postgres_tuples_inserted_total",
	"karakoram_postgres_tuples_updated_total",
	"karakoram_postgres_tuples_deleted_total",
	"karakoram_postgres_connections",
	"karakoram_postgres_max_connections",
	"karakoram_postgres_replication_lag_bytes",
	"karakoram_postgres_archiver_failed_total",
	"karakoram_postgres_last_archived_age_seconds",
	"karakoram_postgres_database_size_bytes",
	"karakoram_postgres_patroni_primary",
	"karakoram_postgres_patroni_timeline",
	"karakoram_postgres_proxysql_connections_used",
	"karakoram_postgres_proxysql_connections_free",
	"karakoram_postgres_proxysql_queries_total",
}
