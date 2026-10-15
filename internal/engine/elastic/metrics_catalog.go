// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import "github.com/KarakoramDev/Karakoram/internal/domain"

// Metrics maps the engine's exposition families to canonical monitoring series.
func (*Provider) Metrics() []domain.MetricDescriptor {
	entries := []struct{ exposition, name, unit string }{
		{"elasticsearch_up", "elasticsearch.up", "boolean"},
		{"elasticsearch_cluster_status", "elasticsearch.cluster.status", "status"},
		{"elasticsearch_cluster_nodes", "elasticsearch.cluster.nodes", "count"},
		{"elasticsearch_active_shards", "elasticsearch.shards.active", "count"},
		{"elasticsearch_unassigned_shards", "elasticsearch.shards.unassigned", "count"},
		{"elasticsearch_relocating_shards", "elasticsearch.shards.relocating", "count"},
		{"elasticsearch_jvm_heap_used_bytes", "elasticsearch.jvm.heap.usedBytes", "bytes"},
		{"elasticsearch_jvm_heap_max_bytes", "elasticsearch.jvm.heap.maxBytes", "bytes"},
		{"elasticsearch_docs", "elasticsearch.docs", "count"},
		{"elasticsearch_store_bytes", "elasticsearch.store.bytes", "bytes"},
		{"elasticsearch_indexing_total", "elasticsearch.indexing.total", "count"},
		{"elasticsearch_search_query_total", "elasticsearch.search.query.total", "count"},
		{"elasticsearch_gc_collection_seconds_total", "elasticsearch.gc.collection.seconds", "seconds"},
		{"elasticsearch_thread_pool_rejected_total", "elasticsearch.threadPool.rejected", "count"},
		{"elasticsearch_fs_available_bytes", "elasticsearch.fs.availableBytes", "bytes"},
		{"kibana_up", "kibana.up", "boolean"},
		{"kibana_status_level", "kibana.status.level", "status"},
		{"logstash_up", "logstash.up", "boolean"},
		{"logstash_events_in_total", "logstash.events.in", "count"},
		{"logstash_events_out_total", "logstash.events.out", "count"},
		{"logstash_jvm_heap_used_bytes", "logstash.jvm.heap.usedBytes", "bytes"},
		{"apm_up", "apm.up", "boolean"},
		{"apm_requests_total", "apm.requests", "count"},
		{"apm_events_accepted_total", "apm.events.accepted", "count"},
	}
	out := make([]domain.MetricDescriptor, 0, len(entries))
	for _, e := range entries {
		out = append(out, domain.MetricDescriptor{Exposition: "karakoram_" + e.exposition, Name: e.name, Unit: e.unit, Scale: 1})
	}
	return out
}
