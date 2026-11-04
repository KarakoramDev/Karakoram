// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import "github.com/KarakoramDev/Karakoram/internal/domain"

func (*Provider) Metrics() []domain.MetricDescriptor {
	entries := []struct{ exposition, name, unit string }{
		{"up", "weaviate.up", "boolean"},
		{"objects", "weaviate.objects", "count"},
		{"shards", "weaviate.shards", "count"},
		{"collections", "weaviate.collections", "count"},
		{"queries_total", "weaviate.queries.total", "count"},
		{"requests_total", "weaviate.requests.total", "count"},
		{"batch_objects_total", "weaviate.batch.objects.total", "count"},
		{"vector_index_capacity", "weaviate.vectorIndex.capacity", "count"},
		{"vector_index_tombstones", "weaviate.vectorIndex.tombstones", "count"},
		{"lsm_bytes", "weaviate.lsm.bytes", "bytes"},
		{"async_queue_length", "weaviate.asyncQueue.length", "count"},
	}
	out := make([]domain.MetricDescriptor, 0, len(entries))
	for _, e := range entries {
		out = append(out, domain.MetricDescriptor{Exposition: "karakoram_weaviate_" + e.exposition, Name: e.name, Unit: e.unit, Scale: 1})
	}
	return out
}
