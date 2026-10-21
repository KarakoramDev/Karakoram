// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
)

func TestMetricsScrapeFailClosed(t *testing.T) {
	for _, kind := range []string{"status", "malformed", "oversized", "redirect", "cancelled", "success"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "status":
					w.WriteHeader(500)
				case "malformed":
					fmt.Fprint(w, "invalid exposition")
				case "oversized":
					fmt.Fprint(w, strings.Repeat(" ", (2<<20)+1))
				case "redirect":
					http.Redirect(w, r, "http://example.invalid/metrics", http.StatusFound)
				default:
					fmt.Fprint(w, "object_count{class_name=\"A\",shard_name=\"a\"} 2\n")
				}
			}))
			defer server.Close()
			ctx := t.Context()
			if kind == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			metrics := scrapeMetrics(ctx, server.Listener.Addr().(*net.TCPAddr).Port)
			if kind == "success" {
				if len(metrics) != 3 || metrics[0].Value != 1 {
					t.Fatal(metrics)
				}
			} else if len(metrics) != 1 || metrics[0].Value != 0 {
				t.Fatal(metrics)
			}
		})
	}
}

type metricExpectation struct {
	value   float64
	counter bool
}

func assertMetricValues(t *testing.T, metrics []agent.Metric, expected map[string]metricExpectation) {
	t.Helper()
	if len(metrics) != len(expected) {
		t.Fatalf("translated %d metrics, want exactly %d: %+v", len(metrics), len(expected), metrics)
	}
	seen := make(map[string]bool, len(metrics))
	for _, metric := range metrics {
		want, ok := expected[metric.Name]
		if !ok || seen[metric.Name] {
			t.Errorf("unexpected or duplicated metric %q", metric.Name)
			continue
		}
		seen[metric.Name] = true
		if metric.Value != want.value || metric.Counter != want.counter {
			t.Errorf("%s: value=%v counter=%t; want value=%v counter=%t", metric.Name, metric.Value, metric.Counter, want.value, want.counter)
		}
		if len(metric.Labels) != 0 || metric.Help == "" {
			t.Errorf("%s: native labels must be dropped and help must be present: %+v", metric.Name, metric)
		}
	}
	for name := range expected {
		if !seen[name] {
			t.Errorf("missing expected metric %q", name)
		}
	}
}

func TestMetricsAggregateClassAndShardLabels(t *testing.T) {
	// Synthetic samples exercise multiple classes/shards and both on-disk
	// segment components, which the real single-shard capture does not contain.
	const exposition = `# TYPE object_count gauge
object_count{class_name="A",shard_name="one"} 2
object_count{class_name="A",shard_name="two"} 3
object_count{class_name="B",shard_name="three"} 7
# TYPE shards_loaded gauge
shards_loaded 1
# TYPE shards_unloaded gauge
shards_unloaded 2
# TYPE shards_loading gauge
shards_loading 3
# TYPE shards_unloading gauge
shards_unloading 4
# TYPE queries_durations_ms histogram
queries_durations_ms_bucket{class_name="A",le="10"} 1
queries_durations_ms_bucket{class_name="A",le="+Inf"} 2
queries_durations_ms_sum{class_name="A"} 1000
queries_durations_ms_count{class_name="A"} 2
queries_durations_ms_bucket{class_name="B",le="10"} 2
queries_durations_ms_bucket{class_name="B",le="+Inf"} 3
queries_durations_ms_sum{class_name="B"} 2000
queries_durations_ms_count{class_name="B"} 3
# TYPE requests_total gauge
requests_total{api="graphql",class_name="A"} 2
requests_total{api="rest",class_name="B"} 5
# TYPE batch_objects_processed_total counter
batch_objects_processed_total{class_name="A",shard_name="one"} 4
batch_objects_processed_total{class_name="B",shard_name="three"} 6
# TYPE vector_index_size gauge
vector_index_size{class_name="A",shard_name="one"} 100
vector_index_size{class_name="B",shard_name="three"} 200
# TYPE vector_index_tombstones gauge
vector_index_tombstones{class_name="A",shard_name="one"} 2
vector_index_tombstones{class_name="B",shard_name="three"} 3
# TYPE lsm_segment_size gauge
lsm_segment_size{class_name="A",shard_name="one",unit="index"} 10
lsm_segment_size{class_name="A",shard_name="one",unit="payload"} 30
# TYPE lsm_memtable_size gauge
lsm_memtable_size{class_name="A",shard_name="one"} 5
lsm_memtable_size{class_name="B",shard_name="three"} 7
# TYPE queue_size gauge
queue_size{class_name="A",shard_name="one"} 11
queue_size{class_name="B",shard_name="three"} 13
`
	metrics, err := translateMetrics(strings.NewReader(exposition))
	if err != nil {
		t.Fatal(err)
	}
	assertMetricValues(t, metrics, map[string]metricExpectation{
		"karakoram_weaviate_up":                      {1, false},
		"karakoram_weaviate_objects":                 {2 + 3 + 7, false},
		"karakoram_weaviate_shards":                  {1 + 2 + 3 + 4, false},
		"karakoram_weaviate_collections":             {2, false},
		"karakoram_weaviate_queries_total":           {2 + 3, true},
		"karakoram_weaviate_requests_total":          {2 + 5, true},
		"karakoram_weaviate_batch_objects_total":     {4 + 6, true},
		"karakoram_weaviate_vector_index_capacity":   {100 + 200, false},
		"karakoram_weaviate_vector_index_tombstones": {2 + 3, false},
		"karakoram_weaviate_lsm_bytes":               {10 + 30 + 5 + 7, false},
		"karakoram_weaviate_async_queue_length":      {11 + 13, false},
	})
}
