// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"os"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestCapturedPrometheus(t *testing.T) {
	file, err := os.Open("testdata/prometheus.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	metrics, err := translateMetrics(file, []string{"rabbit@rmqone"})
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != len(rmqspec.MetricNames) {
		t.Fatalf("got %d allowlisted metrics, want %d", len(metrics), len(rmqspec.MetricNames))
	}
	got := map[string]float64{}
	for _, metric := range metrics {
		if !slices.Contains(rmqspec.MetricNames, metric.Name) || len(metric.Labels) != 0 {
			t.Fatal(metric)
		}
		got[metric.Name] = metric.Value
	}
	for name, want := range map[string]float64{"up": 1, "uptime_seconds": 56.373, "resident_memory_bytes": 143691776, "memory_limit_bytes": 536870912, "disk_free_bytes": 437795672064, "disk_free_limit_bytes": 52428800, "cluster_members_running": 1} {
		if got["karakoram_rabbitmq_"+name] != want {
			t.Fatal(name, got)
		}
	}
}
