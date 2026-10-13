// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/internal/apiclient"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMetricsFreshnessRejectsRebucketedHistoricalSamples(t *testing.T) {
	ctx := context.Background()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "metrics.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	collector := monitoring.New(monitoring.Options{Store: st})
	end := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	sample := domain.MetricSample{Cluster: "demo", Member: "m0", Metric: "connections.current", Timestamp: end.Add(-10 * time.Second), Value: 4}
	if err := st.InsertMetricSamples(ctx, []domain.MetricSample{sample}); err != nil {
		t.Fatal(err)
	}
	query := func(queryEnd time.Time) api.ClusterMetrics {
		t.Helper()
		start := queryEnd.Add(-15 * time.Minute)
		series, err := collector.Series(ctx, domain.ClusterRef{Namespace: domain.DefaultNamespace, Name: "demo"}, nil, start, queryEnd, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		metrics := api.ClusterMetrics{Start: start, End: queryEnd}
		for _, v := range series {
			out := api.MetricSeries{Name: v.Name, Member: v.Member}
			for _, point := range v.Points {
				out.Points = append(out.Points, api.MetricPoint{Timestamp: point.Timestamp, Value: float32(point.Value)})
			}
			metrics.Series = append(metrics.Series, out)
		}
		return metrics
	}
	baseline := query(end)
	later := query(end.Add(5 * time.Second))
	if len(baseline.Series) != 1 || len(baseline.Series[0].Points) != 1 || len(later.Series) != 1 || len(later.Series[0].Points) != 1 {
		t.Fatal("expected the same historical sample in both queries")
	}
	if !later.Series[0].Points[0].Timestamp.After(baseline.Series[0].Points[0].Timestamp) {
		t.Fatal("fixture must demonstrate the old point-watermark false positive")
	}
	if freshMetricNames(later, baseline.End)[sample.Member][sample.Metric] {
		t.Fatal("unchanged historical sample passed freshness after rebucketing")
	}

	// A real scrape sufficiently beyond End must pass once its bucket also
	// starts beyond End; no server/workstation clock comparison is involved.
	sample.Timestamp = end.Add(40 * time.Second)
	if err := st.InsertMetricSamples(ctx, []domain.MetricSample{sample}); err != nil {
		t.Fatal(err)
	}
	if !freshMetricNames(query(end.Add(45*time.Second)), baseline.End)[sample.Member][sample.Metric] {
		t.Fatal("new post-baseline scrape did not pass freshness")
	}
}

func TestMetricsFreshnessRequiresStrictlyAfterBaselineEnd(t *testing.T) {
	end := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	metrics := api.ClusterMetrics{Series: []api.MetricSeries{
		{Member: "m0", Name: "connections.current", Points: []api.MetricPoint{{Timestamp: end}}},
		{Member: "m1", Name: "connections.current", Points: []api.MetricPoint{{Timestamp: end.Add(time.Nanosecond)}}},
	}}
	fresh := freshMetricNames(metrics, end)
	if fresh["m0"]["connections.current"] || !fresh["m1"]["connections.current"] {
		t.Fatalf("incorrect strict/per-member freshness: %v", fresh)
	}
}
