// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var metricSources = [][]string{nil, {"object_count"}, {"shards_loaded", "shards_unloaded", "shards_loading", "shards_unloading"}, nil, {"queries_durations_ms"}, {"requests_total"}, {"batch_objects_processed_total"}, {"vector_index_size"}, {"vector_index_tombstones"}, {"lsm_segment_size", "lsm_memtable_size"}, {"queue_size"}}

func translateMetrics(r io.Reader) ([]agent.Metric, error) {
	data, err := io.ReadAll(io.LimitReader(r, (2<<20)+1))
	if err != nil || len(data) > 2<<20 {
		return nil, fmt.Errorf("metrics exceed limit")
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if len(families) == 0 {
		return nil, fmt.Errorf("empty metrics")
	}
	result := []agent.Metric{{Name: wvspec.MetricNames[0], Help: "Native metrics endpoint is available.", Value: 1}}
	for i, sources := range metricSources {
		if len(sources) == 0 {
			continue
		}
		found := false
		var value float64
		for _, source := range sources {
			if family := families[source]; family != nil {
				found = true
				for _, m := range family.Metric {
					var v float64
					switch {
					case m.Gauge != nil:
						v = m.Gauge.GetValue()
					case m.Counter != nil:
						v = m.Counter.GetValue()
					case m.Untyped != nil:
						v = m.Untyped.GetValue()
					case m.Histogram != nil:
						v = float64(m.Histogram.GetSampleCount())
					default:
						continue
					}
					if math.IsNaN(v) || math.IsInf(v, 0) {
						continue
					}
					value += v
				}
			}
		}
		if found && !math.IsNaN(value) && !math.IsInf(value, 0) {
			result = append(result, agent.Metric{Name: wvspec.MetricNames[i], Help: "Native aggregated " + wvspec.MetricNames[i] + ".", Value: value, Counter: i >= 4 && i <= 6})
		}
	}
	if f := families["object_count"]; f != nil {
		classes := map[string]bool{}
		for _, m := range f.Metric {
			var value float64
			switch {
			case m.Gauge != nil:
				value = m.Gauge.GetValue()
			case m.Counter != nil:
				value = m.Counter.GetValue()
			case m.Untyped != nil:
				value = m.Untyped.GetValue()
			default:
				continue
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
			for _, l := range m.Label {
				if l.GetName() == "class_name" && l.GetValue() != "" {
					classes[l.GetValue()] = true
				}
			}
		}
		result = append(result, agent.Metric{Name: wvspec.MetricNames[3], Help: "Collections with local object metrics.", Value: float64(len(classes))})
	}
	return result, nil
}
func (*Engine) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	down := []agent.Metric{{Name: wvspec.MetricNames[0], Help: "Native metrics endpoint is available."}}
	s, err := spec(n.Config())
	if err != nil {
		return down
	}
	return scrapeMetrics(ctx, s.Ports.Metrics)
}

func scrapeMetrics(ctx context.Context, port int) []agent.Metric {
	down := []agent.Metric{{Name: wvspec.MetricNames[0], Help: "Native metrics endpoint is available."}}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", port), nil)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return down
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return down
	}
	result, err := translateMetrics(resp.Body)
	if err != nil {
		return down
	}
	return result
}
