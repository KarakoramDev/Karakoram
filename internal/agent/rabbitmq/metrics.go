// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var metricSources = [][]string{
	nil, {"rabbitmq_erlang_uptime_seconds"}, {"rabbitmq_connections"}, {"rabbitmq_channels"},
	{"rabbitmq_consumers"}, {"rabbitmq_queues"}, {"rabbitmq_queue_messages_ready"}, {"rabbitmq_queue_messages_unacked"},
	{"rabbitmq_channel_messages_published_total"}, {"rabbitmq_channel_messages_delivered_total", "rabbitmq_channel_messages_delivered_ack_total"},
	{"rabbitmq_alarms_memory_used_watermark"}, {"rabbitmq_alarms_free_disk_space_watermark"},
	{"rabbitmq_process_resident_memory_bytes"}, {"rabbitmq_resident_memory_limit_bytes"},
	{"rabbitmq_disk_space_available_bytes"}, {"rabbitmq_disk_space_available_limit_bytes"}, nil,
}

func translateMetrics(r io.Reader, peers []string) ([]agent.Metric, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, err
	}
	if len(families) == 0 {
		return nil, fmt.Errorf("empty RabbitMQ metrics")
	}
	result := []agent.Metric{{Name: rmqspec.MetricNames[0], Help: "RabbitMQ Prometheus endpoint is available.", Value: 1}}
	for i, sources := range metricSources {
		if len(sources) == 0 {
			continue
		}
		var value float64
		found := false
		for _, source := range sources {
			if family := families[source]; family != nil {
				found = true
				for _, m := range family.Metric {
					if m.Gauge != nil {
						value += m.Gauge.GetValue()
					} else if m.Counter != nil {
						value += m.Counter.GetValue()
					} else if m.Untyped != nil {
						value += m.Untyped.GetValue()
					}
				}
			}
		}
		if found && !math.IsNaN(value) && !math.IsInf(value, 0) {
			result = append(result, agent.Metric{Name: rmqspec.MetricNames[i], Help: "RabbitMQ " + rmqspec.MetricNames[i] + ".", Value: value, Counter: i == 8 || i == 9})
		}
	}
	// Count local node plus established Erlang links to configured RabbitMQ peers;
	// do not count transient CLI distribution nodes.
	running := 1.0
	if family := families["erlang_vm_dist_node_state"]; family != nil {
		seen := map[string]bool{}
		for _, m := range family.Metric {
			if m.Gauge.GetValue() != 3 {
				continue
			}
			for _, label := range m.Label {
				node := label.GetValue()
				if slices.Contains(peers, node) && !seen[node] {
					running++
					seen[node] = true
				}
			}
		}
	}
	result = append(result, agent.Metric{Name: rmqspec.MetricNames[16], Help: "RabbitMQ members with established distribution links, including this node.", Value: running})
	return result, nil
}
func (*Runner) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	down := []agent.Metric{{Name: rmqspec.MetricNames[0], Help: "RabbitMQ Prometheus endpoint is available."}}
	s, err := spec(n.Config())
	if err != nil || s.PrometheusPort == 0 {
		return down
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", s.PrometheusPort), nil)
	if err != nil {
		return down
	}
	// Never consult proxy environment for a local secret-bearing process.
	client := http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		return down
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return down
	}
	metrics, err := translateMetrics(io.LimitReader(response.Body, 2<<20), s.Peers)
	if err != nil {
		return down
	}
	return metrics
}
