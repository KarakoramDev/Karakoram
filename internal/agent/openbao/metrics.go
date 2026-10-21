// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func boolean(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
func (*Runner) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	st := status(ctx, n)
	out := []agent.Metric{{Name: baospec.MetricNames[0], Help: "OpenBao local API is reachable.", Value: boolean(st.Error == "")}}
	add := func(i int, v float64, counter bool) {
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			out = append(out, agent.Metric{Name: baospec.MetricNames[i], Help: baospec.MetricNames[i] + ".", Value: v, Counter: counter})
		}
	}
	if st.Error != "" {
		return out
	}
	add(1, boolean(st.Initialized), false)
	add(2, boolean(st.Sealed), false)
	add(3, boolean(st.Active), false)
	if !st.Sealed {
		add(4, float64(st.RaftCommittedIndex), false)
		add(5, float64(st.RaftAppliedIndex), false)
	}
	client, e := localClient(n.Config(), true)
	if e != nil {
		return out
	}
	body, e := client.Stream(ctx, http.MethodGet, "sys/metrics?format=prometheus", nil, "")
	if e != nil {
		return out
	}
	defer body.Close()
	b, e := io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if e != nil || len(b) > 2<<20 {
		return out
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, e := parser.TextToMetricFamilies(bytes.NewReader(b))
	if e != nil {
		return out
	}
	sources := map[int]string{6: "vault_raft_storage_follower_last_heartbeat_ms", 7: "vault_autopilot_healthy", 8: "vault_autopilot_failure_tolerance", 9: "vault_core_handle_request", 10: "vault_expire_num_leases", 11: "vault_runtime_alloc_bytes"}
	for i, name := range sources {
		f := families[name]
		if f == nil {
			continue
		}
		v := 0.0
		found := false
		for _, m := range f.Metric {
			switch {
			case m.Gauge != nil:
				if i == 6 {
					v = math.Max(v, m.Gauge.GetValue())
				} else {
					v += m.Gauge.GetValue()
				}
				found = true
			case m.Counter != nil:
				v += m.Counter.GetValue()
				found = true
			case m.Summary != nil:
				if i == 9 {
					v += float64(m.Summary.GetSampleCount())
					found = true
				}
			}
		}
		if i == 6 {
			v /= 1000
		}
		if found {
			add(i, v, i == 9)
		}
	}
	return out
}
