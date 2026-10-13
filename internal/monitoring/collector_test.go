// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const exposition = `# HELP karakoram_opcounters_total Operations.
# TYPE karakoram_opcounters_total counter
karakoram_opcounters_total 120
# HELP karakoram_connections_current Connections.
# TYPE karakoram_connections_current gauge
karakoram_connections_current 4
# TYPE karakoram_mongodb_documents_total counter
karakoram_mongodb_documents_total{operation="inserted",cluster="spoofed"} 9
# TYPE unknown_metric gauge
unknown_metric 42
`

func TestParseMetrics(t *testing.T) {
	families, samples, err := parseMetrics([]byte(exposition), Target{Cluster: "demo", Member: "m0"}, time.Now())
	if err != nil || len(samples) != 3 || len(families) != 4 {
		t.Fatal(families, samples, err)
	}
	for _, s := range samples {
		if s.Cluster != "demo" || s.Member != "m0" || s.Labels["cluster"] != "" {
			t.Fatal(s)
		}
		if s.Metric == "opcounters.total" && s.Labels["__type"] != "counter" {
			t.Fatal(s)
		}
	}
	if _, _, err = parseMetrics([]byte("not exposition \""), Target{}, time.Now()); err == nil {
		t.Fatal("accepted invalid exposition")
	}
}
func monitoringStore(t *testing.T) *store.Store {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "metrics.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}
func TestSeriesRatesResetsAndBuckets(t *testing.T) {
	ctx := context.Background()
	st := monitoringStore(t)
	c := New(Options{Store: st, Interval: 10 * time.Second})
	start := time.Now().UTC().Truncate(time.Second)
	var samples []domain.MetricSample
	for i, value := range []float64{100, 120, 150, 5, 25} {
		samples = append(samples, domain.MetricSample{Cluster: "demo", Member: "m0", Metric: "opcounters.total", Timestamp: start.Add(time.Duration(i-1) * 10 * time.Second), Value: value, Labels: map[string]string{"__type": "counter"}})
	}
	for i, value := range []float64{2, 4, 8, 10} {
		samples = append(samples, domain.MetricSample{Cluster: "demo", Member: "m0", Metric: "connections.current", Timestamp: start.Add(time.Duration(i) * 10 * time.Second), Value: value})
	}
	if err := st.InsertMetricSamples(ctx, samples); err != nil {
		t.Fatal(err)
	}
	series, err := c.Series(ctx, "demo", []string{"opcounters.total", "connections.current"}, start, start.Add(40*time.Second), 20*time.Second)
	if err != nil || len(series) != 2 {
		t.Fatal(series, err)
	}
	for _, s := range series {
		if len(s.Points) != 2 {
			t.Fatal(s)
		}
		if s.Name == "opcounters.total" {
			if s.Points[0].Value != 2.5 || s.Points[1].Value != 1.25 || s.Unit != "operations/second" || s.Labels["__type"] != "" {
				t.Fatal(s)
			}
		} else if s.Points[0].Value != 3 || s.Points[1].Value != 9 {
			t.Fatal(s)
		}
	}
	n, err := st.PruneMetricSamples(ctx, start)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

type testDialer struct {
	url string
	ca  []byte
	err error
}

func (d testDialer) Dial(context.Context, string, string) (*agentapi.Client, error) {
	if d.err != nil {
		return nil, d.err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "token", CAPEM: d.ca})
}
func TestScrapeFederationAndRetention(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics" || r.Header.Get("Authorization") != "Bearer token" {
			t.Error("unexpected request", r.URL.Path)
			http.Error(w, "bad", 400)
			return
		}
		w.Write([]byte(exposition))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	st := monitoringStore(t)
	if err := st.InsertMetricSamples(ctx, []domain.MetricSample{{Cluster: "demo", Member: "m0", Metric: "up", Timestamp: time.Now().Add(-48 * time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	c := New(Options{Store: st, Dialer: testDialer{url: server.URL, ca: ca}, Targets: func(context.Context) ([]Target, error) {
		return []Target{{Cluster: "demo", Member: "m0"}, {Cluster: "hidden", Member: "m1"}}, nil
	}})
	if err := c.scrape(ctx); err != nil {
		t.Fatal(err)
	}
	samples, err := st.QueryMetricSamples(ctx, store.MetricQuery{Cluster: "demo"})
	if err != nil || len(samples) != 3 {
		t.Fatal(samples, err)
	}
	var output bytes.Buffer
	if err = c.WritePrometheus(ctx, &output, func(cluster string) bool { return cluster == "demo" }); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, `cluster="demo"`) || !strings.Contains(text, `member="m0"`) || strings.Contains(text, `cluster="hidden"`) || strings.Contains(text, `cluster="spoofed"`) {
		t.Fatal(text)
	}
	if _, _, err = parseMetrics(output.Bytes(), Target{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.o.Targets = func(context.Context) ([]Target, error) { return nil, nil }
	if err = c.scrape(ctx); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err = c.WritePrometheus(ctx, &output, nil); err != nil || output.Len() != 0 {
		t.Fatal(err, output.String())
	}
}

func TestFailedMemberScrapeInvalidatesCachedMetrics(t *testing.T) {
	for _, failure := range []string{"dial", "request", "parse"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			var failing atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failing.Load() {
					if failure == "request" {
						http.Error(w, "exporter unavailable", http.StatusServiceUnavailable)
						return
					}
					if failure == "parse" {
						w.Write([]byte("not exposition \""))
						return
					}
				}
				w.Write([]byte("# TYPE karakoram_mongodb_up gauge\nkarakoram_mongodb_up 1\n" + exposition + usageExposition))
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			dialer := testDialer{url: server.URL, ca: ca}
			st := monitoringStore(t)
			c := New(Options{Store: st, Dialer: dialer, Targets: func(context.Context) ([]Target, error) {
				return []Target{{Cluster: "demo", Member: "m0"}}, nil
			}})
			if err := c.scrape(ctx); err != nil {
				t.Fatal(err)
			}
			assertScrape := func(success bool) {
				t.Helper()
				var output bytes.Buffer
				if err := c.WritePrometheus(ctx, &output, nil); err != nil {
					t.Fatal(err)
				}
				families, _, err := parseMetrics(output.Bytes(), Target{}, time.Now())
				if err != nil {
					t.Fatal(output.String(), err)
				}
				signal := families["karakoram_member_scrape_success"]
				usage := c.MemberUsage("demo", "m0")
				if success && (usage.RAMBytes == nil || usage.DiskBytes == nil) {
					t.Fatal("successful scrape missing member usage", usage)
				}
				if !success && (usage.CPUPercent != nil || usage.RAMBytes != nil || usage.DiskBytes != nil) {
					t.Fatal("failed scrape retained member usage", usage)
				}
				expected := float64(0)
				if success {
					expected = 1
				}
				if signal == nil || len(signal.Metric) != 1 || signal.Metric[0].GetGauge().GetValue() != expected {
					t.Fatal("missing scrape health signal", output.String())
				}
				if success {
					if families["karakoram_mongodb_up"] == nil {
						t.Fatal("successful health scrape missing", output.String())
					}
				} else if len(families) != 1 || strings.Contains(output.String(), "karakoram_mongodb_up") {
					t.Fatal("failed exporter still reports cached health", output.String())
				}
			}
			assertScrape(true)
			failing.Store(true)
			if failure == "dial" {
				c.o.Dialer = testDialer{err: errors.New("missing agent credentials")}
			}
			if err := c.scrape(ctx); err != nil {
				t.Fatal(err)
			}
			assertScrape(false)
			samples, err := st.QueryMetricSamples(ctx, store.MetricQuery{Cluster: "demo"})
			if err != nil || len(samples) == 0 {
				t.Fatal("historical samples should remain available", samples, err)
			}
			failing.Store(false)
			c.o.Dialer = dialer
			if err = c.scrape(ctx); err != nil {
				t.Fatal(err)
			}
			assertScrape(true)
		})
	}
}

func cacheUsage(t *testing.T, c *Collector, target Target, at time.Time, text string) []domain.MetricSample {
	t.Helper()
	families, samples, err := parseMetrics([]byte(text), target, at)
	if err != nil {
		t.Fatal(err)
	}
	c.cacheScrape(target, families, samples, at)
	return samples
}

const usageExposition = `# TYPE karakoram_process_cpu_seconds_total counter
karakoram_process_cpu_seconds_total 10
# TYPE karakoram_ram_used_bytes gauge
karakoram_ram_used_bytes 209715200
# TYPE karakoram_disk_used_bytes gauge
karakoram_disk_used_bytes 1073741824
`

func TestMemberUsageFreshnessAndMissingValues(t *testing.T) {
	c := New(Options{Interval: 30 * time.Second})
	target := Target{Cluster: "demo", Member: "m0"}
	assertAbsent := func(usage MemberUsage) {
		t.Helper()
		if usage.CPUPercent != nil || usage.RAMBytes != nil || usage.DiskBytes != nil {
			t.Fatalf("expected absent usage, got %+v", usage)
		}
	}
	assertAbsent(c.MemberUsage("demo", "m0"))
	assertAbsent((*Collector)(nil).MemberUsage("demo", "m0"))

	now := time.Now().UTC()
	cacheUsage(t, c, target, now.Add(-30*time.Second), usageExposition)
	first := c.MemberUsage("demo", "m0")
	if first.CPUPercent != nil || first.RAMBytes == nil || *first.RAMBytes != 209715200 || first.DiskBytes == nil || *first.DiskBytes != 1073741824 {
		t.Fatalf("first scrape should omit CPU rate only: %+v", first)
	}
	cacheUsage(t, c, target, now, strings.Replace(usageExposition, "total 10", "total 10.6", 1))
	usage := c.MemberUsage("demo", "m0")
	if usage.CPUPercent == nil || *usage.CPUPercent < 1.999 || *usage.CPUPercent > 2.001 {
		t.Fatalf("expected 2%% process CPU: %+v", usage)
	}
	assertAbsent(c.MemberUsage("other", "m0"))
	assertAbsent(c.MemberUsage("demo", "other"))

	c.cacheScrape(target, nil, nil, now)
	assertAbsent(c.MemberUsage("demo", "m0"))
	cacheUsage(t, c, target, now.Add(time.Second), usageExposition)
	if c.MemberUsage("demo", "m0").CPUPercent != nil {
		t.Fatal("failed scrape retained the CPU baseline")
	}

	cacheUsage(t, c, target, now.Add(-61*time.Second), usageExposition)
	assertAbsent(c.MemberUsage("demo", "m0"))
	cacheUsage(t, c, target, now, usageExposition)
	if c.MemberUsage("demo", "m0").CPUPercent != nil {
		t.Fatal("stale CPU baseline produced a fresh rate")
	}

	cacheUsage(t, c, target, now, "# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 0\n")
	usage = c.MemberUsage("demo", "m0")
	if usage.CPUPercent != nil || usage.DiskBytes != nil || usage.RAMBytes == nil || *usage.RAMBytes != 0 {
		t.Fatalf("missing metrics must be absent and measured zero must be present: %+v", usage)
	}
	cacheUsage(t, c, target, now, "# TYPE unknown_metric gauge\nunknown_metric 1\n")
	assertAbsent(c.MemberUsage("demo", "m0"))
}

func TestMemberUsageMatchesCanonicalCPU(t *testing.T) {
	for _, tc := range []struct {
		name     string
		previous string
		current  string
		want     float64
	}{
		{"rate", "10", "10.6", 2},
		{"reset", "10", "0.3", 1},
		{"clamp", "10", "100", 100},
		{"zero", "10", "10", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := monitoringStore(t)
			c := New(Options{Store: st, Interval: 30 * time.Second})
			target := Target{Cluster: "demo", Member: "m0"}
			now := time.Now().UTC()
			previous := cacheUsage(t, c, target, now.Add(-30*time.Second), strings.Replace(usageExposition, "total 10", "total "+tc.previous, 1))
			current := cacheUsage(t, c, target, now, strings.Replace(usageExposition, "total 10", "total "+tc.current, 1))
			if err := st.InsertMetricSamples(ctx, append(previous, current...)); err != nil {
				t.Fatal(err)
			}
			series, err := c.Series(ctx, "demo", []string{"cpu.percent"}, now, now.Add(time.Second), 30*time.Second)
			usage := c.MemberUsage("demo", "m0")
			if err != nil || len(series) != 1 || len(series[0].Points) != 1 || usage.CPUPercent == nil {
				t.Fatal(series, usage, err)
			}
			if *usage.CPUPercent != series[0].Points[0].Value || *usage.CPUPercent < tc.want-0.001 || *usage.CPUPercent > tc.want+0.001 {
				t.Fatalf("CPU usage %v differs from series %v or expected %v", *usage.CPUPercent, series[0].Points[0].Value, tc.want)
			}
		})
	}
}
