// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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

func TestProviderVocabulary(t *testing.T) {
	ctx := t.Context()
	st := monitoringStore(t)
	descriptor := domain.MetricDescriptor{Exposition: "provider_work_total", Name: "work.rate", Unit: "items/second", Scale: 2}
	c := New(Options{Store: st, Interval: 10 * time.Second, Vocabulary: []domain.MetricDescriptor{descriptor}})
	start := time.Now().UTC().Truncate(time.Second)
	for i, value := range []int{10, 20} {
		at := start.Add(time.Duration(i-1) * 10 * time.Second)
		text := fmt.Sprintf("# TYPE provider_work_total counter\nprovider_work_total %d\n# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 42\n", value)
		_, samples, err := parseMetricsWithVocabulary([]byte(text), Target{Cluster: "demo", Member: "m0"}, at, c.vocabulary)
		if err != nil || len(samples) != 2 {
			t.Fatal(samples, err)
		}
		if err := st.InsertMetricSamples(ctx, samples); err != nil {
			t.Fatal(err)
		}
	}
	series, err := c.Series(ctx, domain.ClusterRef{Name: "demo"}, []string{"work.rate"}, start, start.Add(10*time.Second), 10*time.Second)
	if err != nil || len(series) != 1 || series[0].Unit != "items/second" || len(series[0].Points) != 1 || series[0].Points[0].Value != 2 {
		t.Fatal(series, err)
	}
	if _, exists := New(Options{}).vocabulary[descriptor.Exposition]; exists {
		t.Fatal("provider vocabulary leaked between collectors")
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
	series, err := c.Series(ctx, domain.ClusterRef{Name: "demo"}, []string{"opcounters.total", "connections.current"}, start, start.Add(40*time.Second), 20*time.Second)
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

func (d testDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
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
	if err = c.WritePrometheus(ctx, &output, func(cluster domain.ClusterRef) bool { return cluster.Namespace == "default" && cluster.Name == "demo" }); err != nil {
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
				usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
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
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"))
	assertAbsent((*Collector)(nil).MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"))

	now := time.Now().UTC()
	cacheUsage(t, c, target, now.Add(-30*time.Second), usageExposition)
	first := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
	if first.CPUPercent != nil || first.RAMBytes == nil || *first.RAMBytes != 209715200 || first.DiskBytes == nil || *first.DiskBytes != 1073741824 {
		t.Fatalf("first scrape should omit CPU rate only: %+v", first)
	}
	cacheUsage(t, c, target, now, strings.Replace(usageExposition, "total 10", "total 10.6", 1))
	usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
	if usage.CPUPercent == nil || *usage.CPUPercent < 1.999 || *usage.CPUPercent > 2.001 {
		t.Fatalf("expected 2%% process CPU: %+v", usage)
	}
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "other"}, "m0"))
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "demo"}, "other"))

	c.cacheScrape(target, nil, nil, now)
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"))
	cacheUsage(t, c, target, now.Add(time.Second), usageExposition)
	if c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0").CPUPercent != nil {
		t.Fatal("failed scrape retained the CPU baseline")
	}

	cacheUsage(t, c, target, now.Add(-61*time.Second), usageExposition)
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"))
	cacheUsage(t, c, target, now, usageExposition)
	if c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0").CPUPercent != nil {
		t.Fatal("stale CPU baseline produced a fresh rate")
	}

	cacheUsage(t, c, target, now, "# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 0\n")
	usage = c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
	if usage.CPUPercent != nil || usage.DiskBytes != nil || usage.RAMBytes == nil || *usage.RAMBytes != 0 {
		t.Fatalf("missing metrics must be absent and measured zero must be present: %+v", usage)
	}
	cacheUsage(t, c, target, now, "# TYPE unknown_metric gauge\nunknown_metric 1\n")
	assertAbsent(c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"))
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
			series, err := c.Series(ctx, domain.ClusterRef{Name: "demo"}, []string{"cpu.percent"}, now, now.Add(time.Second), 30*time.Second)
			usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
			if err != nil || len(series) != 1 || len(series[0].Points) != 1 || usage.CPUPercent == nil {
				t.Fatal(series, usage, err)
			}
			if *usage.CPUPercent != series[0].Points[0].Value || *usage.CPUPercent < tc.want-0.001 || *usage.CPUPercent > tc.want+0.001 {
				t.Fatalf("CPU usage %v differs from series %v or expected %v", *usage.CPUPercent, series[0].Points[0].Value, tc.want)
			}
		})
	}
}

func TestNamespaceMetricIsolationAndAuthoritativeLabels(t *testing.T) {
	ctx := context.Background()
	st := monitoringStore(t)
	c := New(Options{Store: st})
	now := time.Now().UTC()
	for i, namespace := range []string{"default", "other"} {
		text := fmt.Sprintf("# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes{namespace=\"spoofed\",cluster=\"spoofed\",member=\"spoofed\"} %d\n", i+1)
		samples := cacheUsage(t, c, Target{Namespace: namespace, Cluster: "demo", Member: "m0"}, now, text)
		if err := st.InsertMetricSamples(ctx, samples); err != nil {
			t.Fatal(err)
		}
		ref := domain.ClusterRef{Namespace: namespace, Name: "demo"}
		series, err := c.Series(ctx, ref, nil, now, now.Add(time.Second), time.Second)
		if err != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != float64(i+1) {
			t.Fatal(series, err)
		}
		usage := c.MemberUsage(ref, "m0")
		if usage.RAMBytes == nil || *usage.RAMBytes != int64(i+1) {
			t.Fatal(usage)
		}
	}
	var out bytes.Buffer
	var objects []string
	if err := c.WritePrometheus(ctx, &out, func(ref domain.ClusterRef) bool {
		objects = append(objects, ref.Namespace+"/"+ref.Name)
		return ref.Namespace == "other"
	}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if len(objects) != 2 || !strings.Contains(text, `namespace="other"`) || strings.Contains(text, `namespace="default"`) || strings.Contains(text, "spoofed") {
		t.Fatal(objects, text)
	}
}

type recordingDialer struct {
	mu   sync.Mutex
	refs []domain.ClusterRef
}

func (d *recordingDialer) Dial(_ context.Context, ref domain.ClusterRef, _ string) (*agentapi.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refs = append(d.refs, ref)
	return nil, errors.New("member unavailable")
}

func TestScrapeDialsNamespacedReferences(t *testing.T) {
	d := &recordingDialer{}
	c := New(Options{Store: monitoringStore(t), Dialer: d, Targets: func(context.Context) ([]Target, error) {
		return []Target{{Cluster: "same", Member: "m0"}, {Namespace: "other", Cluster: "same", Member: "m0"}}, nil
	}})
	if err := c.scrape(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := map[domain.ClusterRef]bool{}
	for _, ref := range d.refs {
		seen[ref] = true
	}
	if len(d.refs) != 2 || !seen[domain.ClusterRef{Namespace: "default", Name: "same"}] || !seen[domain.ClusterRef{Namespace: "other", Name: "same"}] {
		t.Fatal(d.refs)
	}
	if len(c.latest) != 2 {
		t.Fatal("namespace scrape caches collided", c.latest)
	}
}

func TestMemberUsageAggregatesProcessCPU(t *testing.T) {
	const previous = `# TYPE karakoram_process_cpu_seconds_total counter
karakoram_process_cpu_seconds_total{process="worker",component="data"} 10
karakoram_process_cpu_seconds_total{process="helper",component="data"} 20
`
	for _, tc := range []struct {
		name, current string
		want          float64
	}{
		{"rates", `karakoram_process_cpu_seconds_total{process="helper",component="data"} 20.9
karakoram_process_cpu_seconds_total{process="worker",component="data"} 10.6
`, 5},
		{"independent-reset", `karakoram_process_cpu_seconds_total{process="worker",component="data"} 0.3
karakoram_process_cpu_seconds_total{process="helper",component="data"} 20.9
`, 4},
		{"clamp-aggregate", `karakoram_process_cpu_seconds_total{process="worker",component="data"} 28
karakoram_process_cpu_seconds_total{process="helper",component="data"} 38
`, 100},
		{"new-process", `karakoram_process_cpu_seconds_total{process="worker",component="data"} 10.6
karakoram_process_cpu_seconds_total{process="new",component="data"} 300
`, 2},
		{"changed-label", `karakoram_process_cpu_seconds_total{process="worker",component="other"} 300
karakoram_process_cpu_seconds_total{process="helper",component="data"} 20.9
`, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(Options{Interval: 30 * time.Second})
			target := Target{Namespace: "default", Cluster: "demo", Member: "m0"}
			now := time.Now().UTC()
			cacheUsage(t, c, target, now.Add(-30*time.Second), previous)
			if usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"); usage.CPUPercent != nil {
				t.Fatal("first scrape has CPU rate", usage)
			}
			cacheUsage(t, c, target, now, "# TYPE karakoram_process_cpu_seconds_total counter\n"+tc.current)
			usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0")
			if usage.CPUPercent == nil || *usage.CPUPercent < tc.want-0.001 || *usage.CPUPercent > tc.want+0.001 {
				t.Fatal(usage.CPUPercent, tc.want)
			}
			other := Target{Namespace: "other", Cluster: "demo", Member: "m0"}
			cacheUsage(t, c, other, now, "# TYPE karakoram_process_cpu_seconds_total counter\n"+tc.current)
			if usage := c.MemberUsage(domain.ClusterRef{Namespace: "other", Name: "demo"}, "m0"); usage.CPUPercent != nil {
				t.Fatal("another target borrowed CPU baseline")
			}
			c.cacheScrape(target, nil, nil, now)
			cacheUsage(t, c, target, now.Add(time.Second), "# TYPE karakoram_process_cpu_seconds_total counter\n"+tc.current)
			if usage := c.MemberUsage(domain.ClusterRef{Name: "demo"}, "m0"); usage.CPUPercent != nil {
				t.Fatal("failed scrape retained process baselines")
			}
		})
	}
}

func TestFederationExpiresCachedPositiveSamples(t *testing.T) {
	target := Target{Namespace: domain.DefaultNamespace, Cluster: "demo", Member: "m0"}
	for _, mode := range []string{"expired", "missing", "future", "discovery-failure"} {
		t.Run(mode, func(t *testing.T) {
			c := New(Options{Store: monitoringStore(t), Interval: time.Second, Targets: func(context.Context) ([]Target, error) {
				return nil, errors.New("discovery unavailable")
			}})
			at := time.Now()
			switch mode {
			case "expired":
				at = at.Add(-3 * time.Second)
			case "missing":
				at = time.Time{}
			case "future":
				at = at.Add(time.Hour)
			}
			families, samples, err := parseMetrics([]byte("# TYPE karakoram_mongodb_up gauge\nkarakoram_mongodb_up 1\n"), target, at)
			if err != nil {
				t.Fatal(err)
			}
			c.cacheScrape(target, families, samples, at)
			if mode == "discovery-failure" {
				if err := c.scrape(t.Context()); err == nil {
					t.Fatal("discovery failure was suppressed")
				}
			}
			for range 2 {
				var out bytes.Buffer
				if err := c.WritePrometheus(t.Context(), &out, nil); err != nil {
					t.Fatal(err)
				}
				exported, _, err := parseMetrics(out.Bytes(), target, time.Now())
				success := exported["karakoram_member_scrape_success"]
				if err != nil || exported["karakoram_mongodb_up"] != nil || success == nil || len(success.Metric) != 1 || success.Metric[0].GetGauge().GetValue() != 0 {
					t.Fatal("stale health was republished as current", out.String(), err)
				}
			}
		})
	}
}
