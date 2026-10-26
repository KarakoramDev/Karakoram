// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package monitoring scrapes member exporters and serves retained metric series.
package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Target identifies one member exporter.
type Target struct{ Namespace, Cluster, Member string }

// Options supplies storage, agent connections, target discovery and retention settings.
type Options struct {
	Store               *store.Store
	Dialer              engine.AgentDialer
	Targets             func(context.Context) ([]Target, error)
	Interval, Retention time.Duration
	Logger              *slog.Logger
	Vocabulary          []domain.MetricDescriptor
}

// Point is a timestamped JSON metric value.
type Point struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// Series is a canonical member series consumed by the UI.
type Series struct {
	Name   string            `json:"name"`
	Member string            `json:"member"`
	Unit   string            `json:"unit"`
	Labels map[string]string `json:"labels,omitempty"`
	Points []Point           `json:"points"`
}

// MemberUsage contains the latest process CPU percentage, resident memory and
// data-volume disk usage. Nil fields mean that the latest scrape has no value.
type MemberUsage struct {
	CPUPercent          *float64
	RAMBytes, DiskBytes *int64
	// DiskTotalBytes is the data filesystem capacity reported by the agent.
	DiskTotalBytes *int64
}

type memberSample struct {
	at    time.Time
	usage MemberUsage
	cpu   map[string]domain.MetricSample
}

// Collector owns scrape scheduling and the latest federation samples.
type Collector struct {
	o           Options
	mu          sync.RWMutex
	latest      map[Target]map[string]*dto.MetricFamily
	latestUsage map[Target]memberSample
	vocabulary  map[string]vocabulary
	retention   chan struct{}
}

// New creates a collector with a 30-second interval and 24-hour retention by default.
func New(o Options) *Collector {
	if o.Interval <= 0 {
		o.Interval = 30 * time.Second
	}
	if o.Retention <= 0 {
		o.Retention = 24 * time.Hour
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	vocab := make(map[string]vocabulary, len(vocabularyByName)+len(o.Vocabulary))
	for name, v := range vocabularyByName {
		vocab[name] = v
	}
	for _, v := range o.Vocabulary {
		vocab[v.Exposition] = vocabulary{name: v.Name, unit: v.Unit, scale: v.Scale}
	}
	return &Collector{o: o, vocabulary: vocab, latest: map[Target]map[string]*dto.MetricFamily{}, latestUsage: map[Target]memberSample{}, retention: make(chan struct{}, 1)}
}

type vocabulary struct {
	name, unit string
	scale      float64
}

var vocabularyByName = map[string]vocabulary{
	"karakoram_mongodb_up": {"up", "count", 1}, "karakoram_mongodb_uptime_seconds": {"uptime", "seconds", 1},
	"karakoram_opcounters_total": {"opcounters.total", "operations/second", 1}, "karakoram_connections_current": {"connections.current", "count", 1}, "karakoram_connections_available": {"connections.available", "count", 1},
	"karakoram_repl_lag_seconds": {"repl.lagSeconds", "seconds", 1}, "karakoram_repl_member_state": {"repl.memberState", "count", 1}, "karakoram_oplog_window_seconds": {"oplog.windowSeconds", "seconds", 1},
	"karakoram_cache_used_bytes": {"cache.usedBytes", "bytes", 1}, "karakoram_cache_max_bytes": {"cache.maxBytes", "bytes", 1},
	"karakoram_process_cpu_seconds_total": {"cpu.percent", "percent", 100}, "karakoram_ram_used_bytes": {"ram.usedBytes", "bytes", 1}, "karakoram_ram_total_bytes": {"ram.totalBytes", "bytes", 1},
	"karakoram_disk_used_bytes": {"disk.usedBytes", "bytes", 1}, "karakoram_disk_total_bytes": {"disk.totalBytes", "bytes", 1}, "karakoram_pbm_agent_up": {"pbm.up", "count", 1},
	"karakoram_mongodb_opcounters_total": {"opcounters.byOperation", "operations/second", 1}, "karakoram_mongodb_documents_total": {"documents.total", "documents/second", 1}, "karakoram_mongodb_network_bytes_total": {"network.bytes", "bytes/second", 1},
}

func parseMetrics(data []byte, target Target, now time.Time) (map[string]*dto.MetricFamily, []domain.MetricSample, error) {
	return parseMetricsWithVocabulary(data, target, now, vocabularyByName)
}

func parseMetricsWithVocabulary(data []byte, target Target, now time.Time, vocab map[string]vocabulary) (map[string]*dto.MetricFamily, []domain.MetricSample, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	var samples []domain.MetricSample
	for name, family := range families {
		v, ok := vocab[name]
		if !ok {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				if label.GetName() != "namespace" && label.GetName() != "cluster" && label.GetName() != "member" {
					labels[label.GetName()] = label.GetValue()
				}
			}
			var value float64
			switch family.GetType() {
			case dto.MetricType_COUNTER:
				value = metric.GetCounter().GetValue()
				labels["__type"] = "counter"
			case dto.MetricType_GAUGE:
				value = metric.GetGauge().GetValue()
			default:
				continue
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
			samples = append(samples, domain.MetricSample{Namespace: domain.NormalizedNamespace(target.Namespace), Cluster: target.Cluster, Member: target.Member, Metric: v.name, Timestamp: now, Value: value, Labels: labels})
		}
	}
	return families, samples, nil
}

// cacheScrape replaces exposition even on failure; old health samples must not
// acquire fresh scrape timestamps when an exporter becomes unreachable.
func (c *Collector) cacheScrape(target Target, families map[string]*dto.MetricFamily, samples []domain.MetricSample, now time.Time) {
	target.Namespace = domain.NormalizedNamespace(target.Namespace)
	success := families != nil
	if families == nil {
		families = make(map[string]*dto.MetricFamily, 1)
	}
	families["karakoram_member_scrape_success"] = scrapeSuccessFamily(success)
	c.mu.Lock()
	latest := memberSample{at: now}
	previous := c.latestUsage[target]
	for _, sample := range samples {
		switch sample.Metric {
		case "cpu.percent":
			key := seriesKey(sample)
			var baseline *domain.MetricSample
			if preceding, ok := previous.cpu[key]; ok && now.Sub(previous.at) <= 2*c.o.Interval {
				baseline = &preceding
			}
			if value, ok := sampleValue(sample, baseline); ok {
				if latest.usage.CPUPercent == nil {
					latest.usage.CPUPercent = new(float64)
				}
				*latest.usage.CPUPercent += value
			}
			if latest.cpu == nil {
				latest.cpu = make(map[string]domain.MetricSample)
			}
			latest.cpu[key] = sample
		case "ram.usedBytes":
			value := int64(sample.Value)
			latest.usage.RAMBytes = &value
		case "disk.usedBytes":
			value := int64(sample.Value)
			latest.usage.DiskBytes = &value
		case "disk.totalBytes":
			value := int64(sample.Value)
			latest.usage.DiskTotalBytes = &value
		}
	}
	if latest.usage.CPUPercent != nil {
		*latest.usage.CPUPercent = min(100, max(0, *latest.usage.CPUPercent))
	}
	c.latestUsage[target] = latest
	c.latest[target] = families
	c.mu.Unlock()
}

func scrapeSuccessFamily(success bool) *dto.MetricFamily {
	name, help := "karakoram_member_scrape_success", "Whether the latest member exporter scrape succeeded and is fresh."
	kind := dto.MetricType_GAUGE
	value := float64(0)
	if success {
		value = 1
	}
	return &dto.MetricFamily{Name: &name, Help: &help, Type: &kind, Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &value}}}}
}

func (c *Collector) sampleFresh(at, now time.Time) bool {
	return !at.IsZero() && !at.After(now) && now.Sub(at) <= 2*c.o.Interval
}

// MemberUsage returns values only from a successful scrape within two scrape
// intervals. CPU uses the same reset-aware, 0–100% counter rate as Series and
// requires a preceding fresh sample; failed scrapes discard that baseline.
func (c *Collector) MemberUsage(cluster domain.ClusterRef, member string) MemberUsage {
	if c == nil {
		return MemberUsage{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	latest := c.latestUsage[Target{Namespace: domain.NormalizedNamespace(cluster.Namespace), Cluster: cluster.Name, Member: member}]
	if !c.sampleFresh(latest.at, time.Now()) {
		return MemberUsage{}
	}
	return latest.usage
}

func (c *Collector) scrape(ctx context.Context) error {
	// Retention must still run when discovery or new ingestion fails, especially
	// when the database has no room for additional metric samples.
	pruneErr := c.pruneRetention(ctx)
	targets, err := c.o.Targets(ctx)
	if err != nil {
		// Discovery did not establish which targets are still current. Do not
		// republish cached successes with the next consumer's scrape time.
		c.mu.Lock()
		for target := range c.latest {
			c.latest[target] = nil
			delete(c.latestUsage, target)
		}
		c.mu.Unlock()
		return errors.Join(pruneErr, err)
	}
	live := make(map[Target]bool, len(targets))
	for i := range targets {
		targets[i].Namespace = domain.NormalizedNamespace(targets[i].Namespace)
		live[targets[i]] = true
	}
	// Bound both fan-out and each target's observation budget. Failed exporters
	// cannot consume a whole interval per member before healthy ones are visited.
	// Divide the interval across worker waves as inventory grows, so a large
	// set of failed exporters cannot make every subsequent sweep stale.
	budget := min(30*time.Second, c.o.Interval/time.Duration(max(1, (len(targets)+7)/8)))
	jobs := make(chan Target)
	var workers sync.WaitGroup
	var errorMu sync.Mutex
	collectionErr := pruneErr
	for range min(8, len(targets)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for target := range jobs {
				if err := c.scrapeTarget(ctx, target, budget); err != nil {
					errorMu.Lock()
					collectionErr = errors.Join(collectionErr, err)
					errorMu.Unlock()
				}
			}
		}()
	}
	for _, target := range targets {
		select {
		case jobs <- target:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return errors.Join(collectionErr, ctx.Err())
		}
	}
	close(jobs)
	workers.Wait()
	c.mu.Lock()
	for target := range c.latest {
		if !live[target] {
			delete(c.latest, target)
			delete(c.latestUsage, target)
		}
	}
	c.mu.Unlock()
	return collectionErr
}

func (c *Collector) scrapeTarget(ctx context.Context, target Target, budget time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	client, err := c.o.Dialer.Dial(ctx, domain.ClusterRef{Namespace: target.Namespace, Name: target.Cluster}, target.Member)
	if err != nil {
		c.o.Logger.Warn("metrics dial failed", "cluster", target.Cluster, "member", target.Member, "error", err)
		c.cacheScrape(target, nil, nil, time.Now().UTC())
		return nil
	}
	data, err := client.Metrics(ctx)
	client.Close()
	if err != nil {
		c.o.Logger.Warn("metrics scrape failed", "cluster", target.Cluster, "member", target.Member, "error", err)
		c.cacheScrape(target, nil, nil, time.Now().UTC())
		return nil
	}
	now := time.Now().UTC()
	families, samples, err := parseMetricsWithVocabulary(data, target, now, c.vocabulary)
	if err != nil {
		c.o.Logger.Warn("invalid member metrics", "member", target.Member, "error", err)
		c.cacheScrape(target, nil, nil, now)
		return nil
	}
	// Persistence failure does not invalidate an independently fresh observation.
	c.cacheScrape(target, families, samples, now)
	if err := c.o.Store.InsertMetricSamples(ctx, samples); err != nil {
		return fmt.Errorf("persist metrics for %s/%s/%s: %w", target.Namespace, target.Cluster, target.Member, err)
	}
	return nil
}

// A saturated sweep schedules another bounded sweep instead of imposing a
// 4096-row-per-scrape ceiling. One worker coalesces wakeups and yields between
// sweeps; discovery/exporter failures cannot stop an existing backlog draining.
func (c *Collector) pruneRetention(ctx context.Context) error {
	n, err := c.o.Store.PruneMetricSamples(ctx, time.Now().Add(-c.o.Retention))
	if ctx.Err() == nil && (n == store.MetricPruneMaxRows || errors.Is(err, context.DeadlineExceeded)) {
		select {
		case c.retention <- struct{}{}:
		default:
		}
	}
	return err
}

func (c *Collector) continueRetention(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.retention:
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := c.pruneRetention(ctx); err != nil && ctx.Err() == nil {
			c.o.Logger.Error("monitoring retention failed", "error", err)
		}
	}
}

// Run scrapes discovered members until cancellation, retaining samples transactionally.
func (c *Collector) Run(ctx context.Context) error {
	if c.o.Store == nil || c.o.Dialer == nil || c.o.Targets == nil {
		return errors.New("monitoring store, dialer and targets are required")
	}
	child, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.continueRetention(child)
	}()
	defer func() { cancel(); <-done }()
	ticker := time.NewTicker(c.o.Interval)
	defer ticker.Stop()
	for {
		if err := c.scrape(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.o.Logger.Error("monitoring collection failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func seriesKey(s domain.MetricSample) string {
	data, _ := json.Marshal(s.Labels)
	return s.Member + "\x00" + s.Metric + "\x00" + string(data)
}
func metricInfoWithVocabulary(name string, vocab map[string]vocabulary) vocabulary {
	for _, v := range vocab {
		if v.name == name {
			return v
		}
	}
	return vocabulary{name: name, unit: "count", scale: 1}
}

// sampleValue is shared by live member usage and historical UI series.
func sampleValue(sample domain.MetricSample, previous *domain.MetricSample) (float64, bool) {
	return sampleValueWithVocabulary(sample, previous, vocabularyByName)
}

func sampleValueWithVocabulary(sample domain.MetricSample, previous *domain.MetricSample, vocab map[string]vocabulary) (float64, bool) {
	if sample.Labels["__type"] != "counter" {
		return sample.Value, true
	}
	if previous == nil {
		return 0, false
	}
	seconds := sample.Timestamp.Sub(previous.Timestamp).Seconds()
	if seconds <= 0 {
		return 0, false
	}
	delta := sample.Value - previous.Value
	if delta < 0 {
		delta = sample.Value
	}
	value := delta / seconds * metricInfoWithVocabulary(sample.Metric, vocab).scale
	if sample.Metric == "cpu.percent" {
		value = min(100, max(0, value))
	}
	return value, true
}

// Series reads canonical samples, derives reset-aware counter rates, and buckets by step.
func (c *Collector) Series(ctx context.Context, cluster domain.ClusterRef, metrics []string, start, end time.Time, step time.Duration) ([]Series, error) {
	if step <= 0 {
		step = c.o.Interval
	}
	if !end.After(start) {
		return nil, errors.New("metric end must follow start")
	}
	samples, err := c.o.Store.QueryMetricSamples(ctx, store.MetricQuery{Namespace: domain.NormalizedNamespace(cluster.Namespace), Cluster: cluster.Name, Start: start.Add(-c.o.Interval * 2), End: end})
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, name := range metrics {
		selected[name] = true
	}
	type accumulation struct {
		series Series
		prev   *domain.MetricSample
		bucket int64
		sum    float64
		count  int
	}
	all := map[string]*accumulation{}
	flush := func(a *accumulation) {
		if a.count > 0 {
			a.series.Points = append(a.series.Points, Point{Timestamp: start.Add(time.Duration(a.bucket) * step), Value: a.sum / float64(a.count)})
			a.sum = 0
			a.count = 0
		}
	}
	for _, sample := range samples {
		if len(selected) > 0 && !selected[sample.Metric] {
			continue
		}
		key := seriesKey(sample)
		a := all[key]
		if a == nil {
			labels := map[string]string{}
			for k, v := range sample.Labels {
				if k != "__type" {
					labels[k] = v
				}
			}
			v := metricInfoWithVocabulary(sample.Metric, c.vocabulary)
			a = &accumulation{series: Series{Name: sample.Metric, Member: sample.Member, Unit: v.unit, Labels: labels, Points: []Point{}}}
			all[key] = a
		}
		previous := a.prev
		copy := sample
		a.prev = &copy
		if sample.Timestamp.Before(start) {
			continue
		}
		value, ok := sampleValueWithVocabulary(sample, previous, c.vocabulary)
		if !ok {
			continue
		}
		bucket := int64(sample.Timestamp.Sub(start) / step)
		if a.count > 0 && bucket != a.bucket {
			flush(a)
		}
		a.bucket = bucket
		a.sum += value
		a.count++
	}
	result := make([]Series, 0, len(all))
	for _, a := range all {
		flush(a)
		if len(a.series.Points) > 0 {
			result = append(result, a.series)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		if result[i].Member != result[j].Member {
			return result[i].Member < result[j].Member
		}
		a, _ := json.Marshal(result[i].Labels)
		b, _ := json.Marshal(result[j].Labels)
		return string(a) < string(b)
	})
	return result, nil
}

// WritePrometheus federates exposition with authoritative cluster/member labels.
// Failed discovery/scrapes and samples older than two collection intervals expose
// only karakoram_member_scrape_success=0, never cached database health values.
func (c *Collector) WritePrometheus(ctx context.Context, w io.Writer, allow func(domain.ClusterRef) bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	families := map[string]*dto.MetricFamily{}
	var staleScrape map[string]*dto.MetricFamily
	now := time.Now()
	for target, scrape := range c.latest {
		if err := ctx.Err(); err != nil {
			return err
		}
		if allow != nil && !allow(domain.ClusterRef{Namespace: domain.NormalizedNamespace(target.Namespace), Name: target.Cluster}) {
			continue
		}
		if !c.sampleFresh(c.latestUsage[target].at, now) {
			if staleScrape == nil {
				staleScrape = map[string]*dto.MetricFamily{"karakoram_member_scrape_success": scrapeSuccessFamily(false)}
			}
			scrape = staleScrape
		}
		for name, family := range scrape {
			dest := families[name]
			if dest == nil {
				dest = &dto.MetricFamily{Name: family.Name, Help: family.Help, Type: family.Type}
				families[name] = dest
			}
			for _, sample := range family.Metric {
				copy := &dto.Metric{Gauge: sample.Gauge, Counter: sample.Counter, Summary: sample.Summary, Histogram: sample.Histogram, Untyped: sample.Untyped}
				for _, label := range sample.Label {
					if label.GetName() != "namespace" && label.GetName() != "cluster" && label.GetName() != "member" {
						copy.Label = append(copy.Label, label)
					}
				}
				namespace, cluster, member := domain.NormalizedNamespace(target.Namespace), target.Cluster, target.Member
				namespaceName, clusterName, memberName := "namespace", "cluster", "member"
				copy.Label = append(copy.Label, &dto.LabelPair{Name: &namespaceName, Value: &namespace}, &dto.LabelPair{Name: &clusterName, Value: &cluster}, &dto.LabelPair{Name: &memberName, Value: &member})
				dest.Metric = append(dest.Metric, copy)
			}
		}
	}
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err := expfmt.MetricFamilyToText(w, families[name]); err != nil {
			return fmt.Errorf("write metrics: %w", err)
		}
	}
	return nil
}
