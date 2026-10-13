// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sys/unix"
)

func diskUsage(dir string) (agentapi.DiskUsage, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return agentapi.DiskUsage{}, err
	}
	size := uint64(st.Bsize)
	return agentapi.DiskUsage{TotalBytes: st.Blocks * size, UsedBytes: (st.Blocks - st.Bfree) * size, FreeBytes: st.Bavail * size}, nil
}
func hostFingerprint() string {
	var signals []string
	if data, err := os.ReadFile("/proc/spl/kstat/zfs/arcstats"); err == nil {
		header := strings.Fields(strings.SplitN(string(data), "\n", 2)[0])
		if len(header) >= 6 {
			signals = append(signals, strings.Join(header[:6], " "))
		}
	}
	if data, err := os.ReadFile("/etc/hostid"); err == nil && len(data) > 0 {
		signals = append(signals, hex.EncodeToString(data))
	}
	// CPU model is not unique. Do not claim a fingerprint using it alone.
	if len(signals) == 0 {
		return ""
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				signals = append(signals, line)
				break
			}
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(signals, "\n")))
	return "best-effort:" + hex.EncodeToString(sum[:12])
}

type metricValue struct {
	name    string
	value   float64
	counter bool
	labels  map[string]string
}

func number(v any) float64 {
	switch x := v.(type) {
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case float64:
		return x
	case float32:
		return float64(x)
	}
	return 0
}
func field(m any, path ...string) any {
	var v any = m
	for _, key := range path {
		switch node := v.(type) {
		case bson.M:
			v = node[key]
		case map[string]any:
			v = node[key]
		case bson.D:
			v = nil
			for _, entry := range node {
				if entry.Key == key {
					v = entry.Value
					break
				}
			}
		default:
			return nil
		}
	}
	return v
}
func (a *nodeAgent) collectMetrics(ctx context.Context) []metricValue {
	values := []metricValue{{name: "karakoram_mongodb_up"}}
	gauge := func(name string, value float64) { values = append(values, metricValue{name: name, value: value}) }
	counter := func(name string, value float64) {
		values = append(values, metricValue{name: name, value: value, counter: true})
	}
	states, _ := a.sup.snapshot()
	if states["pbm-agent"].State == "running" {
		gauge("karakoram_pbm_agent_up", 1)
	} else {
		gauge("karakoram_pbm_agent_up", 0)
	}
	if disk, err := diskUsage(a.o.DataDir); err == nil {
		gauge("karakoram_disk_total_bytes", float64(disk.TotalBytes))
		gauge("karakoram_disk_used_bytes", float64(disk.UsedBytes))
	}
	engine := "mongod"
	c := a.config()
	if c.Role == agentapi.RoleRouter {
		engine = "mongos"
	}
	cpu, rss, ok := processUsage(states[engine].PID)
	if ok {
		counter("karakoram_process_cpu_seconds_total", cpu)
		gauge("karakoram_ram_used_bytes", rss)
	}
	gauge("karakoram_ram_total_bytes", memoryTotal())
	cred, ok := c.SystemUsers["clusterMonitor"]
	if !ok {
		return values
	}
	if cred.Username == "" {
		cred.Username = "clusterMonitor"
	}
	client, err := localMongo(c, &cred)
	if err != nil {
		return values
	}
	defer client.Disconnect(context.Background())
	var status bson.M
	if err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		return values
	}
	values[0].value = 1
	for name, path := range map[string][]string{"karakoram_mongodb_uptime_seconds": {"uptime"}, "karakoram_connections_current": {"connections", "current"}, "karakoram_connections_available": {"connections", "available"}, "karakoram_cache_used_bytes": {"wiredTiger", "cache", "bytes currently in the cache"}, "karakoram_cache_max_bytes": {"wiredTiger", "cache", "maximum bytes configured"}} {
		if value := field(status, path...); value != nil {
			gauge(name, number(value))
		}
	}
	var total float64
	for _, op := range []string{"insert", "query", "update", "delete", "getmore", "command"} {
		v := number(field(status, "opcounters", op))
		total += v
		values = append(values, metricValue{name: "karakoram_mongodb_opcounters_total", value: v, counter: true, labels: map[string]string{"operation": op}})
	}
	counter("karakoram_opcounters_total", total)
	for _, key := range []string{"inserted", "updated", "deleted", "returned"} {
		v := field(status, "metrics", "document", key)
		if v != nil {
			values = append(values, metricValue{name: "karakoram_mongodb_documents_total", value: number(v), counter: true, labels: map[string]string{"operation": key}})
		}
	}
	for _, key := range []string{"bytesIn", "bytesOut"} {
		values = append(values, metricValue{name: "karakoram_mongodb_network_bytes_total", value: number(field(status, "network", key)), counter: true, labels: map[string]string{"direction": key}})
	}
	if c.Role == agentapi.RoleRouter {
		return values
	}
	var repl bson.M
	if client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&repl) == nil {
		gauge("karakoram_repl_member_state", number(repl["myState"]))
		members, _ := repl["members"].(bson.A)
		var latest, self float64
		for _, item := range members {
			stamp, _ := field(item, "optime", "ts").(bson.Timestamp)
			t := float64(stamp.T)
			if t > latest {
				latest = t
			}
			if field(item, "self") == true {
				self = t
			}
		}
		if self > 0 {
			gauge("karakoram_repl_lag_seconds", max(0, latest-self))
		}
	}
	oplog := client.Database("local").Collection("oplog.rs")
	var first, last bson.M
	if oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}}).SetProjection(bson.M{"ts": 1})).Decode(&first) == nil && oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(bson.M{"ts": 1})).Decode(&last) == nil {
		f, _ := first["ts"].(bson.Timestamp)
		l, _ := last["ts"].(bson.Timestamp)
		gauge("karakoram_oplog_window_seconds", max(0, float64(l.T)-float64(f.T)))
	}
	return values
}

var clockTicks = sync.OnceValue(func() float64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "getconf", "CLK_TCK").Output()
	if err != nil {
		return 0
	}
	value, _ := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	return value
})

func processUsage(pid int) (cpu, rss float64, ok bool) {
	if pid <= 0 {
		return
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 22 {
		return
	}
	utime, _ := strconv.ParseFloat(fields[11], 64)
	stime, _ := strconv.ParseFloat(fields[12], 64)
	pages, _ := strconv.ParseFloat(fields[21], 64)
	ticks := clockTicks()
	if ticks <= 0 {
		return
	}
	return (utime + stime) / ticks, pages * float64(os.Getpagesize()), true
}
func memoryTotal() float64 {
	var total float64
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) > 1 {
					total, _ = strconv.ParseFloat(fields[1], 64)
					total *= 1024
				}
				break
			}
		}
	}
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		if data, err := os.ReadFile(path); err == nil {
			limit, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
			if err == nil && limit > 0 && (total == 0 || limit < total) {
				total = limit
			}
		}
	}
	return total
}

type scrapeCollector struct{ values []metricValue }

func (c scrapeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, v := range c.values {
		labels := make([]string, 0, len(v.labels))
		for name := range v.labels {
			labels = append(labels, name)
		}
		ch <- prometheus.NewDesc(v.name, "Karakoram member metric.", labels, nil)
	}
}
func (c scrapeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range c.values {
		names, values := []string{}, []string{}
		for name, value := range v.labels {
			names = append(names, name)
			values = append(values, value)
		}
		kind := prometheus.GaugeValue
		if v.counter {
			kind = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(v.name, "Karakoram member metric.", names, nil), kind, v.value, values...)
	}
}
func (a *nodeAgent) writeMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	registry := prometheus.NewRegistry()
	registry.MustRegister(scrapeCollector{values: a.collectMetrics(ctx)})
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}
