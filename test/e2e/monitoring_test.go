//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"io"
	"strings"
	"testing"
	"time"
)

func TestMonitoring(t *testing.T) {
	t.Run("ReplicaSet", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.privateSpec(3))
		s.assertMetrics(name)
		s.assertPrometheus(name)
	})
	t.Run("Sharded", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.shardedSpec(false))
		s.assertMetrics(name)
		s.assertPrometheus(name)
	})
}

func (s *suite) assertMetrics(name string) {
	s.t.Helper()
	members := s.members(name)
	var baseline api.ClusterMetrics
	s.expect(&baseline)(s.client.GetClusterMetrics(s.ctx, name, &api.GetClusterMetricsParams{Range: ptr("15m")}))
	// Point timestamps are request-relative bucket starts, not scrape times.
	// Only a bucket strictly after the baseline response End proves a new scrape.
	s.poll("per-member JSON metrics", 5*time.Minute, func() bool {
		var metrics api.ClusterMetrics
		s.expect(&metrics)(s.client.GetClusterMetrics(s.ctx, name, &api.GetClusterMetricsParams{Range: ptr("15m")}))
		// Keep this list aligned with the canonical panels in MonitoringTab.tsx.
		canonical := []string{"opcounters.total", "connections.current", "repl.lagSeconds", "cache.usedBytes", "cache.maxBytes", "cpu.percent", "ram.usedBytes", "ram.totalBytes", "disk.usedBytes", "disk.totalBytes"}
		seen := freshMetricNames(metrics, baseline.End)
		for _, member := range members {
			for _, metric := range canonical {
				// mongos has neither a WiredTiger cache nor a replica-set oplog.
				if member.Role == api.MemberRoleRouter && (metric == "repl.lagSeconds" || strings.HasPrefix(metric, "cache.")) {
					continue
				}
				if !seen[member.Name][metric] {
					s.t.Logf("waiting for member=%s canonical metric=%s", member.Name, metric)
					return false
				}
			}
		}
		s.snapshot(name+"-metrics", metrics)
		return true
	})
}

func (s *suite) assertPrometheus(name string) {
	s.t.Helper()
	members := s.members(name)
	s.poll("labelled Prometheus metrics", 5*time.Minute, func() bool {
		r, err := s.root.GetPrometheusMetrics(s.ctx, nil)
		if err != nil {
			s.t.Fatalf("Prometheus transport: %s", s.redact(err.Error()))
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			s.t.Fatalf("Prometheus HTTP %d", r.StatusCode)
		}
		b, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		if err != nil {
			s.t.Fatal(err)
		}
		for _, member := range members {
			found := false
			for _, line := range strings.Split(string(b), "\n") {
				if !strings.HasPrefix(line, "#") && strings.Contains(line, `cluster="`+name+`"`) && strings.Contains(line, `member="`+member.Name+`"`) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		s.t.Logf("Prometheus labelled series found cluster=%s members=%d", name, len(members))
		return true
	})
}
