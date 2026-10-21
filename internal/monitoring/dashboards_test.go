// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
	"gopkg.in/yaml.v3"
)

func TestDashboardArtifacts(t *testing.T) {
	known := map[string]bool{"karakoram_member_scrape_success": true}
	for _, metric := range monitoring.StateMetrics {
		if known[metric.Name] || metric.Help == "" || (metric.Type != "gauge" && metric.Type != "counter") {
			t.Fatalf("invalid or duplicate state metric: %+v", metric)
		}
		known[metric.Name] = true
		labels := map[string]bool{}
		for _, label := range metric.Labels {
			if label == "" || labels[label] {
				t.Fatalf("invalid labels for %s: %v", metric.Name, metric.Labels)
			}
			labels[label] = true
		}
	}
	for _, names := range [][]string{rmqspec.MetricNames, pgspec.MetricNames} {
		for _, name := range names {
			known[name] = true
		}
	}
	for _, metric := range (&elastic.Provider{}).Metrics() {
		known[metric.Exposition] = true
	}
	// MongoDB and shared runtime families have no exported catalog: see
	// internal/agent/metrics.go and internal/monitoring/collector.go.
	// Supabase's two additions are defined in internal/agent/supabase/runner.go
	// and internal/engine/supabase/provider.go; its remaining catalog is pgspec.
	for _, name := range strings.Fields(`
		karakoram_mongodb_up karakoram_pbm_agent_up karakoram_mongodb_uptime_seconds
		karakoram_connections_current karakoram_connections_available
		karakoram_cache_used_bytes karakoram_cache_max_bytes karakoram_opcounters_total
		karakoram_mongodb_opcounters_total karakoram_mongodb_documents_total
		karakoram_mongodb_network_bytes_total karakoram_repl_member_state
		karakoram_repl_lag_seconds karakoram_oplog_window_seconds
		karakoram_process_cpu_seconds_total karakoram_process_resident_memory_bytes
		karakoram_ram_used_bytes karakoram_ram_total_bytes
		karakoram_disk_used_bytes karakoram_disk_total_bytes
		karakoram_supabase_up karakoram_supabase_restarts_total`) {
		known[name] = true
	}
	metricRE := regexp.MustCompile(`\bkarakoram_[a-z0-9_]+\b`)
	checkExpression := func(source, expr string) {
		t.Helper()
		for _, name := range metricRE.FindAllString(expr, -1) {
			if !known[name] {
				t.Errorf("%s references unknown exposition series %q", source, name)
			}
		}
	}
	paths, err := filepath.Glob("../../deploy/grafana/dashboards/*.json")
	if err != nil || len(paths) != 6 {
		t.Fatalf("dashboard paths: %v, error: %v", paths, err)
	}
	uids := map[string]bool{}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var dashboard struct {
				UID           string `json:"uid"`
				SchemaVersion int    `json:"schemaVersion"`
				Templating    struct {
					List []struct {
						Name  string          `json:"name"`
						Type  string          `json:"type"`
						Query json.RawMessage `json:"query"`
					} `json:"list"`
				} `json:"templating"`
				Panels []struct {
					Targets []struct {
						Expr string `json:"expr"`
					} `json:"targets"`
				} `json:"panels"`
			}
			if err := json.Unmarshal(data, &dashboard); err != nil {
				t.Fatal(err)
			}
			wantUID := "karakoram-" + strings.TrimSuffix(filepath.Base(path), ".json")
			if dashboard.UID != wantUID || uids[dashboard.UID] || dashboard.SchemaVersion < 38 {
				t.Fatalf("invalid uid/schema: %s/%d", dashboard.UID, dashboard.SchemaVersion)
			}
			uids[dashboard.UID] = true
			variables := map[string]bool{}
			for _, variable := range dashboard.Templating.List {
				variables[variable.Name] = true
				if variable.Name == "datasource" && (variable.Type != "datasource" || string(variable.Query) != `"prometheus"`) {
					t.Error("datasource variable must select Prometheus")
				}
				checkExpression(path, string(variable.Query))
				if dashboard.UID == "karakoram-overview" {
					metric := "karakoram_cluster_info"
					if variable.Name == "member" {
						metric = "karakoram_member_agent_outdated"
					}
					if variable.Name != "datasource" && !strings.Contains(string(variable.Query), metric) {
						t.Errorf("overview %s selector must use durable state metric %s", variable.Name, metric)
					}
				}
			}
			for _, name := range []string{"datasource", "namespace", "cluster", "member"} {
				if !variables[name] {
					t.Errorf("missing variable %s", name)
				}
			}
			if len(dashboard.Panels) == 0 {
				t.Fatal("dashboard has no panels")
			}
			for _, panel := range dashboard.Panels {
				for _, target := range panel.Targets {
					if target.Expr == "" {
						t.Error("empty panel expression")
					}
					checkExpression(path, target.Expr)
				}
			}
		})
	}
	data, err := os.ReadFile("../../deploy/prometheus/karakoram-rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var rules struct {
		Groups []struct {
			Name  string `yaml:"name"`
			Rules []struct {
				Alert       string            `yaml:"alert"`
				Expr        string            `yaml:"expr"`
				For         string            `yaml:"for"`
				Labels      map[string]string `yaml:"labels"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&rules); err != nil {
		t.Fatal(err)
	}
	if len(rules.Groups) == 0 {
		t.Fatal("no alert groups")
	}
	alerts := map[string]bool{}
	for _, group := range rules.Groups {
		if group.Name == "" || len(group.Rules) == 0 {
			t.Error("empty alert group")
		}
		for _, rule := range group.Rules {
			if rule.Alert == "" || alerts[rule.Alert] || rule.Expr == "" || rule.For == "" {
				t.Errorf("invalid or duplicate alert %q", rule.Alert)
			}
			alerts[rule.Alert] = true
			if rule.Labels["severity"] != "warning" && rule.Labels["severity"] != "critical" {
				t.Errorf("%s has invalid severity", rule.Alert)
			}
			for _, key := range []string{"summary", "description", "runbook_url"} {
				if rule.Annotations[key] == "" {
					t.Errorf("%s missing annotation %s", rule.Alert, key)
				}
			}
			checkExpression(rule.Alert, rule.Expr)
		}
	}
}
