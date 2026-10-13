// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
)

type usageDialer struct {
	url string
	ca  []byte
}

func (d usageDialer) Dial(context.Context, string, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "token", CAPEM: d.ca})
}

func TestMemberResponsesUseFreshCollectorSamples(t *testing.T) {
	s, _, token := testServer(t)
	assertResponses := func(populated bool) {
		t.Helper()
		for _, path := range []string{
			"/api/v1/clusters",
			"/api/v1/clusters/prod",
			"/api/v1/clusters/prod/status",
			"/api/v1/clusters/prod/members",
			"/api/v1/clusters/prod/members/prodm0",
		} {
			r := call(s, http.MethodGet, path, "", token)
			if r.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", path, r.Code, r.Body.String())
			}
			var body any
			if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			found := false
			var visit func(any)
			visit = func(value any) {
				switch value := value.(type) {
				case map[string]any:
					if value["name"] == "prodm0" || value["name"] == "devm0" {
						want := populated && value["name"] == "prodm0"
						if value["name"] == "prodm0" {
							found = true
						}
						for _, field := range []string{"cpuUsage", "ramUsageBytes", "diskUsageBytes", "diskTotalBytes"} {
							if v, present := value[field]; present != want || (present && v == nil) {
								t.Fatalf("%s %s: %s presence=%v value=%v, want presence=%v", path, value["name"], field, present, v, want)
							}
						}
						if want {
							cpu, ok := value["cpuUsage"].(float64)
							if !ok || cpu <= 0 || cpu > 100 || value["ramUsageBytes"] != float64(209715200) || value["diskUsageBytes"] != float64(1073741824) || value["diskTotalBytes"] != float64(5368709120) {
								t.Fatalf("%s: incorrect member usage: %+v", path, value)
							}
						}
					}
					for _, child := range value {
						visit(child)
					}
				case []any:
					for _, child := range value {
						visit(child)
					}
				}
			}
			visit(body)
			if !found {
				t.Fatalf("%s: member absent from response: %s", path, r.Body.String())
			}
		}
	}
	assertResponses(false)

	var scrapes atomic.Int64
	exporter := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics" || r.Header.Get("Authorization") != "Bearer token" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "# TYPE karakoram_process_cpu_seconds_total counter\nkarakoram_process_cpu_seconds_total %.3f\n# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 209715200\n# TYPE karakoram_disk_used_bytes gauge\nkarakoram_disk_used_bytes 1073741824\n# TYPE karakoram_disk_total_bytes gauge\nkarakoram_disk_total_bytes 5368709120\n", 10+float64(scrapes.Add(1))/1000)
	}))
	defer exporter.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: exporter.Certificate().Raw})
	collector := monitoring.New(monitoring.Options{
		Store: s.Store, Dialer: usageDialer{url: exporter.URL, ca: ca}, Interval: 100 * time.Millisecond,
		Targets: func(context.Context) ([]monitoring.Target, error) {
			return []monitoring.Target{{Cluster: "prod", Member: "prodm0"}}, nil
		},
	})
	s.Monitoring = collector
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for collector.MemberUsage("prod", "prodm0").CPUPercent == nil {
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatal("collector did not receive two fresh CPU samples")
		}
	}
	assertResponses(true)
	s.Monitoring = nil
	assertResponses(false)
}
