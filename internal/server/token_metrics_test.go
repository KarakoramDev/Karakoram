// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
)

func TestRestrictedTokenPrometheusFiltersClusters(t *testing.T) {
	s, _, admin := testServer(t)
	exporter := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 1234\n")
	}))
	defer exporter.Close()
	collector := monitoring.New(monitoring.Options{
		Store: s.Store, Dialer: usageDialer{url: exporter.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: exporter.Certificate().Raw})}, Interval: 100 * time.Millisecond,
		Targets: func(context.Context) ([]monitoring.Target, error) {
			return []monitoring.Target{{Cluster: "prod", Member: "prodm0"}, {Cluster: "dev", Member: "devm0"}}, nil
		},
	})
	s.Monitoring = collector
	ctx, cancel := context.WithCancel(t.Context())
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
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for collector.MemberUsage(domain.ClusterRef{Name: "prod"}, "prodm0").RAMBytes == nil || collector.MemberUsage(domain.ClusterRef{Name: "dev"}, "devm0").RAMBytes == nil {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("collector did not receive both samples")
		}
	}
	all := call(s, "GET", "/metrics", "", admin)
	if all.Code != 200 || !strings.Contains(all.Body.String(), `cluster="dev"`) || !strings.Contains(all.Body.String(), `cluster="prod"`) {
		t.Fatal(all.Code, all.Body.String())
	}
	token := restrictedAdmin(t, s, &domain.TokenRestrictions{Clusters: []string{"default/prod"}})
	filtered := call(s, "GET", "/metrics", "", token)
	if filtered.Code != 200 || strings.Contains(filtered.Body.String(), `cluster="dev"`) || !strings.Contains(filtered.Body.String(), `cluster="prod"`) {
		t.Fatal(filtered.Code, filtered.Body.String())
	}
	denied := restrictedAdmin(t, s, &domain.TokenRestrictions{Permissions: []string{"clusters:read"}})
	if w := call(s, "GET", "/metrics", "", denied); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
}
