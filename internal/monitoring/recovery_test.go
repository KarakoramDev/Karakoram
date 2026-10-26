// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"context"
	"database/sql"
	"encoding/pem"
	"fmt"
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

func TestPruneRunsAfterInsertFailure(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "metrics.db")
	key, _ := secrets.New(strings.Repeat("k", 32))
	st, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.InsertMetricSamples(ctx, []domain.MetricSample{{Cluster: "expired", Member: "m0", Metric: "up", Timestamp: time.Now().Add(-48 * time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TRIGGER reject_metrics BEFORE INSERT ON metrics BEGIN SELECT RAISE(FAIL, 'metrics unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Write([]byte("# TYPE karakoram_ram_used_bytes gauge\nkarakoram_ram_used_bytes 42\n"))
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	c := New(Options{Store: st, Dialer: testDialer{url: srv.URL, ca: ca}, Targets: func(context.Context) ([]Target, error) {
		return []Target{{Cluster: "a", Member: "m0"}, {Cluster: "b", Member: "m0"}}, nil
	}})
	if err := c.scrape(ctx); err == nil {
		t.Fatal("lost persistence failure")
	}
	if requests.Load() != 2 {
		t.Fatalf("scraped %d targets", requests.Load())
	}
	samples, err := st.QueryMetricSamples(ctx, store.MetricQuery{Cluster: "expired"})
	if err != nil || len(samples) != 0 {
		t.Fatalf("expired samples retained: %v %v", samples, err)
	}
	if usage := c.MemberUsage(domain.ClusterRef{Name: "b"}, "m0"); usage.RAMBytes == nil || *usage.RAMBytes != 42 {
		t.Fatalf("fresh observation discarded: %+v", usage)
	}
}

type memberDialer struct{ slow, healthy testDialer }

func (d memberDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if member == "healthy" {
		return d.healthy.Dial(ctx, ref, member)
	}
	return d.slow.Dial(ctx, ref, member)
}

func TestSlowMembersDoNotStarveHealthyCluster(t *testing.T) {
	for _, slowCount := range []int{2, 18} {
		t.Run(fmt.Sprint(slowCount), func(t *testing.T) {
			testSlowMembersDoNotStarveHealthyCluster(t, slowCount)
		})
	}
}

func testSlowMembersDoNotStarveHealthyCluster(t *testing.T, slowCount int) {
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	observed := make(chan struct{}, 1)
	healthy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(exposition)); observed <- struct{}{} }))
	defer healthy.Close()
	dial := func(s *httptest.Server) testDialer {
		return testDialer{url: s.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}
	}
	c := New(Options{Store: monitoringStore(t), Interval: 200 * time.Millisecond, Dialer: memberDialer{slow: dial(slow), healthy: dial(healthy)}, Targets: func(context.Context) ([]Target, error) {
		targets := make([]Target, 0, slowCount+1)
		for i := range slowCount {
			targets = append(targets, Target{Cluster: "demo", Member: fmt.Sprint("slow", i)})
		}
		return append(targets, Target{Cluster: "demo", Member: "healthy"}), nil
	}})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.scrape(ctx) }()
	select {
	case <-observed:
	case <-time.After(180 * time.Millisecond):
		t.Error("healthy exporter starved behind failed members")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Error("scrape did not respect per-target budget")
	}
}
