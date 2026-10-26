// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package monitoring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRetentionContinuesPastSweepBudgetDuringDiscoveryStall(t *testing.T) {
	st := monitoringStore(t)
	var expired []domain.MetricSample
	for cluster := range 6 {
		for range 800 {
			expired = append(expired, domain.MetricSample{Cluster: fmt.Sprint(cluster), Member: "m0", Metric: "up", Timestamp: time.Now().Add(-48 * time.Hour)})
		}
	}
	if err := st.InsertMetricSamples(t.Context(), expired); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	discovery := make(chan struct{})
	c := New(Options{Store: st, Dialer: testDialer{}, Targets: func(ctx context.Context) ([]Target, error) {
		close(discovery)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("collector did not stop its retention worker")
		}
	})
	select {
	case <-discovery:
	case <-time.After(3 * time.Second):
		t.Fatal("initial retention sweep blocked discovery")
	}
	deadline := time.Now().Add(time.Second)
	for {
		samples, err := st.QueryMetricSamples(t.Context(), store.MetricQuery{})
		if err != nil {
			t.Fatal(err)
		}
		if len(samples) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d expired rows stranded until the next 30-second scrape", len(samples))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
