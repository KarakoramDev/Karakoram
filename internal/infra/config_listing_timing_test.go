// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigListingProofUsesRequestStart(t *testing.T) {
	base := time.Now()
	now := base
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	snapshot := &TargetSnapshot{}
	var starts []time.Time
	snapshot.SetListing(func(ctx context.Context) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > SnapshotListingRequestBudget {
			t.Fatal("configuration listing is not bounded")
		}
		starts = append(starts, now)
		now = now.Add(250 * time.Millisecond)
		return "same", nil
	})
	if !snapshot.EnsureListing(t.Context()) || !snapshot.checkedAt.Equal(base) {
		t.Fatal("HTTP completion advanced proof time", snapshot.checkedAt, base)
	}
	// Absolute renewal ticks: HTTP response latency must not move the next
	// request from the 60-second tick to the 65-second tick.
	for second := 1; second <= 3600; second++ {
		now = base.Add(time.Duration(second) * time.Second)
		if second%5 == 0 && !snapshot.EnsureListing(t.Context()) {
			t.Fatal("listing failed")
		}
	}
	for i, start := range starts {
		if i > 0 && start.Sub(starts[i-1]) != SnapshotListingInterval {
			t.Fatal("listing cadence drifted off 60-second grid", start.Sub(starts[i-1]))
		}
	}
	if len(starts) != 61 {
		t.Fatal("unexpected hour listing count", len(starts))
	}
	now = snapshot.checkedAt.Add(SnapshotListingInterval)
	if _, _, valid := snapshot.ConfigGeneration(); valid {
		t.Fatal("config proof silently gained a freshness grace period")
	}
}

func TestConfigListingRefreshSharesFlight(t *testing.T) {
	now := time.Now()
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	snapshot := &TargetSnapshot{}
	snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	if !snapshot.EnsureListing(t.Context()) {
		t.Fatal("baseline failed")
	}
	now = now.Add(65 * time.Second)
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	snapshot.SetListing(func(ctx context.Context) (string, error) {
		if RequestCaller(ctx) != CallerListing {
			t.Error("listing caller missing")
		}
		calls.Add(1)
		close(entered)
		<-release
		return "same", nil
	})
	var workers sync.WaitGroup
	results := make(chan bool, 12)
	for range 12 {
		workers.Add(1)
		go func() { defer workers.Done(); results <- snapshot.EnsureListing(t.Context()) }()
	}
	<-entered
	close(release)
	workers.Wait()
	close(results)
	for ok := range results {
		if !ok {
			t.Fatal("shared configuration proof failed")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("stale configuration pulls duplicated the shared listing", calls.Load())
	}
}
