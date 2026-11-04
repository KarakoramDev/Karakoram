// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRetryScheduleProvenanceAndMaximum(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	for _, delay := range []time.Duration{0, time.Minute, time.Hour, 48 * time.Hour, MaxPlatformRetryDuration} {
		var schedule RetrySchedule
		at := schedule.Schedule(now, now.Add(delay))
		raw, err := json.Marshal(schedule)
		if err != nil {
			t.Fatal(err)
		}
		schedule = RetrySchedule{}
		if err := json.Unmarshal(raw, &schedule); err != nil {
			t.Fatal(err)
		}
		if !at.Equal(now.Add(delay)) || schedule.RetryDuration != delay || !schedule.RetryRecordedAt.Equal(now) {
			t.Fatalf("provenance lost: %+v at=%v", schedule, at)
		}
		if !schedule.Waiting(now.Add(-platformRetryClockMargin), at) {
			t.Fatal("margin boundary made retry early")
		}
		if schedule.Waiting(now.Add(-platformRetryClockMargin-time.Nanosecond), at) {
			t.Fatal("rollback beyond margin retained wait")
		}
		if schedule.Waiting(at, at) {
			t.Fatal("due retry still waiting")
		}
	}
	var schedule RetrySchedule
	at := schedule.Schedule(now, now.Add(2*MaxPlatformRetryDuration))
	if !at.Equal(now.Add(MaxPlatformRetryDuration)) || schedule.RetryDuration != MaxPlatformRetryDuration {
		t.Fatalf("global maximum not honored: %+v at=%v", schedule, at)
	}
}

func TestRetryScheduleLegacyCompatibility(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	var legacy struct {
		RetrySchedule
		At time.Time `json:"retryAfter"`
	}
	// Old JSON has no schedule fields; a genuine long reset is still honored.
	if err := json.Unmarshal([]byte(`{"retryAfter":"2026-10-20T12:00:00Z"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if !legacy.Waiting(now, legacy.At) {
		t.Fatal("legacy multi-day reset was shortened")
	}
	at := now.Add(MaxPlatformRetryDuration + platformRetryClockMargin)
	if !legacy.Waiting(now, at) || legacy.Waiting(now, at.Add(time.Nanosecond)) {
		t.Fatal("legacy clamp boundary incorrect")
	}
	// Provenance must match the deadline; a later direct overwrite is legacy,
	// not permission to shorten an otherwise legitimate long reset.
	var stale RetrySchedule
	_ = stale.Schedule(now, now.Add(time.Minute))
	if !stale.Waiting(now, now.Add(48*time.Hour)) {
		t.Fatal("stale provenance shortened reset")
	}
}

func TestRetryScheduleOldCPRoundTrip(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	type currentRetry struct {
		RetrySchedule
		At time.Time `json:"retryAfter"`
	}
	type oldRetry struct {
		At time.Time `json:"retryAfter"`
	}
	var current currentRetry
	current.At = current.Schedule(now, now.Add(48*time.Hour))
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var old oldRetry
	if err := json.Unmarshal(raw, &old); err != nil || !old.At.Equal(current.At) {
		t.Fatalf("old CP cannot read additive retry record: %+v err=%v", old, err)
	}
	raw, err = json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	current = currentRetry{}
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatal(err)
	}
	if !current.RetryRecordedAt.IsZero() || !current.Waiting(now, current.At) {
		t.Fatalf("old CP rewrite must retain deadline with legacy scheduling: %+v", current)
	}
}

func TestPlatformFailureDefinitive(t *testing.T) {
	for _, err := range []error{infra.ErrPlacementUnsatisfiable, infra.ErrDeploymentRetriesExhausted, &infra.ResourceUpdateRejectedError{Message: "node n: dimension cpu exhausted"}} {
		if !PlatformFailureDefinitive(fmt.Errorf("wrapped: %w", err)) {
			t.Fatalf("definitive failure classified retryable: %v", err)
		}
	}
	if PlatformFailureDefinitive(infra.ErrUnavailable) || PlatformFailureDefinitive(nil) {
		t.Fatal("transient or absent failure classified definitive")
	}
}
