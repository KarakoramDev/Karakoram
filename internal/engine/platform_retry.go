// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import "time"

// PlatformRetryWait reports an already scheduled retry, not a new failed
// attempt. Reconcilers retain its status message without recording another
// error event, and wake at RetryAt instead of applying error backoff.
type PlatformRetryWait struct {
	RetryAt time.Time
	Message string
	// Cause preserves backend retry constraints through wait conversion.
	Cause error
}

func (e *PlatformRetryWait) Error() string { return e.Message }
func (e *PlatformRetryWait) Unwrap() error { return e.Cause }

// PlatformRetryDelay backs off safe, re-issuable infrastructure actions from
// thirty seconds to ten minutes, without limiting the number of attempts.
func PlatformRetryDelay(failures int) time.Duration {
	delay := 30 * time.Second
	for n := 1; n < failures && delay < 10*time.Minute; n++ {
		delay = min(delay*2, 10*time.Minute)
	}
	return delay
}

// MaxPlatformRetryDuration is the longest durable engine retry schedule honored,
// including external Retry-After resets. It is deliberately independent of the
// ten-minute exponential backoff ceiling.
const MaxPlatformRetryDuration = 30 * 24 * time.Hour

const platformRetryClockMargin = 5 * time.Second

// RetrySchedule preserves the duration actually honored when a wall-clock
// deadline was recorded. The fields are additive so old checkpoints still load.
type RetrySchedule struct {
	RetryRecordedAt time.Time     `json:"retryRecordedAt,omitempty"`
	RetryDuration   time.Duration `json:"retryDuration,omitempty"`
}

// Schedule records a deadline and its provenance using one clock observation.
func (r *RetrySchedule) Schedule(now, at time.Time) time.Time {
	r.RetryDuration = max(0, min(at.Sub(now), MaxPlatformRetryDuration))
	r.RetryRecordedAt = now
	return now.Add(r.RetryDuration)
}

// Waiting rejects impossible future deadlines after clock rollback. A legacy
// deadline (or one overwritten without matching provenance) retains its ordinary
// behavior up to the global maximum. Forward clock steps may make retries early.
// This is cadence only: callers must still apply every backend/process fence.
func (r RetrySchedule) Waiting(now, at time.Time) bool {
	limit := MaxPlatformRetryDuration
	if !r.RetryRecordedAt.IsZero() && r.RetryDuration >= 0 &&
		r.RetryDuration <= limit && r.RetryRecordedAt.Add(r.RetryDuration).Equal(at) {
		limit = r.RetryDuration
	}
	remaining := at.Sub(now)
	return remaining > 0 && remaining <= limit+platformRetryClockMargin
}
