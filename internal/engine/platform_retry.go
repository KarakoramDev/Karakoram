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
}

func (e *PlatformRetryWait) Error() string { return e.Message }

// PlatformRetryDelay backs off safe, re-issuable infrastructure actions from
// thirty seconds to ten minutes, without limiting the number of attempts.
func PlatformRetryDelay(failures int) time.Duration {
	delay := 30 * time.Second
	for n := 1; n < failures && delay < 10*time.Minute; n++ {
		delay = min(delay*2, 10*time.Minute)
	}
	return delay
}
