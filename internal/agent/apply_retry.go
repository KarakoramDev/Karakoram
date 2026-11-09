// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// RetryableApplyError marks a transient runtime prerequisite. The agent retries
// the accepted configuration locally, without depending on another CP fetch.
// Engines must not mark invalid configuration or unsafe recovery as retryable.
type RetryableApplyError struct {
	Err error
	// MaxDelay optionally bounds local retries for a short startup health budget.
	// Zero retains the ordinary 30-second maximum.
	MaxDelay time.Duration
}

func (e *RetryableApplyError) Error() string { return e.Err.Error() }
func (e *RetryableApplyError) Unwrap() error { return e.Err }

// Called under applyMu. A newer accepted attempt always supersedes a pending
// retry, including when the newer attempt fails for a non-retryable reason.
func (a *nodeAgent) retainApplyRetry(c agentapi.MemberConfig, err error) {
	if c.ConfigVersion < a.acceptedVersion {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var retry *RetryableApplyError
	if errors.As(err, &retry) {
		copy := c
		a.applyRetry = &copy
	} else {
		a.applyRetry = nil
	}
}

func (a *nodeAgent) retryApplies(ctx context.Context) {
	delay := time.Second
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		a.mu.RLock()
		pending := a.applyRetry
		a.mu.RUnlock()
		if pending == nil {
			delay = time.Second
			timer.Reset(delay)
			continue
		}
		if _, err := a.apply(ctx, *pending); err != nil {
			if ctx.Err() != nil {
				return
			}
			if a.o.Logger != nil {
				a.o.Logger.Warn("retained configuration apply failed", "error", err)
			}
			limit := 30 * time.Second
			var retry *RetryableApplyError
			if errors.As(err, &retry) && retry.MaxDelay > 0 {
				limit = retry.MaxDelay
			}
			delay = min(delay*2, limit)
		} else {
			delay = time.Second
		}
		timer.Reset(delay)
	}
}
