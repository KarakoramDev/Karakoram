// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveParallelPreservesErrorsRetryConstraintsAndCancellation(t *testing.T) {
	for _, cancelSweep := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		ordinary := errors.New("ordinary observation failure")
		early := time.Now().Add(time.Hour)
		latest := early.Add(time.Hour)
		started := make(chan struct{})
		rateDone := make(chan struct{})
		var calls atomic.Int32
		err := ObserveParallel(ctx, 3, func(i int) error {
			if calls.Add(1) == 3 {
				close(started)
			}
			<-started
			switch i {
			case 0:
				<-rateDone
				return ordinary
			case 1:
				<-rateDone
				return &RetryAfterError{Cause: ErrUnavailable, At: early}
			default:
				close(rateDone)
				if cancelSweep {
					cancel()
				}
				return &RetryAfterError{Cause: ErrUnavailable, At: latest}
			}
		})
		if !errors.Is(err, ordinary) || !RetryAt(err).Equal(latest) || errors.Is(err, context.Canceled) != cancelSweep {
			t.Fatal("observation reduction lost a completed result", err, RetryAt(err), cancelSweep)
		}
		cancel()
	}
}
