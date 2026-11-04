// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestRetainedApplyRetriesWithoutControlPlane(t *testing.T) {
	a, c, e := fakeNode(t, buildFake(t))
	c.ConfigVersion = 7
	var dnsRecovered atomic.Bool
	applied := make(chan struct{}, 1)
	e.onApply = func(_ *Node, got agentapi.MemberConfig) error {
		if !dnsRecovered.Load() {
			return &RetryableApplyError{Err: errors.New("private DNS unavailable")}
		}
		if got.ConfigVersion != c.ConfigVersion {
			t.Errorf("wrong retained version %d", got.ConfigVersion)
		}
		select {
		case applied <- struct{}{}:
		default:
		}
		return nil
	}
	// There is deliberately no CP client: a retry cannot fetch another config.
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("initial DNS failure lost")
	}
	if a.config().ConfigVersion == c.ConfigVersion {
		t.Fatal("failed apply acknowledged")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); a.retryApplies(ctx) }()
	dnsRecovered.Store(true)
	select {
	case <-applied:
	case <-time.After(4 * time.Second):
		t.Fatal("retained config did not recover independently of CP")
	}
	deadline := time.Now().Add(3 * time.Second)
	for a.config().ConfigVersion != c.ConfigVersion || workerState(a).PID == 0 {
		if time.Now().After(deadline) {
			t.Fatal("retained apply did not start and acknowledge worker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry cancellation stalled")
	}
	if a.config().ConfigVersion != c.ConfigVersion || workerState(a).PID == 0 {
		t.Fatal("retained apply did not start and acknowledge worker")
	}
}

func TestRetainedRetryCannotSupersedeNewerConfiguration(t *testing.T) {
	for _, newerFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer-success", true: "newer-invalid"}[newerFails], func(t *testing.T) {
			a, old, e := fakeNode(t, buildFake(t))
			e.onApply = func(*Node, agentapi.MemberConfig) error {
				return &RetryableApplyError{Err: errors.New("DNS unavailable")}
			}
			if _, err := a.apply(t.Context(), old); err == nil {
				t.Fatal("initial failure lost")
			}
			a.mu.RLock()
			retained := *a.applyRetry
			a.mu.RUnlock()
			next := old
			next.ConfigVersion++
			e.onApply = func(*Node, agentapi.MemberConfig) error {
				if newerFails {
					return errors.New("invalid newer config")
				}
				return nil
			}
			_, _ = a.apply(t.Context(), next)
			if _, err := a.apply(t.Context(), retained); err == nil {
				t.Fatal("stale retry accepted")
			}
			a.mu.RLock()
			pending := a.applyRetry
			a.mu.RUnlock()
			if pending != nil {
				t.Fatal("newer accepted attempt did not retire retry")
			}
		})
	}
}

func TestRetainedApplyBackoffCancellation(t *testing.T) {
	a, c, e := fakeNode(t, buildFake(t))
	e.onApply = func(*Node, agentapi.MemberConfig) error {
		return &RetryableApplyError{Err: errors.New("DNS unavailable")}
	}
	_, _ = a.apply(t.Context(), c)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); a.retryApplies(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("backoff ignored cancellation")
	}
}
