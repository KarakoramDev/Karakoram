// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
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

func TestProxyAdminApplyRetriesBeforeRefresh(t *testing.T) {
	a, c, e := fakeNode(t, buildFake(t))
	c.ConfigVersion = 33
	var attempts atomic.Int32
	applied := make(chan struct{}, 1)
	e.onApply = func(_ *Node, got agentapi.MemberConfig) error {
		if attempts.Add(1) == 1 {
			return &RetryableApplyError{Err: syscall.ECONNREFUSED, MaxDelay: 5 * time.Second}
		}
		if got.ConfigVersion != 33 {
			t.Errorf("retry changed desired revision: %d", got.ConfigVersion)
		}
		applied <- struct{}{}
		return nil
	}
	if _, err := a.apply(t.Context(), c); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatal("initial admin refusal not reproduced", err)
	}
	if a.config().ConfigVersion != 0 {
		t.Fatal("unapplied config acknowledged")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); a.retryApplies(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case <-applied:
	case <-time.After(5 * time.Second):
		t.Fatal("admin-up recovery waited for the 60-second CP refresh")
	}
}

func TestProxyAdminApplyRetryDelayBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, c, e := fakeNode(t, "unused")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var times []time.Time
		e.onApply = func(*Node, agentapi.MemberConfig) error {
			times = append(times, time.Now())
			if len(times) == 7 {
				cancel()
			}
			return &RetryableApplyError{Err: syscall.ECONNREFUSED, MaxDelay: 5 * time.Second}
		}
		_, _ = a.apply(ctx, c)
		go a.retryApplies(ctx)
		synctest.Wait()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if len(times) != 7 {
			t.Fatalf("short startup retries lost: %d attempts", len(times))
		}
		for i := 1; i < len(times); i++ {
			if delay := times[i].Sub(times[i-1]); delay > 5*time.Second {
				t.Fatalf("startup retry entered long backoff: %s", delay)
			}
		}
	})
}
