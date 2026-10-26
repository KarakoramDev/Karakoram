// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestBackendRateLimitBlocksTriggeredRetries(t *testing.T) {
	testBackendDeadlineBlocksTriggers(t, func(at time.Time) error {
		return fmt.Errorf("observe: %w", errors.Join(errors.New("observation failed"), &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: at}))
	})
}

func TestConvertedDeploymentWaitBlocksTriggeredRetries(t *testing.T) {
	testBackendDeadlineBlocksTriggers(t, func(at time.Time) error {
		cause := &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: "service/process/RUNNING", Detail: "occupied", RetryAt: at}
		return errors.Join(&engine.PlatformRetryWait{RetryAt: at, Message: cause.Error(), Cause: cause}, &engine.PlatformRetryWait{RetryAt: at.Add(-75 * time.Millisecond), Message: "sibling waiting"})
	})
}

func testBackendDeadlineBlocksTriggers(t *testing.T, cause func(time.Time) error) {
	t.Helper()
	var calls atomic.Int32
	first := make(chan time.Time, 1)
	second := make(chan time.Time, 1)
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		now := time.Now()
		if calls.Add(1) == 1 {
			first <- now
			return engine.Result{}, cause(now.Add(150 * time.Millisecond))
		}
		select {
		case second <- now:
		default:
		}
		return engine.Result{RequeueAfter: time.Hour}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseUpdating}, nil
	}
	m, s := newTestManager(t, p)
	m.opts.BackoffMin = time.Millisecond
	m.opts.BackoffMax = 2 * time.Millisecond
	createOperationCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	var started time.Time
	select {
	case started = <-first:
	case <-time.After(time.Second):
		t.Fatal("missing first attempt")
	}
	for range 10 {
		m.Trigger(domain.ClusterRef{Name: "demo"})
	}
	select {
	case at := <-second:
		if at.Sub(started) < 140*time.Millisecond {
			t.Fatalf("retry before backend reset: %v", at.Sub(started))
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not resume")
	}
}
