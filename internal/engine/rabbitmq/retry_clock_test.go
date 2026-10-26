// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type retryStepClock struct{ at time.Time }

func (c *retryStepClock) Now() time.Time { return c.at }

type retryStepBackend struct {
	infra.Backend
	calls int
}

func (b *retryStepBackend) SetResources(context.Context, string, infra.Resources) (infra.Op, error) {
	b.calls++
	return infra.Op{}, infra.ErrUnavailable
}

func TestPlatformRetryClockSteps(t *testing.T) {
	for _, delay := range []time.Duration{time.Minute, 10 * time.Minute, 48 * time.Hour} {
		for _, step := range []struct {
			name  string
			shift time.Duration
			due   bool
		}{
			{"unchanged", 0, false},
			{"small-backward", -2 * time.Second, false},
			{"large-backward", -time.Hour, true},
			{"forward", delay + time.Second, true},
		} {
			t.Run(delay.String()+"/"+step.name, func(t *testing.T) {
				p, rc, _, _ := setup(t)
				ready(t, p, rc)
				s := state(t, rc)
				clock := &retryStepClock{at: rc.Clock.Now()}
				rc.Clock = clock
				target := &pending{Action: "resources", Member: s.Members[0].Name, Resources: s.Members[0].Resources}
				s.Pending = target
				at := clock.at.Add(delay)
				cause := &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: at}
				_ = s.failPendingCause(rc, cause)
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				var err error
				s, err = loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				// Reconstruct the provider and state as on CP restart.
				p = &Provider{}
				backend := &retryStepBackend{Backend: rc.Backend}
				rc.Backend = backend
				clock.at = clock.at.Add(step.shift)
				_, err = p.retryPending(t.Context(), rc, s)
				var wait *engine.PlatformRetryWait
				if step.due {
					if backend.calls != 1 || errors.As(err, &wait) {
						t.Fatalf("retry did not reach backend: calls=%d err=%v", backend.calls, err)
					}
				} else if backend.calls != 0 || !errors.As(err, &wait) || !wait.RetryAt.Equal(at) {
					t.Fatalf("legitimate retry changed: calls=%d err=%v", backend.calls, err)
				}
			})
		}
	}
}

func TestPlatformRetryLegacyCompatibility(t *testing.T) {
	for _, delay := range []time.Duration{time.Minute, 48 * time.Hour, 31 * 24 * time.Hour} {
		t.Run(delay.String(), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			target := &pending{Action: "resources", Member: s.Members[0].Name, Resources: s.Members[0].Resources}
			s.Pending, s.Retry = nil, target
			at := rc.Clock.Now().Add(delay)
			s.PlatformRetries = map[string]*platformRetry{platformRetryKey(target): {RetryAfter: at}}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			var err error
			s, err = loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			backend := &retryStepBackend{Backend: rc.Backend}
			rc.Backend = backend
			_, err = p.retryPending(t.Context(), rc, s)
			var wait *engine.PlatformRetryWait
			if delay <= engine.MaxPlatformRetryDuration {
				if backend.calls != 0 || !errors.As(err, &wait) || !wait.RetryAt.Equal(at) {
					t.Fatalf("legacy wait changed: calls=%d err=%v", backend.calls, err)
				}
			} else if backend.calls != 1 || errors.As(err, &wait) {
				t.Fatalf("legacy impossible deadline stalled: calls=%d err=%v", backend.calls, err)
			}
		})
	}
}
