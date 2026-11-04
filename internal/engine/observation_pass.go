// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"sync"
	"time"
)

type observationPassKey struct{}

type observationPass struct {
	cache   sync.Map
	mu      sync.Mutex
	started time.Time
}

// WithObservationPass starts a new, bounded-lifetime observation cache. Call it
// once at the reconcile boundary, not for each status attempt or retry. Derived
// contexts share the cache; a new pass never inherits an earlier pass's samples.
func WithObservationPass(ctx context.Context) context.Context {
	return context.WithValue(ctx, observationPassKey{}, new(observationPass))
}

// ObservationPassCache returns nil outside a reconcile pass. Providers must use
// private key types including cluster identity, and serialize sampling and cache
// replacement with their observation/commit lock. Cached values are immutable.
func ObservationPassCache(ctx context.Context) *sync.Map {
	pass, _ := ctx.Value(observationPassKey{}).(*observationPass)
	if pass == nil {
		return nil
	}
	return &pass.cache
}

// ObservationPassStart opts a provider into same-pass sample reuse. The caller
// supplies its clock domain; unrelated providers retain ordinary sweep timing.
func ObservationPassStart(ctx context.Context, at time.Time) time.Time {
	pass, _ := ctx.Value(observationPassKey{}).(*observationPass)
	if pass == nil {
		return at
	}
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.started.IsZero() {
		pass.started = at
	}
	return pass.started
}

// ObservationPassStartedAt reports the conservative start of an opted-in pass.
// It never creates or renews a sample timestamp.
func ObservationPassStartedAt(ctx context.Context) (time.Time, bool) {
	pass, _ := ctx.Value(observationPassKey{}).(*observationPass)
	if pass == nil {
		return time.Time{}, false
	}
	pass.mu.Lock()
	defer pass.mu.Unlock()
	return pass.started, !pass.started.IsZero()
}
