// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"sync"
)

type observationPassKey struct{}

// WithObservationPass starts a new, bounded-lifetime observation cache. Call it
// once at the reconcile boundary, not for each status attempt or retry. Derived
// contexts share the cache; a new pass never inherits an earlier pass's samples.
func WithObservationPass(ctx context.Context) context.Context {
	return context.WithValue(ctx, observationPassKey{}, new(sync.Map))
}

// ObservationPassCache returns nil outside a reconcile pass. Providers must use
// private key types including cluster identity, and serialize sampling and cache
// replacement with their observation/commit lock. Cached values are immutable.
func ObservationPassCache(ctx context.Context) *sync.Map {
	cache, _ := ctx.Value(observationPassKey{}).(*sync.Map)
	return cache
}
