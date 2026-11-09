// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type observationKey struct {
	backend    *Backend
	kind, name string
	steady     bool
}

func (b *Backend) readKey(kind, name string) observationKey {
	return observationKey{backend: b, kind: kind, name: name}
}

type observationCall[T any] struct {
	done  chan struct{}
	value T
	err   error
}

// Sweep discovery snapshots are immutable for the pass. Task enumeration for
// pinned-transport authorization bypasses this cache via WithFreshTaskInventory.
// Mutation callers use WithoutObservation and always reload authoritative state.
func observationValue[T any](ctx context.Context, key observationKey, read func() (T, error)) (T, error) {
	cache := infra.ObservationCache(ctx)
	if cache == nil {
		return read()
	}
	if infra.SteadyObservation(ctx) {
		// Fresh discovery may serve either class, but snapshot-eligible facts
		// must never satisfy pinned dial authorization. Preserve ordinary
		// fresh-pass sharing without letting the first memo writer decide
		// whether an authorization reaches the platform.
		if _, fresh := cache.Load(key); !fresh {
			key.steady = true
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		actual, loaded := cache.Load(key)
		if !loaded {
			actual, loaded = cache.LoadOrStore(key, &observationCall[T]{done: make(chan struct{})})
		}
		call := actual.(*observationCall[T])
		if loaded {
			select {
			case <-call.done:
				if ctx.Err() == nil && (errors.Is(call.err, context.Canceled) || errors.Is(call.err, context.DeadlineExceeded)) {
					// The initiating caller owns its cancellation, not the
					// pass. A live waiter can lead a new bounded attempt.
					cache.CompareAndDelete(key, call)
					continue
				}
				return call.value, call.err
			case <-ctx.Done():
				var zero T
				return zero, ctx.Err()
			}
		}
		call.value, call.err = read()
		if call.err != nil {
			cache.CompareAndDelete(key, call)
		}
		close(call.done)
		return call.value, call.err
	}
}
