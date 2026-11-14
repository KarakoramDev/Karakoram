// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type observationKey struct {
	backend    *Backend
	kind, name string
	steady     bool
	epoch      uint64
}

func (b *Backend) readKey(kind, name string) observationKey {
	return observationKey{backend: b, kind: kind, name: name}
}

type observationCall[T any] struct {
	done  chan struct{}
	value T
	err   error
}

// ObservationPassSnapshots opts in to pre/post-Sync platform memoization.
// The shared target epoch changes at both boundaries of every HTTP mutation,
// including failures, and therefore also invalidates sibling client snapshots.
func (*Backend) ObservationPassSnapshots() {}

// Descriptive reads may reuse a cross-Sync snapshot. Authorization only uses
// fresh facts from its current status attempt; task rows remain unconditionally
// fresh. Mutation callers strip both caches and reload authoritative state.
func observationValue[T any](ctx context.Context, key observationKey, read func() (T, error)) (T, error) {
	cache := infra.ObservationCache(ctx)
	if cache == nil {
		return read()
	}
	if key.backend != nil {
		if key.backend.c.snapshot == nil {
			return read()
		}
		key.epoch, _, _ = key.backend.c.snapshot.ConfigGeneration()
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
	shared := infra.PassObservationCache(ctx)
	if shared != nil {
		if _, fresh := cache.Load(key); !fresh {
			if _, inherited := shared.Load(key); inherited {
				return observationMemoValue(ctx, shared, nil, key, read)
			}
		}
	}
	return observationMemoValue(ctx, cache, shared, key, read)
}

func observationMemoValue[T any](ctx context.Context, cache, shared *sync.Map, key observationKey, read func() (T, error)) (T, error) {
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
		if shared != nil {
			shared.Store(key, call)
		}
		call.value, call.err = read()
		if call.err != nil {
			cache.CompareAndDelete(key, call)
			if shared != nil {
				shared.CompareAndDelete(key, call)
			}
		}
		close(call.done)
		return call.value, call.err
	}
}
