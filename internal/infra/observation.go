// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"errors"
	"sync"
)

// ObservationConcurrency limits both traversal workers and platform HTTP reads.
const ObservationConcurrency = 8

// ObserveParallel joins every completed result in input order, followed by
// cancellation. RetryAt can therefore retain the latest observed retry deadline.
// Callbacks own distinct result slots and must honor ctx. No mutation is implied.
func ObserveParallel(ctx context.Context, count int, observe func(int) error) error {
	if count == 0 {
		return ctx.Err()
	}
	failures := make([]error, count+1)
	var workers sync.WaitGroup
	var queue sync.Mutex
	next := 0
	for range min(ObservationConcurrency, count) {
		workers.Go(func() {
			for ctx.Err() == nil {
				queue.Lock()
				i := next
				next++
				queue.Unlock()
				if i >= count {
					return
				}
				failures[i] = observe(i)
			}
		})
	}
	workers.Wait()
	failures[count] = ctx.Err()
	return errors.Join(failures...)
}

type observationContextKey struct{}
type observationCacheKey struct{}
type freshTaskInventoryKey struct{}

// WithFreshTaskInventory requires task enumeration to bypass sweep snapshots.
// Authorization must apply it separately for every pinned transport connection.
func WithFreshTaskInventory(ctx context.Context) context.Context {
	return context.WithValue(ctx, freshTaskInventoryKey{}, true)
}

func FreshTaskInventory(ctx context.Context) bool {
	fresh, _ := ctx.Value(freshTaskInventoryKey{}).(bool)
	return fresh
}

// WithObservationCache attaches a single-pass cache for immutable platform
// facts. Callers must discard its entries after mutations or identity changes.
// Live task authorization must still fetch current tasks on every connection.
func WithObservationCache(ctx context.Context, cache *sync.Map) context.Context {
	return context.WithValue(ctx, observationCacheKey{}, cache)
}

// ObservationCache is deliberately unavailable to lifecycle/mutation reads.
func ObservationCache(ctx context.Context) *sync.Map {
	if !IsObservation(ctx) {
		return nil
	}
	cache, _ := ctx.Value(observationCacheKey{}).(*sync.Map)
	return cache
}

// ObservationContext marks read-only inventory HTTP requests for admission.
func ObservationContext(ctx context.Context) context.Context {
	if IsObservation(ctx) {
		return ctx
	}
	return context.WithValue(ctx, observationContextKey{}, true)
}

// WithoutObservation lets serialized lifecycle work bypass the observation queue.
func WithoutObservation(ctx context.Context) context.Context {
	if cache, _ := ctx.Value(observationCacheKey{}).(*sync.Map); cache != nil {
		ctx = WithObservationCache(ctx, nil)
	}
	if !IsObservation(ctx) {
		return ctx
	}
	return context.WithValue(ctx, observationContextKey{}, false)
}

// IsObservation reports whether HTTP reads belong to an observation sweep.
func IsObservation(ctx context.Context) bool {
	marked, _ := ctx.Value(observationContextKey{}).(bool)
	return marked
}

// ReadBudget is shared by all clients for one backend/platform origin. Only
// observation GETs enter it; lifecycle reads cannot be starved by sweep workers.
type ReadBudget struct{ slots chan struct{} }

var observationBudgets struct {
	sync.Mutex
	values map[string]*ReadBudget
}

func ObservationBudget(scope string) *ReadBudget {
	observationBudgets.Lock()
	defer observationBudgets.Unlock()
	if observationBudgets.values == nil {
		observationBudgets.values = map[string]*ReadBudget{}
	}
	if existing := observationBudgets.values[scope]; existing != nil {
		return existing
	}
	budget := &ReadBudget{slots: make(chan struct{}, ObservationConcurrency)}
	observationBudgets.values[scope] = budget
	return budget
}

func (b *ReadBudget) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case b.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *ReadBudget) Release() { <-b.slots }
