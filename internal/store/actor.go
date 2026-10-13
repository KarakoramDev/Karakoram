// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import "context"

type actorContextKey struct{}

// WithActor associates an initiating identity with desired-state writes and
// events. Explicit event and operation actors take precedence over this value.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

// Actor returns the initiating identity, or reconciler for automatic work.
// It is only a default for new writes, never for reading historical records.
func Actor(ctx context.Context) string {
	actor, _ := ctx.Value(actorContextKey{}).(string)
	if actor == "" {
		return "reconciler"
	}
	return actor
}
