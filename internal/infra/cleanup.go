// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import "context"

type cleanupIntentKey struct{}

// CleanupIntent binds a cleanup pass to persisted member application identities.
// FullDelete excludes suspension and retained-volume cleanup. Backends must still
// prove native ownership and absence of competing work before escalating removal.
// A nil Services map marks shared cluster cleanup observation only: providers
// still verify ownership before mutations, and escalation requires exact IDs.
type CleanupIntent struct {
	FullDelete bool
	Services   map[string]string
}

func WithCleanupIntent(ctx context.Context, intent CleanupIntent) context.Context {
	return context.WithValue(ctx, cleanupIntentKey{}, intent)
}

func CleanupIntentFromContext(ctx context.Context) (CleanupIntent, bool) {
	intent, ok := ctx.Value(cleanupIntentKey{}).(CleanupIntent)
	return intent, ok
}
