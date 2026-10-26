// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import "context"

// RuntimeStopFailure reports a live runtime which has exceeded its platform's
// shutdown grace. It is diagnostic only, never proof that the runtime is fenced.
type RuntimeStopFailure struct {
	Member, Allocation, Node, Message string
}

// RuntimeStopObserver optionally exposes current, read-only shutdown diagnostics
// for exact engine-owned member identities, including during cluster deletion.
// An empty successful sample clears prior diagnostics; observation errors do not.
type RuntimeStopObserver interface {
	RuntimeStopFailures(context.Context, []string) ([]RuntimeStopFailure, error)
}
