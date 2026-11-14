// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package tombstone

import "context"

// Writer makes a complete set of retired hostnames resolve locally to the sentinels.
type Writer interface {
	Kind() string
	Preflight(ctx context.Context) error
	// Install replaces the complete desired set; an empty set removes all managed entries.
	Install(ctx context.Context, hosts []string) error
}
