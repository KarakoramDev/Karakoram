// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import "context"

type mcpTrafficKey struct{}

// WithMCPTraffic marks a trusted in-process REST dispatch. Network headers cannot
// set this marker. OAuth authentication may accept MCP-audience tokens only when
// IsMCPTraffic is true; verified client hosts may later refine the audit suffix.
func WithMCPTraffic(ctx context.Context) context.Context {
	return context.WithValue(ctx, mcpTrafficKey{}, true)
}

// IsMCPTraffic reports whether this is a trusted MCP-to-REST dispatch.
func IsMCPTraffic(ctx context.Context) bool {
	marked, _ := ctx.Value(mcpTrafficKey{}).(bool)
	return marked
}
