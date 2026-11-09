// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import "context"

// Heartbeat pushes observations over the same pinned, authenticated channel as Config.
func (c *Client) Heartbeat(ctx context.Context, request HeartbeatRequest) (HeartbeatResponse, error) {
	var response HeartbeatResponse
	err := c.request(ctx, "POST", HeartbeatPath, request, &response)
	return response, err
}
