// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package client provides the generated Karakoram management API client.
// Use an /api/v1 base URL and WithBearerToken for API token authentication.
// WithResponse methods decode HTTP responses; callers must check their status
// separately from transport errors. Accepted asynchronous work must be polled
// through GetOperation until it reaches a terminal phase.
package client

import (
	"context"
	"net/http"
)

// WithBearerToken adds API token authentication to every request. Pass the token
// value without the "Bearer " prefix. Load it from a secret manager and never log it.
func WithBearerToken(token string) ClientOption {
	authorization := "Bearer " + token
	return WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", authorization)
		return nil
	})
}
