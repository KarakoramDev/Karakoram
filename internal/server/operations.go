// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"net/http"
)

// CancelOperation implements the operation-specific authorization and cancellation handler.
func (s *Server) CancelOperation(ctx context.Context, request api.CancelOperationRequestObject) (api.CancelOperationResponseObject, error) {
	return s.handle(ctx, "CancelOperation", input{Operation: request.Operation}), nil
}
func (r response) VisitCancelOperationResponse(w http.ResponseWriter) error { return r.write(w) }
