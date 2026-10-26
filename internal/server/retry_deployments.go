// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"net/http"
)

func (s *Server) RetryClusterDeployments(ctx context.Context, request api.RetryClusterDeploymentsRequestObject) (api.RetryClusterDeploymentsResponseObject, error) {
	return s.handle(ctx, "RetryClusterDeployments", input{Cluster: request.Cluster}), nil
}
func (r response) VisitRetryClusterDeploymentsResponse(w http.ResponseWriter) error {
	return r.write(w)
}
