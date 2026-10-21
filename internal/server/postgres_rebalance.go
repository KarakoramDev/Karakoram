// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
)

func (s *Server) RebalancePostgres(ctx context.Context, request api.RebalancePostgresRequestObject) (api.RebalancePostgresResponseObject, error) {
	return s.handle(ctx, "RebalancePostgres", input{Cluster: request.Cluster, Body: request.Body}), nil
}

func (r response) VisitRebalancePostgresResponse(w http.ResponseWriter) error { return r.write(w) }
