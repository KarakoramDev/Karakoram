// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// The production Trigger is the reconciler.Manager, wired in run.go. Keeping
// this extension optional preserves lightweight triggers used by other callers.
type forceReconciler interface{ ForceReconcile(domain.ClusterRef) }

func (s *Server) ReconcileCluster(ctx context.Context, request api.ReconcileClusterRequestObject) (api.ReconcileClusterResponseObject, error) {
	return s.handle(ctx, "ReconcileCluster", input{Cluster: request.Cluster}), nil
}

func (r response) VisitReconcileClusterResponse(w http.ResponseWriter) error { return r.write(w) }

func (s *Server) forceClusterReconcile(ctx context.Context, c *domain.Cluster) response {
	if c.Deleting || c.Status.Phase == domain.PhaseDeleting {
		return fail(409, "conflict", "Cannot request reconciliation of a deleting cluster")
	}
	force, ok := s.Trigger.(forceReconciler)
	if !ok {
		return fail(503, "unavailable", "Cluster reconciler is unavailable")
	}
	// Suspended clusters are accepted: the ordinary reconciler retains suspension;
	// only ResumeCluster is allowed to request runtime recreation.
	force.ForceReconcile(c.Ref())
	return response{status: http.StatusAccepted, body: s.clusterAPI(*c)}
}
