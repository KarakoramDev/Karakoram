// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ListOrphanedVolumes implements the generated strict API operation.
func (s *Server) ListOrphanedVolumes(ctx context.Context, request api.ListOrphanedVolumesRequestObject) (api.ListOrphanedVolumesResponseObject, error) {
	return s.handle(ctx, "ListOrphanedVolumes", input{}), nil
}

func (r response) VisitListOrphanedVolumesResponse(w http.ResponseWriter) error { return r.write(w) }

func (s *Server) orphanedVolumes(ctx context.Context) response {
	items, err := s.Store.ListOrphanedVolumes(ctx)
	if err != nil {
		return s.failure(err)
	}
	out := api.OrphanedVolumeList{Items: make([]api.OrphanedVolume, 0, len(items))}
	for _, volume := range items {
		// The originating cluster may already be deleted; its name remains the
		// authorization object, just as for retained backups.
		if s.allowed(ctx, "clusters", "read", volume.Cluster) {
			out.Items = append(out.Items, orphanedVolumeAPI(volume))
		}
	}
	return ok(out)
}

func orphanedVolumeAPI(v domain.OrphanedVolume) api.OrphanedVolume {
	return api.OrphanedVolume{
		Hostname:      v.Hostname,
		ServiceId:     v.ServiceID,
		Cluster:       v.Cluster,
		Member:        v.Member,
		FirstError:    v.FirstError,
		LastError:     v.LastError,
		FirstFailedAt: v.FirstFailedAt,
		OrphanedAt:    v.OrphanedAt,
		LastAttemptAt: v.LastAttemptAt,
		NextAttemptAt: v.NextAttemptAt,
		Attempts:      v.Attempts,
		LastProcessId: v.LastProcessID,
	}
}
