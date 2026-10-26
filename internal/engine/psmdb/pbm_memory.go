// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// appliedPBMResources uses only applied state. Older imported members may lack
// persisted resource bounds; the backend's current service is their authoritative
// fallback, never a pending desired spec or a host-wide agent memory reading.
// Missing/unavailable observations remain unknown without failing native polling.
func appliedPBMResources(ctx context.Context, rc *engine.Context, s *internalState, m *member) domain.Resources {
	if m.Resources.MinRAM > 0 {
		return m.Resources
	}
	if rc.Backend == nil {
		return domain.Resources{}
	}
	// A legacy member with an unobserved resource/deploy process cannot use its
	// newly requested backend configuration as proof of applied allocation RAM.
	if s.Pending != nil && s.Pending.Member == m.Name && (s.Pending.Action == "resources" || s.Pending.Action == "deploy") {
		return domain.Resources{}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	service, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil || service == nil || service.Importing || service.Resources.MinRAM <= 0 {
		return domain.Resources{}
	}
	if m.ServiceID != "" && service.ID != m.ServiceID {
		return domain.Resources{}
	}
	return service.Resources
}
