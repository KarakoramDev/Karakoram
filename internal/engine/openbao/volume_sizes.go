// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// VolumeSizes reports acknowledged bounds, not a pending desired resize.
func (*Provider) VolumeSizes(c *domain.Cluster) (domain.Components, error) {
	s, err := loadState(c)
	if err != nil {
		return domain.Components{}, err
	}
	var out domain.Components
	for _, m := range s.Members {
		if m.VolumeID != "" {
			engine.AddVolumeSize(&out, "engine", m.Disk)
		}
	}
	return out, nil
}
