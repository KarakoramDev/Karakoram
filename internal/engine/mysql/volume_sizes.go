// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// VolumeSizes reports acknowledged bounds, not a pending desired resize.
func (*Core) VolumeSizes(c *domain.Cluster) (domain.Components, error) {
	s, err := load(c)
	if err != nil {
		return domain.Components{}, err
	}
	var out domain.Components
	for _, m := range s.Members {
		if m.VolumeID != "" {
			engine.AddVolumeSize(&out, "engine", m.Resources)
		}
	}
	return out, nil
}
