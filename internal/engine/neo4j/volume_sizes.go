// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// VolumeSizes reports acknowledged bounds, not a pending desired resize.
func (*Provider) VolumeSizes(c *domain.Cluster) (domain.Components, error) {
	s, err := load(c)
	if err != nil {
		return domain.Components{}, err
	}
	var out domain.Components
	if s.Member != nil && s.Member.VolumeID != "" {
		engine.AddVolumeSize(&out, "engine", s.Member.Disk)
	}
	return out, nil
}
