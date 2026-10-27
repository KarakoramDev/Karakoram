// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
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
	for _, m := range s.Members {
		if m.VolumeID != "" {
			name := "engine"
			if m.Role == agentapi.RoleConfigServer {
				name = "configServer"
			}
			engine.AddVolumeSize(&out, name, m.Disk)
		}
	}
	return out, nil
}
