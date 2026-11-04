// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// VolumeReferences exposes persistent identities for backend sizing observation.
func (*Provider) VolumeReferences(c *domain.Cluster) ([]engine.VolumeReference, error) {
	s, err := load(c)
	if err != nil {
		return nil, err
	}
	var out []engine.VolumeReference
	for _, m := range s.Members {
		if m.VolumeID != "" {
			name := m.Kind
			if m.Kind == kindNode {
				name = "engine"
			}
			out = append(out, engine.VolumeReference{Component: name, Hostname: m.Volume, ID: m.VolumeID})
		}
	}
	return out, nil
}
