// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

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
	if s.Member != nil && s.Member.VolumeID != "" {
		out = append(out, engine.VolumeReference{Component: "engine", Hostname: s.Member.Volume, ID: s.Member.VolumeID})
	}
	return out, nil
}
