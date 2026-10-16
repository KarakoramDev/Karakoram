// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.CapabilityRequirer = (*Provider)(nil)

// RequiredCapabilities reports optional backend features needed by a validated spec.
func (*Provider) RequiredCapabilities(spec domain.ClusterSpec) []string {
	if spec.Topology.Type == "sharded" && spec.Components.Router.Replicas > 1 {
		return []string{"horizontalScaling"}
	}
	return nil
}
