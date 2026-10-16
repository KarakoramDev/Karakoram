// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.CapabilityRequirer = (*Provider)(nil)

// RequiredCapabilities reports optional backend features needed by a validated spec.
func (*Provider) RequiredCapabilities(spec domain.ClusterSpec) []string {
	options, _ := parseOptions(spec.EngineOptions)
	if options.ManagementUI.Public {
		return []string{"publicHttps"}
	}
	return nil
}
