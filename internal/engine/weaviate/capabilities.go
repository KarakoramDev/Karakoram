// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (*Provider) RequiredCapabilities(spec domain.ClusterSpec) []string {
	return engine.RequiredCapabilities(spec)
}

var _ engine.CapabilityRequirer = (*Provider)(nil)
