// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.CapabilityRequirer = (*Provider)(nil)

// RequiredCapabilities reports optional backend features needed by a validated spec.
func (*Provider) RequiredCapabilities(spec domain.ClusterSpec) []string {
	var required []string
	for _, def := range stackComponents {
		if spec.Components.Extra[def.Name].Replicas > 1 {
			required = append(required, "horizontalScaling")
			break
		}
	}
	options, _ := parseStoredOptions(spec.EngineOptions)
	if options.Kibana.Public || options.APM.Public {
		required = append(required, "publicHttps")
	}
	return required
}
