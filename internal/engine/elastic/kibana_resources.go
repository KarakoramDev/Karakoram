// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func kibanaMinimumRAMMessage(spec domain.ClusterSpec) string {
	kibana := spec.Components.Extra[kindKibana]
	minimum := KibanaActiveMinimumRAM(spec.Version)
	if kibana.Replicas <= 0 || minimum <= 1 || kibana.Resources.MinRAM >= minimum {
		return ""
	}
	return fmt.Sprintf("Kibana %s minimum RAM %.3g GiB is below the supported %.3g-GiB minimum for 9.x; resize spec.components.extra.kibana.resources minRam and maxRam together to at least %.3g GiB. Existing resources are not changed automatically", spec.Version, kibana.Resources.MinRAM, minimum, minimum)
}
