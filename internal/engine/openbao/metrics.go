// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func (*Provider) Metrics() []domain.MetricDescriptor {
	names := []string{"up", "initialized", "sealed", "active", "raftCommittedIndex", "raftAppliedIndex", "raftFollowerLastHeartbeatSeconds", "autopilotHealthy", "autopilotFailureTolerance", "requests", "leases", "memoryBytes"}
	out := make([]domain.MetricDescriptor, len(names))
	for i, name := range names {
		unit := "count"
		switch i {
		case 6:
			unit = "seconds"
		case 9:
			unit = "requests/second"
		case 11:
			unit = "bytes"
		}
		out[i] = domain.MetricDescriptor{Exposition: baospec.MetricNames[i], Name: "openbao." + name, Unit: unit, Scale: 1}
	}
	return out
}

var _ engine.MetricsCatalog = (*Provider)(nil)
