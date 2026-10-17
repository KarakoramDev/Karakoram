// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"strings"
)

// Metrics maps the native runner's stable vocabulary into canonical series.
func (*Core) Metrics() []domain.MetricDescriptor {
	out := make([]domain.MetricDescriptor, 0, len(pgspec.MetricNames))
	for _, name := range pgspec.MetricNames {
		unit := "count"
		if strings.HasSuffix(name, "_total") {
			unit = "operations/second"
		} else if strings.HasSuffix(name, "_bytes") {
			unit = "bytes"
		} else if strings.HasSuffix(name, "_seconds") {
			unit = "seconds"
		}
		out = append(out, domain.MetricDescriptor{Exposition: name, Name: strings.TrimPrefix(name, "karakoram_"), Unit: unit, Scale: 1})
	}
	return out
}
