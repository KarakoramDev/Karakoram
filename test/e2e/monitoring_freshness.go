// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

// freshMetricNames requires bucket starts beyond the entire baseline query.
// A bucket start cannot be later than the historical sample it contains, even
// when a subsequent request shifts the bucket boundaries.
func freshMetricNames(metrics api.ClusterMetrics, baselineEnd time.Time) map[string]map[string]bool {
	seen := map[string]map[string]bool{}
	for _, series := range metrics.Series {
		for _, point := range series.Points {
			if point.Timestamp.After(baselineEnd) {
				if seen[series.Member] == nil {
					seen[series.Member] = map[string]bool{}
				}
				seen[series.Member][series.Name] = true
				break
			}
		}
	}
	return seen
}
