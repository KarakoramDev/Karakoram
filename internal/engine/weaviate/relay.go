// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"sort"
)

func (*Provider) RelayTargets(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(s.Members)*3)
	for _, m := range s.Members {
		if !ValidHostname(m.Name) {
			continue
		}
		for _, port := range []string{"9443", "8080", "7947"} {
			out = append(out, m.Name+":"+port)
		}
	}
	sort.Strings(out)
	return out
}
