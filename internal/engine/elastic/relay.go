// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// RelayTargets lists observed and planned member destinations.
func (*Provider) RelayTargets(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, m := range s.Members {
		ports := []int{9443}
		switch m.Kind {
		case kindNode, kindMaster:
			ports = append(ports, 9200)
		case kindKibana:
			ports = append(ports, 5601)
		case kindLogstash:
			ports = append(ports, 9600, 5044, 8080)
		case kindAPM:
			ports = append(ports, 8200)
		}
		for _, port := range ports {
			out = append(out, m.Name+":"+strconv.Itoa(port))
		}
	}
	sort.Strings(out)
	return out
}
