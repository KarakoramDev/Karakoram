// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"net"
	"sort"
	"strconv"
)

func (*Provider) RelayTargets(c *domain.Cluster) []string { return RelayTargets(c) }
func RelayTargets(c *domain.Cluster) []string {
	if c == nil {
		return nil
	}
	hosts := map[string]bool{}
	var s internalState
	_ = json.Unmarshal(c.Internal, &s)
	for _, m := range s.Members {
		if m != nil && m.Name != "" {
			hosts[m.Name] = true
		}
	}
	for _, m := range c.Status.Members {
		if m.Name != "" {
			hosts[m.Name] = true
		}
	}
	if len(s.Members) == 0 {
		for i := range c.Spec.Components.Engine.Replicas {
			hosts[fmt.Sprintf("%sm%d", c.Name, i)] = true
		}
	}
	out := []string{}
	for host := range hosts {
		for _, port := range []int{9443, 5672, 5671, 15672, 15671} {
			out = append(out, net.JoinHostPort(host, strconv.Itoa(port)))
		}
	}
	sort.Strings(out)
	return out
}

var _ engine.RelayTargeter = (*Provider)(nil)
