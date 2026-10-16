// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"math"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// MemberResources returns the CPU, memory, and root disk a member container
// currently has. Values the member agent read from its cgroup and filesystem
// in a successful status call win; each value the agent did not report (an
// unreachable agent, or one that predates the reading) falls back to the
// service's effective vertical autoscaling: start CPU or minimum CPU, minimum
// RAM, and minimum root disk. Zerops container records are not used: they
// keep creation-time values after vertical scaling. Nil when neither source
// is known.
func MemberResources(agent *agentapi.ContainerResources, autoscaling *domain.Resources) *domain.ObservedResources {
	var r domain.ObservedResources
	if agent != nil {
		r = domain.ObservedResources{CPUCores: round2(agent.CPUCores), MemoryGB: gib(agent.MemoryBytes), DiskGB: gib(agent.RootDiskBytes)}
	}
	if autoscaling != nil {
		if r.CPUCores <= 0 {
			r.CPUCores = autoscaling.MinCPU
			if autoscaling.StartCPU != nil && *autoscaling.StartCPU > 0 {
				r.CPUCores = *autoscaling.StartCPU
			}
		}
		if r.MemoryGB <= 0 {
			r.MemoryGB = autoscaling.MinRAM
		}
		if r.DiskGB <= 0 {
			r.DiskGB = autoscaling.MinDisk
		}
	}
	if r == (domain.ObservedResources{}) {
		return nil
	}
	return &r
}

func gib(bytes uint64) float64 { return round2(float64(bytes) / (1 << 30)) }

func round2(v float64) float64 { return math.Round(v*100) / 100 }
