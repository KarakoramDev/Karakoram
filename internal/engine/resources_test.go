// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMemberResourcesPrecedence(t *testing.T) {
	start := 2.0
	bounds := &domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 4, MinRAM: 0.5, MaxRAM: 2, MinDisk: 1, MaxDisk: 5}
	started := *bounds
	started.StartCPU = &start
	for _, c := range []struct {
		name        string
		agent       *agentapi.ContainerResources
		autoscaling *domain.Resources
		want        *domain.ObservedResources
	}{
		{"agent wins", &agentapi.ContainerResources{CPUCores: 2, MemoryBytes: 1 << 30, RootDiskBytes: 5 << 30}, bounds, &domain.ObservedResources{CPUCores: 2, MemoryGB: 1, DiskGB: 5}},
		{"agent without autoscaling", &agentapi.ContainerResources{CPUCores: 1.5, MemoryBytes: 3 << 28, RootDiskBytes: 1 << 30}, nil, &domain.ObservedResources{CPUCores: 1.5, MemoryGB: 0.75, DiskGB: 1}},
		{"rounded", &agentapi.ContainerResources{CPUCores: 1.0 / 3, MemoryBytes: 1<<30 - 4096, RootDiskBytes: 5<<30 - 1<<20}, nil, &domain.ObservedResources{CPUCores: 0.33, MemoryGB: 1, DiskGB: 5}},
		{"no agent reading uses minimum", nil, bounds, &domain.ObservedResources{CPUCores: 1, MemoryGB: 0.5, DiskGB: 1}},
		{"no agent reading uses start CPU", nil, &started, &domain.ObservedResources{CPUCores: 2, MemoryGB: 0.5, DiskGB: 1}},
		{"partial agent reading", &agentapi.ContainerResources{CPUCores: 3}, bounds, &domain.ObservedResources{CPUCores: 3, MemoryGB: 0.5, DiskGB: 1}},
		{"unknown", nil, nil, nil},
		{"empty autoscaling", nil, &domain.Resources{}, nil},
		{"empty agent reading", &agentapi.ContainerResources{}, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := MemberResources(c.agent, c.autoscaling)
			if (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestMemberResourcesFromAgentsWithoutReading(t *testing.T) {
	// Agents released before the resource reading omit the field entirely.
	var old agentapi.Status
	if err := json.Unmarshal([]byte(`{"processes":{"mongod":{"state":"running","pid":1,"restarts":0}},"engineVersion":"8.0.32-14","configVersion":3,"disk":{"totalBytes":1,"usedBytes":1,"freeBytes":0},"hostFingerprint":"","maintenance":false,"uptimeSeconds":9}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Resources != nil {
		t.Fatalf("old agent status decoded resources: %+v", old.Resources)
	}
	bounds := &domain.Resources{CPUMode: "SHARED", MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}
	if got := MemberResources(old.Resources, bounds); got == nil || *got != (domain.ObservedResources{CPUCores: 2, MemoryGB: 1, DiskGB: 5}) {
		t.Fatalf("old agent must fall back to autoscaling: %+v", got)
	}
	// Current agents add the reading, which older control planes ignore.
	data, err := json.Marshal(agentapi.Status{Resources: &agentapi.ContainerResources{CPUCores: 2, MemoryBytes: 1 << 30, RootDiskBytes: 5 << 30}})
	if err != nil {
		t.Fatal(err)
	}
	var current agentapi.Status
	if err := json.Unmarshal(data, &current); err != nil || current.Resources == nil || current.Resources.CPUCores != 2 {
		t.Fatalf("round trip: %s %+v %v", data, current.Resources, err)
	}
}
