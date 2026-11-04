// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Volumes carry disk bounds only. Placement must use their runtime's task
// bounds from the same import, without changing persisted volume resources.
func volumeTaskResources(spec infra.ServiceSpec, specs []infra.ServiceSpec) infra.Resources {
	for _, runtime := range specs {
		if runtime.Kind == infra.KindRuntime && runtime.Volume == spec.Hostname {
			return runtime.Resources
		}
	}
	return spec.Resources
}

type capacityResources struct {
	Cpu struct {
		CpuShares          int
		ReservableCpuCores []uint16
		ReservedCores      []uint16
	}
	Memory struct{ MemoryMB int }
}

type volumePlacement struct {
	Node               string
	CPU, Memory, Cores int
	Group              map[string]any
}

func (b *Backend) chooseVolumeNode(ctx context.Context, records []*record, spec infra.ServiceSpec, resources infra.Resources, fixedNode string, pending *[]volumePlacement) (string, error) {
	nodes, err := b.nodes(ctx)
	if err != nil {
		return "", err
	}
	// Preserve the established unavailable/exclusion classification before
	// querying capacity, then retain the same spreading and deterministic ties.
	first, err := chooseNode(nodes, records, spec.SpreadGroup, spec.SpreadReplaces, spec.SpreadAvoid...)
	if err != nil {
		return "", err
	}
	groupCounts := make(map[string]int)
	for _, r := range records {
		if r.Kind == infra.KindVolume && r.SpreadGroup == spec.SpreadGroup && r.Hostname != spec.SpreadReplaces {
			groupCounts[r.Node]++
		}
	}
	fitting := make([]node, 0, len(nodes))
	taskResources := make(map[string]map[string]any, len(nodes))
	demand := make(map[string]volumePlacement, len(nodes))
	for _, n := range nodes {
		if _, err := chooseNode([]node{n}, nil, "", "", spec.SpreadAvoid...); err != nil {
			continue
		}
		// Capacity must not weaken the original same-group spread priority.
		// If unused eligible nodes exist, an occupied node is not a fallback.
		if fixedNode != "" {
			if n.ID != fixedNode {
				continue
			}
		} else if groupCounts[n.ID] != groupCounts[first] {
			continue
		}
		var full struct {
			Attributes        map[string]string
			NodeResources     capacityResources
			ReservedResources capacityResources
		}
		if _, err := b.c.get(ctx, "/v1/node/"+n.ID, nil, &full); err != nil {
			return "", err
		}
		var allocations []struct {
			ClientStatus       string
			AllocatedResources struct{ Tasks map[string]capacityResources }
		}
		// Allocation reads are namespace-filtered. DesiredStatus=stop is not
		// terminal: its resources remain occupied until the client finishes it.
		if _, err := b.c.get(ctx, "/v1/node/"+n.ID+"/allocations", nil, &allocations); err != nil {
			return "", err
		}
		cpu := full.NodeResources.Cpu.CpuShares - full.ReservedResources.Cpu.CpuShares
		memory := full.NodeResources.Memory.MemoryMB - full.ReservedResources.Memory.MemoryMB
		cores := len(full.NodeResources.Cpu.ReservableCpuCores)
		for _, allocation := range allocations {
			switch allocation.ClientStatus {
			case "complete", "failed", "lost":
				continue
			}
			for _, task := range allocation.AllocatedResources.Tasks {
				cpu -= task.Cpu.CpuShares
				memory -= task.Memory.MemoryMB
				cores -= len(task.Cpu.ReservedCores)
			}
		}
		for _, prior := range *pending {
			if prior.Node == n.ID {
				cpu -= prior.CPU
				memory -= prior.Memory
				cores -= prior.Cores
			}
		}
		frequency := 1000
		total, e1 := strconv.ParseFloat(full.Attributes["cpu.totalcompute"], 64)
		count, e2 := strconv.ParseFloat(full.Attributes["cpu.numcores"], 64)
		if e1 == nil && e2 == nil && total > 0 && count > 0 && int(total/count) > 0 {
			frequency = int(total / count)
		}
		requestedCPU := resources.MaxCPU
		if requestedCPU == 0 {
			requestedCPU = resources.MinCPU
		}
		if requestedCPU == 0 {
			requestedCPU = 0.1
		}
		requestedRAM := resources.MaxRAM
		if requestedRAM == 0 {
			requestedRAM = resources.MinRAM
		}
		// Match renderJob's CPU conversion and MiB rounding exactly.
		if memory < max(32, int(math.Ceil(requestedRAM*1024))) {
			continue
		}
		if resources.CPUMode == infra.CPUDedicated {
			if cores < int(requestedCPU) || cpu < int(math.Ceil(requestedCPU*float64(frequency))) {
				continue
			}
		} else if cpu < max(1, int(math.Ceil(requestedCPU*float64(frequency)))) {
			continue
		}
		task := map[string]any{"MemoryMB": max(32, int(math.Ceil(requestedRAM*1024)))}
		if resources.CPUMode == infra.CPUDedicated {
			task["Cores"] = int(requestedCPU)
		} else {
			task["CPU"] = max(1, int(math.Ceil(requestedCPU*float64(frequency))))
		}
		taskResources[n.ID] = task
		demand[n.ID] = volumePlacement{Node: n.ID, CPU: max(1, int(math.Ceil(requestedCPU*float64(frequency)))), Memory: max(32, int(math.Ceil(requestedRAM*1024)))}
		if resources.CPUMode == infra.CPUDedicated {
			value := demand[n.ID]
			value.Cores = int(requestedCPU)
			demand[n.ID] = value
		}
		fitting = append(fitting, n)
	}
	// Node-allocation reads are ACL-filtered. A new-job plan lets Nomad
	// account for hidden allocations too, without cross-namespace read grants.
	// Never reuse an existing job ID: that could credit its old allocation.
	id, err := newID()
	if err != nil {
		return "", err
	}
	for len(fitting) != 0 {
		selected, err := chooseNode(fitting, records, spec.SpreadGroup, spec.SpreadReplaces, spec.SpreadAvoid...)
		if err != nil {
			return "", err
		}
		var candidate node
		for i, n := range fitting {
			if n.ID == selected {
				candidate = n
				fitting = append(fitting[:i], fitting[i+1:]...)
				break
			}
		}
		jobID := "kk-placement-" + id
		group := map[string]any{"Name": fmt.Sprintf("member-%d", len(*pending)), "Count": 1,
			"Constraints": []any{map[string]any{"LTarget": "${node.unique.id}", "Operand": "=", "RTarget": selected}},
			"Tasks": []any{map[string]any{"Name": "member", "Driver": "exec",
				"Config": map[string]any{"command": "/bin/true"}, "Resources": taskResources[selected]}}}
		groups := make([]any, 0, len(*pending)+1)
		for _, prior := range *pending {
			groups = append(groups, prior.Group)
		}
		groups = append(groups, group)
		body := map[string]any{"Job": map[string]any{
			"ID": jobID, "Name": jobID, "Namespace": b.target.ID,
			"Type": "service", "NodePool": candidate.NodePool,
			"Datacenters": datacenters(b.target.Config),
			"TaskGroups":  groups,
		}}
		var plan evaluation
		if err := b.c.post(ctx, "/v1/job/"+jobID+"/plan", nil, body, &plan); err != nil {
			return "", err
		}
		if len(plan.FailedTGAllocs) == 0 {
			value := demand[selected]
			value.Group = group
			*pending = append(*pending, value)
			return selected, nil
		}
	}
	return "", fmt.Errorf("choose volume node: %w: insufficient free allocatable CPU/memory capacity for requested task (CPU %.3g–%.3g cores, memory %.3g–%.3g GiB); free capacity or add an eligible node", infra.ErrPlacementUnsatisfiable, resources.MinCPU, resources.MaxCPU, resources.MinRAM, resources.MaxRAM)
}
