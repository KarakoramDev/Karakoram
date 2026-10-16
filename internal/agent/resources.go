// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// cgroupRoot is the container's cgroup v2 mount.
const cgroupRoot = "/sys/fs/cgroup"

// containerResources reads what this container can use right now. Vertical
// scaling changes the cgroup in place without restarting the container, so
// nothing is cached. Nil when nothing could be read.
func containerResources() *agentapi.ContainerResources {
	r := agentapi.ContainerResources{CPUCores: cpuCores(cgroupRoot)}
	if memory := memoryTotal(); memory > 0 {
		r.MemoryBytes = uint64(memory)
	}
	if disk, err := diskUsage("/"); err == nil {
		r.RootDiskBytes = disk.TotalBytes
	}
	if r == (agentapi.ContainerResources{}) {
		return nil
	}
	return &r
}

// cpuCores counts the CPUs in the cgroup's effective cpuset, falling back to
// the scheduler affinity mask, and caps the result by a cpu.max quota.
func cpuCores(root string) float64 {
	var cores float64
	if data, err := os.ReadFile(filepath.Join(root, "cpuset.cpus.effective")); err == nil {
		if n, err := cpuSetSize(string(data)); err == nil {
			cores = float64(n)
		}
	}
	if cores == 0 {
		cores = float64(affinityCPUs())
	}
	if data, err := os.ReadFile(filepath.Join(root, "cpu.max")); err == nil {
		if quota, ok := cpuQuota(string(data)); ok && (cores == 0 || quota < cores) {
			cores = quota
		}
	}
	return cores
}

// cpuSetSize counts the CPUs in a cgroup cpuset list such as "0-3,7".
func cpuSetSize(list string) (int, error) {
	list = strings.TrimSpace(list)
	if list == "" {
		return 0, errors.New("empty cpuset")
	}
	n := 0
	for _, part := range strings.Split(list, ",") {
		first, last, isRange := strings.Cut(part, "-")
		lo, err := strconv.Atoi(first)
		if err != nil {
			return 0, fmt.Errorf("invalid cpuset %q: %w", list, err)
		}
		hi := lo
		if isRange {
			if hi, err = strconv.Atoi(last); err != nil {
				return 0, fmt.Errorf("invalid cpuset %q: %w", list, err)
			}
		}
		if lo < 0 || hi < lo {
			return 0, fmt.Errorf("invalid cpuset range %q", part)
		}
		n += hi - lo + 1
	}
	return n, nil
}

// cpuQuota converts a cgroup v2 cpu.max "quota period" pair into cores. The
// quota "max" means unlimited and reports false.
func cpuQuota(cpuMax string) (float64, bool) {
	fields := strings.Fields(cpuMax)
	if len(fields) != 2 || fields[0] == "max" {
		return 0, false
	}
	quota, qerr := strconv.ParseFloat(fields[0], 64)
	period, perr := strconv.ParseFloat(fields[1], 64)
	if qerr != nil || perr != nil || quota <= 0 || period <= 0 {
		return 0, false
	}
	return quota / period, true
}
