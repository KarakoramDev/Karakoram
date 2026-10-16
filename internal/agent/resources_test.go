// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCPUSetSize(t *testing.T) {
	for list, want := range map[string]int{"0": 1, "3\n": 1, "0-1": 2, "0-3,7": 5, "2,4-5,8-11": 7, " 12-13 \n": 2} {
		if got, err := cpuSetSize(list); err != nil || got != want {
			t.Errorf("cpuSetSize(%q) = %d, %v; want %d", list, got, err, want)
		}
	}
	for _, list := range []string{"", "\n", "a", "1-", "-1", "3-1", "0,,1", "0-x"} {
		if got, err := cpuSetSize(list); err == nil {
			t.Errorf("cpuSetSize(%q) = %d; want error", list, got)
		}
	}
}

func TestCPUQuota(t *testing.T) {
	for cpuMax, want := range map[string]float64{"200000 100000\n": 2, "50000 100000": 0.5, "150000 100000": 1.5} {
		if got, ok := cpuQuota(cpuMax); !ok || got != want {
			t.Errorf("cpuQuota(%q) = %v, %v; want %v", cpuMax, got, ok, want)
		}
	}
	for _, cpuMax := range []string{"max 100000\n", "", "0 100000", "100000 0", "x 100000", "100000"} {
		if got, ok := cpuQuota(cpuMax); ok {
			t.Errorf("cpuQuota(%q) = %v; want no quota", cpuMax, got)
		}
	}
}

func TestCPUCoresReadsCgroup(t *testing.T) {
	write := func(t *testing.T, dir, name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("cpuset without quota", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "cpuset.cpus.effective", "4-5\n")
		write(t, dir, "cpu.max", "max 100000\n")
		if got := cpuCores(dir); got != 2 {
			t.Fatalf("cores = %v; want 2", got)
		}
	})
	t.Run("quota caps cpuset", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "cpuset.cpus.effective", "0-3,7\n")
		write(t, dir, "cpu.max", "150000 100000\n")
		if got := cpuCores(dir); got != 1.5 {
			t.Fatalf("cores = %v; want 1.5", got)
		}
	})
	t.Run("quota above cpuset", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "cpuset.cpus.effective", "0\n")
		write(t, dir, "cpu.max", "400000 100000\n")
		if got := cpuCores(dir); got != 1 {
			t.Fatalf("cores = %v; want 1", got)
		}
	})
	t.Run("affinity fallback", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("scheduler affinity is read on Linux only")
		}
		if got := cpuCores(t.TempDir()); got < 1 {
			t.Fatalf("cores = %v; want the affinity mask size", got)
		}
	})
}

func TestContainerResourcesReportsRootDisk(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("member agents read Linux cgroup and procfs")
	}
	r := containerResources()
	if r == nil || r.CPUCores <= 0 || r.MemoryBytes == 0 || r.RootDiskBytes == 0 {
		t.Fatalf("resources = %+v", r)
	}
}
