// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemoryCgroupDirScopesExecAndNamespacedTasks(t *testing.T) {
	for _, tc := range []struct{ name, group, root, mount, want string }{
		{"exec task", "/nomad/alloc/task", "/", "/sys/fs/cgroup", "/sys/fs/cgroup/nomad/alloc/task"},
		{"private namespace", "/", "/", "/sys/fs/cgroup", "/sys/fs/cgroup"},
		{"subtree mount", "/nomad/alloc/task", "/nomad/alloc", "/sys/fs/cgroup", "/sys/fs/cgroup/task"},
		{"escaped mount", "/task", "/", `/sys/fs/cgroup\040private`, "/sys/fs/cgroup private/task"},
		{"unrelated mount", "/nomad/task", "/other", "/sys/fs/cgroup", ""},
		{"outside namespace root", "/../sibling", "/", "/sys/fs/cgroup", ""},
		{"nested traversal", "/nomad/../sibling", "/", "/sys/fs/cgroup", ""},
		{"parent-relative mount root", "/task", "/../task", "/sys/fs/cgroup", ""},
		{"parent-relative mountpoint", "/task", "/", "/sys/../cgroup", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mounts := "29 23 0:26 " + tc.root + " " + tc.mount + " rw - cgroup2 cgroup rw\n"
			if got := memoryCgroupDir("0::"+tc.group+"\n", mounts); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := memoryCgroupDir("2:memory:/task\n", ""); got != "" {
		t.Fatal("legacy membership guessed unified mount", got)
	}
}

func TestMemoryPressureRequiresFreshEvidenceAndClears(t *testing.T) {
	files := map[string]string{
		"memory.events":   "max 21542\noom_kill 0\n",
		"memory.current":  "1070000000",
		"memory.max":      "1073741824",
		"memory.pressure": "some avg10=0.00 avg60=1.00 total=999999\nfull avg10=0.00 total=0\n",
	}
	read := func(path string) ([]byte, error) {
		data, ok := files[filepath.Base(path)]
		if !ok {
			return nil, os.ErrNotExist
		}
		return []byte(data), nil
	}
	var sampler memoryPressureSampler
	at := time.Unix(100, 0)
	if got := sampler.sample("/task", read, at); got != "" {
		t.Fatal("historical events/PSI total or near-limit usage asserted pressure", got)
	}
	files["memory.events"] = "max 21543\noom_kill 0\n"
	got := sampler.sample("/task", read, at)
	for _, want := range []string{"max increased", "current=1070000000", "limit=1073741824", "minRam and maxRam", "not proof of an OOM kill"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
	if got := sampler.sample("/task", read, at.Add(time.Second)); got == "" {
		t.Fatal("another readiness RPC consumed recent evidence")
	}
	at = at.Add(11 * time.Second)
	if got := sampler.sample("/task", read, at); got != "" {
		t.Fatal("unchanged lifetime counter kept pressure latched", got)
	}
	files["memory.pressure"] = "some avg10=2.50 avg60=0.50 total=1000000\n"
	if got := sampler.sample("/task", read, at); !strings.Contains(got, "avg10=2.50%") {
		t.Fatal("recent reclaim stalls not diagnosed", got)
	}
	files["memory.pressure"] = "some avg10=0.00 total=1000000\n"
	files["memory.events"] = "max 1\n"
	if got := sampler.sample("/task", read, at); got != "" {
		t.Fatal("counter reset treated as increase", got)
	}
	files["memory.events"] = "max 90000\n"
	if got := sampler.sample("/replacement", read, at); got != "" {
		t.Fatal("different cgroup inherited previous baseline", got)
	}
	files["memory.events"] = "max invalid\n"
	files["memory.pressure"] = "some avg10=NaN\n"
	if got := sampler.sample("/replacement", read, at); got != "" {
		t.Fatal("invalid data asserted pressure", got)
	}
	delete(files, "memory.events")
	delete(files, "memory.pressure")
	if got := sampler.sample("/replacement", read, at); got != "" {
		t.Fatal("unavailable cgroup data asserted pressure", got)
	}
}

func TestMemoryPressureInvalidatesGappedAndInterruptedBaselines(t *testing.T) {
	for _, interruption := range []string{"long gap", "clock reversal", "membership unavailable", "mounts unavailable", "unresolvable membership", "events unavailable"} {
		t.Run(interruption, func(t *testing.T) {
			files := map[string]string{
				"/proc/self/cgroup":                 "0::/task\n",
				"/proc/self/mountinfo":              "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
				"/sys/fs/cgroup/task/memory.events": "max 10\n",
			}
			read := func(path string) ([]byte, error) {
				value, ok := files[path]
				if !ok {
					return nil, os.ErrNotExist
				}
				return []byte(value), nil
			}
			var sampler memoryPressureSampler
			at := time.Unix(100, 0)
			if got := sampler.observeAt(read, at); got != "" {
				t.Fatal("initial lifetime count asserted pressure", got)
			}
			next := at.Add(time.Second)
			switch interruption {
			case "long gap":
				next = at.Add(time.Minute)
			case "clock reversal":
				next = at.Add(-time.Second)
			default:
				path := "/proc/self/cgroup"
				if interruption == "mounts unavailable" {
					path = "/proc/self/mountinfo"
				} else if interruption == "events unavailable" {
					path = "/sys/fs/cgroup/task/memory.events"
				}
				saved := files[path]
				delete(files, path)
				if interruption == "unresolvable membership" {
					files[path] = "0::/../sibling\n"
				}
				if got := sampler.observeAt(read, at.Add(500*time.Millisecond)); got != "" {
					t.Fatal("interrupted observation asserted pressure", got)
				}
				files[path] = saved
			}
			files["/sys/fs/cgroup/task/memory.events"] = "max 1000\n"
			if got := sampler.observeAt(read, next); got != "" {
				t.Fatal("historical delta across interruption asserted fresh pressure", got)
			}
			files["/sys/fs/cgroup/task/memory.events"] = "max 1001\n"
			if got := sampler.observeAt(read, next.Add(time.Second)); got == "" {
				t.Fatal("new uninterrupted delta was not diagnosed")
			}
		})
	}
}

func TestMemoryPressureBoundsFileReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mountinfo")
	data := make([]byte, memoryPressureReadLimit+1)
	if err := os.WriteFile(path, data[:memoryPressureReadLimit], 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readPressureFile(path); err != nil || len(got) != memoryPressureReadLimit {
		t.Fatalf("exact bound rejected: bytes=%d error=%v", len(got), err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readPressureFile(path); err == nil || got != nil {
		t.Fatal("oversized inventory accepted or returned partial evidence", err)
	}
}
