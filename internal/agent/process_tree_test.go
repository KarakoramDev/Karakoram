// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeProcessTreeStat(t *testing.T, procRoot string, pid, parent int, command string, utime, stime, cutime, cstime, pages int) {
	t.Helper()
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 22)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	for index, value := range map[int]int{1: parent, 11: utime, 12: stime, 13: cutime, 14: cstime, 21: pages} {
		fields[index] = strconv.Itoa(value)
	}
	stat := fmt.Sprintf("%d (%s) %s\n", pid, command, strings.Join(fields, " "))
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestProcessTreeUsageIncludesBrokerDescendants(t *testing.T) {
	procRoot := t.TempDir()
	writeProcessTreeStat(t, procRoot, 100, 1, "rabbitmq-server ) shell", 10, 5, 40, 20, 256)
	writeProcessTreeStat(t, procRoot, 200, 100, "beam.smp", 700, 300, 90, 60, 65536)
	writeProcessTreeStat(t, procRoot, 300, 200, "erl_child_setup", 25, 15, 0, 0, 10)
	writeProcessTreeStat(t, procRoot, 400, 100, "epmd", 4, 6, 0, 0, 20)
	writeProcessTreeStat(t, procRoot, 50, 1, "unrelated", 9000, 5000, 0, 0, 7777)
	// A disappearing child must not erase the remaining broker's usage.
	if err := os.Mkdir(filepath.Join(procRoot, "500"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procRoot, "meminfo"), []byte("not a process"), 0600); err != nil {
		t.Fatal(err)
	}
	cpu, rss, ok := processTreeUsage(100, procRoot)
	wantCPU := float64(10+5+40+20+700+300+25+15+4+6) / clockTicks()
	wantRSS := float64(256+65536+10+20) * float64(os.Getpagesize())
	if !ok || cpu != wantCPU || rss != wantRSS {
		t.Fatalf("processTreeUsage = (%g, %g, %t), want (%g, %g, true)", cpu, rss, ok, wantCPU, wantRSS)
	}
}

func TestProcessTreeUsageSingleProcess(t *testing.T) {
	procRoot := t.TempDir()
	writeProcessTreeStat(t, procRoot, 100, 1, "broker", 200, 50, 0, 0, 128)
	cpu, rss, ok := processTreeUsage(100, procRoot)
	if !ok || cpu != 250/clockTicks() || rss != 128*float64(os.Getpagesize()) {
		t.Fatalf("processTreeUsage = (%g, %g, %t)", cpu, rss, ok)
	}
}

func TestProcessTreeUsageRejectsMissingOrMalformedRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		pid  int
		stat string
	}{
		{name: "invalid pid", pid: 0},
		{name: "missing root", pid: 100},
		{name: "truncated stat", pid: 100, stat: "100 (shell) S 1"},
		{name: "invalid CPU time", pid: 100, stat: "100 (shell) S 1 0 0 0 0 0 0 0 0 0 invalid 0 0 0 0 0 0 0 0 0 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			procRoot := t.TempDir()
			if tc.stat != "" {
				dir := filepath.Join(procRoot, strconv.Itoa(tc.pid))
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(tc.stat), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cpu, rss, ok := processTreeUsage(tc.pid, procRoot)
			if ok || cpu != 0 || rss != 0 {
				t.Fatalf("processTreeUsage = (%g, %g, %t), want unavailable", cpu, rss, ok)
			}
		})
	}
}
