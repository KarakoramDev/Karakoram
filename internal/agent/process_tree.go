// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type processTreeStat struct {
	pid, parent int
	ticks, rss  float64
}

// processTreeUsage includes the supervised wrapper and its live descendants.
// The root's reaped-child time retains work from children no longer in /proc.
func processTreeUsage(pid int, procRoot string) (cpu, rss float64, ok bool) {
	if pid <= 0 {
		return
	}
	ticks := clockTicks()
	if ticks <= 0 {
		return
	}
	root, ok := readProcessTreeStat(procRoot, pid, true)
	if !ok {
		return 0, 0, false
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return 0, 0, false
	}
	children := make(map[int][]processTreeStat)
	for _, entry := range entries {
		childPID, err := strconv.Atoi(entry.Name())
		if err != nil || childPID <= 0 || childPID == pid || !entry.IsDir() {
			continue
		}
		child, ok := readProcessTreeStat(procRoot, childPID, false)
		if !ok {
			// Processes may exit between the directory listing and stat read.
			continue
		}
		children[child.parent] = append(children[child.parent], child)
	}
	cpu, rss = root.ticks, root.rss
	pending := []int{pid}
	// The queue grows during traversal, so its length must be checked each time.
	for i := 0; i < len(pending); i++ {
		for _, child := range children[pending[i]] {
			cpu += child.ticks
			rss += child.rss
			pending = append(pending, child.pid)
		}
	}
	return cpu / ticks, rss * float64(os.Getpagesize()), true
}

func readProcessTreeStat(procRoot string, pid int, includeReaped bool) (stat processTreeStat, ok bool) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return
	}
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 22 {
		return
	}
	stat.pid = pid
	stat.parent, err = strconv.Atoi(fields[1])
	if err != nil {
		return stat, false
	}
	lastTime := 12
	if includeReaped {
		lastTime = 14
	}
	for i := 11; i <= lastTime; i++ {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return stat, false
		}
		stat.ticks += value
	}
	stat.rss, err = strconv.ParseFloat(fields[21], 64)
	return stat, err == nil
}
