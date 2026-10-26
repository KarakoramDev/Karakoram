// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// memoryPressureSampler compares fresh samples, never treating a lifetime max
// counter alone as current pressure. It is shared by concurrent readiness RPCs.
type memoryPressureSampler struct {
	mu        sync.Mutex
	dir       string
	maxEvents uint64
	valid     bool
	eventAt   time.Time
	sampleAt  time.Time
	readFile  func(string) ([]byte, error)
}

func (s *memoryPressureSampler) observe() string {
	readFile := s.readFile
	if readFile == nil {
		readFile = readPressureFile
	}
	return s.observeAt(readFile, time.Now())
}

func (s *memoryPressureSampler) available() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.valid
}

// Procfs mount inventories can grow with host activity. Reject oversized reads
// rather than parsing a truncated inventory or allocating without a bound.
const memoryPressureReadLimit = 1024 * 1024

func readPressureFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, memoryPressureReadLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > memoryPressureReadLimit {
		return nil, fmt.Errorf("memory pressure observation exceeds size limit")
	}
	return data, nil
}

func (s *memoryPressureSampler) invalidate() {
	s.mu.Lock()
	s.valid, s.eventAt, s.sampleAt = false, time.Time{}, time.Time{}
	s.mu.Unlock()
}

func (s *memoryPressureSampler) observeAt(readFile func(string) ([]byte, error), at time.Time) string {
	membership, err := readFile("/proc/self/cgroup")
	if err != nil {
		s.invalidate()
		return ""
	}
	mounts, err := readFile("/proc/self/mountinfo")
	if err != nil {
		s.invalidate()
		return ""
	}
	dir := memoryCgroupDir(string(membership), string(mounts))
	if dir == "" {
		s.invalidate()
		return ""
	}
	return s.sample(dir, readFile, at)
}

// Resolve the agent's own unified cgroup, including exec tasks without a
// cgroup namespace. Do not read the host mount root for a nested task.
func memoryCgroupDir(membership, mounts string) string {
	group := ""
	for _, line := range strings.Split(membership, "\n") {
		if strings.HasPrefix(line, "0::/") {
			group = strings.TrimPrefix(line, "0::")
			break
		}
	}
	if !resolvableCgroupPath(group) {
		return ""
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(mounts, "\n") {
		before, after, ok := strings.Cut(line, " - ")
		fields := strings.Fields(before)
		if !ok || !strings.HasPrefix(after, "cgroup2 ") || len(fields) < 5 {
			continue
		}
		root, mount := unescape.Replace(fields[3]), unescape.Replace(fields[4])
		if !resolvableCgroupPath(root) || !resolvableCgroupPath(mount) {
			continue
		}
		relative, err := filepath.Rel(root, group)
		if err != nil || relative == ".." || strings.HasPrefix(relative, "../") {
			continue
		}
		return filepath.Join(mount, relative)
	}
	return ""
}

// A /../ membership can identify a task outside a cgroup namespace's root.
// Cleaning it would silently turn an unresolvable path into a sibling path.
func resolvableCgroupPath(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." {
			return false
		}
	}
	return true
}

func (s *memoryPressureSampler) sample(dir string, readFile func(string) ([]byte, error), at time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	read := func(name string) string {
		data, _ := readFile(filepath.Join(dir, name))
		return strings.TrimSpace(string(data))
	}
	var events uint64
	found := false
	for _, line := range strings.Split(read("memory.events"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "max" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				events, found = value, true
			}
		}
	}
	baselineFresh := s.valid && s.dir == dir && !at.Before(s.sampleAt) && at.Sub(s.sampleAt) < 10*time.Second
	increased := found && baselineFresh && events > s.maxEvents
	if !found || !baselineFresh || events < s.maxEvents {
		s.eventAt = time.Time{}
	}
	if increased {
		s.eventAt = s.sampleAt
	}
	// Sync and Status RPCs must see the same recent event, not consume it.
	// Retention is bounded to the PSI avg10 timescale.
	recentEvent := !s.eventAt.IsZero() && !at.Before(s.eventAt) && at.Sub(s.eventAt) < 10*time.Second
	s.dir, s.maxEvents, s.valid = dir, events, found
	s.sampleAt = at
	psi := 0.0
	for _, line := range strings.Split(read("memory.pressure"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "avg10="); ok {
				parsed, err := strconv.ParseFloat(value, 64)
				if err == nil && parsed > 0 && parsed <= 100 {
					psi = parsed
				}
			}
		}
	}
	if !recentEvent && psi == 0 {
		return ""
	}
	evidence := fmt.Sprintf("memory PSI some avg10=%.2f%%", psi)
	if recentEvent {
		evidence = "cgroup memory.events max increased within the last 10s"
		if psi > 0 {
			evidence += fmt.Sprintf("; memory PSI some avg10=%.2f%%", psi)
		}
	}
	// Only numeric values enter the diagnostic; never expose procfs paths or logs.
	current, currentErr := strconv.ParseUint(read("memory.current"), 10, 64)
	limit, limitErr := strconv.ParseUint(read("memory.max"), 10, 64)
	if currentErr == nil && limitErr == nil && limit > 0 {
		evidence += fmt.Sprintf("; current=%d bytes, limit=%d bytes", current, limit)
	}
	return "Kibana memory pressure observed (" + evidence + "). Increase spec.components.extra.kibana.resources minRam and maxRam together for a fixed larger allocation; inspect native logs and workload. Pressure is not proof of an OOM kill"
}
