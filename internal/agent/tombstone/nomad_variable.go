// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package tombstone

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const nomadMarker = "# karakoram resolution tombstones: enabled"

type nomadVariable struct{ hostsPath string }

// NewNomadVariableWriter acknowledges the watched hosts projection at hostsPath.
func NewNomadVariableWriter(hostsPath string) Writer {
	return &nomadVariable{hostsPath: hostsPath}
}

func (*nomadVariable) Kind() string { return "nomad-variable" }
func (w *nomadVariable) Preflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := os.ReadFile(w.hostsPath)
	if err != nil {
		return err
	}
	if !hasNomadMarker(string(data)) {
		return errors.New("hosts template of this Nomad allocation does not support resolution tombstones")
	}
	return nil
}
func (w *nomadVariable) Install(ctx context.Context, hosts []string) error {
	if _, err := hostInput(hosts); err != nil {
		return err
	}
	if err := w.Preflight(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(w.hostsPath)
		if err != nil {
			return err
		}
		if !hasNomadMarker(string(data)) {
			return errors.New("hosts template of this Nomad allocation does not support resolution tombstones")
		}
		if hostsMatch(string(data), hosts) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for Nomad resolution tombstones: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func hasNomadMarker(data string) bool {
	for line := range strings.SplitSeq(data, "\n") {
		if line == nomadMarker {
			return true
		}
	}
	return false
}

func hostsMatch(data string, hosts []string) bool {
	wanted := make(map[string]bool, len(hosts))
	for _, host := range hosts {
		wanted[host] = true
	}
	seen := make(map[string]uint8, len(hosts))
	for _, line := range strings.Split(data, "\n") {
		line, _, _ = strings.Cut(line, "#")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		sentinel := fields[0] == SentinelIPv4 || fields[0] == SentinelIPv6
		for _, host := range fields[1:] {
			if sentinel && !wanted[host] {
				return false
			}
			if !wanted[host] {
				continue
			}
			if !sentinel {
				return false
			}
			if fields[0] == SentinelIPv4 {
				seen[host] |= 1
			} else {
				seen[host] |= 2
			}
		}
	}
	for host := range wanted {
		if seen[host] != 3 {
			return false
		}
	}
	return true
}
