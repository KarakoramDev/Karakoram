//go:build linux

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
	"time"

	"golang.org/x/sys/unix"
)

const ownedGroupAnchors = true

// WNOWAIT leaves the exited child unreaped: its PID/PGID cannot be recycled
// while the supervisor finishes signalling and observing the owned group.
func observeChildExit(pid int) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// The caller has sent SIGKILL and still owns an unreaped group leader. A killed
// member may remain visible as a zombie until its own parent reaps it, but can
// no longer execute, fork, retain open descriptors, or access the datadir.
func awaitOwnedGroupExit(pgid int) error {
	for {
		alive, err := ownedGroupAlive(pgid)
		if err != nil || !alive {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func ownedGroupAlive(pgid int) (bool, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == pgid || !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if os.IsNotExist(err) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("read process %d during group shutdown: %w", pid, err)
		}
		end := strings.LastIndexByte(string(data), ')')
		if end < 0 {
			return false, fmt.Errorf("invalid process %d stat during group shutdown", pid)
		}
		fields := strings.Fields(string(data[end+1:]))
		if len(fields) < 3 {
			return false, fmt.Errorf("incomplete process %d stat during group shutdown", pid)
		}
		group, err := strconv.Atoi(fields[2])
		if err != nil {
			return false, fmt.Errorf("invalid process %d group during shutdown: %w", pid, err)
		}
		if group == pgid && fields[0] != "Z" && fields[0] != "X" && fields[0] != "x" {
			return true, nil
		}
	}
	return false, nil
}
