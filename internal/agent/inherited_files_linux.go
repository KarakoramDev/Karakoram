//go:build linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"errors"
	"os"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// Mark inherited descriptors, rather than closing them: caller-owned locks stay
// held by the agent but cannot leak into servers. Explicit exec.Cmd file maps
// (including stdout/stderr and ExtraFiles) still survive the child-side dup.
func closeInheritedFilesOnExec() error {
	if err := unix.CloseRange(3, ^uint(0), unix.CLOSE_RANGE_CLOEXEC); err == nil {
		return nil
	} else if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return err
	}
	// CLOSE_RANGE_CLOEXEC requires Linux 5.11. Older kernels still get startup
	// hygiene, before the agent creates runtime goroutines or engine children.
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return err
	}
	for _, file := range files {
		fd, err := strconv.Atoi(file.Name())
		if err == nil && fd > 2 {
			syscall.CloseOnExec(fd)
		}
	}
	return nil
}
