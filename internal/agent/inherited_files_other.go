//go:build !linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"strconv"
	"syscall"
)

func closeInheritedFilesOnExec() error {
	files, err := os.ReadDir("/dev/fd")
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
