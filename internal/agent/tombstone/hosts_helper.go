// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package tombstone

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type helperRunner func(context.Context, string, []string, string) error
type hostsHelper struct {
	path string
	run  helperRunner
}

func (*hostsHelper) Kind() string { return "hosts-helper" }
func (w *hostsHelper) Preflight(ctx context.Context) error {
	info, err := os.Lstat(w.path)
	if err != nil {
		return fmt.Errorf("inspect hosts helper: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Mode().Perm()&0111 == 0 || !ok || stat.Uid != 0 {
		return fmt.Errorf("hosts helper must be a root-owned executable, not group/world writable")
	}
	for dir := filepath.Dir(w.path); ; dir = filepath.Dir(dir) {
		parent, err := os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("inspect hosts helper directory: %w", err)
		}
		stat, ok := parent.Sys().(*syscall.Stat_t)
		if !parent.IsDir() || parent.Mode().Perm()&0022 != 0 || !ok || stat.Uid != 0 {
			return fmt.Errorf("hosts helper directories must be root-owned and not group/world writable")
		}
		if dir == string(filepath.Separator) {
			break
		}
	}
	return w.run(ctx, "/usr/bin/sudo", []string{"-n", "-l", w.path}, "")
}
func (w *hostsHelper) Install(ctx context.Context, hosts []string) error {
	input, err := hostInput(hosts)
	if err != nil {
		return err
	}
	return w.run(ctx, "/usr/bin/sudo", []string{"-n", w.path}, input)
}
func runHelper(ctx context.Context, name string, args []string, input string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL=C"}
	cmd.Stdin = strings.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("hosts helper: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}
