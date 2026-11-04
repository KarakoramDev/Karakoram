// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

type preparedProcess struct {
	process agent.ProcessSpec
	files   map[string][]byte
	changes []string
}

// prepare performs no process action. Explicit start admission and normal apply
// use the same DNS, executable, data-directory and protected-file preparation.
func prepare(ctx context.Context, n host, c agentapi.MemberConfig, s wvspec.MemberSpec) (preparedProcess, error) {
	var out preparedProcess
	address, err := resolveAdvertiseAddr(ctx, s, net.DefaultResolver.LookupIP)
	if err != nil {
		return out, &agent.RetryableApplyError{Err: err}
	}
	out.process, out.files = render(n.DataDir(), s, c, address)
	info, err := os.Stat(out.process.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return out, fmt.Errorf("executable for Weaviate %s unavailable at %s", s.Version, out.process.Binary)
	}
	dir := filepath.Join(n.DataDir(), "weaviate")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return out, err
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		return out, errors.New("native Weaviate data path must be a directory, not a symlink")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return out, err
	}
	names := make([]string, 0, len(out.files))
	for name := range out.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		existing, err := os.ReadFile(filepath.Join(n.DataDir(), "karakoram", name))
		if err != nil && !os.IsNotExist(err) {
			return out, err
		}
		info, statErr := os.Lstat(filepath.Join(n.DataDir(), "karakoram", name))
		private := statErr == nil && info.Mode().IsRegular() && info.Mode().Perm() == 0600
		if !bytes.Equal(existing, out.files[name]) || !private {
			if err := n.WriteFile(name, out.files[name], 0600); err != nil {
				return out, err
			}
			out.changes = append(out.changes, name)
		}
	}
	return out, n.Configure(wvspec.Process, out.process)
}
