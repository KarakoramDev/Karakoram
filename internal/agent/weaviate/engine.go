// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

type Engine struct {
	mu      sync.RWMutex
	config  agentapi.MemberConfig
	applied bool
}

var _ agent.Engine = (*Engine)(nil)

func New() *Engine                  { return &Engine{} }
func (*Engine) Name() string        { return wvspec.Engine }
func (*Engine) Processes() []string { return []string{wvspec.Process} }

func spec(c agentapi.MemberConfig) (wvspec.MemberSpec, error) {
	var s wvspec.MemberSpec
	if c.Engine != wvspec.Engine || len(c.EngineSpec) == 0 || json.Unmarshal(c.EngineSpec, &s) != nil {
		return s, errors.New("missing or invalid Weaviate engine spec")
	}
	if err := s.Validate(); err != nil {
		return s, fmt.Errorf("invalid Weaviate spec: %w", err)
	}
	if c.EngineVersion != "" && c.EngineVersion != s.Version {
		return s, errors.New("configured Weaviate engine version does not match spec")
	}
	if s.TLS {
		if _, err := tls.X509KeyPair([]byte(c.TLS.CertPEM), []byte(c.TLS.KeyPEM)); err != nil {
			return s, errors.New("invalid Weaviate TLS certificate or key")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(c.TLS.CAPEM)) {
			return s, errors.New("invalid Weaviate TLS CA")
		}
	}
	return s, nil
}

type host interface {
	DataDir() string
	WriteFile(string, []byte, os.FileMode) error
	Configure(string, agent.ProcessSpec) error
	Snapshot() (map[string]agentapi.ProcessStatus, bool)
	Stopped(string) bool
	Action(context.Context, string, string) error
}

func (e *Engine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	s, err := spec(c)
	if err != nil {
		return nil, err
	}
	changes, err := apply(ctx, n, c, s)
	if err != nil {
		return changes, err
	}
	e.mu.Lock()
	e.config = c
	e.applied = true
	e.mu.Unlock()
	return changes, nil
}
func apply(ctx context.Context, n host, c agentapi.MemberConfig, s wvspec.MemberSpec) ([]string, error) {
	p, files := render(n.DataDir(), s, c)
	info, err := os.Stat(p.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, fmt.Errorf("executable for Weaviate %s unavailable at %s", s.Version, p.Binary)
	}
	statePath := filepath.Join(n.DataDir(), "karakoram", "weaviate-applied.json")
	var state struct {
		Fingerprint string `json:"fingerprint"`
	}
	old, err := os.ReadFile(statePath)
	if err == nil {
		if json.Unmarshal(old, &state) != nil {
			return nil, errors.New("invalid applied Weaviate state")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	dir := filepath.Join(n.DataDir(), "weaviate")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		return nil, errors.New("native Weaviate data path must be a directory, not a symlink")
	}
	if err = os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	changes := []string{}
	for _, name := range names {
		existing, e := os.ReadFile(filepath.Join(n.DataDir(), "karakoram", name))
		if e != nil && !os.IsNotExist(e) {
			return changes, e
		}
		info, statErr := os.Lstat(filepath.Join(n.DataDir(), "karakoram", name))
		private := statErr == nil && info.Mode().IsRegular() && info.Mode().Perm() == 0600
		if !bytes.Equal(existing, files[name]) || !private {
			if e = n.WriteFile(name, files[name], 0600); e != nil {
				return changes, e
			}
			changes = append(changes, name)
		}
	}
	if err = n.Configure(wvspec.Process, p); err != nil {
		return changes, err
	}
	states, maintenance := n.Snapshot()
	desired := c.DesiredProcesses == nil || slices.Contains(c.DesiredProcesses, wvspec.Process)
	if !desired {
		err = n.Action(ctx, wvspec.Process, "stop")
	} else if !maintenance && !c.Maintenance && !n.Stopped(wvspec.Process) {
		next := fingerprint(s, p, files)
		action := "start"
		if states[wvspec.Process].PID != 0 && state.Fingerprint != next {
			action = "restart"
		}
		if err = n.Action(ctx, wvspec.Process, action); err == nil {
			state.Fingerprint = next
		}
	}
	if err != nil {
		return changes, err
	}
	data, _ := json.Marshal(state)
	if !bytes.Equal(old, data) {
		if err = n.WriteFile("weaviate-applied.json", data, 0600); err != nil {
			return changes, err
		}
		changes = append(changes, "weaviate-applied.json")
	}
	return changes, nil
}
func (e *Engine) Version(ctx context.Context) string {
	e.mu.RLock()
	c, ok := e.config, e.applied
	e.mu.RUnlock()
	if !ok {
		return ""
	}
	s, err := spec(c)
	if err != nil {
		return ""
	}
	if v, err := nativeVersion(ctx, s, c); err == nil {
		return v
	}
	return s.Version
}
