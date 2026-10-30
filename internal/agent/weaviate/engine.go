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
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

type Engine struct {
	mu             sync.RWMutex
	config         agentapi.MemberConfig
	applied        bool
	prepareOnStart bool
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
	_, maintenance := n.Snapshot()
	e.mu.Lock()
	e.prepareOnStart = maintenance || c.Maintenance || n.Stopped(wvspec.Process)
	e.mu.Unlock()
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

// BeforeStart prepares a process whose retained configuration was applied under
// a durable stop/maintenance hold. Admission runs this before releasing the hold.
func (e *Engine) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	if name != wvspec.Process {
		return nil
	}
	e.mu.RLock()
	needed := e.prepareOnStart
	e.mu.RUnlock()
	if !needed {
		return nil
	}
	c := n.Config()
	s, err := spec(c)
	if err != nil {
		return err
	}
	prepared, err := prepare(ctx, n, c, s)
	if err != nil {
		return err
	}
	// Record the inputs prepared for admission. If start subsequently fails,
	// PID remains zero and a later apply still issues start rather than restart.
	data, err := json.Marshal(struct {
		Fingerprint string `json:"fingerprint"`
	}{
		Fingerprint: fingerprint(s, prepared.process, prepared.files),
	})
	if err != nil {
		return err
	}
	if err := n.WriteFile("weaviate-applied.json", data, 0600); err != nil {
		return err
	}
	e.mu.Lock()
	e.prepareOnStart = false
	e.mu.Unlock()
	return nil
}

func apply(ctx context.Context, n host, c agentapi.MemberConfig, s wvspec.MemberSpec) ([]string, error) {
	current, maintenance := n.Snapshot()
	if c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, wvspec.Process) {
		if _, configured := current[wvspec.Process]; !configured {
			return nil, nil
		}
		return nil, n.Action(ctx, wvspec.Process, "stop")
	}
	if maintenance || c.Maintenance || n.Stopped(wvspec.Process) {
		// Maintenance release can resume a desired child directly in the
		// supervisor. Preserve its already-valid spec for that path.
		if _, configured := current[wvspec.Process]; configured {
			return nil, nil
		}
		// Register a non-startable placeholder. BeforeStart replaces it with the
		// fully resolved configuration before an explicit start can be admitted.
		return nil, n.Configure(wvspec.Process, agent.ProcessSpec{})
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
	prepared, err := prepare(ctx, n, c, s)
	changes := prepared.changes
	if err != nil {
		return changes, err
	}
	p, files := prepared.process, prepared.files
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

// Resolve the member's private service identity in the runtime network, not in
// the control plane's network. Memberlist requires a bare IP, not a DNS name or
// bracketed IPv6 literal. Raft adds its port with net.JoinHostPort upstream.
func resolveAdvertiseAddr(ctx context.Context, s wvspec.MemberSpec, lookup func(context.Context, string, string) ([]net.IP, error)) (string, error) {
	bind := net.ParseIP(s.BindHost)
	if bind != nil && !bind.IsUnspecified() {
		return bind.String(), nil
	}
	network := "ip6"
	if bind.To4() != nil {
		network = "ip4"
	}
	addresses, err := lookup(ctx, network, s.NodeName)
	if err != nil {
		return "", fmt.Errorf("resolve Weaviate private member address: %w", err)
	}
	for _, ip := range addresses {
		if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		if (ip.To4() != nil) == (network == "ip4") {
			return ip.String(), nil
		}
	}
	return "", errors.New("Weaviate private member identity has no advertisable address in the bind address family")
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
