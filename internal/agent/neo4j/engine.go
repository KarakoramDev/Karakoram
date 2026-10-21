// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package neo4j supervises a single native Neo4j Community server JVM.
package neo4j

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

type Engine struct {
	mu                 sync.RWMutex
	version            string
	configVersion      int64
	configurationError string
	appliedToken       string
	commandHash        [32]byte
	command            agent.ProcessSpec
	jobs               *jobManager
	// Port overrides isolate native tests; production always uses spec ports.
	boltPort, httpsPort, recoveryBoltPort, recoveryHTTPSPort int
	listenAddress                                            string
}

func New() *Engine                  { return &Engine{} }
func (*Engine) Name() string        { return neo4jspec.Engine }
func (*Engine) Processes() []string { return []string{neo4jspec.Process} }
func (e *Engine) Version(context.Context) string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.version
}
func (e *Engine) ports(recovery bool) (int, int) {
	b, h := e.boltPort, e.httpsPort
	if recovery {
		b, h = e.recoveryBoltPort, e.recoveryHTTPSPort
	}
	if b == 0 {
		if recovery {
			b = neo4jspec.RecoveryPortBolt
		} else {
			b = neo4jspec.PortBolt
		}
	}
	if h == 0 {
		if recovery {
			h = neo4jspec.RecoveryPortHTTPS
		} else {
			h = neo4jspec.PortHTTPS
		}
	}
	return b, h
}

var versionPattern = regexp.MustCompile(`^2026\.(09|10)\.0$`)

func decodeSpec(c agentapi.MemberConfig) (neo4jspec.Spec, error) {
	var s neo4jspec.Spec
	d := json.NewDecoder(bytes.NewReader(c.EngineSpec))
	d.DisallowUnknownFields()
	if c.Engine != neo4jspec.Engine || d.Decode(&s) != nil {
		return s, errors.New("invalid Neo4j member payload")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return s, errors.New("invalid Neo4j member payload")
	}
	if !versionPattern.MatchString(s.Version) || !filepath.IsAbs(s.Install.Neo4jHome) || !filepath.IsAbs(s.Install.JavaHome) || s.HeapMB <= 0 || s.PageCacheMB <= 0 || s.CAPEM == "" || s.Server.CertPEM == "" || s.Server.KeyPEM == "" || s.AdminPassword == "" || s.RestartToken == "" || s.AdvertisedHost == "" || strings.ContainsAny(s.AdvertisedHost, "\r\n\x00= ") {
		return s, errors.New("invalid Neo4j member configuration")
	}
	for _, home := range []string{s.Install.Neo4jHome, s.Install.JavaHome} {
		if strings.ContainsAny(home, "\r\n\x00") {
			return s, errors.New("invalid Neo4j install directory")
		}
	}
	if _, err := tlsClient(s); err != nil {
		return s, errors.New("invalid Neo4j TLS configuration")
	}
	return s, nil
}

type appliedState struct {
	RestartToken string `json:"restartToken"`
	Applied      bool   `json:"applied"`
}

func loadApplied(base string) (appliedState, error) {
	var state appliedState
	b, err := os.ReadFile(filepath.Join(base, "applied.json"))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if json.Unmarshal(b, &state) != nil {
		return state, errors.New("invalid applied Neo4j state")
	}
	return state, nil
}
func restartAction(running bool, state appliedState, token string) string {
	if !running {
		return "start"
	}
	if state.Applied && state.RestartToken != token {
		return "restart"
	}
	return ""
}
func (e *Engine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) (changes []string, err error) {
	defer func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if err != nil {
			e.configurationError = "Neo4j configuration could not be applied"
		} else {
			e.configurationError = ""
			e.configVersion = c.ConfigVersion
		}
	}()
	s, err := decodeSpec(c)
	if err != nil {
		return nil, err
	}
	for _, home := range []string{s.Install.Neo4jHome, s.Install.JavaHome} {
		st, statErr := os.Stat(home)
		if errors.Is(statErr, os.ErrNotExist) {
			return nil, n.Action(ctx, neo4jspec.Process, "stop")
		}
		if statErr != nil {
			return nil, statErr
		}
		if !st.IsDir() {
			return nil, errors.New("Neo4j install path is not a directory")
		}
	}
	stock, err := os.ReadFile(filepath.Join(s.Install.Neo4jHome, "conf", "neo4j.conf"))
	if err != nil {
		return nil, errors.New("Neo4j stock configuration unavailable")
	}
	bolt, https := e.ports(false)
	files, err := render(s, baseDir(n), string(stock), false, bolt, https, e.listenAddress)
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{"data", "logs", "run", "import", "home", "tmp"} {
		if err := os.MkdirAll(filepath.Join(baseDir(n), dir), 0700); err != nil {
			return nil, err
		}
	}
	changes, err = writeFiles(n, files)
	if err != nil {
		return changes, err
	}
	// The derivation cache includes all non-secret JVM inputs and config contents.
	h := sha256.New()
	h.Write([]byte(s.Version))
	h.Write([]byte(s.Install.Neo4jHome))
	h.Write([]byte(s.Install.JavaHome))
	h.Write(files["neo4j/conf/neo4j.conf"])
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	e.mu.RLock()
	cached, hash := e.command, e.commandHash
	e.mu.RUnlock()
	if cached.Binary == "" || hash != digest {
		running, _ := n.Snapshot()
		cached, err = serverCommand(ctx, s, confDir(n), running[neo4jspec.Process].PID)
		if err != nil {
			return changes, err
		}
		e.mu.Lock()
		e.command, e.commandHash = cached, digest
		e.mu.Unlock()
	}
	if err := n.Configure(neo4jspec.Process, cached); err != nil {
		return changes, err
	}
	if err := e.reconcileJobs(ctx, n); err != nil {
		return changes, err
	}
	state, err := loadApplied(baseDir(n))
	if err != nil {
		return changes, err
	}
	states, _ := n.Snapshot()
	if !n.Maintenance() && !n.Stopped(neo4jspec.Process) && desired(c) {
		action := restartAction(states[neo4jspec.Process].State == "running", state, s.RestartToken)
		if action != "" {
			stopCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			err = n.Action(stopCtx, neo4jspec.Process, action)
			cancel()
			if err != nil {
				return changes, err
			}
		}
		state = appliedState{RestartToken: s.RestartToken, Applied: true}
		b, _ := json.Marshal(state)
		if err := n.WriteFile("neo4j/applied.json", b, 0600); err != nil {
			return changes, err
		}
	} else if !desired(c) {
		if err := n.Action(ctx, neo4jspec.Process, "stop"); err != nil {
			return changes, err
		}
	}
	e.mu.Lock()
	e.version = s.Version
	e.appliedToken = state.RestartToken
	e.mu.Unlock()
	return changes, nil
}
func desired(c agentapi.MemberConfig) bool {
	if c.DesiredProcesses == nil {
		return true
	}
	for _, p := range c.DesiredProcesses {
		if p == neo4jspec.Process {
			return true
		}
	}
	return false
}
func (e *Engine) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	if name != neo4jspec.Process {
		return nil
	}
	s, err := decodeSpec(n.Config())
	if err != nil {
		return err
	}
	initialized := filepath.Join(baseDir(n), "credentials-initialized.json")
	_, markerErr := os.Stat(initialized)
	_, storeErr := os.Stat(filepath.Join(dataDir(n), "databases", "system"))
	if markerErr == nil && storeErr == nil {
		return nil
	}
	if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
		return markerErr
	}
	if storeErr != nil && !errors.Is(storeErr, os.ErrNotExist) {
		return storeErr
	}
	// Completion, not intent, is durable: any interrupted first initialization is
	// replayed before the normal supervisor is allowed to open its connectors.
	if err := e.recoverAdmin(ctx, n, s, s.AdminPassword); err != nil {
		return err
	}
	return n.WriteFile("neo4j/credentials-initialized.json", []byte("{}\n"), 0600)
}
func (e *Engine) Shutdown(context.Context, *agent.Node) error { e.cancelJobs(); return nil }
