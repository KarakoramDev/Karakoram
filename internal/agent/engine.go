// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"golang.org/x/sys/unix"
)

// Engine supplies node-local mechanisms; the agent owns transport and supervision.
type Engine interface {
	Name() string
	Processes() []string
	Apply(context.Context, *Node, agentapi.MemberConfig) ([]string, error)
	Readiness(context.Context, Options, []string) error
	Version(context.Context) string
	Metrics(context.Context, *Node) []Metric
	Routes(*http.ServeMux, *Node)
}

// Metric is an engine metric added to the generic member exposition.
type Metric struct {
	Name, Help string
	Labels     map[string]string
	Value      float64
	Counter    bool
}

// ProcessSpec describes a supervised executable, not a remote execution request.
type ProcessSpec struct {
	Binary    string
	Args, Env []string
	Dir       string
}

// Node exposes protected files and process control to an in-process engine.
type Node struct {
	a *nodeAgent
	// applyConfig belongs to a synchronous Apply or WithLock callback holding applyMu.
	// Other toolkits (routes and scrapes) serialize actions themselves.
	applyConfig *agentapi.MemberConfig
}

func (n *Node) Options() Options { return n.a.o }
func (n *Node) Config() agentapi.MemberConfig {
	if n.applyConfig != nil {
		return *n.applyConfig
	}
	return n.a.config()
}
func (n *Node) Logger() *slog.Logger                                { return n.a.o.Logger }
func (n *Node) DataDir() string                                     { return n.a.o.DataDir }
func (n *Node) Snapshot() (map[string]agentapi.ProcessStatus, bool) { return n.a.sup.snapshot() }
func (n *Node) Maintenance() bool                                   { _, maintenance := n.Snapshot(); return maintenance }
func (n *Node) Stopped(name string) bool {
	n.a.controlsMu.RLock()
	defer n.a.controlsMu.RUnlock()
	return n.a.controls.Stopped[name]
}
func (n *Node) LogTail(name string, lines int) []agentapi.LogLine {
	return n.a.sup.logTail(name, min(max(lines, 1), 1000))
}

// WriteFile atomically replaces a file below DATA_DIR/karakoram with owner-only permissions.
// Rooted filesystem operations also reject symlink escapes from this directory.
func (n *Node) WriteFile(name string, data []byte, mode os.FileMode) error {
	if !filepath.IsLocal(name) || mode.Perm()&0077 != 0 || mode & ^os.FileMode(0700) != 0 {
		return errors.New("engine files require a local path and owner-only mode")
	}
	dir := filepath.Join(n.DataDir(), "karakoram")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	parent := filepath.Dir(name)
	// Go 1.24's Root has no MkdirAll or Rename. Resolve each directory through
	// Root, then rename relative to its opened descriptor without following links.
	dirPath := ""
	for _, part := range strings.Split(parent, string(filepath.Separator)) {
		dirPath = filepath.Join(dirPath, part)
		if err := root.Mkdir(dirPath, 0700); err != nil && !os.IsExist(err) {
			return err
		}
	}
	d, err := root.Open(parent)
	if err != nil {
		return err
	}
	defer d.Close()
	tmp := ".karakoram-" + rand.Text()
	fd, err := unix.Openat(int(d.Fd()), tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, uint32(mode))
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer unix.Unlinkat(int(d.Fd()), tmp, 0)
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = unix.Renameat(int(d.Fd()), tmp, int(d.Fd()), filepath.Base(name)); err != nil {
		return err
	}
	return d.Sync()
}

func (n *Node) Configure(name string, spec ProcessSpec) error {
	if !n.a.allowedProcess(name) {
		return errors.New("unknown process")
	}
	n.a.sup.configure(name, processSpec{binary: spec.Binary, args: spec.Args, env: spec.Env, dir: spec.Dir})
	return nil
}

// Action applies engine intent without releasing durable operator holds.
// Explicit API start/restart actions, unlike configuration pulls, release stop holds.
func (n *Node) Action(ctx context.Context, name, action string) error {
	if n.applyConfig == nil {
		if err := n.a.applyMu.LockContext(ctx); err != nil {
			return err
		}
		defer n.a.applyMu.Unlock()
	}
	return n.actionLocked(ctx, name, action)
}

// WithLock serializes an engine command with Apply and generic control changes.
// The callback toolkit allows nested Actions without re-locking. It is scoped to
// the callback and must not escape it. Context cancellation bounds the lock wait;
// the callback is responsible for using ctx to bound its own work.
func (n *Node) WithLock(ctx context.Context, fn func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.applyConfig != nil {
		return fn(n)
	}
	if err := n.a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer n.a.applyMu.Unlock()
	c := n.a.config()
	return fn(&Node{a: n.a, applyConfig: &c})
}

// actionLocked checks safety holds and performs the transition under applyMu.
func (n *Node) actionLocked(ctx context.Context, name, action string) error {
	if !n.a.allowedProcess(name) {
		return errors.New("unknown process")
	}
	if action != "start" && action != "restart" && action != "stop" {
		return errors.New("invalid process action")
	}
	if action != "stop" {
		if n.Maintenance() || n.Stopped(name) {
			return nil
		}
		if desired := n.Config().DesiredProcesses; desired != nil {
			found := false
			for _, process := range desired {
				if process == name {
					found = true
					break
				}
			}
			if !found {
				return n.a.sup.action(ctx, name, "stop")
			}
		}
	}
	return n.a.sup.action(ctx, name, action)
}

func engineName(name string) string {
	if name == "" {
		return "psmdb"
	}
	return name
}
func selectEngine(name string, engines []Engine) (Engine, error) {
	name = engineName(name)
	if name == "psmdb" {
		return nil, nil
	}
	for _, engine := range engines {
		if engine.Name() == name {
			return engine, nil
		}
	}
	return nil, fmt.Errorf("unknown KARAKORAM_ENGINE %q", name)
}
func (a *nodeAgent) allowedProcess(name string) bool {
	if a.engine == nil {
		return name == "mongod" || name == "mongos" || name == "pbm-agent"
	}
	for _, process := range a.engine.Processes() {
		if process == name {
			return true
		}
	}
	return false
}

// applyEngine runs under applyMu, sharing the built-in runner's reload/control serialization.
func (a *nodeAgent) applyEngine(ctx context.Context, c agentapi.MemberConfig) ([]string, error) {
	cert, err := tls.X509KeyPair([]byte(c.AgentTLS.CertPEM), []byte(c.AgentTLS.KeyPEM))
	if err != nil {
		return nil, fmt.Errorf("agent TLS: %w", err)
	}
	c.Maintenance = c.Maintenance || a.controls.Maintenance
	a.sup.setMaintenance(c.Maintenance)
	changed, err := a.engine.Apply(ctx, &Node{a: a, applyConfig: &c}, c)
	if err != nil {
		return nil, err
	}
	if err = cacheConfig(c, a.o.DataDir); err != nil {
		return nil, err
	}
	versionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	version := a.engine.Version(versionCtx)
	cancel()
	a.mu.Lock()
	a.c, a.cert, a.version = c, &cert, version
	a.mu.Unlock()
	a.o.Logger.Info("member configuration applied", "version", c.ConfigVersion, "changed", changed)
	return changed, nil
}
