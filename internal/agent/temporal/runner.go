// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package temporal supervises native Temporal server roles and the Web UI.
package temporal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

var _ agent.Engine = (*Runner)(nil)

// Runner supervises one native Temporal component; its zero value is ready for use.
type Runner struct {
	mu                               sync.RWMutex
	spec                             tspec.Spec
	applied                          bool
	digest                           [32]byte
	schemaVersion, visibilityVersion string
	// Trusted native-test overrides; never read from the member payload.
	installDir, bindAddress string
}

// Name identifies the member engine.
func (*Runner) Name() string { return tspec.Engine }

// Processes lists the supervised native executables.
func (*Runner) Processes() []string { return []string{"temporal-server", "ui-server"} }

// Version returns the currently applied component version.
func (r *Runner) Version(context.Context) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.spec.Version
}

// Metrics leaves application metrics on Temporal's loopback Prometheus endpoint.
func (*Runner) Metrics(context.Context, *agent.Node) []agent.Metric { return nil }

func (r *Runner) current() (tspec.Spec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.spec, r.applied
}

func (r *Runner) install(s tspec.Spec) string {
	if r.installDir != "" {
		return r.installDir
	}
	return s.InstallDir
}

func memberIPv4(ctx context.Context, database *tspec.Database) (string, error) {
	// A service DNS name may represent several replicas. Ask the kernel which
	// local address routes to the bound writer; UDP connect sends no packets.
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "udp4", net.JoinHostPort(database.Host, strconv.Itoa(database.Port)))
	if err != nil {
		return "", errors.New("discover Temporal member route failed")
	}
	defer connection.Close()
	address, ok := connection.LocalAddr().(*net.UDPAddr)
	if !ok || address.IP.To4() == nil || !address.IP.IsPrivate() {
		return "", errors.New("temporal persistence route has no private local IPv4")
	}
	return address.IP.String(), nil
}

// Apply installs private configuration and reconciles the component process.
func (r *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	if c.Engine != tspec.Engine {
		return nil, errors.New("configuration engine does not match Temporal")
	}
	var s tspec.Spec
	if len(c.EngineSpec) == 0 || json.Unmarshal(c.EngineSpec, &s) != nil {
		return nil, errors.New("invalid Temporal payload")
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	address, bind := r.bindAddress, r.bindAddress
	if address == "" {
		address = "127.0.0.1"
		if s.Component != tspec.UI {
			var err error
			address, err = memberIPv4(ctx, s.Persistence)
			if err != nil {
				return nil, err
			}
		}
		bind = "0.0.0.0"
	}
	p, files, err := render(s, n.DataDir(), address, bind, r.install(s))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(c.EngineSpec)
	r.mu.RLock()
	old, applied, changed := r.spec, r.applied, !r.applied || r.digest != digest
	r.mu.RUnlock()
	if changed && applied {
		if err := n.Action(ctx, processName(old), "stop"); err != nil {
			return nil, err
		}
	}
	for name, data := range files {
		if err := n.WriteFile(name, data, 0600); err != nil {
			return nil, err
		}
	}
	readiness, _ := json.Marshal(struct{ Component, Address string }{s.Component, address})
	if err := n.WriteFile("temporal/readiness.json", readiness, 0600); err != nil {
		return nil, err
	}
	if err := n.Configure(processName(s), p); err != nil {
		return nil, err
	}
	states, _ := n.Snapshot()
	for name := range states {
		if name != processName(s) {
			if err := n.Action(ctx, name, "stop"); err != nil {
				return nil, err
			}
		}
	}
	// BeforeStart also covers explicit restart after a maintenance/stop hold.
	r.mu.Lock()
	r.spec, r.applied = s, true
	r.mu.Unlock()
	if !c.Maintenance && !n.Stopped(processName(s)) && (c.DesiredProcesses == nil || slices.Contains(c.DesiredProcesses, processName(s))) {
		if changed || states[processName(s)].State != "running" {
			action := "start"
			if changed && applied {
				action = "restart"
			}
			if err := n.Action(ctx, processName(s), action); err != nil {
				return nil, err
			}
		}
	} else if err := n.Action(ctx, processName(s), "stop"); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.digest = digest
	r.mu.Unlock()
	if changed {
		return []string{processName(s)}, nil
	}
	return nil, nil
}

// BeforeStart ensures both SQL schemas before any server start or restart.
func (r *Runner) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	s, ok := r.current()
	if !ok || name != processName(s) {
		return errors.New("temporal configuration is not applied")
	}
	if s.Component == tspec.UI {
		return nil
	}
	version, visibility, err := ensureSchema(ctx, s, r.install(s), filepath.Join(n.DataDir(), "karakoram", "temporal"))
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.schemaVersion, r.visibilityVersion = version, visibility
	r.mu.Unlock()
	return nil
}

func readiness(ctx context.Context, component, address string) error {
	if component == tspec.UI {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:8080/", nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 3 * time.Second}
		response, err := client.Do(request)
		if err != nil {
			return errors.New("temporal UI is not listening")
		}
		response.Body.Close()
		if response.StatusCode >= 500 {
			return errors.New("temporal UI is not ready")
		}
		return nil
	}
	roles := tspec.Roles(component)
	if len(roles) == 0 {
		return errors.New("unknown Temporal readiness component")
	}
	for _, role := range roles {
		ports := tspec.Ports[role]
		for _, port := range []int{ports.GRPC, ports.Membership} {
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(address, strconv.Itoa(port)))
			if err != nil {
				return errors.New("temporal role listeners are not ready")
			}
			conn.Close()
		}
	}
	return nil
}

// Readiness probes the component selected after the shared readiness flags.
func (*Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	if len(args) != 1 || (args[0] != tspec.UI && len(tspec.Roles(args[0])) == 0) {
		return errors.New("readiness requires one Temporal component")
	}
	data, err := os.ReadFile(filepath.Join(o.DataDir, "karakoram", "temporal", "readiness.json"))
	if err != nil {
		return errors.New("temporal readiness snapshot unavailable")
	}
	var snapshot struct{ Component, Address string }
	if json.Unmarshal(data, &snapshot) != nil || snapshot.Component != args[0] || net.ParseIP(snapshot.Address) == nil {
		return errors.New("invalid Temporal readiness snapshot")
	}
	return readiness(ctx, args[0], snapshot.Address)
}

func (r *Runner) status(ctx context.Context, n *agent.Node) tspec.Status {
	s, ok := r.current()
	r.mu.RLock()
	status := tspec.Status{Component: s.Component, Version: s.Version, RestartToken: s.RestartToken, SchemaVersion: r.schemaVersion, VisibilitySchemaVersion: r.visibilityVersion}
	r.mu.RUnlock()
	if !ok {
		status.Detail = "configuration not applied"
		return status
	}
	states, maintenance := n.Snapshot()
	if maintenance || states[processName(s)].State != "running" {
		status.Detail = "process is not running"
		return status
	}
	address := r.bindAddress
	if address == "" {
		address = "127.0.0.1"
	}
	if err := readiness(ctx, s.Component, address); err != nil {
		status.Detail = err.Error()
		return status
	}
	if hasFrontend(s) {
		if _, err := frontendRequest(ctx, s, http.MethodGet, "/api/v1/namespaces/temporal-system", nil); err != nil {
			status.Detail = "frontend health is not ready"
			return status
		}
	}
	status.Ready = true
	return status
}
