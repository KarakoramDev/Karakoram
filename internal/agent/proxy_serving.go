// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"time"
)

// proxyServingAddress is container-local; neither service DNS nor a persisted
// readiness marker may admit a replacement whose supervisor has not applied config.
func proxyServingAddress(dir string) string {
	return fmt.Sprintf("@karakoram-proxy-serving-%x", sha256.Sum256([]byte(filepath.Clean(dir))))
}

func proxyServingReadiness(ctx context.Context, dir string) error {
	if !filepath.IsAbs(dir) {
		return errors.New("serving proxy readiness requires an absolute data directory")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", proxyServingAddress(dir))
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err = conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	var response [1]byte
	if _, err = io.ReadFull(conn, response[:]); err != nil {
		return err
	}
	if response[0] != 1 {
		return errors.New("proxy has not applied a serving configuration and admitted an online writer")
	}
	return nil
}

func (a *nodeAgent) serveProxyServing() (net.Listener, error) {
	admission, err := proxyAdmissionForContainer(a.o.DataDir)
	if err != nil {
		return nil, err
	}
	return a.serveProxyServingAdmission(admission)
}

func (a *nodeAgent) serveProxyServingAdmission(admission proxyContainerAdmission) (net.Listener, error) {
	admitted, err := admission.admitted()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", proxyServingAddress(a.o.DataDir))
	if err != nil {
		return nil, err
	}
	go func() {
		// Admission belongs to this container/task, not an agent process. Its
		// provenance-bound writable-layer record survives an agent-only re-exec,
		// but a new task must earn native admission before receiving traffic.
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if !admitted {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				if a.proxyServingHealth(ctx) == nil {
					admitted = admission.record() == nil
				}
				cancel()
			}
			// Post-admission health is supervisor liveness only. In particular,
			// do not take applyMu: a long config apply/maintenance operation is
			// not evidence that this already admitted agent has died.
			alive := !a.sup.closing.Load()
			response := byte(0)
			if admitted && alive {
				response = 1
			}
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write([]byte{response})
			_ = conn.Close()
		}
	}()
	return listener, nil
}

func (a *nodeAgent) proxyServingHealth(ctx context.Context) error {
	// Serialize with config publication and controls. The cache and live runner
	// must describe the same successfully applied revision throughout this check.
	if err := a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer a.applyMu.Unlock()
	c := a.config()
	if a.engine == nil || c.Role != "proxy" || c.ConfigVersion <= 0 || c.Maintenance || a.controlsErr != nil || a.controls.Maintenance || a.controls.Stopped["proxysql"] || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "proxysql") {
		return errors.New("proxy serving intent is unavailable or held")
	}
	a.mu.RLock()
	applyFailed := a.lastError != "" || a.applyRetry != nil
	a.mu.RUnlock()
	if applyFailed || a.acceptedVersion != c.ConfigVersion {
		return errors.New("latest accepted proxy configuration has not been applied")
	}
	states, maintenance := a.sup.snapshot()
	if maintenance || states["proxysql"].PID == 0 || states["proxysql"].State != "running" {
		return errors.New("proxy process is not running")
	}
	if err := a.startupHealth(); err != nil {
		return err
	}
	// PostgreSQL's ordinary deployment readiness is transport-only; its native
	// serving hook additionally requires writer admission. Do not silently fall
	// back to a weaker probe on an engine that does not implement that hook.
	if probe, ok := a.engine.(interface {
		ProxyServingReadiness(context.Context) error
	}); ok {
		return probe.ProxyServingReadiness(ctx)
	}
	if a.engine.Name() == "mysql" {
		return a.engine.Readiness(ctx, a.o, nil)
	}
	return errors.New("engine does not support native proxy serving readiness")
}
