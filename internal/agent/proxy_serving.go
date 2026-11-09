// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
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
	if err := proxyServingPeer(conn.(*net.UnixConn)); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err = conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	var response [1]byte
	if _, err = io.ReadFull(conn, response[:]); err != nil {
		return err
	}
	if response[0] != 1 {
		diagnostic, err := io.ReadAll(io.LimitReader(conn, 4096))
		if err == nil && len(diagnostic) > 0 {
			return fmt.Errorf("proxy serving readiness unavailable: %s", diagnostic)
		}
		return errors.New("proxy has not applied a serving configuration and admitted an online writer")
	}
	return nil
}

// Docker's Swarm executor can treat a zero-exit container as successfully
// started even when it stopped that container for failing its initial healthcheck.
// Never let a task that has not earned native admission exit successfully.
func proxyServingShutdownError(o Options) error {
	admission, err := proxyAdmissionForContainer(o.DataDir)
	if o.ProxyServingAdmission {
		admitted, err := proxyControlAdmitted(o)
		if err == nil && admitted {
			return nil
		}
		if err != nil {
			return fmt.Errorf("proxy stopped without verified control-plane admission: %w", err)
		}
		return errors.New("proxy stopped before control-plane serving admission")
	}
	if err == nil {
		var admitted bool
		admitted, err = admission.admitted()
		if err == nil && admitted {
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("proxy stopped without verified serving admission: %w", err)
	}
	return errors.New("proxy stopped before serving admission")
}

func (a *nodeAgent) startProxyServing() net.Listener {
	// The runtime marker is available before the first config pull; ordinary
	// data agents must not depend on or create proxy admission state.
	if !a.o.ProxyServingProbe {
		return nil
	}
	listener, err := a.serveProxyServing()
	if err != nil {
		a.o.Logger.Error("proxy serving readiness unavailable", "error", err)
	}
	return listener
}

func (a *nodeAgent) serveProxyServing() (net.Listener, error) {
	admission, err := proxyAdmissionForContainer(a.o.DataDir)
	if a.o.ProxyServingAdmission {
		admitted, err := proxyControlAdmitted(a.o)
		return a.serveProxyServingState(admission, admitted, err)
	}
	if err != nil {
		return a.serveProxyServingState(admission, false, fmt.Errorf("initialize proxy admission: %w", err))
	}
	return a.serveProxyServingAdmission(admission)
}

func (a *nodeAgent) serveProxyServingAdmission(admission proxyContainerAdmission) (net.Listener, error) {
	admitted, admissionErr := admission.admitted()
	if admissionErr != nil {
		admissionErr = fmt.Errorf("initialize proxy admission: %w", admissionErr)
	}
	return a.serveProxyServingState(admission, admitted, admissionErr)
}

func (a *nodeAgent) serveProxyServingState(admission proxyContainerAdmission, admitted bool, admissionErr error) (net.Listener, error) {
	// Keep provenance in the container writable layer, not a persistent data
	// volume. Pre-emption or corruption must hold admission, never agent startup.
	if admissionErr != nil && a.o.Logger != nil {
		a.o.Logger.Error("proxy serving readiness unavailable", "error", admissionErr)
	}
	a.proxyAdmitted.Store(admitted)
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
			if !a.o.ProxyServingAdmission && !admitted && admissionErr == nil {
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
			if (admitted || a.proxyAdmitted.Load()) && alive {
				response = 1
			}
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write([]byte{response})
			if admissionErr != nil {
				_, _ = io.WriteString(conn, admissionErr.Error())
			}
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
	return a.proxyServingHealthLocked(ctx)
}

func (a *nodeAgent) proxyServingHealthLocked(ctx context.Context) error {
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

// The authenticated record extends container provenance, rather than trusting a
// locally writable boolean. Its key is the sealed per-member agent credential;
// copied records and unsigned legacy markers cannot admit a different task.
func proxyControlRecord(o Options, revision int64) (proxyContainerAdmission, error) {
	if o.TaskID == "" || o.Token == "" || revision <= 0 {
		return proxyContainerAdmission{}, errors.New("control-plane admission requires task identity, credential and applied revision")
	}
	admission, err := proxyAdmissionForContainer(o.DataDir)
	if err != nil {
		return admission, err
	}
	mac := hmac.New(sha256.New, []byte(o.Token))
	_, _ = fmt.Fprintf(mac, "proxy-control-admission-v1\x00%s\x00%s\x00%d", o.TaskID, admission.identity, revision)
	admission.identity = fmt.Sprintf("v2:%d:%x\n", revision, mac.Sum(nil))
	return admission, nil
}

func proxyControlAdmitted(o Options) (bool, error) {
	record, err := proxyControlRecord(o, 1)
	if err != nil {
		return false, err
	}
	data, err := record.readRecord(256)
	if err != nil || len(data) == 0 {
		return false, err
	}
	fields := strings.Split(string(data), ":")
	if len(fields) != 3 || fields[0] != "v2" {
		return false, nil
	}
	revision, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || revision <= 0 {
		return false, nil
	}
	record, err = proxyControlRecord(o, revision)
	if err != nil {
		return false, err
	}
	return hmac.Equal(data, []byte(record.identity)), nil
}

func (a *nodeAgent) proxyServingProof(ctx context.Context) (agentapi.ProxyServingProof, error) {
	proof := agentapi.ProxyServingProof{TaskID: a.o.TaskID, Member: a.o.Member}
	// A marked fixed agent running under an older CP still exposes protocol
	// capability, so a later fixed CP can safely preflight count/resize before
	// replacing it with an opt-in task. Reading proof never grants admission.
	if !a.o.ProxyServingProbe || a.o.ProxyServingAdmission && a.o.TaskID == "" {
		return proof, errors.New("proxy serving admission identity is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := a.applyMu.LockContext(ctx); err != nil {
		return proof, err
	}
	defer a.applyMu.Unlock()
	c := a.config()
	proof.ConfigVersion = c.ConfigVersion
	proof.Admitted = a.proxyAdmitted.Load()
	proof.Ready = c.Member == a.o.Member && a.proxyServingHealthLocked(ctx) == nil
	return proof, nil
}

func (a *nodeAgent) admitProxyServing(ctx context.Context, request agentapi.ProxyServingAdmission) error {
	if !a.o.ProxyServingProbe || !a.o.ProxyServingAdmission || a.o.TaskID == "" || request.TaskID != a.o.TaskID || request.ConfigVersion <= 0 {
		return errors.New("proxy admission task identity does not match this task")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer a.applyMu.Unlock()
	c := a.config()
	if c.Member != a.o.Member || c.ConfigVersion != request.ConfigVersion || a.acceptedVersion != request.ConfigVersion {
		return errors.New("proxy admission does not match the current applied member configuration")
	}
	if a.proxyAdmitted.Load() {
		return nil
	}
	if err := a.proxyServingHealthLocked(ctx); err != nil {
		return err
	}
	record, err := proxyControlRecord(a.o, request.ConfigVersion)
	if err != nil {
		return err
	}
	if err := record.record(); err != nil {
		return err
	}
	a.proxyAdmitted.Store(true)
	return nil
}
