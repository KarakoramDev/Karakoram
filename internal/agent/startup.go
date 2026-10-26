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
	"time"
)

// startupAddress uses Linux's abstract Unix socket namespace: SIGKILL cannot
// leave a stale readiness marker, and neither volume contents nor peer DNS are
// needed. The address identifies only this container's local data directory.
func startupAddress(dir string) string {
	return fmt.Sprintf("@karakoram-startup-%x", sha256.Sum256([]byte(filepath.Clean(dir))))
}

// serveStartup marks the running supervisor as initialising before its first
// configuration pull. A database may be waiting for configuration, DNS, or a
// quorum; none of those may retain a platform deployment admission slot.
// Native engine health remains the control plane's responsibility.
func serveStartup(dir string, health func() error) (net.Listener, error) {
	listener, err := net.Listen("unix", startupAddress(dir))
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			response := byte(1)
			if health() != nil {
				response = 0
			}
			_, _ = conn.Write([]byte{response})
			_ = conn.Close()
		}
	}()
	return listener, nil
}

func startupReadiness(ctx context.Context, dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("startup readiness requires an absolute data directory")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", startupAddress(dir))
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
		return fmt.Errorf("agent local initialization is unavailable")
	}
	return nil
}

func (a *nodeAgent) setStartupError(err error) {
	a.mu.Lock()
	a.startupErr = err
	a.mu.Unlock()
}

func (a *nodeAgent) startupHealth() error {
	a.mu.RLock()
	err := a.startupErr
	a.mu.RUnlock()
	if err != nil {
		return err
	}
	a.sup.mu.Lock()
	defer a.sup.mu.Unlock()
	if a.sup.closed {
		return errors.New("supervisor is shut down")
	}
	for _, process := range a.sup.children {
		if process.desired && process.startError != nil {
			return process.startError
		}
	}
	return nil
}
