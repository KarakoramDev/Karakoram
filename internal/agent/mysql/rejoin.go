// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Recovery credentials supplied to START GROUP_REPLICATION are memory-only.
// Observe supervisor child identities, not remote liveness: each restarted child
// gets one authenticated join attempt, and no path here can bootstrap a group.
func (e *Runner) ensureRejoinWatcher() {
	e.mu.Lock()
	if (e.current.Topology != myspec.TopologyGroup && e.current.Galera == nil) || e.rejoinCancel != nil {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.rejoinCancel = cancel
	e.mu.Unlock()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastPID := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			s, p := e.state()
			e.mu.RLock()
			n := e.node
			e.mu.RUnlock()
			if s.Galera != nil {
				if n != nil {
					attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
					_ = n.WithLock(attempt, func(_ *agent.Node) error {
						_, err := e.galeraStatus(attempt)
						return err
					})
					cancel()
				}
				continue
			}
			if n == nil || s.Role != myspec.RoleData || s.Topology != myspec.TopologyGroup || !GroupJoined(p.root, s) || n.Maintenance() || n.Stopped("mysqld") {
				continue
			}
			states, _ := n.Snapshot()
			pid := states["mysqld"].PID
			if pid == 0 || pid == lastPID {
				continue
			}
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, err := e.LocalAccessDB(attempt)
			if err == nil {
				lastPID = pid
				err = StartGroup(attempt, e)
			}
			cancel()
			if err != nil && ctx.Err() == nil {
				n.Logger().Warn("MySQL group rejoin unavailable after process restart")
			}
		}
	}()
}
