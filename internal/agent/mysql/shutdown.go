// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func (e *Runner) Shutdown(ctx context.Context, n *agent.Node) error {
	e.resetArchiveLoop(false)
	e.mu.Lock()
	if e.rejoinCancel != nil {
		e.rejoinCancel()
		e.rejoinCancel = nil
	}
	e.mu.Unlock()
	s, p := e.state()
	stopBackups(p)
	stopRestores(p)
	var err error
	if s.Role == myspec.RoleData && s.Topology == myspec.TopologyGroup {
		err = HandoffGroup(ctx, e)
	}
	e.mu.Lock()
	if e.db != nil {
		_ = e.db.Close()
		e.db = nil
	}
	e.mu.Unlock()
	return err
}
