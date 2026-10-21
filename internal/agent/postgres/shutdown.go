// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func (e *Runner) ShutdownBudget() time.Duration {
	s, _ := e.state()
	if s.Role == pgspec.RoleProxy {
		return proxyDrainTimeout
	}
	return 4 * time.Second
}

// Shutdown requests a planned handoff before Patroni or its local etcd voter is
// stopped. This path is shared by every backend; it never removes DCS members,
// deletes the leader key, or promotes PostgreSQL outside Patroni's safety checks.
func (e *Runner) Shutdown(ctx context.Context, n *agent.Node) error {
	return n.WithLock(ctx, func(locked *agent.Node) error {
		// Local/CP stop intent belongs to the lifecycle operation, not to
		// runtime-termination failover. In particular, never undo writer-last
		// upgrade ordering or an explicit Patroni stop with a new switchover.
		c := locked.Config()
		s, _ := e.state()
		if s.Role == pgspec.RoleProxy {
			if c.Maintenance || locked.Maintenance() || locked.Stopped("proxysql") {
				locked.Logger().Info("proxy drain skipped", "reason", "lifecycle hold")
				return nil
			}
			return drainProxy(ctx, s, locked.Logger())
		}
		if c.Maintenance || locked.Maintenance() || locked.Stopped("patroni") ||
			c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "patroni") {
			return nil
		}
		return gracefulHandoff(ctx, s, patroniRequest)
	})
}

type patroniRequester func(context.Context, pgspec.Spec, string, string, any, any) error

func gracefulHandoff(ctx context.Context, s pgspec.Spec, request patroniRequester) error {
	if s.Role == pgspec.RoleProxy || !usesPatroni(s) || s.Retiring {
		return nil
	}
	var cluster patroniCluster
	if err := request(ctx, s, http.MethodGet, "/cluster", nil, &cluster); err != nil {
		return err
	}
	if cluster.Pause {
		return errors.New("patroni is paused; skipping planned handoff")
	}
	leader := ""
	timeline := 0
	for _, m := range cluster.Members {
		if m.Role == "leader" || m.Role == "primary" {
			leader = m.Name
			timeline = m.Timeline
			break
		}
	}
	if leader != s.Member {
		return nil
	}
	candidate := ""
	var newest uint64
	synchronous := s.Postgres.Synchronous == pgspec.SyncOn || s.Postgres.Synchronous == pgspec.SyncStrict
	for _, m := range cluster.Members {
		if m.Name == s.Member || m.State != "streaming" || m.Tags.failoverDisabled() || timeline != 0 && m.Timeline != timeline {
			continue
		}
		sync := m.Role == "sync_standby" || m.Role == "quorum_standby"
		if !sync && (synchronous || m.Role != "replica") {
			continue
		}
		lsn, ok := parseLSN(m.ReceiveLSN)
		if !ok {
			continue
		}
		if candidate == "" || lsn > newest || lsn == newest && m.Name < candidate {
			candidate, newest = m.Name, lsn
		}
	}
	if candidate == "" {
		return errors.New("no streaming eligible standby for planned handoff")
	}
	// /switchover validates the current leader, candidate health, sync membership
	// and WAL safety again. Never fall back to /failover if the request times out.
	return request(ctx, s, http.MethodPost, "/switchover", map[string]string{"leader": s.Member, "candidate": candidate}, nil)
}
