// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

// Observe the entire group before choosing a replacement WAL barrier. Skipping
// an unreachable peer could hide the real primary behind a stale partition.
func agentUpgradePrimary(ctx context.Context, rc *engine.Context, group groupView) (*member, pgspec.StatusResponse, error) {
	var primary *member
	var writer pgspec.StatusResponse
	for _, peer := range group.data() {
		st, err := live(ctx, rc, peer.Name)
		if err != nil {
			return nil, writer, err
		}
		if !st.Running {
			return nil, writer, engine.ErrUnavailable
		}
		if st.Role == "primary" && st.Leader == peer.Name && st.EtcdHealthy {
			if primary != nil {
				return nil, writer, engine.ErrUnavailable
			}
			primary, writer = peer, st
		} else if st.Role != "replica" || !st.Streaming {
			return nil, writer, engine.ErrUnavailable
		}
	}
	if primary == nil {
		return nil, writer, engine.ErrUnavailable
	}
	return primary, writer, nil
}

// The replica's DCS leader and local receive/replay backlog do not identify its
// WAL sender. Ask the selected primary for its own streaming/replay observation.
// Use the existing namespace-aware native dialer, monitor credential and pinned
// cluster CA; this works with existing agents and never exposes arbitrary SQL.
func agentUpgradeReplayProof(ctx context.Context, rc *engine.Context, primary, replica string, timeline int, barrier, systemID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return false, err
	}
	password, err := secret(ctx, rc, "users/kk_monitor")
	if err != nil {
		return false, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return false, fmt.Errorf("invalid cluster CA for agent rollout replay proof")
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), primary)
	if err != nil {
		return false, err
	}
	defer a.Close()
	cfg, err := pgx.ParseConfig("host=localhost port=5432 user=kk_monitor dbname=postgres sslmode=verify-full")
	if err != nil {
		return false, fmt.Errorf("initialize agent rollout replay proof")
	}
	cfg.Host, cfg.Password = primary, string(password)
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: primary}
	cfg.Fallbacks = nil
	cfg.RuntimeParams = map[string]string{"default_transaction_read_only": "on", "statement_timeout": "3000"}
	cfg.ConnectTimeout = 3 * time.Second
	cfg.DialFunc = a.DialContext
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return false, fmt.Errorf("connect to primary for agent rollout replay proof: %w", engine.ErrUnavailable)
	}
	defer c.Close(ctx)
	var proved bool
	err = c.QueryRow(ctx, `SELECT NOT pg_catalog.pg_is_in_recovery()
AND pg_catalog.substr(pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()),1,8) = $3
AND (SELECT system_identifier::text FROM pg_catalog.pg_control_system()) = $4
AND (SELECT count(*) = 1 AND bool_and(state = 'streaming' AND replay_lsn >= $2::pg_lsn)
     FROM pg_catalog.pg_stat_replication WHERE application_name = $1)`, replica, barrier, fmt.Sprintf("%08X", timeline), systemID).Scan(&proved)
	if err != nil {
		return false, fmt.Errorf("read primary agent rollout replay proof: %w", engine.ErrUnavailable)
	}
	return proved, nil
}
