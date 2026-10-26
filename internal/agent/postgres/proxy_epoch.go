// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"path/filepath"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

// RunBackground checks actual postmaster identity, not frontend demand. In
// ProxySQL 3.0.11 async_ping does no socket I/O on idle PostgreSQL connections.
func (e *Runner) RunBackground(ctx context.Context, n *agent.Node) {
	epochs := map[string]time.Time{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		check, cancel := context.WithTimeout(ctx, 12*time.Second)
		_ = n.WithLock(check, func(locked *agent.Node) error {
			s, p := e.state()
			states, maintenance := locked.Snapshot()
			if s.Role != pgspec.RoleProxy || s.Proxy == nil || maintenance || locked.Stopped("proxysql") || states["proxysql"].PID == 0 {
				return nil
			}
			query := func(ctx context.Context, sql string) ([]tableRow, error) {
				c, err := proxyConnect(ctx, s.Proxy)
				if err != nil {
					return nil, err
				}
				defer c.Close(ctx)
				return queryTable(ctx, c, sql)
			}
			recovery, err := readProxyEpochRecovery(filepath.Join(p.root, "proxy-epoch-recovery.json"), func(data []byte) error {
				return locked.WriteFile("postgres/proxy-epoch-recovery.json", data, 0600)
			})
			if err != nil {
				locked.Logger().Error("proxy epoch recovery journal unavailable", "error", err)
				return err
			}
			return checkProxyEpochs(check, s.Proxy.Servers, epochs, func(ctx context.Context, host string) (time.Time, error) {
				return proxyBackendEpoch(ctx, s, host)
			}, query, recovery, locked.Logger())
		})
		cancel()
	}
}

func proxyBackendEpoch(ctx context.Context, s pgspec.Spec, host string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return time.Time{}, err
	}
	cfg.Host, cfg.Port, cfg.Database = host, 5432, "postgres"
	cfg.User, cfg.Password = s.Proxy.Monitor.Username, s.Proxy.Monitor.Password
	cfg.ConnectTimeout = time.Second
	cfg.TLSConfig, cfg.Fallbacks = nil, nil
	cfg.RuntimeParams = map[string]string{"application_name": "karakoram-epoch"}
	if s.Proxy.BackendTLS {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(s.CAPEM)) {
			return time.Time{}, errors.New("invalid backend CA")
		}
		cfg.TLSConfig = &tls.Config{RootCAs: roots, ServerName: host, MinVersion: tls.VersionTLS12}
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return time.Time{}, err
	}
	defer c.Close(ctx)
	var epoch time.Time
	err = c.QueryRow(ctx, "SELECT pg_catalog.pg_postmaster_start_time()").Scan(&epoch)
	return epoch, err
}

type proxyEpochSource func(context.Context, string) (time.Time, error)
type proxyEpochQuery func(context.Context, string) ([]tableRow, error)

func checkProxyEpochs(ctx context.Context, hosts []string, epochs map[string]time.Time, source proxyEpochSource, query proxyEpochQuery, recovery *proxyEpochRecovery, log *slog.Logger) error {
	known := make(map[string]bool, len(hosts))
	var result error
	// Retry owned cleanup before any epoch query or ONLINE guard. In particular,
	// restoration must not depend on the backend itself being reachable.
	if err := recovery.restoreStatuses(ctx, query); err != nil {
		return err
	}
	for host, old := range recovery.pending {
		if err := recovery.restore(ctx, host, query); err != nil {
			return err
		}
		epochs[host] = old
	}
	for _, host := range hosts {
		known[host] = true
		epoch, err := source(ctx, host)
		if err != nil || epoch.IsZero() {
			continue // Unreachable is not a new postmaster.
		}
		old, observed := epochs[host]
		if observed && !old.Equal(epoch) {
			flushed, err := flushProxyEpoch(ctx, host, old, epoch, query, recovery, log)
			if err != nil {
				result = errors.Join(result, err)
				continue
			}
			if !flushed {
				continue // Native SHUNNED recovery owns the route; retry later.
			}
		}
		epochs[host] = epoch
	}
	for host := range epochs {
		if !known[host] {
			delete(epochs, host)
		}
	}
	return result
}

func proxyEpochPool(ctx context.Context, host string, query proxyEpochQuery) (uint64, error) {
	rows, err := query(ctx, "SELECT ConnFree,ConnUsed FROM stats_pgsql_connection_pool WHERE srv_host="+sqlQuote(host)+" AND srv_port=5432")
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, row := range rows {
		for _, column := range []string{"ConnFree", "ConnUsed"} {
			n, err := strconv.ParseUint(row[column], 10, 64)
			if err != nil {
				return 0, errors.New("invalid ProxySQL pool count")
			}
			total += n
		}
	}
	return total, nil
}

func flushProxyEpoch(ctx context.Context, host string, old, epoch time.Time, query proxyEpochQuery, recovery *proxyEpochRecovery, log *slog.Logger) (flushed bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	filter := " WHERE hostname=" + sqlQuote(host) + " AND port=5432"
	rows, err := query(ctx, "SELECT hostgroup_id,status FROM runtime_pgsql_servers"+filter)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	for _, row := range rows {
		if row["status"] != "ONLINE" {
			return false, nil // Never turn a native SHUNNED backend ONLINE.
		}
	}
	configured, err := query(ctx, "SELECT hostgroup_id,status FROM pgsql_servers"+filter)
	if err != nil {
		return false, err
	}
	if len(configured) == 0 {
		return false, nil
	}
	for _, row := range configured {
		if row["status"] != "ONLINE" {
			return false, nil
		}
	}
	count, err := proxyEpochPool(ctx, host, query)
	if err != nil {
		return false, err
	}
	if err = recovery.begin(host, old); err != nil {
		return false, err // No durable ownership means no administrative fence.
	}
	started := time.Now()
	defer func() {
		// A timed-out pgx operation can close its admin connection. The query
		// adapter reconnects, and restoration has its own cancellation budget.
		restore, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		err = errors.Join(err, recovery.restore(restore, host, query))
		log.Info("proxy backend epoch flush", "server", host, "old_epoch", old, "new_epoch", epoch, "duration", time.Since(started), "connections_dropped", count, "error", err)
	}()
	if _, err = query(ctx, "UPDATE pgsql_servers SET status='OFFLINE_HARD'"+filter); err != nil {
		return false, err
	}
	if err = loadProxyEpochServers(ctx, host, query, recovery); err != nil {
		return false, err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		remaining, readErr := proxyEpochPool(ctx, host, query)
		if readErr != nil {
			return false, readErr
		}
		if remaining == 0 {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ticker.C:
		}
	}
}
