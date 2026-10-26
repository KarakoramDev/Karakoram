// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

const (
	proxyDrainDetectionWindow = 10 * time.Second
	proxyDrainTimeout         = 25 * time.Second
)

// drainProxy runs only on agent termination, before the supervisor signals the
// native process. A crash/SIGKILL never enters this path. Do not spend a drain
// window on a proxy which has already lost its frontend or writer route.
func drainProxy(ctx context.Context, s pgspec.Spec, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, proxyDrainTimeout)
	defer cancel()
	frontend, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", frontendAddress(s.ListenHost))
	if err != nil {
		log.Info("proxy drain skipped", "reason", "frontend unavailable")
		return nil
	}
	_ = frontend.Close()
	c, err := proxyConnect(ctx, s.Proxy)
	if err != nil {
		log.Info("proxy drain failed", "stage", "admin connect")
		return err
	}
	defer c.Close(context.Background())
	return drainProxyQueries(ctx, s.Proxy, log, func(ctx context.Context, sql string) ([]tableRow, error) {
		return queryTable(ctx, c, sql)
	})
}

func drainProxyQueries(ctx context.Context, p *pgspec.Proxy, log *slog.Logger, query func(context.Context, string) ([]tableRow, error)) (err error) {
	started := time.Now()
	paused := false
	defer func() {
		if paused && (errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)) {
			log.Warn("proxy drain bound reached; remaining sessions may be terminated", "budget", proxyDrainTimeout)
		}
		log.Info("proxy drain finished", "paused", paused, "duration", time.Since(started), "error", err)
	}()
	// Bound the health check, not PAUSE: native listener removal waits for
	// worker acknowledgements and can exceed an ordinary admin query timeout.
	prepare, cancel := context.WithTimeout(ctx, 3*time.Second)
	backends, err := query(prepare, "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers")
	cancel()
	if err != nil {
		return err
	}
	writer := false
	for _, backend := range backends {
		if backend["hostgroup_id"] == "10" && backend["status"] == "ONLINE" {
			writer = true
		}
	}
	if len(p.Servers) == 0 || !writer {
		log.Info("proxy drain skipped", "reason", "writer unavailable")
		return nil
	}
	// PAUSE closes listeners, but (unlike STOP/SIGTERM) preserves established
	// PostgreSQL sessions. Its acknowledgement shares the complete drain budget;
	// aborting it at the health-check deadline would signal established sessions
	// before they had a chance to finish. The external LB must retry refused TCP
	// connections on another backend while detecting the closed listener.
	if _, err := query(ctx, "PROXYSQL PAUSE"); err != nil {
		return err
	}
	paused = true
	log.Info("proxy drain paused", "minimumWait", proxyDrainDetectionWindow, "budget", proxyDrainTimeout)
	detection := time.NewTimer(proxyDrainDetectionWindow)
	defer detection.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-detection.C:
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var previous uint64
	observed := false
	for {
		rows, err := query(ctx, "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='Client_Connections_connected'")
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return errors.New("proxy drain session count unavailable")
		}
		count, err := strconv.ParseUint(rows[0]["Variable_Value"], 10, 64)
		if err != nil {
			return errors.New("proxy drain session count invalid")
		}
		if !observed || count != previous {
			log.Info("proxy drain sessions", "connected", count)
			previous, observed = count, true
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
