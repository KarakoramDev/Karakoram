// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// ProxyServingReadiness is the native half of the supervisor's serving probe.
// Its caller holds the apply lock and checks the applied configuration revision,
// startup health, process state and maintenance/stop holds. Ordinary bootstrap
// Readiness intentionally remains a frontend-listener check.
func (e *Runner) ProxyServingReadiness(ctx context.Context) error {
	s, p := e.state()
	if s.Role != pgspec.RoleProxy || s.Proxy == nil || len(s.Proxy.Servers) == 0 {
		return errors.New("proxy serving configuration is unavailable")
	}
	// Reuse native status admission, including its bounded read-only writer probe
	// and runtime managed-user verification, rather than treating TCP as serving.
	status := e.status(ctx, s, p)
	if !status.Running || !proxyWriterReady(s.Proxy, status.ProxyBackends) {
		return errors.New("proxy has not admitted a native ONLINE writer")
	}
	return nil
}
