// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Explicit process starts do not run Apply. Complete the credential transition
// before the control plane removes temporary source authentication or starts
// replicas, even when recovery is resumed without a configuration-pull cycle.
func rekeyRecoveredSystemRoles(ctx context.Context, s pgspec.Spec, p paths) error {
	c, err := localConnect(ctx, s, p)
	if err != nil {
		return errors.New("recovered PostgreSQL authentication unavailable")
	}
	defer c.Close(ctx)
	var recovery bool
	if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil || recovery {
		return errors.New("system-role rekey requires the recovered primary")
	}
	if usesPatroni(s) {
		if err = patroniRequest(ctx, s, "GET", "/primary", nil, nil); err != nil {
			return errors.New("system-role rekey requires the Patroni primary")
		}
	}
	if err = bootstrapRoles(ctx, c, s); err != nil {
		return err
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		return durableJSON(filepath.Join(p.root, "credentials-generation.json"), s.Bootstrap.Generation)
	}
	return nil
}
