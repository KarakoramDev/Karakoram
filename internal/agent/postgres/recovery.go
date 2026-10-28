// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5/pgconn"
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

// Keep native diagnostics (which can contain SQL/passwords) out of errors while
// retaining enough classification to distinguish rejection from unavailability.
type systemRoleError struct {
	permanent bool
}

func (*systemRoleError) Error() string { return "PostgreSQL system-role transaction rejected" }

func systemRoleFailure(err error) error {
	var native *pgconn.PgError
	permanent := false
	if errors.As(err, &native) && len(native.Code) == 5 {
		switch native.Code[:2] {
		case "0A", "22", "23", "28", "2B", "3D", "3F", "42":
			permanent = true
		}
	}
	return &systemRoleError{permanent: permanent}
}

func rekeyErrorResponse(err error) (int, string) {
	var failure *systemRoleError
	if errors.As(err, &failure) && failure.permanent {
		return http.StatusUnprocessableEntity, failure.Error()
	}
	return http.StatusServiceUnavailable, "PostgreSQL system-role rekey unavailable"
}
