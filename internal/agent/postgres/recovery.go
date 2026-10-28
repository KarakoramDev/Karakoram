// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
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
	if err = reconcileCitusAuthentication(ctx, c, s, p); err != nil {
		return err
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		return durableJSON(filepath.Join(p.root, "credentials-generation.json"), s.Bootstrap.Generation)
	}
	return nil
}

type citusAuthConfig struct {
	Credentials pgspec.Credentials
	Database    string
	Root        string
	TLS         bool
	Timeline    int
}

func citusAuthSettings(s pgspec.Spec, p paths) citusAuthConfig {
	return citusAuthConfig{Credentials: s.Superuser, Database: s.Citus.Database, Root: p.root, TLS: s.ServerCert != nil}
}

func citusAuthConninfo(config citusAuthConfig) string {
	escape := strings.NewReplacer(`\`, `\\`, "'", `\'`)
	value := func(v string) string { return "'" + escape.Replace(v) + "'" }
	info := "password=" + value(config.Credentials.Password)
	if config.TLS {
		info += " sslcert=" + value(filepath.Join(config.Root, "server.pem")) +
			" sslkey=" + value(filepath.Join(config.Root, "server.key"))
	}
	return info
}

// Call only on a writable local member. Citus's outbound authentication is
// separate from pg_authid: a restored Patroni authinfo row still holds source
// passwords and certificate paths even after system roles have been rekeyed.
func reconcileCitusAuthentication(ctx context.Context, c *pgx.Conn, s pgspec.Spec, p paths) (err error) {
	if s.Citus == nil {
		return nil
	}
	defer func() {
		if err != nil {
			err = errors.New("Citus inter-node authentication reconciliation failed")
		}
	}()
	if s.Superuser.Username == "" || s.Superuser.Password == "" {
		return errors.New("target system credentials required")
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL citus.enable_ddl_propagation = off; SET LOCAL citus.enable_metadata_sync = off"); err != nil {
		return err
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" || s.Postgres != nil && s.Postgres.RecoveryClientsHeld {
		if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = local"); err != nil {
			return err
		}
	}
	// Patroni owns initial insertion. Updating, rather than inserting, avoids
	// racing its bootstrap INSERT after CREATE EXTENSION on a fresh cluster.
	var updated pgconn.CommandTag
	updated, err = tx.Exec(ctx, "UPDATE pg_catalog.pg_dist_authinfo SET authinfo=$2 WHERE nodeid=0 AND rolename=$1", s.Superuser.Username, citusAuthConninfo(citusAuthSettings(s, p)))
	if err != nil {
		return err
	}
	if updated.RowsAffected() != 1 {
		return errors.New("Citus system authentication bootstrap is incomplete")
	}
	// Specific-node and loopback entries override node 0. Remove stale entries
	// for the managed superuser only, retaining other users' authentication.
	if _, err = tx.Exec(ctx, "DELETE FROM pg_catalog.pg_dist_authinfo WHERE rolename=$1 AND nodeid<>0", s.Superuser.Username); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Like scoped ACL reconciliation, repair restored outbound credentials before
// acknowledging a writable member as ready. Cache only successful writes;
// promotion/restart or a local credential/path change requires reconciliation.
func (e *Runner) refreshCitusAuthentication(ctx context.Context, c *pgx.Conn, s pgspec.Spec, p paths, timeline int) error {
	if s.Citus == nil {
		return nil
	}
	config := citusAuthSettings(s, p)
	config.Timeline = timeline
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.citusAuthApplied && e.citusAuth == config {
		return nil
	}
	if err := reconcileCitusAuthentication(ctx, c, s, p); err != nil {
		return err
	}
	e.citusAuth, e.citusAuthApplied = config, timeline > 0
	return nil
}

// Keep native diagnostics (which can contain SQL/passwords) out of errors while
// retaining enough classification to distinguish rejection from unavailability.
type systemRoleError struct {
	permanent bool
}

func (e *systemRoleError) Error() string {
	if e.permanent {
		return "PostgreSQL system-role transaction rejected"
	}
	return "PostgreSQL system-role transaction unavailable"
}

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
