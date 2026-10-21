// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/jackc/pgx/v5"
)

// The lock is scoped to the persistence database, shared by all role members.
const schemaLock int64 = 0x4b4b54454d504f52

func databaseConfig(d *tspec.Database, database string) (*pgx.ConnConfig, error) {
	c, err := pgx.ParseConfig("")
	if err != nil {
		return nil, errors.New("initialize PostgreSQL connection")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(d.CAPEM)) {
		return nil, errors.New("invalid PostgreSQL CA")
	}
	c.Host, c.Port, c.Database, c.User, c.Password = d.Host, uint16(d.Port), database, d.Username, d.Password
	c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: d.Host}
	c.Fallbacks = nil
	c.ConnectTimeout = 10 * time.Second
	return c, nil
}

func toolError(err error) error {
	if err == nil {
		return nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return fmt.Errorf("temporal schema tool failed (exit %d; output withheld)", exit.ExitCode())
	}
	return errors.New("temporal schema tool could not run (details withheld)")
}

func runSchemaTool(ctx context.Context, s tspec.Spec, install, dir, database string, command ...string) error {
	d := s.Persistence
	args := []string{"--endpoint", d.Host, "--port", strconv.Itoa(d.Port), "--user", d.Username, "--database", database, "--plugin", "postgres12_pgx", "--tls", "--tls-ca-file", filepath.Join(dir, "postgres-ca.pem"), "--tls-server-name", d.Host}
	args = append(args, command...)
	cmd := exec.CommandContext(ctx, filepath.Join(install, "temporal-sql-tool"), args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "GOMAXPROCS=2", "SQL_PASSWORD=" + d.Password}
	// Upstream tools can include connection strings in errors and log output.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	return toolError(cmd.Run())
}

func readSchema(ctx context.Context, d *tspec.Database, database string) (string, error) {
	c, err := databaseConfig(d, database)
	if err != nil {
		return "", err
	}
	conn, err := pgx.ConnectConfig(ctx, c)
	if err != nil {
		return "", errors.New("connect to schema database failed")
	}
	defer conn.Close(context.Background())
	var version string
	if conn.QueryRow(ctx, "SELECT curr_version FROM schema_version WHERE version_partition = 0 AND db_name = $1", database).Scan(&version) != nil {
		return "", errors.New("read Temporal schema version failed")
	}
	return version, nil
}

func ensureSchema(ctx context.Context, s tspec.Spec, install, dir string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	config, err := databaseConfig(s.Persistence, s.Persistence.Database)
	if err != nil {
		return "", "", err
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return "", "", errors.New("connect to Temporal persistence failed")
	}
	// Closing the dedicated session always releases its advisory lock, including
	// cancellation or migration failure; no pool can retain the session lock.
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn.Close(closeCtx)
	}()
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", schemaLock); err != nil {
		return "", "", errors.New("acquire Temporal schema lock failed")
	}
	for _, db := range []struct{ name, schema string }{{s.Persistence.Database, "temporal"}, {s.Persistence.VisibilityDatabase, "visibility"}} {
		if err = runSchemaTool(ctx, s, install, dir, db.name, "setup-schema", "-v", "0.0"); err != nil {
			return "", "", err
		}
		if err = runSchemaTool(ctx, s, install, dir, db.name, "update-schema", "--schema-name", "postgresql/v12/"+db.schema); err != nil {
			return "", "", err
		}
	}
	version, err := readSchema(ctx, s.Persistence, s.Persistence.Database)
	if err != nil {
		return "", "", err
	}
	visibility, err := readSchema(ctx, s.Persistence, s.Persistence.VisibilityDatabase)
	return version, visibility, err
}
