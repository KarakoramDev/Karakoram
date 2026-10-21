// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func validateExtensionNames(names, available []string) error {
	if len(names) > 32 {
		return invalidAccess("at most 32 extensions may be requested")
	}
	for _, name := range names {
		if !pgspec.ValidExtensionName(name) || !slices.Contains(available, name) {
			return invalidAccess("invalid or unavailable extension")
		}
	}
	return nil
}

func createExtensionStatement(ctx context.Context, c *pgx.Conn, name string) (string, error) {
	var fixedSchema string
	err := c.QueryRow(ctx, `SELECT COALESCE(v.schema, '')
FROM pg_catalog.pg_available_extension_versions AS v
JOIN pg_catalog.pg_available_extensions AS e
  ON e.name = v.name AND e.default_version = v.version
WHERE v.name = $1`, name).Scan(&fixedSchema)
	if err != nil {
		return "", err
	}
	return pgprofile.CreateExtensionSQL(name, fixedSchema), nil
}
func updateExtensionStatement(name string) string {
	return "ALTER EXTENSION " + pgx.Identifier{name}.Sanitize() + " UPDATE"
}

func createDatabaseExtensions(ctx context.Context, c *pgx.Conn, db pgspec.Database, available []string) error {
	if err := validateExtensionNames(db.Extensions, available); err != nil {
		return err
	}
	if len(db.Extensions) == 0 {
		return nil
	}
	target, err := databaseConnect(ctx, c, db.Name)
	if err != nil {
		return err
	}
	defer target.Close(ctx)
	for _, name := range db.Extensions {
		statement, err := createExtensionStatement(ctx, target, name)
		if err != nil {
			return err
		}
		if _, err := target.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func updateInstalledExtensions(ctx context.Context, c *pgx.Conn, names []string) ([]pgspec.ExtensionUpdateResult, error) {
	rows, err := c.Query(ctx, "SELECT datname FROM pg_catalog.pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname")
	if err != nil {
		return nil, err
	}
	databases, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	results := make([]pgspec.ExtensionUpdateResult, 0, len(databases))
	for _, database := range databases {
		result, err := updateDatabaseExtensions(ctx, c, database, names)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func updateDatabaseExtensions(ctx context.Context, c *pgx.Conn, database string, names []string) (pgspec.ExtensionUpdateResult, error) {
	result := pgspec.ExtensionUpdateResult{Database: database, Updated: []string{}}
	target, err := databaseConnect(ctx, c, database)
	if err != nil {
		return result, err
	}
	defer target.Close(ctx)
	for _, name := range names {
		var installed bool
		if err := target.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = $1)", name).Scan(&installed); err != nil {
			return result, err
		}
		if !installed {
			continue
		}
		if _, err := target.Exec(ctx, updateExtensionStatement(name)); err != nil {
			return result, err
		}
		result.Updated = append(result.Updated, name)
	}
	return result, nil
}
