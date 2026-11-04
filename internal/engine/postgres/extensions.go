// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

var contribExtensions = []string{"pgcrypto", "uuid-ossp", "pg_stat_statements", "hstore", "citext", "btree_gist", "btree_gin", "unaccent", "fuzzystrmatch", "pg_trgm", "tablefunc", "intarray", "ltree", "cube", "earthdistance"}

func (p *Core) availableExtensions(ctx context.Context, rc *engine.Context) ([]string, error) {
	if p.options.AvailableExtensions != nil {
		return p.options.AvailableExtensions(ctx, rc)
	}
	return contribExtensions, nil
}

func validateExtensions(names, available []string) error {
	if len(names) > 32 {
		return engine.Invalid("at most 32 extensions may be requested")
	}
	for _, name := range names {
		if !pgspec.ValidExtensionName(name) || !slices.Contains(available, name) {
			return engine.Invalid("extension %q is not available for this cluster", name)
		}
	}
	return nil
}

func (p *Core) updateExtensions(ctx context.Context, rc *engine.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	available, err := p.availableExtensions(ctx, rc)
	if err != nil {
		return err
	}
	if err := validateExtensions(names, available); err != nil {
		return err
	}
	var results []pgspec.ExtensionUpdateResult
	if err := p.accessAction(ctx, rc, pgspec.ActionUpdateExtensions, pgspec.AccessRequest{Extensions: names, AvailableExtensions: available}, &results); err != nil {
		return fmt.Errorf("update PostgreSQL extensions: %w", err)
	}
	return nil
}

func (p *Core) updateProfileExtensions(ctx context.Context, rc *engine.Context) error {
	if p.options.ProfileExtensions == nil {
		return nil
	}
	return p.updateExtensions(ctx, rc, p.options.ProfileExtensions(rc))
}

// EnsureDatabaseExtensions resumes extension creation on an existing database,
// refusing to adopt a database owned by a different role.
func (p *Core) EnsureDatabaseExtensions(ctx context.Context, rc *engine.Context, v Database) error {
	if err := identifier(v.Name); err != nil {
		return err
	}
	if err := identifier(v.Owner); err != nil {
		return err
	}
	if reservedDatabase(v.Name) {
		return engine.Invalid("database is reserved")
	}
	available, err := p.availableExtensions(ctx, rc)
	if err != nil {
		return err
	}
	if err := validateExtensions(v.Extensions, available); err != nil {
		return err
	}
	if len(v.Extensions) == 0 {
		return nil
	}
	return p.accessAction(ctx, rc, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "extensions", Database: &v, AvailableExtensions: available}, nil)
}
