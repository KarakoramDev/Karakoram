// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func bindingRole(name string) bool {
	if !strings.HasPrefix(name, "kb_") || len(name) != 35 {
		return false
	}
	_, err := hex.DecodeString(name[3:])
	return err == nil
}

func ownBindingPublicSchema(ctx context.Context, c *pgx.Conn, db pgspec.Database) error {
	if !managedDatabase(db.Name) || !bindingRole(db.Owner) {
		return invalidAccess("invalid binding database")
	}
	target, err := databaseConnect(ctx, c, db.Name)
	if err != nil {
		return err
	}
	defer target.Close(ctx)
	tx, err := target.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var owner string
	if err := tx.QueryRow(ctx, "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()").Scan(&owner); err != nil {
		return err
	}
	if owner != db.Owner {
		return invalidAccess("binding database owner does not match")
	}
	if _, err := tx.Exec(ctx, "ALTER SCHEMA public OWNER TO "+pgx.Identifier{db.Owner}.Sanitize()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
