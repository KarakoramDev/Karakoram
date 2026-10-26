// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

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
	// The maintenance connection can set ACLs even while a newly created
	// database refuses all connections. Publish ACLs and enable access together.
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var owner string
	var ownerOID uint32
	if err := tx.QueryRow(ctx, "SELECT pg_get_userbyid(datdba),datdba FROM pg_database WHERE datname=$1", db.Name).Scan(&owner, &ownerOID); err != nil {
		return err
	}
	if owner != db.Owner || db.OwnerOID != 0 && ownerOID != db.OwnerOID {
		return &accessError{http.StatusConflict, "binding database owner does not match"}
	}
	for _, statement := range bindingDatabaseStatements(db) {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// REVOKE prevents new logins but does not revoke existing sessions. Drain
	// only roles without CONNECT; named grants and administrators stay online.
	if err := drainBindingSessions(ctx, c, db.Name); err != nil {
		return err
	}
	target, err := databaseConnect(ctx, c, db.Name)
	if err != nil {
		return err
	}
	defer target.Close(ctx)
	_, err = target.Exec(ctx, "ALTER SCHEMA public OWNER TO "+pgx.Identifier{db.Owner}.Sanitize())
	return err
}

func bindingDatabaseStatements(db pgspec.Database) []string {
	database := pgx.Identifier{db.Name}.Sanitize()
	owner := pgx.Identifier{db.Owner}.Sanitize()
	return []string{
		"REVOKE CONNECT, TEMPORARY ON DATABASE " + database + " FROM PUBLIC",
		"GRANT CONNECT, TEMPORARY ON DATABASE " + database + " TO " + owner,
		"ALTER DATABASE " + database + " WITH ALLOW_CONNECTIONS true",
	}
}

const foreignBindingSessions = ` FROM pg_catalog.pg_stat_activity a
 JOIN pg_catalog.pg_roles r ON r.oid=a.usesysid
 WHERE a.datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=$1)
 AND NOT r.rolsuper AND NOT pg_catalog.has_database_privilege(r.oid,a.datid,'CONNECT')`

func drainBindingSessions(ctx context.Context, c *pgx.Conn, database string) error {
	for {
		if _, err := c.Exec(ctx, "SELECT pg_catalog.pg_terminate_backend(a.pid)"+foreignBindingSessions, database); err != nil {
			return err
		}
		var remaining int
		if err := c.QueryRow(ctx, "SELECT count(*)"+foreignBindingSessions, database).Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
