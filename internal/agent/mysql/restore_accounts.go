// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type restoreAccounts struct {
	Users    []myspec.RecoveryUser
	Existing []string
}
type recoveryAccount struct {
	Name       string
	Statements []accessStatement
}

func recoveryAccountPlan(users []myspec.RecoveryUser, existing map[string]bool) ([]recoveryAccount, error) {
	result := make([]recoveryAccount, 0, len(users))
	seen := map[string]bool{}
	for _, user := range users {
		if !myspec.ManagedUser(user.Name) || seen[user.Name] || !validPassword(user.Current) || user.Secondary != "" && !validPassword(user.Secondary) {
			return nil, errors.New("invalid recovery account")
		}
		seen[user.Name] = true
		secondary, present := existing[user.Name]
		if !present {
			continue
		}
		entry := recoveryAccount{Name: user.Name}
		if secondary {
			entry.Statements = append(entry.Statements, accessStatement{prefix: "ALTER USER " + account(user.Name) + " DISCARD OLD PASSWORD"})
		}
		password := user.Current
		if user.Secondary != "" {
			password = user.Secondary
		}
		entry.Statements = append(entry.Statements, accessStatement{prefix: "ALTER USER " + account(user.Name) + " IDENTIFIED BY ", password: &password, suffix: " REQUIRE SSL"})
		if user.Secondary != "" {
			current := user.Current
			entry.Statements = append(entry.Statements, accessStatement{prefix: "ALTER USER " + account(user.Name) + " IDENTIFIED BY ", password: &current, suffix: " RETAIN CURRENT PASSWORD REQUIRE SSL"})
		}
		result = append(result, entry)
	}
	return result, nil
}

// This connection belongs to the isolated --skip-networking restore server.
// Password recovery must not add GTIDs or replicate into a predecessor group.
func rekeyRestoredAccounts(ctx context.Context, db *sql.DB, users []myspec.RecoveryUser) ([]string, error) {
	restored := make([]string, 0, len(users))
	if len(users) == 0 {
		return restored, nil
	}
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, "SET SESSION sql_log_bin=0"); err != nil {
		return nil, err
	}
	if _, err = c.ExecContext(ctx, "SET SESSION sql_mode='STRICT_ALL_TABLES'"); err != nil {
		return nil, err
	}
	rows, err := c.QueryContext(ctx, "SELECT User,COALESCE(JSON_CONTAINS_PATH(User_attributes,'one','$.additional_password'),0) FROM mysql.user WHERE Host='%'")
	if err != nil {
		return nil, err
	}
	existing := map[string]bool{}
	for rows.Next() {
		var name string
		var secondary bool
		if err = rows.Scan(&name, &secondary); err != nil {
			rows.Close()
			return nil, err
		}
		existing[name] = secondary
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	plan, err := recoveryAccountPlan(users, existing)
	if err != nil {
		return nil, err
	}
	for _, entry := range plan {
		for _, statement := range entry.Statements {
			if err = executeUserStatement(ctx, c, statement); err != nil {
				return nil, err
			}
		}
		restored = append(restored, entry.Name)
	}
	return restored, nil
}
