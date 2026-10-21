// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func (e *Runner) reload(ctx context.Context, db *sql.DB, s, previous myspec.Spec) error {
	if _, err := db.ExecContext(ctx, "ALTER INSTANCE RELOAD TLS"); err != nil {
		return errors.New("MySQL TLS reload failed")
	}
	parameters := make(map[string]string, len(s.Parameters)+2)
	parameters["max_connections"] = strconv.Itoa(s.MaxConnections)
	parameters["innodb_buffer_pool_size"] = strconv.FormatInt(s.BufferPoolBytes, 10)
	for k, v := range s.Parameters {
		parameters[k] = v
	}
	parameters["innodb_fast_shutdown"] = "1"
	if s.SlowShutdown {
		parameters["innodb_fast_shutdown"] = "0"
	}
	for k := range previous.Parameters {
		if myspec.RestartRequiredParameter(k) {
			continue
		}
		if _, retained := parameters[k]; !retained {
			if _, err := db.ExecContext(ctx, "SET GLOBAL "+quoteIdentifier(k)+" = DEFAULT"); err != nil {
				return errors.New("MySQL dynamic variable reset failed: " + k)
			}
		}
	}
	keys := make([]string, 0, len(parameters))
	for k := range parameters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if myspec.RestartRequiredParameter(k) {
			continue
		}
		// MySQL requires numeric system variables to have a numeric SQL type;
		// a bound Go string is rejected even when it contains only digits.
		value := parameters[k]
		if k == "transaction_isolation" {
			value = sqlLiteral(value)
		}
		if _, err := db.ExecContext(ctx, "SET GLOBAL "+quoteIdentifier(k)+" = "+value); err != nil {
			return errors.New("MySQL dynamic variable reload failed: " + k)
		}
	}
	return nil
}
