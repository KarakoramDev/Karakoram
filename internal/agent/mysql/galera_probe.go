// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// The caller holds the node lock. The receipt makes response-loss retries reuse
// the same probe. A crash between DDL commit and receipt persistence can repeat
// only this idempotent managed-schema DDL, never an application transaction.
func (e *Runner) galeraHistoryProbe(ctx context.Context, req myspec.GaleraHistoryProbeRequest) (myspec.GaleraStatusResponse, error) {
	s, p := e.state()
	var out myspec.GaleraStatusResponse
	if s.Galera == nil || !s.Galera.RestoreFence || req.Receipt == "" || len(req.Receipt) > 256 || req.ClusterName != s.Galera.ClusterName || req.FormationEpoch != s.Galera.FormationEpoch {
		return out, errors.New("restore GTID probe authorization mismatch")
	}
	digest := sha256.Sum256([]byte(req.Receipt))
	name := filepath.Join(p.root, fmt.Sprintf("galera-history-probe-%x.json", digest))
	var receipt struct {
		Request myspec.GaleraHistoryProbeRequest `json:"request"`
		Result  myspec.GaleraStatusResponse      `json:"result"`
	}
	raw, err := os.ReadFile(name)
	if err == nil {
		if err = json.Unmarshal(raw, &receipt); err != nil {
			return out, err
		}
		if receipt.Request != req {
			return out, errors.New("restore GTID probe receipt mismatch")
		}
		return receipt.Result, nil
	}
	if !os.IsNotExist(err) {
		return out, err
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return out, err
	}
	defer db.Close()
	out, err = executeGaleraHistoryProbe(ctx, db)
	if err != nil {
		return out, err
	}
	receipt.Request, receipt.Result = req, out
	if err = durableJSON(name, receipt); err != nil {
		return out, err
	}
	return out, nil
}

func executeGaleraHistoryProbe(ctx context.Context, db *sql.DB) (myspec.GaleraStatusResponse, error) {
	connection, err := db.Conn(ctx)
	if err != nil {
		return myspec.GaleraStatusResponse{}, err
	}
	defer connection.Close()
	out, err := galeraStatusDB(ctx, connection)
	if err != nil {
		return out, err
	}
	if !out.Eligible() || !out.OfflineMode {
		return out, errors.New("restore GTID probe requires a fenced Synced member")
	}
	var wsrep, binlog bool
	if err = connection.QueryRowContext(ctx, "SELECT @@SESSION.wsrep_on,@@SESSION.sql_log_bin").Scan(&wsrep, &binlog); err != nil {
		return out, err
	}
	if !wsrep || !binlog {
		return out, errors.New("restore GTID probe requires replicated binary logging")
	}
	// Do not disable wsrep or binary logging: this ordinary TOI statement is the
	// evidence of the native SID chosen for replicated transactions.
	if _, err = connection.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS _karakoram"); err != nil {
		return out, err
	}
	after, err := galeraStatusDB(ctx, connection)
	if err != nil {
		return after, err
	}
	if !after.Eligible() || !after.OfflineMode || after.StateUUID != out.StateUUID || after.ServerUUID != out.ServerUUID {
		return after, errors.New("restore GTID probe lost its fenced component")
	}
	return after, nil
}
