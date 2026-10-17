// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func postgresProcess(s pgspec.Spec, p paths) agent.ProcessSpec {
	return agent.ProcessSpec{Binary: filepath.Join(s.BinDir, "postgres"), Args: []string{"-D", p.pgdata, "-c", "config_file=" + filepath.Join(p.root, "postgresql.conf")}, Dir: p.pgdata, StopSignal: syscall.SIGINT}
}
func singleReloadChanged(changed []string) bool {
	for _, name := range []string{"postgresql.conf", "pg_hba.conf", "server.pem", "server.key", "ca.pem"} {
		if slices.Contains(changed, name) {
			return true
		}
	}
	return false
}

// pg_file_settings parses the current files even before an asynchronous SIGHUP
// has finished. PostgreSQL itself validates and normalizes GUC units and values;
// an overridden entry has no error and must not trigger a restart. Also account
// for a removed postmaster setting that must return to its boot default.
const singleRestartRequiredSQL = `WITH file_settings AS MATERIALIZED (
 SELECT name,applied,error FROM pg_catalog.pg_file_settings
)
SELECT EXISTS (
 SELECT 1 FROM file_settings f JOIN pg_catalog.pg_settings s USING(name)
 WHERE s.context='postmaster' AND NOT f.applied AND f.error IS NOT NULL
) OR EXISTS (
 SELECT 1 FROM pg_catalog.pg_settings s WHERE s.context='postmaster' AND (
 s.pending_restart OR (
 s.source='configuration file' AND s.setting IS DISTINCT FROM s.boot_val
 AND NOT EXISTS (SELECT 1 FROM file_settings f WHERE f.name=s.name)
 ))
)`

func reloadSingle(ctx context.Context, s pgspec.Spec, p paths) (bool, error) {
	if _, err := command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "reload", "-D", p.pgdata); err != nil {
		return false, errors.New("PostgreSQL configuration reload failed")
	}
	connection, err := localConnect(ctx, s, p)
	if err != nil {
		return false, errors.New("cannot inspect PostgreSQL restart requirements")
	}
	defer connection.Close(ctx)
	var restart bool
	if err = connection.QueryRow(ctx, singleRestartRequiredSQL).Scan(&restart); err != nil {
		return false, errors.New("cannot inspect PostgreSQL restart requirements")
	}
	return restart, nil
}
