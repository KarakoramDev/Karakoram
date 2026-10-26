// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"time"
)

// A server LOAD is global. Carry native roles into configuration, as ordinary
// reconciliation does, so stale configured hostgroups cannot remove the healthy
// writer or churn its connections. Carry native statuses only across the load;
// afterward restore configured ONLINE intent without loading it into runtime.
func loadProxyEpochServers(ctx context.Context, host string, query proxyEpochQuery, recovery *proxyEpochRecovery) (err error) {
	runtime, err := query(ctx, "SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers")
	if err != nil {
		return err
	}
	configured, err := query(ctx, "SELECT hostname,port,hostgroup_id,status FROM pgsql_servers")
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = errors.Join(err, recovery.restoreStatuses(cleanup, query))
	}()
	for _, prior := range configured {
		for _, row := range runtime {
			if prior["hostname"] != row["hostname"] || prior["port"] != row["port"] {
				continue
			}
			filter := " WHERE hostname=" + sqlQuote(row["hostname"]) + " AND port=" + sqlQuote(row["port"])
			if prior["hostgroup_id"] != row["hostgroup_id"] {
				if _, err = query(ctx, "UPDATE pgsql_servers SET hostgroup_id="+sqlQuote(row["hostgroup_id"])+filter); err != nil {
					return err
				}
			}
			// The target's owned fence is intentionally changed. Preserve a
			// native SHUNNED target too if this is a later cleanup retry.
			if prior["status"] == "ONLINE" && row["status"] != "ONLINE" && (row["hostname"] != host || row["status"] != "OFFLINE_HARD") {
				if err = recovery.rememberStatusRestore("UPDATE pgsql_servers SET status='ONLINE'" + filter + " AND status=" + sqlQuote(row["status"])); err != nil {
					return err
				}
				if _, err = query(ctx, "UPDATE pgsql_servers SET status="+sqlQuote(row["status"])+filter+" AND status='ONLINE'"); err != nil {
					return err
				}
			}
			break
		}
	}
	_, err = query(ctx, "LOAD PGSQL SERVERS TO RUNTIME")
	return err
}
