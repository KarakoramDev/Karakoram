// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

// The restored catalogs must be repaired independently on every group primary:
// citus_update_node would propagate to peers that are deliberately HBA-fenced.
// Keep node/group IDs (and therefore shard placements) unchanged.
func citusRestoreMetadataSQL(s pgspec.Spec, req pgspec.CitusRestoreMetadataRequest) (string, error) {
	if err := validateCitusInput(&req); err != nil {
		return "", err
	}
	if s.Citus == nil || s.Postgres == nil || !s.Postgres.RecoveryClientsHeld {
		return "", errors.New("citus restore metadata requires client fences")
	}
	var sql strings.Builder
	sql.WriteString("BEGIN;\nSET LOCAL synchronous_commit = local;\nSET LOCAL citus.enable_ddl_propagation = off;\nSET LOCAL citus.enable_metadata_sync = off;\nLOCK TABLE pg_catalog.pg_dist_node, pg_catalog.pg_dist_poolinfo IN EXCLUSIVE MODE;\n")
	primaries := 0
	for _, node := range req.Nodes {
		if node.Role == "primary" {
			primaries++
		}
	}
	fmt.Fprintf(&sql, "DO $kk$ BEGIN IF (SELECT count(*) FROM pg_catalog.pg_dist_node WHERE noderole='primary') <> %d OR (SELECT count(DISTINCT groupid) FROM pg_catalog.pg_dist_node) <> %d OR EXISTS (SELECT FROM pg_catalog.pg_dist_node WHERE noderole NOT IN ('primary','secondary')) THEN RAISE EXCEPTION 'restored Citus groups differ from target'; END IF; END $kk$;\n", primaries, primaries)
	local := ""
	for _, node := range req.Nodes {
		if node.Role != "primary" {
			continue
		}
		fmt.Fprintf(&sql, "DO $kk$ BEGIN IF (SELECT count(*) FROM pg_catalog.pg_dist_node WHERE groupid=%d AND noderole='primary') <> 1 THEN RAISE EXCEPTION 'restored Citus group differs from target'; END IF; END $kk$;\n", node.GroupID)
		if node.GroupID == s.Citus.Group {
			local = node.Host
		}
	}
	if local == "" {
		return "", errors.New("restore target omits local group")
	}
	// Stage secondary rows without catalog uniqueness constraints. A promoted
	// target primary may currently have a secondary entry at the same address.
	// Remove/reinsert those rows atomically so primary identity is preserved,
	// with no transient address collision visible outside this transaction.
	// Drop source pool overrides before removing any referenced node IDs.
	sql.WriteString("CREATE TEMP TABLE _karakoram_restore_secondaries ON COMMIT DROP AS SELECT * FROM pg_catalog.pg_dist_node WHERE noderole='secondary';\nDELETE FROM pg_catalog.pg_dist_poolinfo;\nDELETE FROM pg_catalog.pg_dist_node WHERE noderole='secondary';\n")
	for _, node := range req.Nodes {
		if node.Role != "primary" {
			continue
		}
		fmt.Fprintf(&sql, "UPDATE pg_catalog.pg_dist_node SET nodename=%s, nodeport=%d WHERE groupid=%d AND noderole='primary';\n", sqlQuote(node.Host), node.Port, node.GroupID)
		var targets strings.Builder
		replicas := 0
		for _, replica := range req.Nodes {
			if replica.GroupID != node.GroupID || replica.Role != "secondary" {
				continue
			}
			if replicas > 0 {
				targets.WriteByte(',')
			}
			replicas++
			fmt.Fprintf(&targets, "(%d,%s,%d)", replicas, sqlQuote(replica.Host), replica.Port)
		}
		// Keep and remap as many source secondary IDs as the target needs.
		// Patroni registers any additional target followers through its normal
		// membership loop; a secondary row is not required for primary recovery.
		fmt.Fprintf(&sql, "DELETE FROM pg_temp._karakoram_restore_secondaries WHERE groupid=%d AND nodeid NOT IN (SELECT nodeid FROM pg_temp._karakoram_restore_secondaries WHERE groupid=%d ORDER BY nodeid LIMIT %d);\n", node.GroupID, node.GroupID, replicas)
		if replicas > 0 {
			fmt.Fprintf(&sql, "WITH ranked AS (SELECT nodeid, row_number() OVER (ORDER BY nodeid) AS ordinal FROM pg_temp._karakoram_restore_secondaries WHERE groupid=%d) UPDATE pg_temp._karakoram_restore_secondaries n SET nodename=target.host, nodeport=target.port FROM ranked JOIN (VALUES %s) AS target(ordinal,host,port) ON ranked.ordinal=target.ordinal WHERE n.nodeid=ranked.nodeid;\n", node.GroupID, targets.String())
		}
	}
	sql.WriteString("INSERT INTO pg_catalog.pg_dist_node SELECT * FROM pg_temp._karakoram_restore_secondaries;\n")
	fmt.Fprintf(&sql, "ALTER DATABASE %s SET citus.local_hostname TO %s;\nCOMMIT;\n", sqlIdent(s.Citus.Database), sqlQuote(local))
	return sql.String(), nil
}

func rewriteCitusRestoreMetadata(ctx context.Context, c *pgx.Conn, s pgspec.Spec, req pgspec.CitusRestoreMetadataRequest) error {
	sql, err := citusRestoreMetadataSQL(s, req)
	if err != nil {
		return err
	}
	_, err = c.Exec(ctx, sql)
	return err
}

func createCitusRestorePoint(ctx context.Context, c *pgx.Conn, s pgspec.Spec, name string) (pgspec.CitusRestorePointResponse, error) {
	out := pgspec.CitusRestorePointResponse{Nodes: []pgspec.CitusRestorePoint{}}
	// The dedicated connection is closed by the caller, releasing this lock even
	// when cancellation prevents an explicit unlock. Share start/stop's lock key.
	if _, err := c.Exec(ctx, "SELECT pg_catalog.pg_advisory_lock(1514886211,1)"); err != nil {
		return out, err
	}
	if _, err := c.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS _karakoram; REVOKE ALL ON SCHEMA _karakoram FROM PUBLIC; CREATE TABLE IF NOT EXISTS _karakoram.citus_restore_points(name text PRIMARY KEY, response jsonb); REVOKE ALL ON _karakoram.citus_restore_points FROM PUBLIC`); err != nil {
		return out, err
	}
	var saved []byte
	err := c.QueryRow(ctx, "SELECT response FROM _karakoram.citus_restore_points WHERE name=$1", name).Scan(&saved)
	if err == nil {
		if len(saved) == 0 {
			return out, errors.New("restore point outcome is uncertain; use a new name only after reconciling the failed coordinated operation")
		}
		err = json.Unmarshal(saved, &out)
		return out, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	// WAL restore points cannot roll back. Commit intent BEFORE calling Citus so
	// a partial failure can never silently create duplicate named points on retry.
	if _, err = c.Exec(ctx, "INSERT INTO _karakoram.citus_restore_points(name) VALUES($1)", name); err != nil {
		return out, err
	}
	coordinator := pgspec.CitusRestorePoint{Host: localHost(s), Port: pgspec.PortPostgres}
	if err = c.QueryRow(ctx, "SELECT pg_catalog.citus_create_restore_point($1)::text", name).Scan(&coordinator.LSN); err != nil {
		return out, err
	}
	var node pgspec.CitusRestorePoint
	err = c.QueryRow(ctx, "SELECT nodeid,nodename,nodeport FROM pg_catalog.pg_dist_node WHERE groupid=0 AND noderole='primary'").Scan(&node.NodeID, &node.Host, &node.Port)
	if err == nil {
		coordinator.NodeID = node.NodeID
		coordinator.Host = node.Host
		coordinator.Port = node.Port
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	out.Nodes = append(out.Nodes, coordinator)
	// Citus returns only the coordinator's exact point. Worker positions are
	// conservative post-point upper bounds, sufficient for archive fencing.
	rows, err := c.Query(ctx, `SELECT n.nodeid,n.nodename,n.nodeport,r.success,r.result FROM pg_catalog.run_command_on_all_nodes('SELECT pg_catalog.pg_current_wal_insert_lsn()::text') r JOIN pg_catalog.pg_dist_node n ON n.nodeid=r.nodeid WHERE n.groupid<>0 ORDER BY n.nodeid`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var point pgspec.CitusRestorePoint
		var success bool
		if err = rows.Scan(&point.NodeID, &point.Host, &point.Port, &success, &point.LSN); err != nil {
			rows.Close()
			return out, err
		}
		if !success || !walLSN.MatchString(point.LSN) {
			rows.Close()
			return out, errors.New("restore point WAL observation failed")
		}
		out.Nodes = append(out.Nodes, point)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if _, err = c.Exec(ctx, "UPDATE _karakoram.citus_restore_points SET response=$2::jsonb WHERE name=$1", name, encoded); err != nil {
		return out, err
	}
	return out, nil
}
