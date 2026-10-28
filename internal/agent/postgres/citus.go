// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func dcsPrefix(s pgspec.Spec) string {
	prefix := "/karakoram/" + s.Scope + "/"
	if s.Citus != nil {
		prefix += strconv.Itoa(s.Citus.Group) + "/"
	}
	return prefix
}

var walLSN = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$`)

func citusInput(action string) any {
	switch action {
	case pgspec.ActionCitusRebalanceStart:
		return &pgspec.CitusRebalanceStartRequest{}
	case pgspec.ActionCitusRebalanceStatus, pgspec.ActionCitusRebalanceStop:
		return &pgspec.CitusRebalanceRequest{}
	case pgspec.ActionCitusDrainNode, pgspec.ActionCitusRemoveNode:
		return &pgspec.CitusNodeRequest{}
	case pgspec.ActionCitusRestorePoint:
		return &pgspec.CitusRestorePointRequest{}
	case pgspec.ActionCitusRestoreMetadata:
		return &pgspec.CitusRestoreMetadataRequest{}
	case pgspec.ActionArchiveCovers:
		return &pgspec.ArchiveCoversRequest{}
	default:
		return &struct{}{}
	}
}

func validateCitusInput(input any) error {
	switch r := input.(type) {
	case *pgspec.CitusRestoreMetadataRequest:
		if len(r.Nodes) == 0 || len(r.Nodes) > 1024 {
			return errors.New("restore target nodes required")
		}
		groups := map[int]bool{}
		hosts := map[string]bool{}
		for _, node := range r.Nodes {
			if node.GroupID < 0 || groups[node.GroupID] || hosts[node.Host] || !validHost(node.Host) || node.Port != pgspec.PortPostgres {
				return errors.New("invalid restore target group or address")
			}
			groups[node.GroupID], hosts[node.Host] = true, true
		}
		if !groups[0] {
			return errors.New("restore target coordinator required")
		}
	case *pgspec.CitusRebalanceStartRequest:
		if r.Receipt == "" || !safeValue(r.Receipt, 256) || r.Strategy != "" && !identifier.MatchString(r.Strategy) {
			return errors.New("invalid rebalance receipt or strategy")
		}
	case *pgspec.CitusRebalanceRequest:
		if r.JobID <= 0 {
			return errors.New("positive rebalance job ID required")
		}
	case *pgspec.CitusNodeRequest:
		if !validHost(r.Host) || r.Port < 1 || r.Port > 65535 {
			return errors.New("invalid Citus node address")
		}
	case *pgspec.CitusRestorePointRequest:
		if len(r.Name) == 0 || len(r.Name) > 63 || !safeValue(r.Name, 63) {
			return errors.New("invalid restore point name")
		}
	case *pgspec.ArchiveCoversRequest:
		if !walLSN.MatchString(r.LSN) {
			return errors.New("invalid WAL LSN")
		}
	}
	return nil
}

func (e *Runner) citusRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{pgspec.ActionCitusStatus, pgspec.ActionCitusRebalanceStart, pgspec.ActionCitusRebalanceStatus, pgspec.ActionCitusRebalanceStop, pgspec.ActionCitusDrainNode, pgspec.ActionCitusRemoveNode, pgspec.ActionCitusRestorePoint, pgspec.ActionCitusRestoreMetadata, pgspec.ActionWALSwitch, pgspec.ActionArchiveCovers} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			input := citusInput(action)
			if err := decodeAction(w, r, input); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := validateCitusInput(input); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
			defer cancel()
			var s pgspec.Spec
			var p paths
			// Snapshot immutable config only; SQL and Patroni requests never hold Node's lock.
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				var err error
				s, err = decodeSpec(locked.Config())
				p = dataPaths(locked.DataDir())
				return err
			})
			if err != nil {
				http.Error(w, "member configuration unavailable", http.StatusServiceUnavailable)
				return
			}
			out, err := runCitusAction(ctx, s, p, action, input)
			if err != nil {
				http.Error(w, "distributed PostgreSQL action failed", http.StatusServiceUnavailable)
				return
			}
			actionJSON(w, 200, out)
		})
	}
}

func runCitusAction(ctx context.Context, s pgspec.Spec, p paths, action string, input any) (any, error) {
	if err := validateCitusInput(input); err != nil {
		return nil, err
	}
	if s.Role != pgspec.RoleData {
		return nil, errors.New("data member required")
	}
	groupAction := action == pgspec.ActionWALSwitch || action == pgspec.ActionArchiveCovers
	if !groupAction && (s.Citus == nil || action != pgspec.ActionCitusStatus && action != pgspec.ActionCitusRestoreMetadata && s.Citus.Group != 0) {
		return nil, errors.New("citus coordinator required")
	}
	if usesPatroni(s) {
		if err := patroniRequest(ctx, s, http.MethodGet, "/primary", nil, nil); err != nil {
			return nil, err
		}
	}
	c, err := localConnect(ctx, s, p)
	if err != nil {
		return nil, err
	}
	defer c.Close(ctx)
	var recovery bool
	if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
		return nil, err
	}
	if recovery {
		return nil, errors.New("primary required")
	}
	switch action {
	case pgspec.ActionCitusRestoreMetadata:
		if s.Postgres == nil || !s.Postgres.RecoveryClientsHeld {
			return nil, errors.New("Citus restore metadata requires client fences")
		}
		err := rewriteCitusRestoreMetadata(ctx, c, s, *input.(*pgspec.CitusRestoreMetadataRequest))
		return pgspec.ActionResponse{OK: err == nil}, err
	case pgspec.ActionCitusStatus:
		out := pgspec.CitusStatusResponse{Group: s.Citus.Group, IsCoordinator: s.Citus.Group == 0, Nodes: []pgspec.CitusNode{}}
		rows, err := c.Query(ctx, "SELECT nodeid, groupid, nodename, nodeport, noderole::text, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid")
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var node pgspec.CitusNode
			if err = rows.Scan(&node.NodeID, &node.GroupID, &node.Host, &node.Port, &node.Role, &node.Active); err != nil {
				rows.Close()
				return nil, err
			}
			out.Nodes = append(out.Nodes, node)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if out.IsCoordinator {
			var job pgspec.CitusRebalanceResponse
			err = c.QueryRow(ctx, "SELECT job_id, state::text FROM pg_catalog.citus_rebalance_status() WHERE state::text NOT IN ('finished','cancelled','failed')").Scan(&job.JobID, &job.State)
			if err == nil {
				out.Rebalance = &job
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
		}
		return out, nil
	case pgspec.ActionCitusRebalanceStart:
		return startCitusRebalance(ctx, c, *input.(*pgspec.CitusRebalanceStartRequest))
	case pgspec.ActionCitusRebalanceStatus:
		var out pgspec.CitusRebalanceResponse
		err = c.QueryRow(ctx, "SELECT job_id, state::text FROM pg_catalog.pg_dist_background_job WHERE job_id=$1 AND job_type='rebalance'", input.(*pgspec.CitusRebalanceRequest).JobID).Scan(&out.JobID, &out.State)
		return out, err
	case pgspec.ActionCitusRebalanceStop:
		// Serialize stop with start. Never cancel a newer job when an old receipt is replayed.
		tx, err := c.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, "SELECT pg_catalog.pg_advisory_xact_lock(1514886211, 1)"); err != nil {
			return nil, err
		}
		var job int64
		err = tx.QueryRow(ctx, "SELECT job_id FROM pg_catalog.citus_rebalance_status() WHERE state::text NOT IN ('finished','cancelled','failed')").Scan(&job)
		if errors.Is(err, pgx.ErrNoRows) {
			return pgspec.ActionResponse{OK: true}, tx.Commit(ctx)
		}
		if err != nil {
			return nil, err
		}
		if job != input.(*pgspec.CitusRebalanceRequest).JobID {
			return nil, errors.New("rebalance job changed")
		}
		if _, err = tx.Exec(ctx, "SELECT pg_catalog.citus_rebalance_stop()"); err != nil {
			return nil, err
		}
		return pgspec.ActionResponse{OK: true}, tx.Commit(ctx)
	case pgspec.ActionCitusDrainNode, pgspec.ActionCitusRemoveNode:
		req := input.(*pgspec.CitusNodeRequest)
		sql := "SELECT pg_catalog.citus_drain_node($1,$2)"
		if action == pgspec.ActionCitusRemoveNode {
			sql = "SELECT pg_catalog.citus_remove_node($1,$2)"
		}
		_, err = c.Exec(ctx, sql, req.Host, req.Port)
		return pgspec.ActionResponse{OK: err == nil}, err
	case pgspec.ActionCitusRestorePoint:
		return createCitusRestorePoint(ctx, c, s, input.(*pgspec.CitusRestorePointRequest).Name)
	case pgspec.ActionWALSwitch:
		var out pgspec.WALSwitchResponse
		err = c.QueryRow(ctx, "SELECT pg_catalog.pg_switch_wal()::text").Scan(&out.LSN)
		return out, err
	case pgspec.ActionArchiveCovers:
		return archiveCovers(ctx, c, s, p, input.(*pgspec.ArchiveCoversRequest).LSN, backupCommand)
	}
	return nil, fmt.Errorf("unsupported distributed action %q", action)
}

// archiveCovers checks the exact segment on PostgreSQL's current timeline.
// A local receipt is sufficient, but recycling removes receipts, not repository
// WAL. Fall back to a synchronous, read-only repository fetch in that case.
func archiveCovers(ctx context.Context, c interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, s pgspec.Spec, p paths, lsn string, run func(context.Context, pgspec.Spec, paths, ...string) ([]byte, error)) (pgspec.ArchiveCoversResponse, error) {
	var out pgspec.ArchiveCoversResponse
	if s.Backup == nil {
		return out, errors.New("WAL archive is not configured")
	}
	var segment string
	err := c.QueryRow(ctx, `SELECT pg_catalog.pg_walfile_name($1::pg_lsn), EXISTS (SELECT 1 FROM pg_catalog.pg_ls_archive_statusdir() WHERE name = pg_catalog.pg_walfile_name($1::pg_lsn) || '.done')`, lsn).Scan(&segment, &out.Covers)
	if err != nil || out.Covers {
		return out, err
	}
	if !walSegment.MatchString(segment) {
		return out, errors.New("invalid WAL segment")
	}
	_, args, err := actionRepository(s, p, nil)
	if err != nil {
		return out, err
	}
	dir, err := os.MkdirTemp(p.root, "archive-covers-")
	if err != nil {
		return out, err
	}
	defer os.RemoveAll(dir)
	destination := filepath.Join(dir, segment)
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Disable asynchronous spool handling: success must mean this exact WAL
	// file was fetched from the configured repository, not a local spool.
	args = append(args, "--archive-async=n", "--log-level-console=off", "--log-level-file=off", "archive-get", segment, destination)
	if _, err = run(probeCtx, s, p, args...); err != nil {
		if probeCtx.Err() != nil {
			return out, probeCtx.Err()
		}
		var exit *exec.ExitError
		// pgBackRest archive-get exits 1 when the requested WAL is not found.
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return out, nil
		}
		return out, errors.New("WAL archive repository probe failed")
	}
	info, err := os.Stat(destination)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return out, errors.New("WAL archive repository probe produced no segment")
	}
	out.Covers = true
	return out, nil
}

var walSegment = regexp.MustCompile(`^[0-9A-F]{24}$`)

func startCitusRebalance(ctx context.Context, c *pgx.Conn, r pgspec.CitusRebalanceStartRequest) (pgspec.CitusRebalanceResponse, error) {
	var out pgspec.CitusRebalanceResponse
	tx, err := c.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_catalog.pg_advisory_xact_lock(1514886211, 1)"); err != nil {
		return out, err
	}
	// Receipts live in PostgreSQL so promotion cannot lose the idempotency record.
	if _, err = tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS _karakoram; REVOKE ALL ON SCHEMA _karakoram FROM PUBLIC; CREATE TABLE IF NOT EXISTS _karakoram.citus_rebalance_receipts(receipt text PRIMARY KEY, strategy text NOT NULL, job_id bigint NOT NULL); REVOKE ALL ON _karakoram.citus_rebalance_receipts FROM PUBLIC`); err != nil {
		return out, err
	}
	var strategy string
	err = tx.QueryRow(ctx, "SELECT job_id,strategy FROM _karakoram.citus_rebalance_receipts WHERE receipt=$1", r.Receipt).Scan(&out.JobID, &strategy)
	if err == nil {
		if strategy != r.Strategy {
			return out, errors.New("rebalance receipt payload changed")
		}
	} else if errors.Is(err, pgx.ErrNoRows) {
		var job *int64
		if err = tx.QueryRow(ctx, "SELECT pg_catalog.citus_rebalance_start(rebalance_strategy := NULLIF($1,'')::name)", r.Strategy).Scan(&job); err != nil {
			return out, err
		}
		if job != nil {
			out.JobID = *job
		}
		if _, err = tx.Exec(ctx, "INSERT INTO _karakoram.citus_rebalance_receipts(receipt,strategy,job_id) VALUES($1,$2,$3)", r.Receipt, r.Strategy, out.JobID); err != nil {
			return out, err
		}
	} else {
		return out, err
	}
	out.State = "finished"
	if out.JobID != 0 {
		if err = tx.QueryRow(ctx, "SELECT state::text FROM pg_catalog.pg_dist_background_job WHERE job_id=$1", out.JobID).Scan(&out.State); err != nil {
			return out, err
		}
	}
	return out, tx.Commit(ctx)
}
