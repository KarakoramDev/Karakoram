// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func (e *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	e.mu.Lock()
	e.node = n
	e.mu.Unlock()
	RegisterAccessRoutes(mux, e)
	RegisterGroupRoutes(mux, e)
	RegisterProxyRoutes(mux, e)
	e.backupRoutes(mux, n)
	e.restoreRoutes(mux, n)
	e.galeraRoutes(mux, n)
	for _, action := range []string{myspec.ActionStatus, myspec.ActionOfflineGTID} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
			defer cancel()
			var result any
			var err error
			if action == myspec.ActionStatus {
				result, err = e.status(ctx)
			} else {
				err = n.WithLock(ctx, func(locked *agent.Node) error {
					var inner error
					result, inner = e.offlineGTID(ctx, locked)
					return inner
				})
			}
			if err != nil {
				http.Error(w, "MySQL "+action+" unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
		})
	}
}
func (e *Runner) status(ctx context.Context) (myspec.StatusResponse, error) {
	s, p := e.state()
	id, err := readIdentity(p)
	out := myspec.StatusResponse{Role: s.Role, DataIdentity: id}
	if err != nil {
		return out, err
	}
	if s.Role != myspec.RoleData {
		return out, nil
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return out, err
	}
	err = observeNativeStatus(ctx, db, s, p, &out)
	return out, err
}

func observeNativeStatus(ctx context.Context, db *sql.DB, s myspec.Spec, p paths, out *myspec.StatusResponse) error {
	err := db.QueryRowContext(ctx, "SELECT @@server_uuid,@@gtid_executed,@@read_only,@@super_read_only,VERSION()").Scan(&out.ServerUUID, &out.GTIDExecuted, &out.ReadOnly, &out.SuperReadOnly, &out.Version)
	if err != nil {
		return err
	}
	if s.Galera != nil {
		status, err := galeraStatusDB(ctx, db)
		if err != nil {
			return err
		}
		out.MemberState = status.LocalStateComment
		if status.Eligible() {
			out.MemberRole = "PRIMARY"
		}
	} else if s.Topology == myspec.TopologyGroup {
		err = db.QueryRowContext(ctx, "SELECT MEMBER_STATE,MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&out.MemberState, &out.MemberRole)
		if errors.Is(err, sql.ErrNoRows) {
			// Before bootstrap (and after leaving), GR can expose only an
			// OFFLINE row with an empty MEMBER_ID. SQL is reachable, but this
			// server is not yet a group member; formation must be able to see it.
			out.MemberState, out.MemberRole = "OFFLINE", ""
		} else if err != nil {
			return err
		}
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'").Scan(&out.GroupMembers); err != nil {
			return err
		}
	} else {
		out.MemberState = "ONLINE"
		out.MemberRole = "PRIMARY"
	}
	out.PhysicalIdentity, err = physicalIdentity(p, out.ServerUUID)
	if err != nil {
		return err
	}
	return nil
}

// offlineGTID is exclusively a control-plane recovery observation of an
// intentionally stopped process. It never races or replaces the supervised
// server, never enables networking or replication, and shuts down before reply.
func (e *Runner) offlineGTID(ctx context.Context, n *agent.Node) (myspec.OfflineGTIDResponse, error) {
	s, p := e.state()
	var out myspec.OfflineGTIDResponse
	if s.Role != myspec.RoleData {
		return out, errors.New("offline GTID requires a data member")
	}
	if s.Galera != nil {
		return out, errors.New("galera offline observation requires wsrep-recover")
	}
	c := n.Config()
	intentional := n.Stopped("mysqld") || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "mysqld")
	states, _ := n.Snapshot()
	if !intentional || states["mysqld"].PID != 0 {
		return out, errors.New("offline GTID requires an intentionally stopped mysqld")
	}
	id, err := readIdentity(p)
	if err != nil || !id.Initialized {
		return out, errors.New("offline GTID requires initialized data")
	}
	out.DataIdentity = id
	if err = refusePersistedVariables(p); err != nil {
		return out, err
	}
	q := p
	q.socket = filepath.Join(p.run, "offline.sock")
	if err = n.WriteFile("mysql/offline-startup.sql", nil, 0600); err != nil {
		return out, err
	}
	args := []string{"--defaults-file=" + p.config, "--skip-networking", "--skip-replica-start", "--init-file=" + filepath.Join(p.root, "offline-startup.sql"), "--socket=" + q.socket, "--pid-file=" + filepath.Join(p.run, "offline.pid"), "--log-error=" + filepath.Join(p.root, "offline-error.log")}
	if s.Topology == myspec.TopologyGroup {
		args = append(args, "--group-replication-start-on-boot=OFF")
	}
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqld"), args...)
	cmd.Env = nativeEnv()
	if err = cmd.Start(); err != nil {
		return out, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	exited := false
	defer func() {
		if exited {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	db, err := waitLocal(ctx, s, q)
	if err != nil {
		return out, err
	}
	defer db.Close()
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid,@@gtid_executed").Scan(&out.ServerUUID, &out.GTIDExecuted); err != nil {
		return out, err
	}
	if err = db.Close(); err != nil {
		return out, err
	}
	// Signal and observe exit before claiming the server is stopped.
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err = <-done:
		exited = true
		if err != nil {
			return out, err
		}
	case <-ctx.Done():
		return out, ctx.Err()
	}
	out.Stopped = true
	return out, nil
}
