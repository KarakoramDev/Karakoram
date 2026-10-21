// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

type jobManager struct {
	mu      sync.Mutex
	active  string
	pending string
	cancel  context.CancelFunc
}

var jobInitMu sync.Mutex

func (e *Engine) jobManager() *jobManager {
	jobInitMu.Lock()
	defer jobInitMu.Unlock()
	if e.jobs == nil {
		e.jobs = &jobManager{}
	}
	return e.jobs
}
func (e *Engine) cancelJobs() {
	m := e.jobManager()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
}

var errJobConflict = errors.New("receipt request mismatch or another offline job is running")

type offlineJob func(context.Context, *agent.Node, neo4jspec.Spec, *jobReceipt, func(string) error) error

func (e *Engine) submitJob(n *agent.Node, kind, id string, request any, run offlineJob) (neo4jspec.JobResponse, error) {
	m := e.jobManager()
	m.mu.Lock()
	defer m.mu.Unlock()
	name, err := jobReceiptPath(baseDir(n), kind, id)
	if err != nil {
		return neo4jspec.JobResponse{}, err
	}
	hash, err := requestHash(request)
	if err != nil {
		return neo4jspec.JobResponse{}, err
	}
	old, err := readReceipt(name)
	if err == nil {
		if old.RequestHash != hash {
			return neo4jspec.JobResponse{}, errJobConflict
		}
		return old.JobResponse, nil
	}
	if !os.IsNotExist(err) {
		return neo4jspec.JobResponse{}, errors.New("cannot read job receipt")
	}
	if m.active != "" || m.pending != "" {
		return neo4jspec.JobResponse{}, errJobConflict
	}
	r := &jobReceipt{RequestHash: hash, JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning, Phase: neo4jspec.PhaseStopping}}
	if err = durableJSON(name, r); err != nil {
		return neo4jspec.JobResponse{}, errors.New("cannot persist job receipt")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.active = name
	m.cancel = cancel
	response := r.JobResponse
	go func() {
		defer cancel()
		err := n.WithLock(ctx, func(locked *agent.Node) error {
			s, err := decodeSpec(locked.Config())
			if err != nil {
				return errors.New("no valid applied configuration")
			}
			phase := func(p string) error { m.mu.Lock(); defer m.mu.Unlock(); r.Phase = p; return durableJSON(name, r) }
			err = run(ctx, locked, s, r, phase)
			if !r.held {
				return err
			}
			// Release the durable hold even on cancellation. Never let an HTTP client
			// cancellation or an upload failure leave a healthy member offline.
			restartCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			// Never expose an uncommitted replacement if offline rollback failed.
			if err != nil && r.Swap != "" {
				if rollbackErr := recoverSwap(dataDir(locked), r); rollbackErr != nil {
					return rollbackErr
				}
				r.Swap = ""
			}
			if phaseErr := phase(neo4jspec.PhaseStarting); err == nil {
				err = phaseErr
			}
			if startErr := releaseJobHold(restartCtx, name, r, func(ctx context.Context) error { return locked.Start(ctx, neo4jspec.Process) }); err == nil && startErr != nil {
				err = errors.New("server restart failed")
			}
			if err == nil && r.Swap != "" {
				r.Swap = "committed"
				if saveErr := phase(neo4jspec.PhaseStarting); saveErr != nil {
					return saveErr
				}
				if cleanupErr := recoverSwap(dataDir(locked), r); cleanupErr != nil {
					return cleanupErr
				}
				r.Swap = ""
			}
			if err != nil && r.Swap != "" {
				r.held = true
				r.ReleasePending = true
				if stopErr := locked.Stop(restartCtx, neo4jspec.Process); stopErr != nil {
					return stopErr
				}
				if rollbackErr := recoverSwap(dataDir(locked), r); rollbackErr != nil {
					return rollbackErr
				}
				r.Swap = ""
				if saveErr := phase(neo4jspec.PhaseStarting); saveErr != nil {
					return saveErr
				}
				if startErr := releaseJobHold(restartCtx, name, r, func(ctx context.Context) error { return locked.Start(ctx, neo4jspec.Process) }); startErr != nil {
					return startErr
				}
			}
			return err
		})
		m.mu.Lock()
		defer m.mu.Unlock()
		// Pending release is never terminal: Apply/restart must retry the hold.
		_ = finishJobReceipt(name, r, err)
		if r.ReleasePending {
			m.pending = name
		}
		m.active = ""
		m.cancel = nil
	}()
	return response, nil
}
func (e *Engine) jobRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, kind := range []string{neo4jspec.ActionBackup, neo4jspec.ActionRestore, neo4jspec.ActionRecoverAdmin} {
		mux.HandleFunc("POST /v1/neo4j/"+kind, func(w http.ResponseWriter, r *http.Request) {
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			var response neo4jspec.JobResponse
			var err error
			switch kind {
			case neo4jspec.ActionBackup:
				var q neo4jspec.BackupRequest
				err = dec.Decode(&q)
				if err == nil {
					err = validateObjectPrefix(q.Target.Prefix)
				}
				if err == nil {
					err = expectJSONEnd(dec)
				}
				if err == nil {
					response, err = e.submitJob(n, kind, q.Receipt, q, e.backupJob(q))
				}
			case neo4jspec.ActionRestore:
				var q neo4jspec.RestoreRequest
				err = dec.Decode(&q)
				if err == nil {
					err = validateObjectPrefix(q.Source.Prefix)
				}
				if err == nil && q.AdminPassword == "" {
					err = errors.New("admin password required")
				}
				if err == nil {
					err = expectJSONEnd(dec)
				}
				if err == nil {
					response, err = e.submitJob(n, kind, q.Receipt, q, e.restoreJob(q))
				}
			default:
				var q neo4jspec.RecoverAdminRequest
				err = dec.Decode(&q)
				if err == nil && q.AdminPassword == "" {
					err = errors.New("admin password required")
				}
				if err == nil {
					err = expectJSONEnd(dec)
				}
				if err == nil {
					response, err = e.submitJob(n, kind, q.Receipt, q, func(ctx context.Context, n *agent.Node, s neo4jspec.Spec, receipt *jobReceipt, phase func(string) error) error {
						receipt.held = true
						if err := n.Stop(ctx, neo4jspec.Process); err != nil {
							return err
						}
						if err := phase(neo4jspec.PhaseRecovering); err != nil {
							return err
						}
						return e.recoverAdmin(ctx, n, s, q.AdminPassword)
					})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				status := http.StatusBadRequest
				if errors.Is(err, errJobConflict) {
					status = http.StatusConflict
				}
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "offline job request rejected"})
				return
			}
			_ = json.NewEncoder(w).Encode(response)
		})
	}
}
func expectJSONEnd(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("one JSON request required")
	}
	return nil
}
func (e *Engine) reconcileJobs(ctx context.Context, n *agent.Node) error {
	m := e.jobManager()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, kind := range []string{neo4jspec.ActionBackup, neo4jspec.ActionRestore, neo4jspec.ActionRecoverAdmin} {
		names, err := filepath.Glob(filepath.Join(baseDir(n), "receipts", kind, "*.json"))
		if err != nil {
			return err
		}
		for _, name := range names {
			if name == m.active {
				continue
			}
			r, err := readReceipt(name)
			if err != nil {
				return errors.New("cannot read offline receipt")
			}
			if r.State != neo4jspec.JobRunning && r.Swap == "" && !r.ReleasePending {
				if err = m.clearTerminalPending(name, syncDir); err != nil {
					return err
				}
				continue
			}
			m.pending = name
			if r.Swap != "" {
				if err = n.Stop(ctx, neo4jspec.Process); err != nil {
					return err
				}
			}
			if err = recoverSwap(dataDir(n), r); err != nil {
				return err
			}
			r.Swap = ""
			if err = interruptReceipt(name, r, func() error { return n.Start(ctx, neo4jspec.Process) }); err != nil {
				return err
			}
			if m.pending == name {
				m.pending = ""
			}
		}
	}
	return nil
}

// A receipt rename may have succeeded even when its parent-directory sync
// failed. Re-establish publication durability before clearing the admission latch.
// The caller holds m.mu and has observed a terminal receipt without pending work.
func (m *jobManager) clearTerminalPending(name string, syncParent func(string) error) error {
	if m.pending != name {
		return nil
	}
	if err := syncParent(filepath.Dir(name)); err != nil {
		return err
	}
	m.pending = ""
	return nil
}
