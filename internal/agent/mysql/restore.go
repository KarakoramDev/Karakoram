// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type restoreReceipt struct {
	Hash          string               `json:"hash"`
	Status        myspec.RestoreStatus `json:"status"`
	Active        bool                 `json:"active"`
	Backup        myspec.BackupResult  `json:"backup"`
	SafePreflight bool                 `json:"safePreflight,omitempty"`
	Replay        *restoreReplayPlan   `json:"replay,omitempty"`
}

type restoreReplayPlan struct {
	Logs     replayLogs     `json:"logs"`
	Target   string         `json:"target"`
	Physical []preparedFile `json:"physical"`
}

var activeRestores sync.Map
var restoreSubmitMu sync.Mutex

type activeRestore struct {
	root   string
	cancel context.CancelFunc
}

func stopRestores(p paths) {
	activeRestores.Range(func(_, value any) bool {
		job := value.(activeRestore)
		if job.root == p.root {
			job.cancel()
		}
		return true
	})
}

func restoreHash(r myspec.RestoreRequest) string {
	r.PlanDigest = "" // derived output, not part of the immutable restore intent
	// Recheck IDs identify verification attempts, not immutable restore intent.
	// Excluding them keeps both receipt conflicts and the frozen digest stable.
	r.RecheckID = ""
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func restoreFence(n *agent.Node, s myspec.Spec) error {
	c := n.Config()
	states, _ := n.Snapshot()
	return restoreFenceIntent(s.Role, n.Stopped("mysqld") || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "mysqld"), states["mysqld"].PID)
}
func restoreFenceIntent(role string, intentional bool, pid int) error {
	if role != myspec.RoleData || !intentional || pid != 0 {
		return errors.New("restore_fence")
	}
	return nil
}
func restoreInterrupted(r restoreReceipt) bool {
	return r.Active && r.Status.Stage != "fetch" && r.Status.Stage != "finalize"
}
func restoreStartGuard(p paths) error {
	entries, err := os.ReadDir(p.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "restore-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		r, err := loadRestoreReceipt(filepath.Join(p.root, entry.Name()))
		if err != nil || !r.SafePreflight && r.Status.State != "done" {
			return errors.New("MySQL restore incomplete; explicit recovery required")
		}
	}
	return nil
}
func loadRestoreReceipt(name string) (restoreReceipt, error) {
	var out restoreReceipt
	b, err := os.ReadFile(name)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}
func (e *Runner) restoreRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{myspec.ActionRestore, myspec.ActionRestorePreflight, myspec.ActionRestoreStatus} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			var req myspec.RestoreRequest
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			d.DisallowUnknownFields()
			if d.Decode(&req) != nil || req.Receipt == "" {
				http.Error(w, "invalid restore request", 400)
				return
			}
			var out myspec.RestoreStatus
			var err error
			_, p := e.state()
			name, e2 := receiptPath(p, "restore", req.Receipt)
			err = e2
			if err == nil && action == myspec.ActionRestoreStatus {
				var receipt restoreReceipt
				receipt, err = observeRestoreReceipt(name)
				out = receipt.Status
			} else if err == nil {
				out, err = e.submitRestore(r.Context(), n, req, name, action == myspec.ActionRestorePreflight)
			}
			if err != nil {
				http.Error(w, "MySQL restore unavailable", http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
	}
}

// A status request after process loss must not advertise an orphan operation as
// running forever. Explicitly resubmitting an identical receipt can resume a
// completed stage; observing an abandoned job instead seals it as failed closed.
func observeRestoreReceipt(name string) (restoreReceipt, error) {
	restoreSubmitMu.Lock()
	defer restoreSubmitMu.Unlock()
	r, err := loadRestoreReceipt(name)
	if err != nil {
		return r, err
	}
	if _, active := activeRestores.Load(name); !active && r.Status.State == "running" {
		r.Status.State = "failed"
		r.Status.Error = "interrupted"
		r.Active = false
		err = durableJSON(name, r)
	}
	return r, err
}
func (e *Runner) submitRestore(ctx context.Context, n *agent.Node, req myspec.RestoreRequest, name string, preflight bool) (myspec.RestoreStatus, error) {
	restoreSubmitMu.Lock()
	defer restoreSubmitMu.Unlock()
	if !preflight && !myspec.ValidDigest(req.PlanDigest) {
		return myspec.RestoreStatus{}, errors.New("restore_requires_frozen_plan_digest")
	}
	receipt, err := loadRestoreReceipt(name)
	if err != nil && !preflight {
		return myspec.RestoreStatus{}, errors.New("restore_preflight_required")
	}
	if err == nil {
		if receipt.Hash != restoreHash(req) {
			return receipt.Status, errors.New("receipt_conflict")
		}
		if err := requireFrozenPlanDigest(req, receipt, preflight); err != nil {
			return receipt.Status, err
		}
		if _, active := activeRestores.Load(name); active {
			return receipt.Status, nil
		}
	}
	err = n.WithLock(ctx, func(n *agent.Node) error {
		s, _ := e.state()
		if !preflight {
			if err := restoreFence(n, s); err != nil {
				return err
			}
		} else if s.Role != myspec.RoleData {
			return errors.New("restore_data_member_required")
		}
		if err := validateRestore(s, req.Restore); err != nil {
			return err
		}
		if os.IsNotExist(err) {
			receipt = restoreReceipt{Hash: restoreHash(req), SafePreflight: true, Status: myspec.RestoreStatus{Receipt: req.Receipt, State: "running", Stage: "fetch"}}
			return durableJSON(name, receipt)
		}
		return err
	})
	if err != nil || receipt.Status.State == "done" || receipt.Status.State == "failed" {
		return receipt.Status, err
	}
	if preflight && receipt.Status.State == "prepared" {
		if req.RecheckID == "" {
			return receipt.Status, nil
		}
		_, p := e.state()
		return startRestoreRecheck(name, receipt, req.RecheckID, p.root, verifyFrozenRestorePlan)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	_, p := e.state()
	activeRestores.Store(name, activeRestore{root: p.root, cancel: cancel})
	go func() {
		defer activeRestores.Delete(name)
		defer cancel()
		_ = n.WithLock(ctx, func(n *agent.Node) error { _, err := e.restore(ctx, n, req, preflight); return err })
	}()
	return receipt.Status, nil
}

// The caller holds restoreSubmitMu until both the durable intent and job exist.
func startRestoreRecheck(name string, receipt restoreReceipt, token, root string, verify func(restoreReceipt, string, bool) error) (myspec.RestoreStatus, error) {
	if _, active := activeRestores.Load(name); active {
		return receipt.Status, nil
	}
	if receipt.Status.VerifiedRecheckID == token {
		return receipt.Status, nil
	}
	receipt.Status.State, receipt.Status.Stage = "running", "rechecking"
	receipt.Status.RecheckID, receipt.Status.VerifiedRecheckID = token, ""
	receipt.Active = true
	if err := durableJSON(name, receipt); err != nil {
		return receipt.Status, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	activeRestores.Store(name, activeRestore{root: root, cancel: cancel})
	go func() {
		defer cancel()
		err := verify(receipt, name+".staging", true)
		restoreSubmitMu.Lock()
		defer restoreSubmitMu.Unlock()
		defer activeRestores.Delete(name)
		receipt.Active = false
		switch {
		case ctx.Err() != nil:
			receipt.Status.State, receipt.Status.Error = "failed", "interrupted"
		case err != nil:
			receipt.Status.State, receipt.Status.Error = "failed", "replay_plan_changed"
		default:
			receipt.Status.State, receipt.Status.Stage = "prepared", "ready"
			receipt.Status.VerifiedRecheckID = token
		}
		_ = durableJSON(name, receipt)
	}()
	return receipt.Status, nil
}
func validateRestore(s myspec.Spec, r myspec.Restore) error {
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if err := myspec.ValidateBackupManifestRefs(r.ObjectPaths, r.ObjectManifests); err != nil {
		return err
	}
	if len(r.ObjectPaths) == 0 || r.TargetGTID != "" && r.TargetTime != "" {
		return errors.New("invalid_restore")
	}
	for _, key := range r.ObjectPaths {
		if key == "" || strings.HasPrefix(key, "-") {
			return errors.New("invalid_restore")
		}
	}
	if s.Topology == myspec.TopologyGroup && (s.GroupReplication == nil || !myspec.ValidUUID(r.NewGroupName) || r.NewGroupName == s.GroupReplication.GroupName) {
		return errors.New("invalid_restore_group")
	}
	if s.Galera != nil && (!myspec.ValidGaleraClusterName(r.NewGroupName) || r.NewGroupName == s.Galera.ClusterName) {
		return errors.New("invalid_restore_galera_cluster")
	}
	return nil
}
func prepareArguments(full string, increments []string) [][]string {
	args := [][]string{{"--prepare", "--apply-log-only", "--target-dir=" + full}}
	for i, inc := range increments {
		a := []string{"--prepare", "--target-dir=" + full, "--incremental-dir=" + inc}
		if i != len(increments)-1 {
			a = append(a, "--apply-log-only")
		}
		args = append(args, a)
	}
	return append(args, []string{"--prepare", "--target-dir=" + full})
}
func (e *Runner) restore(ctx context.Context, n *agent.Node, req myspec.RestoreRequest, preflight bool) (myspec.RestoreStatus, error) {
	s, p := e.state()
	var empty myspec.RestoreStatus
	if !preflight {
		if err := restoreFence(n, s); err != nil {
			return empty, err
		}
	} else if s.Role != myspec.RoleData {
		return empty, errors.New("restore_data_member_required")
	}
	r := req.Restore
	if err := validateRestore(s, r); err != nil {
		return empty, err
	}
	name, err := receiptPath(p, "restore", req.Receipt)
	if err != nil {
		return empty, err
	}
	receipt, err := loadRestoreReceipt(name)
	if os.IsNotExist(err) {
		receipt = restoreReceipt{Hash: restoreHash(req), SafePreflight: true, Status: myspec.RestoreStatus{Receipt: req.Receipt, State: "running", Stage: "fetch"}}
		err = durableJSON(name, receipt)
	}
	if err != nil {
		return empty, err
	}
	if receipt.Hash != restoreHash(req) {
		return empty, errors.New("receipt_conflict")
	}
	if err := requireFrozenPlanDigest(req, receipt, preflight); err != nil {
		return empty, err
	}
	if preflight && !receipt.SafePreflight {
		return empty, errors.New("restore_preflight_already_destructive")
	}
	if receipt.Status.State == "done" || receipt.Status.State == "failed" {
		return receipt.Status, nil
	}
	fail := func(class string) (myspec.RestoreStatus, error) {
		receipt.Status.State = "failed"
		receipt.Status.Error = class
		receipt.Active = false
		return receipt.Status, durableJSON(name, receipt)
	}
	// Native prepare/move-back are not retryable after an ambiguous process exit.
	// A durable intent with no completion always fails closed after agent restart.
	if restoreInterrupted(receipt) {
		return fail("interrupted")
	}
	stageDir := name + ".staging"
	old := name + ".previous"
	full := filepath.Join(stageDir, "0")
	for receipt.Status.Stage != "done" {
		receipt.Active = true
		if err = durableJSON(name, receipt); err != nil {
			return receipt.Status, err
		}
		switch receipt.Status.Stage {
		case "fetch":
			if err = os.RemoveAll(stageDir); err != nil {
				return fail("fetch_failed")
			}
			if err = privateDir(stageDir); err != nil {
				return fail("fetch_failed")
			}
			for i, ref := range r.ObjectManifests {
				dir := filepath.Join(stageDir, fmt.Sprint(i))
				if err = privateDir(dir); err != nil {
					break
				}
				if err = extractVerifiedBackup(ctx, s, r.Source, ref, dir, nil); err != nil {
					break
				}
			}
			if err != nil {
				return fail("fetch_failed")
			}
			// Capture metadata before prepare mutates the full checkpoint. The
			// source UUID comes from the same stream as the binlog coordinates.
			last := filepath.Join(stageDir, fmt.Sprint(len(r.ObjectPaths)-1))
			receipt.Backup, err = ParseBackupMetadata(last)
			if err != nil {
				return fail("metadata_invalid")
			}
			receipt.Backup.SourceUUID, err = backupSourceUUID(last)
			if err != nil {
				return fail("metadata_invalid")
			}
			receipt.Backup.ManifestDigest = r.ObjectManifests[len(r.ObjectManifests)-1].Digest
			receipt.Backup.ManifestSize = r.ObjectManifests[len(r.ObjectManifests)-1].Size
			receipt.Status.Stage = "prepare"
		case "prepare":
			increments := make([]string, 0, len(r.ObjectPaths)-1)
			for i := 1; i < len(r.ObjectPaths); i++ {
				increments = append(increments, filepath.Join(stageDir, fmt.Sprint(i)))
			}
			for _, args := range prepareArguments(full, increments) {
				if err = backupCommand(ctx, s, "xtrabackup", args...).Run(); err != nil {
					return fail("prepare_failed")
				}
			}
			plan := &restoreReplayPlan{Target: receipt.Backup.GTIDSet}
			if r.TargetGTID != "" || r.TargetTime != "" {
				plan.Logs, plan.Target, err = fetchReplayLogs(ctx, s, r, stageDir, receipt.Backup)
				if err != nil {
					return fail("pitr_preflight_unavailable_take_fresh_full_from_current_writer")
				}
			}
			plan.Physical, err = preparedPhysicalFiles(full)
			if err != nil {
				return fail("physical_staging_manifest_failed")
			}
			receipt.Replay = plan
			receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
			if err != nil {
				return fail("restore_plan_digest_failed")
			}
			receipt.Status.Stage = "ready"
		case "ready":
			if receipt.Replay == nil {
				return fail("replay_plan_missing")
			}
			if err = verifyFrozenRestorePlan(receipt, stageDir, true); err != nil {
				return fail("replay_plan_changed")
			}
			if preflight {
				receipt.Active = false
				receipt.Status.State = "prepared"
				return receipt.Status, durableJSON(name, receipt)
			}
			receipt.Status.State = "running"
			receipt.SafePreflight = false
			receipt.Status.Stage = "move-back"
		case "move-back":
			if receipt.Replay == nil {
				return fail("replay_plan_missing")
			}
			if err = verifyFrozenRestorePlan(receipt, stageDir, true); err != nil {
				receipt.SafePreflight = true
				return fail("replay_plan_changed")
			}
			if _, err = os.Lstat(old); !os.IsNotExist(err) {
				return fail("move_back_failed")
			}
			if err = os.Rename(p.data, old); err != nil && !os.IsNotExist(err) {
				return fail("move_back_failed")
			}
			if err = privateDir(p.data); err != nil {
				return fail("move_back_failed")
			}
			if err = backupCommand(ctx, s, "xtrabackup", "--move-back", "--target-dir="+full, "--datadir="+p.data).Run(); err != nil {
				return fail("move_back_failed")
			}
			for _, file := range []string{"auto.cnf", "mysqld-auto.cnf"} {
				if err = os.Remove(filepath.Join(p.data, file)); err != nil && !os.IsNotExist(err) {
					return fail("move_back_failed")
				}
			}
			if s.Galera != nil {
				for _, file := range []string{"grastate.dat", "gvwstate.dat", "galera.cache"} {
					if err = os.Remove(filepath.Join(p.data, file)); err != nil && !os.IsNotExist(err) {
						return fail("move_back_failed")
					}
				}
			}
			receipt.Status.Stage = "replay"
		case "replay":
			if verifyFrozenRestorePlan(receipt, stageDir, false) != nil {
				return fail("replay_plan_changed")
			}
			accounts := &restoreAccounts{Users: r.RecoveryUsers}
			receipt.Status.ServerUUID, receipt.Status.GTIDExecuted, err = restoreReplay(ctx, s, p, receipt.Replay.Logs, receipt.Backup.GTIDSet, receipt.Replay.Target, accounts)
			receipt.Status.RecoveredUsers = accounts.Existing
			if err != nil {
				return fail("replay_failed")
			}
			receipt.Status.Stage = "finalize"
		case "finalize":
			if s.GroupReplication != nil {
				if err = os.Remove(groupReceiptPath(p.root, s)); err != nil && !os.IsNotExist(err) {
					return fail("finalize_failed")
				}
			}
			if err = durableJSON(filepath.Join(p.root, "initialized.json"), myspec.DataIdentity{Initialized: true, ServerUUID: receipt.Status.ServerUUID, FormationEpoch: 0}); err != nil {
				return fail("finalize_failed")
			}
			if s.Galera != nil {
				if err = clearGaleraStartState(p); err != nil {
					return fail("finalize_failed")
				}
				if err = durableJSON(filepath.Join(p.root, "sst-authorized.json"), galeraIdentity{
					ClusterName: r.NewGroupName, Member: s.Member, FormationEpoch: s.Galera.FormationEpoch,
					Pending: true,
				}); err != nil {
					return fail("finalize_failed")
				}
			}
			if err = recordCredentials(s, p); err != nil {
				return fail("finalize_failed")
			}
			if err = durableText(filepath.Join(p.root, "startup.sql"), renderStartup(s, p, false)); err != nil {
				return fail("finalize_failed")
			}
			if err = os.RemoveAll(stageDir); err != nil {
				return fail("finalize_failed")
			}
			if err = os.RemoveAll(old); err != nil {
				return fail("finalize_failed")
			}
			receipt.Status.Stage = "done"
			receipt.Status.State = "done"
		default:
			return fail("receipt_invalid")
		}
		receipt.Active = false
		if err = durableJSON(name, receipt); err != nil {
			return receipt.Status, err
		}
	}
	return receipt.Status, nil
}
func restoreReplay(ctx context.Context, s myspec.Spec, p paths, logs replayLogs, backup, target string, recovery ...*restoreAccounts) (uuid, gtid string, err error) {
	q := p
	q.socket = filepath.Join(p.run, "restore.sock")
	if err = durableText(filepath.Join(p.root, "startup.sql"), renderStartup(s, p, true)); err != nil {
		return
	}
	args := []string{"--defaults-file=" + p.config, "--skip-networking", "--skip-replica-start", "--socket=" + q.socket, "--pid-file=" + filepath.Join(p.run, "restore.pid"), "--log-error=" + filepath.Join(p.root, "restore-error.log")}
	if s.GroupReplication != nil {
		args = append(args, "--group-replication-start-on-boot=OFF")
	}
	if s.Galera != nil {
		args = append(args, "--wsrep-on=OFF", "--wsrep-provider=none")
	}
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqld"), args...)
	cmd.Env = nativeEnv()
	if err = cmd.Start(); err != nil {
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case stopErr := <-done:
			if err == nil {
				err = stopErr
			}
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			if err == nil {
				err = errors.New("restore_shutdown")
			}
		}
	}()
	isolated := s
	isolated.Galera = nil // replay deliberately runs with wsrep disabled
	db, e := waitLocal(ctx, isolated, q)
	if e != nil {
		err = e
		return
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "SET GLOBAL super_read_only=OFF"); err != nil {
		return
	}
	if len(logs.Files) > 0 {
		client := filepath.Join(p.run, "restore-client.cnf")
		if err = durableText(client, "[client]\nuser="+optionQuote(s.Superuser.Username)+"\npassword="+optionQuote(s.Superuser.Password)+"\nsocket="+optionQuote(q.socket)+"\n"); err != nil {
			return
		}
		defer os.Remove(client)
		argv := replayBinlogArguments(logs, backup, target)
		a := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqlbinlog"), argv...)
		a.Env = nativeEnv()
		b := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysql"), "--defaults-file="+client, "--binary-mode")
		b.Env = nativeEnv()
		if err = pipeline(a, b); err != nil {
			return
		}
	}
	for _, accounts := range recovery {
		accounts.Existing, err = rekeyRestoredAccounts(ctx, db, accounts.Users)
		if err != nil {
			return
		}
	}
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid,@@gtid_executed").Scan(&uuid, &gtid); err != nil {
		return
	}
	actual, e := parseRecoveryGTIDs(gtid)
	if e != nil {
		err = e
		return
	}
	want, e := parseRecoveryGTIDs(target)
	if e != nil {
		err = e
		return
	}
	if !actual.contains(want) || !want.contains(actual) {
		err = errors.New("restored_gtid_mismatch")
	}
	return
}

func replayBinlogArguments(logs replayLogs, backup, target string) []string {
	argv := []string{"--skip-gtids=false", "--exclude-gtids=" + backup, "--include-gtids=" + target}
	if logs.StartPosition != 0 {
		argv = append(argv, "--start-position="+strconv.FormatUint(logs.StartPosition, 10))
	}
	for _, file := range logs.Files {
		argv = append(argv, file.Path)
	}
	return argv
}
