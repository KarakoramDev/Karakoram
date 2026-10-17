// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type upgradeReceipt struct {
	Request       pgspec.UpgradeRequest `json:"request"`
	Phase         string                `json:"phase"`
	TargetCreated bool                  `json:"targetCreated,omitempty"`
	Initialized   bool                  `json:"initialized,omitempty"`
	Checked       bool                  `json:"checked"`
	Done          bool                  `json:"done"`
}
type upgradePaths struct{ target, retired, work, password, passfile string }

func pathsForUpgrade(p paths, request pgspec.UpgradeRequest) upgradePaths {
	sum := sha256.Sum256([]byte(request.Receipt))
	suffix := fmt.Sprintf("%x", sum[:12])
	base := filepath.Dir(p.pgdata)
	return upgradePaths{target: filepath.Join(base, "postgres-upgrade-"+suffix), retired: filepath.Join(base, "postgres-retired-"+suffix), work: filepath.Join(p.root, "upgrade-work", suffix), password: filepath.Join(p.root, "upgrade.password"), passfile: filepath.Join(p.root, "upgrade.pgpass")}
}
func validateUpgradeRequest(request pgspec.UpgradeRequest) error {
	if request.Receipt == "" || !safeValue(request.Receipt, 256) {
		return errors.New("upgrade receipt required")
	}
	if request.OldMajor < 16 || request.NewMajor > 18 || request.NewMajor != request.OldMajor+1 {
		return errors.New("only adjacent PostgreSQL major upgrades are supported")
	}
	if !request.Replica && (request.Locale == "" || !safeValue(request.Locale, 128)) {
		return errors.New("existing template1 locale required for upgrade")
	}
	return nil
}
func pgMajorAt(path string) (int, error) {
	b, err := os.ReadFile(filepath.Join(path, "PG_VERSION"))
	if err != nil {
		return 0, err
	}
	major, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, errors.New("invalid PostgreSQL data version")
	}
	return major, nil
}
func upgradeStopped(n *agent.Node) error {
	c := n.Config()
	desiredStopped := c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "postgres") && !slices.Contains(c.DesiredProcesses, "patroni")
	if !n.Maintenance() && !c.Maintenance && !desiredStopped {
		return errors.New("upgrade requires PostgreSQL maintenance or explicit stopped process intent")
	}
	states, _ := n.Snapshot()
	for _, name := range []string{"postgres", "patroni"} {
		if states[name].PID != 0 {
			return errors.New("stop PostgreSQL and Patroni before upgrade")
		}
	}
	return nil
}
func assertOfflineData(path string) error {
	data, err := os.ReadFile(filepath.Join(path, "postmaster.pid"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	line, _, _ := strings.Cut(string(data), "\n")
	pid, err := strconv.Atoi(line)
	if err != nil || pid <= 0 {
		return errors.New("invalid PostgreSQL process identity")
	}
	if err = syscall.Kill(pid, 0); err == nil || errors.Is(err, syscall.EPERM) {
		return errors.New("PostgreSQL is still running for the upgrade data directory")
	}
	if !errors.Is(err, syscall.ESRCH) {
		return errors.New("cannot establish PostgreSQL stopped state")
	}
	return nil
}
func upgradeCommand(ctx context.Context, binary, dir, passfile string, args ...string) error {
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = dir
	command.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "PGPASSFILE=" + passfile}
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = time.Second
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	if err := command.Run(); err != nil {
		return fmt.Errorf("PostgreSQL upgrade command failed: %w", err)
	}
	return nil
}
func upgradeArgs(request pgspec.UpgradeRequest, s pgspec.Spec, p paths, u upgradePaths, check bool) []string {
	oldBin := fmt.Sprintf("/usr/lib/postgresql/%d/bin", request.OldMajor)
	newBin := fmt.Sprintf("/usr/lib/postgresql/%d/bin", request.NewMajor)
	oldConfig := filepath.Join(p.pgdata, "postgresql.conf")
	if s.Topology == pgspec.TopologySingle {
		oldConfig = filepath.Join(p.root, "postgresql.conf")
	}
	args := []string{"--old-bindir=" + oldBin, "--new-bindir=" + newBin, "--old-datadir=" + p.pgdata, "--new-datadir=" + u.target, "--username=" + s.Superuser.Username, "--old-port=55432", "--new-port=55433", "--socketdir=" + p.socket, "--old-options=-c config_file=" + shellQuote(oldConfig) + " -c archive_mode=off", "--link"}
	if check {
		args = append(args, "--check")
	}
	return args
}
func stopUpgradeServers(request pgspec.UpgradeRequest, p paths, u upgradePaths) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var result error
	for _, item := range []struct {
		major int
		dir   string
	}{{request.OldMajor, p.pgdata}, {request.NewMajor, u.target}} {
		if _, err := os.Stat(filepath.Join(item.dir, "postmaster.pid")); os.IsNotExist(err) {
			continue
		}
		if err := upgradeCommand(ctx, fmt.Sprintf("/usr/lib/postgresql/%d/bin/pg_ctl", item.major), u.work, u.passfile, "-D", item.dir, "-m", "fast", "-w", "-t", "10", "stop"); err != nil {
			result = errors.Join(result, errors.New("temporary PostgreSQL upgrade server did not stop"))
		}
	}
	return result
}
func prepareUpgradeTarget(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths, u upgradePaths, receipt *upgradeReceipt, receiptName string) error {
	if err := privateDir(u.work); err != nil {
		return err
	}
	for _, major := range []int{receipt.Request.OldMajor, receipt.Request.NewMajor} {
		for _, binary := range []string{"postgres", "pg_ctl"} {
			if _, err := os.Stat(fmt.Sprintf("/usr/lib/postgresql/%d/bin/%s", major, binary)); err != nil {
				return errors.New("both PostgreSQL major packages must be installed before upgrade")
			}
		}
	}
	if err := n.WriteFile("postgres/upgrade.password", []byte(s.Superuser.Password+"\n"), 0600); err != nil {
		return err
	}
	escape := strings.NewReplacer(`\`, `\\`, ":", `\:`)
	if err := n.WriteFile("postgres/upgrade.pgpass", []byte("*:*:*:"+escape.Replace(s.Superuser.Username)+":"+escape.Replace(s.Superuser.Password)+"\n"), 0600); err != nil {
		return err
	}
	if receipt.Initialized {
		major, err := pgMajorAt(u.target)
		if err != nil || major != receipt.Request.NewMajor {
			return errors.New("upgrade target data version changed")
		}
		return nil
	}
	if !receipt.TargetCreated {
		if err := os.Mkdir(u.target, 0700); err != nil {
			return errors.New("upgrade target must be a fresh owned directory")
		}
		if err := syncUpgradeParent(p); err != nil {
			return err
		}
		receipt.TargetCreated = true
		if err := durableJSON(receiptName, receipt); err != nil {
			return err
		}
	} else {
		if err := assertOfflineData(u.target); err != nil {
			return err
		}
		// Only an uninitialised directory created under this receipt is disposable.
		info, err := os.Lstat(u.target)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("upgrade target is not an owned data directory")
		}
		if err = os.RemoveAll(u.target); err != nil {
			return err
		}
		if err = os.Mkdir(u.target, 0700); err != nil {
			return err
		}
	}
	args := []string{"-D", u.target, "--username=" + s.Superuser.Username, "--pwfile=" + u.password, "--data-checksums", "--encoding=UTF8", "--locale=" + receipt.Request.Locale, "--auth-local=scram-sha-256", "--auth-host=scram-sha-256"}
	if err := upgradeCommand(ctx, fmt.Sprintf("/usr/lib/postgresql/%d/bin/initdb", receipt.Request.NewMajor), u.work, u.passfile, args...); err != nil {
		return err
	}
	receipt.Initialized = true
	return durableJSON(receiptName, receipt)
}
func syncUpgradeParent(p paths) error {
	dir, err := os.Open(filepath.Dir(p.pgdata))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func activateUpgrade(p paths, u upgradePaths, receipt *upgradeReceipt, receiptName string) error {
	if receipt.Phase == "linked" {
		_, oldErr := os.Lstat(p.pgdata)
		_, retiredErr := os.Lstat(u.retired)
		switch {
		case oldErr == nil && os.IsNotExist(retiredErr):
			if err := os.Rename(p.pgdata, u.retired); err != nil {
				return err
			}
		case os.IsNotExist(oldErr) && retiredErr == nil:
		default:
			return errors.New("upgrade activation found conflicting old data directories")
		}
		if err := syncUpgradeParent(p); err != nil {
			return err
		}
		receipt.Phase = "old-retired"
		if err := durableJSON(receiptName, receipt); err != nil {
			return err
		}
	}
	if receipt.Phase == "old-retired" {
		_, currentErr := os.Lstat(p.pgdata)
		_, targetErr := os.Lstat(u.target)
		switch {
		case os.IsNotExist(currentErr) && targetErr == nil:
			if err := os.Rename(u.target, p.pgdata); err != nil {
				return err
			}
		case currentErr == nil && os.IsNotExist(targetErr):
			major, err := pgMajorAt(p.pgdata)
			if err != nil || major != receipt.Request.NewMajor {
				return errors.New("activated PostgreSQL data has an unexpected version")
			}
		default:
			return errors.New("upgrade activation found conflicting new data directories")
		}
		if err := syncUpgradeParent(p); err != nil {
			return err
		}
		receipt.Done = true
		receipt.Phase = "done"
		return durableJSON(receiptName, receipt)
	}
	return nil
}
func rebuildUpgradedReplica(p paths, u upgradePaths, receipt *upgradeReceipt, receiptName string) error {
	if receipt.Phase == "planned" {
		if err := assertOfflineData(p.pgdata); err != nil {
			return err
		}
		major, err := pgMajorAt(p.pgdata)
		if err != nil || major != receipt.Request.OldMajor {
			return errors.New("replica data does not match the old PostgreSQL major")
		}
		if _, err = os.Lstat(u.retired); !os.IsNotExist(err) {
			return errors.New("replica upgrade retirement directory already exists")
		}
		receipt.Phase = "retiring-replica"
		if err = durableJSON(receiptName, receipt); err != nil {
			return err
		}
	}
	if receipt.Phase == "retiring-replica" {
		if _, err := os.Lstat(u.retired); os.IsNotExist(err) {
			if err = os.Rename(p.pgdata, u.retired); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := privateDir(p.pgdata); err != nil {
			return err
		}
		entries, err := os.ReadDir(p.pgdata)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return errors.New("replica upgrade destination is not empty")
		}
		// The caller explicitly selected replica rebuilding after its primary was
		// upgraded; the retired old-major copy must not consume basebackup capacity.
		if err = os.RemoveAll(u.retired); err != nil {
			return err
		}
		if err := syncUpgradeParent(p); err != nil {
			return err
		}
		receipt.Checked = true
		receipt.Done = true
		receipt.Phase = "done"
		return durableJSON(receiptName, receipt)
	}
	return nil
}
func performUpgrade(ctx context.Context, n *agent.Node, request pgspec.UpgradeRequest, run bool) (pgspec.UpgradeResponse, error) {
	var response pgspec.UpgradeResponse
	if err := validateUpgradeRequest(request); err != nil {
		return response, err
	}
	s, err := decodeSpec(n.Config())
	if err != nil {
		return response, err
	}
	if s.Role != pgspec.RoleData {
		return response, errors.New("upgrade requires a data member")
	}
	if request.Replica && s.Topology != pgspec.TopologyHA {
		return response, errors.New("replica rebuilding requires an HA data member")
	}
	p := dataPaths(n.DataDir())
	name, err := receiptPath(p, "upgrade", request.Receipt)
	if err != nil {
		return response, err
	}
	u := pathsForUpgrade(p, request)
	receipt := upgradeReceipt{Request: request, Phase: "planned"}
	data, err := os.ReadFile(name)
	if err == nil {
		if json.Unmarshal(data, &receipt) != nil || receipt.Request != request {
			return response, errors.New("upgrade receipt does not match the requested upgrade")
		}
	} else if !os.IsNotExist(err) {
		return response, err
	}
	response = pgspec.UpgradeResponse{Checked: receipt.Checked, Done: receipt.Done}
	if receipt.Done {
		return response, nil
	}
	if err = upgradeStopped(n); err != nil {
		return response, err
	}
	if request.Replica {
		if !run {
			return response, errors.New("replicas are rebuilt, not checked with pg_upgrade")
		}
		if err = rebuildUpgradedReplica(p, u, &receipt, name); err != nil {
			return response, err
		}
		return pgspec.UpgradeResponse{Checked: receipt.Checked, Done: receipt.Done}, nil
	}
	if receipt.Phase == "linking" {
		return response, errors.New("an interrupted pg_upgrade link requires recovery before retry; old linked data must remain stopped")
	}
	if receipt.Phase == "linked" || receipt.Phase == "old-retired" {
		if err = activateUpgrade(p, u, &receipt, name); err != nil {
			return response, err
		}
		return pgspec.UpgradeResponse{Checked: receipt.Checked, Done: receipt.Done}, nil
	}
	major, err := pgMajorAt(p.pgdata)
	if err != nil || major != request.OldMajor {
		return response, errors.New("data directory does not match the old PostgreSQL major")
	}
	if err = assertOfflineData(p.pgdata); err != nil {
		return response, err
	}
	for _, signal := range []string{"standby.signal", "recovery.signal"} {
		if _, err = os.Stat(filepath.Join(p.pgdata, signal)); err == nil {
			return response, errors.New("pg_upgrade requires the former writable primary, not a recovery data directory")
		} else if !os.IsNotExist(err) {
			return response, err
		}
	}
	if err = prepareUpgradeTarget(ctx, n, s, p, u, &receipt, name); err != nil {
		return response, err
	}
	binary := fmt.Sprintf("/usr/lib/postgresql/%d/bin/pg_upgrade", request.NewMajor)
	if !receipt.Checked {
		err = upgradeCommand(ctx, binary, u.work, u.passfile, upgradeArgs(request, s, p, u, true)...)
		stopErr := stopUpgradeServers(request, p, u)
		if err != nil {
			return response, err
		}
		if stopErr != nil {
			return response, stopErr
		}
		receipt.Checked = true
		receipt.Phase = "checked"
		if err = durableJSON(name, receipt); err != nil {
			return response, err
		}
	}
	response.Checked = true
	if !run {
		return response, nil
	}
	receipt.Phase = "linking"
	if err = durableJSON(name, receipt); err != nil {
		return response, err
	}
	err = upgradeCommand(ctx, binary, u.work, u.passfile, upgradeArgs(request, s, p, u, false)...)
	stopErr := stopUpgradeServers(request, p, u)
	if err != nil {
		return response, err
	}
	if stopErr != nil {
		return response, stopErr
	}
	receipt.Phase = "linked"
	if err = durableJSON(name, receipt); err != nil {
		return response, err
	}
	if err = activateUpgrade(p, u, &receipt, name); err != nil {
		return response, err
	}
	_ = os.Remove(u.password)
	_ = os.Remove(u.passfile)
	return pgspec.UpgradeResponse{Checked: true, Done: true}, nil
}

// applyUpgradeData preserves the deliberately absent PGDATA between activation
// renames. Apply and upgrade actions already share the member configuration lock.
// Only the upgrade action may advance the receipt; conflicting paths remain its
// fail-closed responsibility.
func applyUpgradeData(n *agent.Node, p paths) error {
	entries, err := os.ReadDir(filepath.Join(p.root, "receipts", "upgrade"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	active := false
	for _, entry := range entries {
		// A crash before durableJSON's atomic rename may leave an unpublished
		// temporary file. It is not a journal and must not mask the last receipt.
		if strings.HasPrefix(entry.Name(), ".receipt-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(p.root, "receipts", "upgrade", entry.Name()))
		if err != nil {
			return err
		}
		var receipt upgradeReceipt
		if json.Unmarshal(data, &receipt) != nil || validateUpgradeRequest(receipt.Request) != nil {
			return errors.New("invalid PostgreSQL upgrade journal")
		}
		if !receipt.Done {
			active = true
		}
	}
	if active {
		return upgradeStopped(n)
	}
	return privateDir(p.pgdata)
}

func stopForUpgrade(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) error {
	desired := n.Config().DesiredProcesses
	if desired == nil || slices.Contains(desired, "postgres") || slices.Contains(desired, "patroni") {
		return errors.New("upgrade stop requires durable stopped process intent")
	}
	// Paused Patroni leaves PostgreSQL running with its inherited log pipes.
	// Stop that postmaster before waiting for Patroni's supervised shutdown,
	// otherwise the supervisor cannot observe EOF and acknowledge stopped intent.
	if err := assertOfflineData(p.pgdata); err != nil {
		if _, err = command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "-D", p.pgdata, "-m", "fast", "-w", "stop"); err != nil {
			return errors.New("PostgreSQL did not stop for major upgrade")
		}
	}
	for _, process := range []string{"postgres", "patroni"} {
		if err := stopConfigured(ctx, n, process); err != nil {
			return err
		}
	}
	if err := upgradeStopped(n); err != nil {
		return err
	}
	return assertOfflineData(p.pgdata)
}
func (e *Runner) upgradeRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{"upgrade-pause", "upgrade-stop"} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			var request struct{}
			if err := decodeAction(w, r, &request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err := n.WithLock(r.Context(), func(locked *agent.Node) error {
				s, err := decodeSpec(locked.Config())
				if err != nil {
					return err
				}
				if s.Role != pgspec.RoleData {
					return errors.New("upgrade fencing requires a data member")
				}
				if action == "upgrade-stop" {
					return stopForUpgrade(r.Context(), locked, s, dataPaths(locked.DataDir()))
				}
				if s.Topology != pgspec.TopologyHA {
					return errors.New("upgrade pause requires Patroni")
				}
				return patroniRequest(r.Context(), s, "PATCH", "/config", map[string]any{"pause": true}, nil)
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			actionJSON(w, http.StatusOK, pgspec.ActionResponse{OK: true})
		})
	}
	for _, action := range []string{pgspec.ActionUpgradeCheck, pgspec.ActionUpgradeRun} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			var request pgspec.UpgradeRequest
			if err := decodeAction(w, r, &request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := validateUpgradeRequest(request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
			defer cancel()
			var response pgspec.UpgradeResponse
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				var err error
				response, err = performUpgrade(ctx, locked, request, action == pgspec.ActionUpgradeRun)
				return err
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			actionJSON(w, http.StatusOK, response)
		})
	}
}
