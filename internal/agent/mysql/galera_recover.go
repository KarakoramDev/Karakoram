// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var recoveredPosition = regexp.MustCompile(`(?:WSREP:|\[WSREP\]) Recovered position: ([0-9a-fA-F-]+:[0-9]+)(?:\r?\n|$)`)

func parseRecoveredPosition(raw []byte) (myspec.WSRepRecoverResponse, error) {
	matches := recoveredPosition.FindAllSubmatch(raw, -1)
	if len(matches) != 1 {
		return myspec.WSRepRecoverResponse{}, errors.New("missing or ambiguous wsrep recovery position")
	}
	u, n, err := myspec.ParseGaleraPosition(string(matches[0][1]))
	return myspec.WSRepRecoverResponse{UUID: u, Seqno: n, Stopped: err == nil}, err
}
func (e *Runner) wsrepRecover(ctx context.Context, n *agent.Node) (myspec.WSRepRecoverResponse, error) {
	s, p := e.state()
	var out myspec.WSRepRecoverResponse
	if s.Galera == nil || s.Role != myspec.RoleData {
		return out, errors.New("galera data member required")
	}
	c := n.Config()
	states, _ := n.Snapshot()
	intentional := n.Stopped("mysqld") || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "mysqld")
	if !intentional || states["mysqld"].PID != 0 {
		return out, errors.New("wsrep recovery requires a CP-intended stopped member")
	}
	id, err := readIdentity(p)
	if err != nil || !id.Initialized {
		return out, errors.New("initialized Galera identity required")
	}
	if _, err := guardInitialization(s, p); err != nil {
		return out, err
	}
	out, err = recoverGaleraPosition(ctx, s, p)
	if err != nil {
		return out, err
	}
	r, err := guardGaleraIdentity(s, p)
	if err != nil || r.StateUUID != "" && out.UUID != r.StateUUID {
		return myspec.WSRepRecoverResponse{}, errors.New("recovered Galera state identity mismatch")
	}
	if err = durableText(filepath.Join(p.root, "galera-state-uuid"), out.UUID+"\n"); err != nil {
		return myspec.WSRepRecoverResponse{}, err
	}
	r.StateUUID = out.UUID
	if err = durableJSON(filepath.Join(p.root, "sst-authorized.json"), r); err != nil {
		return myspec.WSRepRecoverResponse{}, err
	}
	// Recovery can materialize auto.cnf deleted by upstream SST. Adopt that
	// UUID only under the still-pending own-member SST authorization.
	if r.Pending {
		raw, err := os.ReadFile(filepath.Join(p.data, "auto.cnf"))
		if err != nil {
			return myspec.WSRepRecoverResponse{}, err
		}
		uuid := ""
		for _, line := range strings.Split(string(raw), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok && strings.TrimSpace(key) == "server-uuid" {
				uuid = strings.TrimSpace(value)
			}
		}
		if !myspec.ValidUUID(uuid) {
			return myspec.WSRepRecoverResponse{}, errors.New("recovered MySQL server identity invalid")
		}
		id.ServerUUID = uuid
		if err = durableJSON(filepath.Join(p.root, "initialized.json"), id); err != nil {
			return myspec.WSRepRecoverResponse{}, err
		}
	}
	return out, nil
}

func recoverGaleraPosition(ctx context.Context, s myspec.Spec, p paths) (myspec.WSRepRecoverResponse, error) {
	var out myspec.WSRepRecoverResponse
	log, err := os.CreateTemp(p.root, "wsrep-recover-*.log")
	if err != nil {
		return out, err
	}
	name := log.Name()
	_ = log.Close()
	defer os.Remove(name)
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqld"), "--defaults-file="+p.config, "--wsrep-recover", "--skip-networking", "--init-file=/dev/null", "--log-error="+name)
	cmd.Env = galeraEnv(s)
	if err = cmd.Run(); err != nil {
		return out, errors.New("wsrep recovery failed")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		return out, err
	}
	return parseRecoveredPosition(raw)
}

func admitGaleraEpoch(s myspec.Spec, p paths) error {
	var epoch uint64
	raw, err := os.ReadFile(filepath.Join(p.root, "galera-epoch.json"))
	if err == nil {
		if err = json.Unmarshal(raw, &epoch); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if epoch > s.Galera.FormationEpoch {
		return errors.New("stale Galera formation epoch")
	}
	return durableJSON(filepath.Join(p.root, "galera-epoch.json"), s.Galera.FormationEpoch)
}
func setSafeToBootstrap(p paths) error {
	name := filepath.Join(p.data, "grastate.dat")
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return errors.New("invalid Galera state file")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	found := false
	for i, line := range lines {
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(k) != "safe_to_bootstrap" {
			continue
		}
		if found || (strings.TrimSpace(v) != "0" && strings.TrimSpace(v) != "1") {
			return errors.New("invalid Galera bootstrap flag")
		}
		found = true
		// Preserve every byte except the value itself.
		at := strings.Index(line, ":") + 1
		tail := line[at:]
		offset := strings.IndexAny(tail, "01")
		lines[i] = line[:at+offset] + "1" + line[at+offset+1:]
	}
	if !found {
		return errors.New("galera bootstrap flag missing")
	}
	return durableText(name, strings.Join(lines, "\n"))
}
func galeraClaimPath(s myspec.Spec, p paths) string {
	return filepath.Join(p.root, fmt.Sprintf("galera-bootstrap-%d", s.Galera.FormationEpoch))
}
func prepareGaleraBootstrap(s myspec.Spec, p paths, n *agent.Node) error {
	if s.Galera == nil || !s.Galera.Bootstrap {
		return nil
	}
	if err := admitGaleraEpoch(s, p); err != nil {
		return err
	}
	if _, err := os.Stat(galeraClaimPath(s, p)); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	states, _ := n.Snapshot()
	if states["mysqld"].PID != 0 {
		return errors.New("galera bootstrap requires a stopped member")
	}
	if _, err := os.Lstat(filepath.Join(p.data, "grastate.dat")); os.IsNotExist(err) {
		if s.Galera.StartPosition != "" {
			return errors.New("recovered bootstrap requires Galera state")
		}
		return nil
	} else if err != nil {
		return err
	}
	if s.Galera.StartPosition == "" {
		return errors.New("existing Galera history requires recovered start position")
	}
	return setSafeToBootstrap(p)
}
func (e *Runner) galeraBootstrap(ctx context.Context, n *agent.Node, req myspec.GaleraBootstrapRequest) error {
	s, p := e.state()
	if s.Galera == nil || !s.Galera.Bootstrap || req.ClusterName != s.Galera.ClusterName || req.FormationEpoch != s.Galera.FormationEpoch || req.StartPosition != s.Galera.StartPosition {
		return errors.New("galera bootstrap intent mismatch")
	}
	if _, err := os.Stat(galeraClaimPath(s, p)); err == nil {
		return errors.New("galera epoch already bootstrapped")
	} else if !os.IsNotExist(err) {
		return err
	}
	if n.Stopped("mysqld") || n.Maintenance() {
		return errors.New("galera bootstrap cannot release a stop hold")
	}
	if err := prepareGaleraBootstrap(s, p, n); err != nil {
		return err
	}
	if err := configureMySQL(n, s, p); err != nil {
		return err
	}
	return n.Action(ctx, "mysqld", "start")
}
func (e *Runner) galeraForcePrimary(ctx context.Context, req myspec.GaleraForcePrimaryRequest) error {
	s, p := e.state()
	if s.Galera == nil || req.ClusterName != s.Galera.ClusterName || req.FormationEpoch != s.Galera.FormationEpoch || req.Confirmation != myspec.GaleraForcePrimaryConfirmation {
		return errors.New("explicit fenced-member operator confirmation required")
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	status, err := galeraStatusDB(ctx, db)
	if err != nil {
		return err
	}
	identity, err := readGaleraIdentity(s, p)
	if err != nil || identity.StateUUID == "" || identity.StateUUID != status.StateUUID {
		return errors.New("minority recovery requires own Galera state identity")
	}
	if status.ClusterStatus == "Primary" {
		return errors.New("member is already Primary")
	}
	_, err = db.ExecContext(ctx, "SET GLOBAL wsrep_provider_options='pc.bootstrap=YES'")
	return err
}
func (e *Runner) galeraRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{myspec.ActionGaleraStatus, myspec.ActionWSRepRecover, myspec.ActionGaleraBootstrap, myspec.ActionGaleraForcePrimary, myspec.ActionGaleraPrepareJoin} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
			defer cancel()
			var result any = struct{}{}
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				var err error
				switch action {
				case myspec.ActionGaleraStatus:
					result, err = e.galeraStatus(ctx)
				case myspec.ActionWSRepRecover:
					result, err = e.wsrepRecover(ctx, locked)
				case myspec.ActionGaleraPrepareJoin:
					var req myspec.GaleraBootstrapRequest
					if err = decodeGroupRequest(w, r, &req); err == nil {
						err = e.galeraPrepareJoin(locked, req)
					}
				case myspec.ActionGaleraBootstrap:
					var req myspec.GaleraBootstrapRequest
					if err = decodeGroupRequest(w, r, &req); err == nil {
						err = e.galeraBootstrap(ctx, locked, req)
					}
				case myspec.ActionGaleraForcePrimary:
					var req myspec.GaleraForcePrimaryRequest
					if err = decodeGroupRequest(w, r, &req); err == nil {
						err = e.galeraForcePrimary(ctx, req)
					}
				}
				return err
			})
			if err != nil {
				http.Error(w, "Galera "+action+" unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
		})
	}
}

func decodeGroupRequest(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("trailing Galera request")
	}
	return nil
}
