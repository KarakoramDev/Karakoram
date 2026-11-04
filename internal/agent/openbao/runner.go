// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package openbao supervises OpenBao without retaining plaintext initialization material.
package openbao

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

type Runner struct {
	mu      sync.RWMutex
	version string
}

func New() *Runner                  { return &Runner{} }
func (*Runner) Name() string        { return baospec.Engine }
func (*Runner) Processes() []string { return []string{baospec.Process} }
func (r *Runner) Version(context.Context) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

type started struct {
	Generation         int64  `json:"generation"`
	Binary             string `json:"binary"`
	PreviousPID        int    `json:"previousPID,omitempty"`
	PreviousGeneration int64  `json:"previousGeneration,omitempty"`
	PreviousBinary     string `json:"previousBinary,omitempty"`
}

func readStarted(dir string, pid int) (started, error) {
	var v started
	b, e := os.ReadFile(filepath.Join(dir, "karakoram/openbao/started.json"))
	if e != nil {
		return v, e
	}
	e = json.Unmarshal(b, &v)
	if e == nil && pid != 0 && pid == v.PreviousPID {
		v.Generation, v.Binary = v.PreviousGeneration, v.PreviousBinary
	}
	return v, e
}

// BeforeStart records launch intent without attributing it to the still-running
// child. If stopping that child fails, its previous generation remains visible.
func (*Runner) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	s, e := spec(n.Config())
	if e != nil {
		return e
	}
	states, _ := n.Snapshot()
	pid := states[baospec.Process].PID
	v := started{Generation: s.ProcessGeneration, Binary: s.BinaryPath}
	if pid != 0 {
		old, err := readStarted(n.DataDir(), pid)
		if err != nil {
			return errors.New("openbao started generation is unavailable")
		}
		v.PreviousPID, v.PreviousGeneration, v.PreviousBinary = pid, old.Generation, old.Binary
	}
	b, _ := json.Marshal(v)
	return n.WriteFile("openbao/started.json", b, 0600)
}
func (r *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	s, e := spec(c)
	if e != nil {
		return nil, e
	}
	files, e := render(n.DataDir(), s, c)
	if e != nil {
		return nil, e
	}
	changed := []string{}
	for name, b := range files {
		p := filepath.Join(n.DataDir(), "karakoram", name)
		old, err := os.ReadFile(p)
		st, se := os.Lstat(p)
		if err == nil && se == nil && st.Mode().IsRegular() && st.Mode().Perm() == 0600 && bytes.Equal(old, b) {
			continue
		}
		if e = n.WriteFile(name, b, 0600); e != nil {
			return nil, e
		}
		changed = append(changed, name)
	}
	root, e := os.OpenRoot(n.DataDir())
	if e != nil {
		return nil, e
	}
	defer root.Close()
	trust, err := root.OpenRoot("karakoram/openbao/trust")
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		defer trust.Close()
		dir, err := trust.Open(".")
		if err != nil {
			return nil, err
		}
		entries, err := dir.ReadDir(-1)
		dir.Close()
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			index, err := strconv.Atoi(strings.TrimSuffix(entry.Name(), ".pem"))
			if err == nil && strings.HasSuffix(entry.Name(), ".pem") && index >= len(s.TrustCAPEMs) {
				if err := trust.Remove(entry.Name()); err != nil {
					return nil, err
				}
				changed = append(changed, "openbao/trust/"+entry.Name())
			}
		}
	}
	for _, p := range []string{"openbao", "openbao/raft"} {
		if e = root.Mkdir(p, 0700); e != nil && !os.IsExist(e) {
			return nil, e
		}
		f, err := root.Open(p)
		if err != nil {
			return nil, err
		}
		err = f.Chmod(0700)
		f.Close()
		if err != nil {
			return nil, err
		}
	}
	env := []string{}
	if len(s.TrustCAPEMs) > 0 {
		env = append(env, "SSL_CERT_DIR=/etc/ssl/certs:"+filepath.Join(n.DataDir(), "karakoram/openbao/trust"))
	}
	if e = n.Configure(baospec.Process, agent.ProcessSpec{Binary: s.BinaryPath, Args: []string{"server", "-config=" + filepath.Join(n.DataDir(), "karakoram/openbao/server.json")}, Env: env, Dir: n.DataDir()}); e != nil {
		return nil, e
	}
	states, _ := n.Snapshot()
	action := "start"
	if states[baospec.Process].PID != 0 {
		old, err := readStarted(n.DataDir(), states[baospec.Process].PID)
		if err != nil {
			return nil, errors.New("openbao started generation is unavailable")
		}
		if old.Generation != s.ProcessGeneration || old.Binary != s.BinaryPath {
			action = "restart"
		} else {
			if len(changed) > 0 {
				if e = n.Signal(baospec.Process, syscall.SIGHUP); e != nil {
					return nil, e
				}
			}
			r.mu.Lock()
			r.version = s.Version
			r.mu.Unlock()
			return changed, nil
		}
	}
	if e = n.Action(ctx, baospec.Process, action); e != nil {
		return nil, e
	}
	r.mu.Lock()
	r.version = s.Version
	r.mu.Unlock()
	return changed, nil
}
func (*Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	for _, arg := range args {
		if len(arg) < 11 || arg[:11] != "--data-dir=" || !filepath.IsAbs(arg[11:]) {
			return errors.New("openbao readiness accepts only --data-dir=<absolute path>")
		}
		o.DataDir = arg[11:]
	}
	if o.DataDir == "" {
		o.DataDir = os.Getenv("KARAKORAM_DATA_DIR")
	}
	if o.DataDir == "" {
		return errors.New("data directory required")
	}
	var controls struct {
		Maintenance bool
		Stopped     map[string]bool
	}
	b, e := os.ReadFile(filepath.Join(o.DataDir, "karakoram/controls.json"))
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if len(b) > 0 {
		if e = json.Unmarshal(b, &controls); e != nil {
			return e
		}
	}
	if controls.Maintenance || controls.Stopped[baospec.Process] {
		return nil
	}
	b, e = os.ReadFile(filepath.Join(o.DataDir, "karakoram/config.json"))
	if e != nil {
		return e
	}
	var c agentapi.MemberConfig
	if e = json.Unmarshal(b, &c); e != nil {
		return e
	}
	if c.Maintenance {
		return nil
	}
	s, e := spec(c)
	if e != nil {
		return e
	}
	a, _, _ := ports(s)
	conn, e := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(a)))
	if e == nil {
		conn.Close()
	}
	return e
}
func localClient(c agentapi.MemberConfig, metrics bool) (*baoapi.Client, error) {
	s, e := spec(c)
	if e != nil {
		return nil, e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(c.TLS.CAPEM)) {
		return nil, errors.New("invalid openbao CA")
	}
	a, _, m := ports(s)
	if metrics {
		a = m
	}
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: s.NodeID, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
	return baoapi.New("https://127.0.0.1:"+strconv.Itoa(a), &http.Client{Transport: tr, Timeout: 30 * time.Second})
}
func status(ctx context.Context, n *agent.Node) baospec.StatusResponse {
	c := n.Config()
	s, _ := spec(c)
	out := baospec.StatusResponse{NodeID: s.NodeID, Sealed: true}
	states, _ := n.Snapshot()
	if v, e := readStarted(n.DataDir(), states[baospec.Process].PID); e == nil && states[baospec.Process].PID != 0 {
		out.ProcessGeneration = v.Generation
	}
	_, e := os.Stat(receiptPath(n))
	out.InitReceipt = e == nil
	client, e := localClient(c, false)
	if e != nil {
		out.Error = "openbao local API unavailable"
		return out
	}
	ss, e := client.SealStatus(ctx)
	if e != nil {
		out.Error = "openbao local API unavailable"
		return out
	}
	out.Initialized = ss.Initialized
	out.Sealed = ss.Sealed
	out.Version = ss.Version
	out.SealType = ss.Type
	out.RecoverySeal = ss.RecoverySeal
	out.Threshold = ss.T
	out.Progress = ss.Progress
	out.ClusterID = ss.ClusterID
	if h, err := client.Health(ctx); err == nil {
		out.Standby = h.Standby
	}
	if l, err := client.Leader(ctx); err == nil {
		out.Active = l.IsSelf
		out.HAEnabled = l.HAEnabled
		out.LeaderAddress = l.LeaderAddress
		out.RaftCommittedIndex = l.RaftCommittedIndex
		out.RaftAppliedIndex = l.RaftAppliedIndex
	}
	return out
}
