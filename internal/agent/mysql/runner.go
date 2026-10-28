// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type Runner struct {
	mu                     sync.RWMutex
	current                myspec.Spec
	dir                    string
	node                   *agent.Node
	db                     *sql.DB
	dbKey                  [32]byte
	metricMu               sync.Mutex
	questions              float64
	questionTime           time.Time
	rejoinCancel           context.CancelFunc
	archiveCancel          context.CancelFunc
	proxyTLSReloadFallback bool
}

func New() *Runner                  { return &Runner{} }
func (*Runner) Name() string        { return myspec.Engine }
func (*Runner) Processes() []string { return []string{"mysqld", "proxysql"} }
func (e *Runner) state() (myspec.Spec, paths) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current, dataPaths(e.dir)
}
func (e *Runner) GroupSpec() myspec.Spec   { s, _ := e.state(); return s }
func (e *Runner) GroupReceiptsDir() string { _, p := e.state(); return p.root }
func (e *Runner) GroupIdentity() (myspec.DataIdentity, error) {
	_, p := e.state()
	return readIdentity(p)
}
func (e *Runner) SaveGroupIdentity(id myspec.DataIdentity) error {
	_, p := e.state()
	return durableJSON(filepath.Join(p.root, "initialized.json"), id)
}
func localDB(s myspec.Spec, p paths) (*sql.DB, error) {
	c := mysqldriver.NewConfig()
	c.Net = "unix"
	c.Addr = p.socket
	c.User = s.Agent.Username
	c.Passwd = s.Agent.Password
	c.Collation = "utf8mb4_0900_ai_ci"
	c.Timeout = 3 * time.Second
	c.WriteTimeout = 5 * time.Second
	db, err := sql.Open("mysql", c.FormatDSN())
	if err == nil {
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(2)
		db.SetConnMaxLifetime(time.Minute)
	}
	return db, err
}
func credentialHash(s myspec.Spec) [32]byte {
	b, _ := json.Marshal([]myspec.Credentials{s.Superuser, s.Agent, s.Replication, s.Monitor, s.BackupUser, s.Admin})
	return sha256.Sum256(b)
}
func (e *Runner) LocalAccessDB(ctx context.Context) (*sql.DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.current.Role != myspec.RoleData || e.node == nil || e.node.Maintenance() || e.node.Stopped("mysqld") {
		return nil, errors.New("MySQL local administration unavailable")
	}
	states, _ := e.node.Snapshot()
	if states["mysqld"].PID == 0 {
		return nil, errors.New("MySQL server stopped")
	}
	key := credentialHash(e.current)
	if e.db == nil || e.dbKey != key {
		if e.db != nil {
			_ = e.db.Close()
		}
		db, err := localDB(e.current, dataPaths(e.dir))
		if err != nil {
			return nil, err
		}
		e.db = db
		e.dbKey = key
	}
	if err := e.db.PingContext(ctx); err != nil {
		return nil, err
	}
	return e.db, nil
}
func (e *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	s, err := decodeSpec(c)
	if err != nil {
		return nil, err
	}
	p := dataPaths(n.DataDir())
	for _, dir := range []string{p.root, p.run} {
		if err = privateDir(dir); err != nil {
			return nil, err
		}
	}
	if err = syncDirectoryHierarchy(p.run, n.DataDir()); err != nil {
		return nil, err
	}
	if s.Galera != nil {
		if err = admitGaleraEpoch(s, p); err != nil {
			return nil, err
		}
		states, _ := n.Snapshot()
		if err = guardGaleraTrafficRotation(s, p, states["mysqld"].PID != 0); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	previous := e.current
	e.current = s
	e.dir = n.DataDir()
	e.mu.Unlock()
	e.resetArchiveLoop(false)
	for name, text := range map[string]string{"ca.pem": s.CAPEM, "server-cert.pem": s.ServerCert.CertPEM, "server-key.pem": s.ServerCert.KeyPEM} {
		if err = n.WriteFile("mysql/"+name, []byte(text), 0600); err != nil {
			return nil, err
		}
	}
	if s.Galera != nil {
		for name, text := range map[string]string{"galera-ca.pem": s.Galera.TrafficCAPEM, "galera-cert.pem": s.Galera.TrafficCert.CertPEM, "galera-key.pem": s.Galera.TrafficCert.KeyPEM} {
			if err = n.WriteFile("mysql/"+name, []byte(text), 0600); err != nil {
				return nil, err
			}
		}
	}
	if s.Role == myspec.RoleProxy {
		for name, text := range map[string]string{"proxysql-ca.pem": s.CAPEM, "proxysql-cert.pem": s.ServerCert.CertPEM, "proxysql-key.pem": s.ServerCert.KeyPEM} {
			if err = n.WriteFile("mysql/"+name, []byte(text), 0600); err != nil {
				return nil, err
			}
		}
		if err = stopConfigured(ctx, n, "mysqld"); err != nil {
			return nil, err
		}
		text, err := RenderProxyConfig(s, p.root, p.root)
		if err != nil {
			return nil, err
		}
		if err = n.WriteFile("mysql/proxysql.cnf", []byte(text), 0600); err != nil {
			return nil, err
		}
		// This component is stateless: sealed configuration, not proxysql.db,
		// owns credentials and startup-only listener settings on every restart.
		if err = n.Configure("proxysql", agent.ProcessSpec{Binary: nativeProxyBinary(), Args: []string{"--foreground", "--initial", "--config=" + filepath.Join(p.root, "proxysql.cnf")}, Dir: p.root, Env: nativeEnv()}); err != nil {
			return nil, err
		}
		tlsChanged := proxyTLSChanged(previous, s)
		states, _ := n.Snapshot()
		wasRunning := states["proxysql"].PID != 0
		listenerChanged := previous.Proxy != nil && (previous.Proxy.ReaderPort != s.Proxy.ReaderPort || previous.ListenHost != s.ListenHost)
		if listenerChanged {
			if err = stopConfigured(ctx, n, "proxysql"); err != nil {
				return nil, err
			}
		}
		if err = n.Action(ctx, "proxysql", "start"); err != nil {
			return nil, err
		}
		if c.Maintenance || n.Stopped("proxysql") || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "proxysql") {
			return nil, nil
		}
		if tlsChanged {
			e.mu.Lock()
			e.proxyTLSReloadFallback = false
			e.mu.Unlock()
			if wasRunning && !listenerChanged {
				if err = ReloadProxyTLS(ctx, s, p.root); err != nil {
					e.mu.Lock()
					e.proxyTLSReloadFallback = true
					e.mu.Unlock()
					n.Logger().Warn("ProxySQL TLS reload failed; restarting proxy")
					if err = n.Action(ctx, "proxysql", "restart"); err != nil {
						return nil, err
					}
				}
			}
		}
		return []string{"proxysql.cnf"}, ApplyProxyConfig(ctx, s)
	}
	if err = stopConfigured(ctx, n, "proxysql"); err != nil {
		return nil, err
	}
	if err = privateDir(p.data); err != nil {
		return nil, err
	}
	if err = syncDirectoryHierarchy(p.data, n.DataDir()); err != nil {
		return nil, err
	}
	// Config-only/offline observations must never start or initialize data.
	wanted := !c.Maintenance && !n.Stopped("mysqld") && (c.DesiredProcesses == nil || slices.Contains(c.DesiredProcesses, "mysqld"))
	fresh := false
	if wanted {
		if err = restoreStartGuard(p); err != nil {
			return nil, err
		}
		fresh, err = guardInitialization(s, p)
		if err != nil {
			return nil, err
		}
		if fresh {
			if err = initialize(ctx, s, p); err != nil {
				return nil, err
			}
			if s.Galera != nil {
				if err = authorizeGaleraStart(s, p); err != nil {
					return nil, err
				}
			}
		}
	}
	if err = refusePersistedVariables(p); err != nil {
		return nil, err
	}
	rekey := fresh || !credentialsRecorded(s, p)
	if s.Galera != nil && !s.Galera.Bootstrap {
		rekey = false // SST supplies the bootstrap member's identical accounts.
	}
	if rekey {
		if err = durableJSON(filepath.Join(p.root, "credentials-pending.json"), true); err != nil {
			return nil, err
		}
	}
	if err = e.writeConfig(n, s, p, rekey); err != nil {
		return nil, err
	}
	if err = configureMySQL(n, s, p); err != nil {
		return nil, err
	}
	if !wanted {
		return []string{"my.cnf"}, stopConfigured(ctx, n, "mysqld")
	}
	states, _ := n.Snapshot()
	running := states["mysqld"].PID != 0
	if running && rekey {
		if err = n.Action(ctx, "mysqld", "stop"); err != nil {
			return nil, err
		}
	}
	if err = n.Action(ctx, "mysqld", "start"); err != nil {
		return nil, err
	}
	db, err := waitLocal(ctx, s, p)
	if err != nil {
		return nil, errors.New(startupDiagnostic(e, err))
	}
	defer db.Close()
	if err = recordCredentials(s, p); err != nil {
		return nil, err
	}
	e.ensureRejoinWatcher()
	e.resetArchiveLoop(s.Backup != nil && s.Backup.Binlog.Enabled)
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return nil, err
	}
	id, err := readIdentity(p)
	if err != nil {
		return nil, err
	}
	if s.Galera != nil {
		status, err := galeraStatusDB(ctx, db)
		if err != nil {
			return nil, err
		}
		if err = refreshGaleraIdentity(ctx, db, s, p, status); err != nil {
			return nil, err
		}
		id, err = readIdentity(p)
		if err != nil {
			return nil, err
		}
	}
	if id.ServerUUID != "" && id.ServerUUID != uuid {
		return nil, errors.New("MySQL server identity changed unexpectedly")
	}
	id.Initialized = true
	id.ServerUUID = uuid
	if s.Galera != nil {
		id.FormationEpoch = s.Galera.FormationEpoch
	}
	if err = e.SaveGroupIdentity(id); err != nil {
		return nil, err
	}
	// Remove credential-changing statements after successful bootstrap.
	if err = e.writeConfig(n, s, p, false); err != nil {
		return nil, err
	}
	if running {
		if err = e.reload(ctx, db, s, previous); err != nil {
			return nil, err
		}
	}
	return []string{"my.cnf"}, nil
}
func stopConfigured(ctx context.Context, n *agent.Node, name string) error {
	states, _ := n.Snapshot()
	if _, ok := states[name]; !ok {
		return nil
	}
	return n.Action(ctx, name, "stop")
}
func (e *Runner) writeConfig(n *agent.Node, s myspec.Spec, p paths, rekey bool) error {
	if err := n.WriteFile("mysql/startup.sql", []byte(renderStartup(s, p, rekey)), 0600); err != nil {
		return err
	}
	return n.WriteFile("mysql/my.cnf", []byte(renderConfig(s, p)), 0600)
}
func (e *Runner) RefreshGroupConfig(ctx context.Context) error {
	e.mu.RLock()
	n := e.node
	e.mu.RUnlock()
	s, p := e.state()
	return e.writeConfig(n, s, p, false)
}
func (e *Runner) CloneRecipientStopped() bool {
	e.mu.RLock()
	n := e.node
	e.mu.RUnlock()
	if n == nil {
		return false
	}
	states, _ := n.Snapshot()
	state, ok := states["mysqld"]
	return ok && state.PID == 0
}

func (e *Runner) RestartAfterClone(ctx context.Context) error {
	e.mu.RLock()
	n := e.node
	e.mu.RUnlock()
	_, p := e.state()
	return n.WithLock(ctx, func(n *agent.Node) error {
		if err := n.Action(ctx, "mysqld", "stop"); err != nil {
			return err
		}
		// CLONE replaces the recipient process and datadir. No session from
		// its old administration pool belongs to the restarted recipient.
		e.mu.Lock()
		if e.db != nil {
			_ = e.db.Close()
			e.db = nil
		}
		e.mu.Unlock()
		if err := refusePersistedVariables(p); err != nil {
			return err
		}
		if err := e.RefreshGroupConfig(ctx); err != nil {
			return err
		}
		if err := n.Action(ctx, "mysqld", "start"); err != nil {
			return err
		}
		s, _ := e.state()
		db, err := waitLocal(ctx, s, p)
		if err != nil {
			return err
		}
		return db.Close()
	})
}
func (e *Runner) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	if name != "mysqld" {
		return nil
	}
	s, p := e.state()
	if err := restoreStartGuard(p); err != nil {
		return err
	}
	if s.Galera != nil {
		if _, err := guardInitialization(s, p); err != nil {
			return err
		}
		if err := authorizeGaleraStart(s, p); err != nil {
			return err
		}
	}
	if err := prepareGaleraBootstrap(s, p, n); err != nil {
		return err
	}
	return refusePersistedVariables(p)
}
func (e *Runner) Version(ctx context.Context) string {
	s, p := e.state()
	if s.Role == myspec.RoleProxy {
		cmd := exec.CommandContext(ctx, "/usr/bin/proxysql", "--version")
		cmd.Env = nativeEnv()
		out, err := cmd.Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	db, err := localDB(s, p)
	if err != nil {
		return ""
	}
	defer db.Close()
	var v string
	_ = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v)
	return v
}
func (e *Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	dir := o.DataDir
	for _, arg := range args {
		v, ok := strings.CutPrefix(arg, "--data-dir=")
		if !ok || !filepath.IsAbs(v) {
			return errors.New("mysql readiness accepts --data-dir=<absolute path>")
		}
		dir = v
	}
	b, err := os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
	if err != nil {
		return err
	}
	var c agentapi.MemberConfig
	if err = json.Unmarshal(b, &c); err != nil {
		return err
	}
	s, err := decodeSpec(c)
	if err != nil {
		return err
	}
	var controls struct {
		Maintenance bool
		Stopped     map[string]bool
	}
	b, err = os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(b) > 0 {
		if err = json.Unmarshal(b, &controls); err != nil {
			return err
		}
	}
	process := "mysqld"
	if s.Role == myspec.RoleProxy {
		process = "proxysql"
	}
	if c.Maintenance || controls.Maintenance || controls.Stopped[process] || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, process) {
		return nil
	}
	if s.Role == myspec.RoleProxy {
		return ProxyReady(ctx, s)
	}
	db, err := localDB(s, dataPaths(dir))
	if err != nil {
		return err
	}
	defer db.Close()
	if s.Galera != nil {
		status, err := galeraStatusDB(ctx, db)
		if err != nil {
			return err
		}
		if !status.Eligible() {
			return errors.New("galera member is not Primary, Synced and ready")
		}
		return nil
	}
	return db.PingContext(ctx)
}

var _ agent.Engine = (*Runner)(nil)
