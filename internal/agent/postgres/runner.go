// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/jackc/pgx/v5"
)

type Runner struct {
	mu                sync.RWMutex
	current           pgspec.Spec
	dir               string
	credentialsHash   [32]byte
	rolesApplied      bool
	roleSQL           []string
	roleSQLHash       [32]byte
	dataAccessHash    [32]byte
	dataAccessApplied bool
	dataGrants        dataGrantCache
	proxyRestartHash  [32]byte
	proxyAdmin        pgspec.Credentials
	configured        bool
	watermark         archiveWatermark
}

func New() *Runner                  { return &Runner{} }
func (*Runner) Name() string        { return pgspec.Engine }
func (*Runner) Processes() []string { return []string{"postgres", "patroni", "etcd", "proxysql"} }
func hash(v any) [32]byte           { b, _ := json.Marshal(v); return sha256.Sum256(b) }
func (e *Runner) state() (pgspec.Spec, paths) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current, dataPaths(e.dir)
}
func unchangedFile(path, content string, mode os.FileMode) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != mode.Perm() || info.Size() != int64(len(content)) {
		return false
	}
	b, err := os.ReadFile(path)
	return err == nil && string(b) == content
}
func stopConfigured(ctx context.Context, n *agent.Node, name string) error {
	states, _ := n.Snapshot()
	if _, ok := states[name]; !ok {
		return nil
	}
	return n.Action(ctx, name, "stop")
}
func privateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("postgres directory must not be a symlink")
	}
	return os.Chmod(path, 0700)
}
func command(ctx context.Context, binary string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"}
	out, err := cmd.CombinedOutput()
	return out, err
}
func localConnect(ctx context.Context, s pgspec.Spec, p paths) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	cfg.Host = p.socket
	cfg.Port = 5432
	cfg.User = s.Superuser.Username
	cfg.Password = s.Superuser.Password
	cfg.Database = "postgres"
	cfg.TLSConfig = nil
	cfg.Fallbacks = nil
	cfg.ConnectTimeout = 3 * time.Second
	// Startup settings protect every management statement, including utility SQL.
	cfg.RuntimeParams = managementParams()
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil && recoveryCredentialsPending(s, p) && s.Bootstrap.RecoverySuperuser != nil {
		cfg.User = s.Bootstrap.RecoverySuperuser.Username
		cfg.Password = s.Bootstrap.RecoverySuperuser.Password
		c, err = pgx.ConnectConfig(ctx, cfg)
	}
	return c, err
}

func managementParams() map[string]string {
	return map[string]string{
		"application_name":                  "karakoram",
		"search_path":                       "pg_catalog",
		"log_statement":                     "none",
		"log_min_duration_statement":        "-1",
		"log_min_duration_sample":           "-1",
		"log_min_error_statement":           "panic",
		"log_parameter_max_length":          "0",
		"log_parameter_max_length_on_error": "0",
	}
}

func recoveryCredentialsPending(s pgspec.Spec, p paths) bool {
	if s.Bootstrap == nil || s.Bootstrap.Method != "restore" {
		return false
	}
	var generation int
	data, err := os.ReadFile(filepath.Join(p.root, "credentials-generation.json"))
	return err != nil || json.Unmarshal(data, &generation) != nil || generation != s.Bootstrap.Generation
}
func waitLocal(ctx context.Context, s pgspec.Spec, p paths) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		c, err := localConnect(ctx, s, p)
		if err == nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.New("PostgreSQL did not accept local connections before deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func roleStatements(s pgspec.Spec) ([]string, error) {
	result := []string{"SET search_path = pg_catalog", "SET log_statement = 'none'", "SET log_min_duration_statement = -1", "SET log_min_duration_sample = -1", "SET log_min_error_statement = 'panic'", "SET log_parameter_max_length = 0", "SET log_parameter_max_length_on_error = 0"}
	for _, c := range []pgspec.Credentials{s.Superuser, s.Monitor, s.Replication, s.Rewind} {
		verifier, err := secrets.SCRAMVerifier(c.Password)
		if err != nil {
			return nil, err
		}
		role := sqlIdent(c.Username)
		result = append(result, "DO $kk$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = "+sqlQuote(c.Username)+") THEN CREATE ROLE "+role+"; END IF; END $kk$")
		options := "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
		if c.Username == s.Superuser.Username {
			options = "LOGIN SUPERUSER"
		} else if c.Username == s.Replication.Username {
			options = "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS"
		}
		result = append(result, "ALTER ROLE "+role+" "+options+" PASSWORD "+sqlQuote(verifier))
	}
	result = append(result, "GRANT pg_monitor TO "+sqlIdent(s.Monitor.Username))
	for _, function := range []string{"pg_catalog.pg_ls_dir(text,boolean,boolean)", "pg_catalog.pg_stat_file(text,boolean)", "pg_catalog.pg_read_binary_file(text)", "pg_catalog.pg_read_binary_file(text,bigint,bigint,boolean)"} {
		result = append(result, "GRANT EXECUTE ON FUNCTION "+function+" TO "+sqlIdent(s.Rewind.Username))
	}
	dataSQL, err := dataRoleStatements(s.DataAccess)
	if err != nil {
		return nil, err
	}
	result = append(result, dataSQL...)
	return result, nil
}
func bootstrapRoles(ctx context.Context, c *pgx.Conn, s pgspec.Spec) error {
	statements, err := roleStatements(s)
	if err != nil {
		return err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		// Clients are still fenced and replicas have not been rebuilt. Persist
		// identities in local WAL without waiting for replicas whose startup
		// depends on this transaction; ordinary commit policy is unchanged.
		if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = local"); err != nil {
			return errors.New("system role recovery commit policy failed")
		}
	}
	for _, sql := range statements {
		if _, err = tx.Exec(ctx, sql); err != nil {
			return errors.New("system role bootstrap failed")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return reconcileDataGrants(ctx, c, s.DataAccess)
}
func initSingle(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) error {
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		return initSingleRestore(ctx, n, s, p, backupCommand)
	}
	if _, err := os.Stat(filepath.Join(p.pgdata, "PG_VERSION")); err == nil {
		return recordInitialization(p)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := guardInitialization(s, p); err != nil {
		return err
	}
	if err := n.WriteFile("postgres/initdb.password", []byte(s.Superuser.Password+"\n"), 0600); err != nil {
		return err
	}
	if _, err := command(ctx, filepath.Join(s.BinDir, "initdb"), "-D", p.pgdata, "--username="+s.Superuser.Username, "--pwfile="+filepath.Join(p.root, "initdb.password"), "--data-checksums", "--auth-local=scram-sha-256", "--auth-host=scram-sha-256", "--encoding=UTF8", "--no-locale"); err != nil {
		return fmt.Errorf("initdb failed (PostgreSQL requires a non-root runtime user): %w", err)
	}
	return os.Remove(filepath.Join(p.root, "initdb.password"))
}

func initSingleRestore(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths, run func(context.Context, pgspec.Spec, paths, ...string) ([]byte, error)) error {
	marker := filepath.Join(p.root, "bootstrap-generation.json")
	var generation *int
	if data, err := os.ReadFile(marker); err == nil && json.Unmarshal(data, &generation) == nil && generation != nil && *generation == s.Bootstrap.Generation {
		if _, err := os.Stat(filepath.Join(p.pgdata, "PG_VERSION")); err != nil {
			if os.IsNotExist(err) {
				return errors.New(missingDataMessage)
			}
			return err
		}
		return recordInitialization(p)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// PG_VERSION can already exist in an interrupted delta restore. Stop the
	// supervised process before touching PGDATA and do not start it on failure.
	if err := stopConfigured(ctx, n, "postgres"); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(p.pgdata, "postmaster.pid")); err == nil {
		if _, err := command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "-D", p.pgdata, "-m", "fast", "-w", "stop"); err != nil {
			return fmt.Errorf("stop PostgreSQL before bootstrap restore: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	args, err := restoreArgs(s, p)
	if err != nil {
		return err
	}
	// backupCommand is bounded by the caller's lifecycle, not the short-command
	// timeout: large clones can legitimately take longer than two minutes.
	if _, err := run(ctx, s, p, args...); err != nil {
		return fmt.Errorf("pgBackRest bootstrap restore: %w", err)
	}
	return durableJSON(marker, s.Bootstrap.Generation)
}
func prepareEtcd(n *agent.Node, s pgspec.Spec, p paths) error {
	marker := filepath.Join(p.root, "etcd-identity.json")
	identity := struct{ Scope, Member, Token, State string }{s.Scope, s.Member, s.Etcd.Token, s.Etcd.State}
	data, err := os.ReadFile(marker)
	if err == nil {
		var prior struct{ Scope, Member, Token, State string }
		if json.Unmarshal(data, &prior) != nil {
			return errors.New("invalid etcd volume identity")
		}
		if prior.Scope != identity.Scope || prior.Member != identity.Member || prior.Token != identity.Token || (identity.State == "existing" && prior.State != "existing") {
			return errors.New("refusing to reuse an etcd data directory for a new membership")
		}
	} else if !os.IsNotExist(err) {
		return err
	} else {
		entries, err := os.ReadDir(p.etcd)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if len(entries) > 0 {
			return errors.New("refusing to adopt existing etcd data without its membership identity")
		}
		data, _ = json.Marshal(identity)
		if err = n.WriteFile("postgres/etcd-identity.json", data, 0600); err != nil {
			return err
		}
	}
	return privateDir(p.etcd)
}

func (e *Runner) systemRoleSQL(s pgspec.Spec) ([]string, error) {
	if s.Role != pgspec.RoleData {
		return nil, nil
	}
	digest := hash([]any{s.Superuser, s.Monitor, s.Replication, s.Rewind, dataAccessDefinition(s.DataAccess)})
	if e.roleSQL == nil || e.roleSQLHash != digest {
		statements, err := roleStatements(s)
		if err != nil {
			return nil, err
		}
		e.roleSQL, e.roleSQLHash = statements, digest
	}
	return e.roleSQL, nil
}
func (e *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	// Identity recovery must never enter normal rollout's postmaster-restart
	// decision, including when an unrelated setting is already pending restart.
	if c.CertificateReloadOnly {
		return e.applyCertificateReload(ctx, n, c)
	}
	s, err := decodeSpec(c)
	if err != nil {
		return nil, err
	}
	p := dataPaths(n.DataDir())
	e.mu.Lock()
	e.current = s
	e.dir = n.DataDir()
	e.mu.Unlock()
	for _, d := range []string{p.root, p.socket, p.proxy, filepath.Join(p.root, "backup-lock"), filepath.Join(p.root, "backup-spool")} {
		if err = privateDir(d); err != nil {
			return nil, err
		}
	}
	statements, err := e.systemRoleSQL(s)
	if err != nil {
		return nil, err
	}
	renderSpec := e.retirementConfig(ctx, n, s, p)
	if s.Role == pgspec.RoleData && s.Topology == pgspec.TopologyHA && s.Bootstrap != nil && s.Bootstrap.Method != "restore" {
		if _, statErr := os.Stat(filepath.Join(p.pgdata, "PG_VERSION")); statErr == nil {
			if err = recordInitialization(p); err != nil {
				return nil, err
			}
		} else if !os.IsNotExist(statErr) {
			return nil, statErr
		}
		recorded, receiptErr := initializationRecorded(p)
		if receiptErr != nil {
			return nil, receiptErr
		}
		if recorded {
			bootstrap := *renderSpec.Bootstrap
			bootstrap.AllowInitDB = false
			renderSpec.Bootstrap = &bootstrap
		}
	}
	files, err := render(renderSpec, p, statements)
	if err != nil {
		return nil, err
	}
	changed := []string{}
	for _, f := range files {
		mode := os.FileMode(0600)
		if f.executable {
			mode = 0700
		}
		if unchangedFile(filepath.Join(p.root, f.name), f.content, mode) {
			continue
		}
		if err = n.WriteFile(filepath.Join("postgres", f.name), []byte(f.content), mode); err != nil {
			return nil, err
		}
		changed = append(changed, f.name)
	}
	if s.Bootstrap == nil || s.Bootstrap.Method != "restore" {
		if err = cleanupRestoreFiles(p); err != nil {
			return nil, err
		}
	}
	states, maintenance := n.Snapshot()
	wanted := func(name string) bool {
		return !maintenance && !n.Stopped(name) && (c.DesiredProcesses == nil || slices.Contains(c.DesiredProcesses, name))
	}
	if s.Role == pgspec.RoleProxy {
		for _, name := range []string{"postgres", "patroni", "etcd"} {
			if err = stopConfigured(ctx, n, name); err != nil {
				return nil, err
			}
		}
		if err = n.Configure("proxysql", agent.ProcessSpec{Binary: "/usr/bin/proxysql", Args: []string{"-f", "-c", filepath.Join(p.root, "proxysql.cnf"), "-D", p.proxy}, Dir: p.proxy}); err != nil {
			return nil, err
		}
		restartHash := hash([]any{s.Proxy.ReaderPort, s.Proxy.Threads, s.Proxy.ServerCert, s.CAPEM, s.ListenHost})
		restart := states["proxysql"].PID != 0 && e.configured && e.proxyRestartHash != restartHash
		if err = n.Action(ctx, "proxysql", "start"); err != nil {
			return nil, err
		}
		if wanted("proxysql") {
			if e.proxyAdmin.Username == "" {
				e.proxyAdmin = s.Proxy.Admin
			}
			startupChanged, reconcileErr := reconcileProxyWait(ctx, s.Proxy, s.ListenHost, filepath.Join(p.root, "ca.pem"), e.proxyAdmin)
			if reconcileErr != nil {
				return nil, reconcileErr
			}
			e.proxyAdmin = s.Proxy.Admin
			if restart || startupChanged {
				if err = n.Action(ctx, "proxysql", "restart"); err != nil {
					return nil, err
				}
				if _, err = reconcileProxyWait(ctx, s.Proxy, s.ListenHost, filepath.Join(p.root, "ca.pem"), e.proxyAdmin); err != nil {
					return nil, err
				}
			}
			e.proxyRestartHash = restartHash
			e.configured = true
		}
		return changed, nil
	}
	if err = stopConfigured(ctx, n, "proxysql"); err != nil {
		return nil, err
	}
	if err = applyUpgradeData(n, p); err != nil {
		return nil, err
	}
	if s.Topology == pgspec.TopologySingle {
		for _, name := range []string{"patroni", "etcd"} {
			if err = stopConfigured(ctx, n, name); err != nil {
				return nil, err
			}
		}
		if wanted("postgres") {
			if err = initSingle(ctx, n, s, p); err != nil {
				return nil, err
			}
		}
		if err = n.Configure("postgres", postgresProcess(s, p)); err != nil {
			return nil, err
		}
		if err = n.Action(ctx, "postgres", "start"); err != nil {
			return nil, err
		}
		if wanted("postgres") {
			current, _ := n.Snapshot()
			if states["postgres"].PID != 0 && current["postgres"].PID == states["postgres"].PID && singleReloadChanged(changed) {
				restart, reloadErr := reloadSingle(ctx, s, p)
				if reloadErr != nil {
					return nil, reloadErr
				}
				if restart {
					if err = n.Action(ctx, "postgres", "restart"); err != nil {
						return nil, err
					}
				}
			}
			conn, err := waitLocal(ctx, s, p)
			if err != nil {
				return nil, err
			}
			defer conn.Close(ctx)
			credentialsHash := hash([]any{s.Superuser, s.Monitor, s.Replication, s.Rewind, dataAccessDefinition(s.DataAccess)})
			if !e.rolesApplied || e.credentialsHash != credentialsHash || recoveryCredentialsPending(s, p) {
				if err = bootstrapRoles(ctx, conn, s); err != nil {
					return nil, err
				}
				if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
					if err = durableJSON(filepath.Join(p.root, "credentials-generation.json"), s.Bootstrap.Generation); err != nil {
						return nil, err
					}
				}
				e.rolesApplied = true
				e.credentialsHash = credentialsHash
				e.mu.Lock()
				e.dataAccessApplied, e.dataAccessHash = true, hash(dataAccessDefinition(s.DataAccess))
				e.dataGrants = dataGrantCache{digest: hash(s.DataAccess), appliedAt: time.Now()}
				e.mu.Unlock()
			}
			if err = e.refreshDataAccess(ctx, conn, s, time.Now()); err != nil {
				return nil, err
			}
		}
	} else {
		if c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, "postgres") && !slices.Contains(c.DesiredProcesses, "patroni") {
			if err = stopForUpgrade(ctx, n, s, p); err != nil {
				return nil, err
			}
		}
		if err = stopConfigured(ctx, n, "postgres"); err != nil {
			return nil, err
		}
		if err = prepareEtcd(n, s, p); err != nil {
			return nil, err
		}
		if err = n.Configure("etcd", etcdProcess(s, p)); err != nil {
			return nil, err
		}
		if err = n.Configure("patroni", patroniProcess(p)); err != nil {
			return nil, err
		}
		if err = n.Action(ctx, "etcd", "start"); err != nil {
			return nil, err
		}
		if err = n.Action(ctx, "patroni", "start"); err != nil {
			return nil, err
		}
		// Patroni installs its signal handlers after startup. Only reload a process
		// which was already running at the beginning of this configuration pull.
		if states["patroni"].PID != 0 && wanted("patroni") {
			if err = reloadPatroniConfig(changed, func() error {
				return n.Signal("patroni", syscall.SIGHUP)
			}, func() error {
				return patchDynamic(ctx, s, p)
			}); err != nil {
				return nil, err
			}
		}
	}
	return changed, nil
}
func (*Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	dir := o.DataDir
	for _, arg := range args {
		v, ok := strings.CutPrefix(arg, "--data-dir=")
		if !ok || !filepath.IsAbs(v) {
			return errors.New("postgres readiness accepts only --data-dir=<absolute path>")
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
	b, err = os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var controls struct {
		Maintenance bool
		Stopped     map[string]bool
	}
	if len(b) > 0 {
		if err = json.Unmarshal(b, &controls); err != nil {
			return err
		}
	}
	process := "postgres"
	if s.Role == pgspec.RoleProxy {
		process = "proxysql"
	} else if s.Topology == pgspec.TopologyHA {
		process = "patroni"
	}
	if c.Maintenance || controls.Maintenance || controls.Stopped[process] {
		return nil
	}
	if s.Role == pgspec.RoleProxy {
		d := net.Dialer{Timeout: 3 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", frontendAddress(s.ListenHost))
		if err == nil {
			conn.Close()
		}
		return err
	}
	conn, err := localConnect(ctx, s, dataPaths(dir))
	if err != nil {
		return errors.New("PostgreSQL does not accept connections")
	}
	defer conn.Close(ctx)
	return conn.Ping(ctx)
}
func (e *Runner) Version(ctx context.Context) string {
	s, _ := e.state()
	binary := filepath.Join(s.BinDir, "postgres")
	if s.Role == pgspec.RoleProxy {
		binary = "/usr/bin/proxysql"
	}
	out, err := command(ctx, binary, "--version")
	if err != nil {
		return ""
	}
	return parseVersion(string(out), s.Role == pgspec.RoleProxy)
}
func parseVersion(output string, proxy bool) string {
	markers := []string{"Percona Server for PostgreSQL ", "postgres (PostgreSQL) "}
	if proxy {
		markers = []string{"ProxySQL version "}
	}
	for _, marker := range markers {
		if _, version, ok := strings.Cut(output, marker); ok {
			if fields := strings.Fields(version); len(fields) > 0 {
				return strings.Split(fields[0], "-")[0]
			}
		}
	}
	return ""
}

var _ agent.Engine = (*Runner)(nil)
