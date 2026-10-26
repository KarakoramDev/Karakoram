//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type nativeGroupNode struct {
	t        *testing.T
	s        myspec.Spec
	dir, bin string
	dsn      string
	db       *sql.DB
	cmd      *exec.Cmd
	done     chan error
	identity myspec.DataIdentity
}

func (n *nativeGroupNode) GroupSpec() myspec.Spec                      { return n.s }
func (n *nativeGroupNode) GroupReceiptsDir() string                    { return filepath.Join(n.dir, "receipts") }
func (n *nativeGroupNode) GroupIdentity() (myspec.DataIdentity, error) { return n.identity, nil }
func (n *nativeGroupNode) SaveGroupIdentity(i myspec.DataIdentity) error {
	n.identity = i
	b := []byte(strconv.FormatUint(i.FormationEpoch, 10))
	return durableGroupFile(filepath.Join(n.dir, "identity"), b)
}
func (n *nativeGroupNode) LocalAccessDB(context.Context) (*sql.DB, error) { return n.db, nil }
func (n *nativeGroupNode) RefreshGroupConfig(context.Context) error       { return n.render() }
func (n *nativeGroupNode) render() error {
	var b strings.Builder
	fmt.Fprintf(&b, "[mysqld]\nbasedir=%s\ndatadir=%s\nsocket=%s\npid-file=%s\nlog-error=%s\nbind-address=127.0.0.1\nreport-host=127.0.0.1\nport=%d\nserver-id=%d\nmysqlx=OFF\ninnodb-buffer-pool-size=128M\ninnodb-redo-log-capacity=64M\nmax-connections=40\ntable-open-cache=128\ngtid-mode=ON\nenforce-gtid-consistency=ON\nbinlog-format=ROW\nlog-bin=binlog\nlog-replica-updates=ON\nrelay-log=relay\nrequire-secure-transport=ON\nssl-ca=%s\nssl-cert=%s\nssl-key=%s\n", filepath.Dir(n.bin), filepath.Join(n.dir, "data"), filepath.Join(n.dir, "mysql.sock"), filepath.Join(n.dir, "mysql.pid"), filepath.Join(n.dir, "error.log"), n.s.Peers[n.s.Ordinal].Port, n.s.Ordinal+1, filepath.Join(n.dir, "ca.pem"), filepath.Join(n.dir, "server-cert.pem"), filepath.Join(n.dir, "server-key.pem"))
	opts := RenderGroupOptions(n.s, n.dir)
	keys := make([]string, 0, len(opts))
	for k := range opts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, opts[k])
	}
	return os.WriteFile(filepath.Join(n.dir, "my.cnf"), []byte(b.String()), 0600)
}
func (n *nativeGroupNode) start() error {
	if err := os.Remove(filepath.Join(n.dir, "data", "mysqld-auto.cnf")); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := n.render(); err != nil {
		return err
	}
	// Match the production supervisor: a restarted recipient gets a new
	// administration pool, never idle sessions from the old process.
	if n.db != nil {
		_ = n.db.Close()
	}
	var err error
	n.db, err = sql.Open("mysql", n.dsn)
	if err != nil {
		return err
	}
	n.cmd = exec.Command(filepath.Join(n.bin, "mysqld"), "--defaults-file="+filepath.Join(n.dir, "my.cnf"))
	if err := n.cmd.Start(); err != nil {
		return err
	}
	n.done = make(chan error, 1)
	cmd := n.cmd
	done := n.done
	go func() { done <- cmd.Wait() }()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if err := n.db.Ping(); err == nil {
			// The supervisor restores memory-only recovery credentials after a
			// new process becomes reachable; GR never bootstraps on restart.
			if GroupJoined(n.GroupReceiptsDir(), n.s) {
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
				defer cancel()
				return StartGroup(ctx, n)
			}
			return nil
		}
		select {
		case err := <-done:
			return fmt.Errorf("mysqld exited: %w", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mysqld did not start")
}
func (n *nativeGroupNode) stop(kill bool) {
	if n.cmd == nil {
		return
	}
	if kill {
		_ = n.cmd.Process.Kill()
	} else {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-n.done:
	case <-time.After(25 * time.Second):
		_ = n.cmd.Process.Kill()
		<-n.done
	}
	n.cmd = nil
}
func (n *nativeGroupNode) RestartAfterClone(context.Context) error { n.stop(false); return n.start() }

func (n *nativeGroupNode) CloneRecipientStopped() bool {
	if n.cmd == nil {
		return true
	}
	select {
	case err := <-n.done:
		// Preserve the exit result for stop/start lifecycle ownership.
		n.done <- err
		return true
	default:
		return false
	}
}

// Simulate the agent disappearing after CLONE returns but before supervisor
// restart/finalization. The durable intent must recover the actual native clone.
type interruptedGroupClone struct{ *nativeGroupNode }

var errInterruptedGroupClone = errors.New("injected interruption before clone restart")

func (n interruptedGroupClone) RestartAfterClone(context.Context) error {
	return errInterruptedGroupClone
}
func nativeGroupTLS(t *testing.T) (string, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "native test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err = x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(leafKey)
	return string(caPEM), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}
func nativeGroupPoll(t *testing.T, limit time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Native mysqld can still be shutting down after CLONE loses its reply. Retry
// only the same durable intent, never the destructive statement or authorization.
func nativeGroupCloneReplay(t *testing.T, ctx context.Context, n GroupDeps, req myspec.CloneRequest, expectedErr error) error {
	t.Helper()
	replayCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := CloneGroup(replayCtx, n, req)
		if err == nil || (expectedErr != nil && errors.Is(err, expectedErr)) {
			return err
		}
		var native *mysqldriver.MySQLError
		var transport net.Error
		transient := errors.Is(err, mysqldriver.ErrInvalidConn) || errors.As(err, &transport) || (errors.As(err, &native) && native.Number == 1053)
		if !transient {
			t.Fatalf("clone or interrupted clone replay: %v", err)
		}
		// A missing intent would make the next call issue CLONE again.
		path := filepath.Join(n.GroupReceiptsDir(), "clone-"+req.Receipt)
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("clone transient error without durable intent: %v (receipt: %v)", err, readErr)
		}
		var record cloneReceipt
		if readErr = json.Unmarshal(b, &record); readErr != nil || record.Request != req || record.Baseline == nil {
			t.Fatalf("clone transient error without matching durable intent: %v (receipt: %v)", err, readErr)
		}
		select {
		case <-replayCtx.Done():
			t.Fatalf("clone replay did not recover from %v: %v", err, replayCtx.Err())
		case <-ticker.C:
		}
	}
}

func TestNativeMySQLGroupFailover(t *testing.T) {
	bin := os.Getenv("KARAKORAM_MYSQL_BIN_DIR")
	proxyBin := os.Getenv("KARAKORAM_PROXYSQL_BIN")
	if bin == "" || proxyBin == "" {
		t.Skip("set KARAKORAM_MYSQL_BIN_DIR and KARAKORAM_PROXYSQL_BIN")
	}
	bin, _ = filepath.Abs(bin)
	proxyBin, _ = filepath.Abs(proxyBin)
	if lib := os.Getenv("KARAKORAM_MYSQL_LD_LIBRARY_PATH"); lib != "" {
		t.Setenv("LD_LIBRARY_PATH", lib)
	}
	dir := nativeTestDir(t)
	var err error
	ca, cert, key := nativeGroupTLS(t)
	peers := []myspec.Peer{{Name: "db1", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db2", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db3", Host: "127.0.0.1", Port: nativeTestPort(t)}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	nodes := make([]*nativeGroupNode, 3)
	for i := range nodes {
		n := &nativeGroupNode{t: t, bin: bin, dir: filepath.Join(dir, fmt.Sprintf("db%d", i+1)), s: testGroupSpec(), identity: myspec.DataIdentity{Initialized: true}}
		nodes[i] = n
		n.s.Member = peers[i].Name
		n.s.Ordinal = i
		n.s.Peers = peers
		n.s.GroupReplication.Seeds = peers
		n.s.GroupReplication.Bootstrap = i == 0
		if i != 0 {
			n.s.GroupReplication.CloneDonor = &peers[0]
		}
		n.s.Replication = myspec.Credentials{Username: "kk_repl", Password: "native-recovery-password"}
		n.s.CAPEM = ca
		if err = os.MkdirAll(filepath.Join(n.dir, "data"), 0700); err != nil {
			t.Fatal(err)
		}
		for name, text := range map[string]string{"ca.pem": ca, "server-cert.pem": cert, "server-key.pem": key} {
			if err = os.WriteFile(filepath.Join(n.dir, name), []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
		}
		init := exec.CommandContext(ctx, filepath.Join(bin, "mysqld"), "--no-defaults", "--basedir="+filepath.Dir(bin), "--datadir="+filepath.Join(n.dir, "data"), "--innodb-buffer-pool-size=128M", "--initialize-insecure")
		if out, e := init.CombinedOutput(); e != nil {
			t.Fatalf("initialize %v: %s", e, out)
		}
		cfg := mysqldriver.NewConfig()
		cfg.User = "root"
		cfg.Net = "unix"
		cfg.Addr = filepath.Join(n.dir, "mysql.sock")
		cfg.Timeout = time.Second
		cfg.ReadTimeout = 60 * time.Second
		cfg.WriteTimeout = 3 * time.Second
		n.dsn = cfg.FormatDSN()
		t.Cleanup(func() { n.stop(false); n.db.Close() })
		if err = n.start(); err != nil {
			log, _ := os.ReadFile(filepath.Join(n.dir, "error.log"))
			t.Fatalf("start %v: %s", err, log)
		}
		for _, q := range []string{"SET SQL_LOG_BIN=0", "CREATE USER 'kk_repl'@'%' IDENTIFIED BY 'native-recovery-password' REQUIRE SSL", "GRANT REPLICATION SLAVE, CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO 'kk_repl'@'%'", "CREATE USER 'kk_monitor'@'%' IDENTIFIED BY 'native-monitor-password' REQUIRE SSL", "GRANT SELECT ON *.* TO 'kk_monitor'@'%'", "GRANT REPLICATION CLIENT ON *.* TO 'kk_monitor'@'%'", "CREATE USER 'app'@'%' IDENTIFIED BY 'native-app-password' REQUIRE SSL", "GRANT ALL ON native_lab.* TO 'app'@'%'"} {
			if _, err = n.db.ExecContext(ctx, q); err != nil {
				t.Fatalf("provision: %v", err)
			}
		}
		if _, err = n.db.ExecContext(ctx, "SET SQL_LOG_BIN=1"); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	if err = BootstrapGroup(ctx, nodes[0], myspec.GroupRequest{GroupName: nodes[0].s.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}); err != nil {
		t.Fatal("bootstrap:", err)
	}
	for i := 1; i < 3; i++ {
		req := myspec.CloneRequest{Receipt: fmt.Sprintf("join-%d", i), Donor: peers[0], Version: nodes[i].s.Version}
		if i == 2 {
			if err = nativeGroupCloneReplay(t, ctx, interruptedGroupClone{nodes[i]}, req, errInterruptedGroupClone); !errors.Is(err, errInterruptedGroupClone) {
				t.Fatalf("clone did not reach injected restart interruption: %v", err)
			}
		}
		nativeGroupCloneReplay(t, ctx, nodes[i], req, nil)
		if i == 1 {
			// A second intent written before CLONE must not adopt the prior
			// operation's Completed row, even with CP recovery authorization.
			baseline, statusErr := readCloneStatus(ctx, nodes[i].db)
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			recovery := req
			recovery.Receipt = "authorized-recovery"
			nodes[i].s.GroupReplication.Reclone = true
			nodes[i].s.GroupReplication.RecloneReceipt = recovery.Receipt
			stale := cloneReceipt{Request: recovery, Baseline: &baseline}
			b, marshalErr := json.Marshal(stale)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if err = durableGroupFile(filepath.Join(nodes[i].GroupReceiptsDir(), "clone-"+recovery.Receipt), b); err != nil {
				t.Fatal(err)
			}
			if err = CloneGroup(ctx, nodes[i], recovery); err == nil {
				t.Fatal("old native completion satisfied a new pre-CLONE intent")
			}
			// A separately authorized actual recovery operation must remain usable.
			recovery.Receipt = "authorized-actual-recovery"
			nodes[i].s.GroupReplication.RecloneReceipt = recovery.Receipt
			nativeGroupCloneReplay(t, ctx, nodes[i], recovery, nil)
			nodes[i].s.GroupReplication.Reclone = false
			nodes[i].s.GroupReplication.RecloneReceipt = ""
		}
		if err = StartGroup(ctx, nodes[i]); err != nil {
			log, _ := os.ReadFile(filepath.Join(nodes[i].dir, "error.log"))
			t.Fatalf("join: %v\n%s", err, log)
		}
	}
	nativeGroupPoll(t, 30*time.Second, "three ONLINE members", func() bool {
		var count int
		return nodes[0].db.QueryRow("SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'").Scan(&count) == nil && count == 3
	})
	t.Logf("three-member clone formation: %s", time.Since(start))
	if _, err = nodes[0].db.Exec("CREATE DATABASE native_lab"); err != nil {
		t.Fatal(err)
	}
	if _, err = nodes[0].db.Exec("CREATE TABLE native_lab.writes(id BIGINT PRIMARY KEY AUTO_INCREMENT)"); err != nil {
		t.Fatal(err)
	}
	pd := filepath.Join(dir, "proxy")
	os.Mkdir(pd, 0700)
	for name, text := range map[string]string{"ca.pem": ca, "server-cert.pem": cert, "server-key.pem": key, "proxysql-ca.pem": ca, "proxysql-cert.pem": cert, "proxysql-key.pem": key} {
		os.WriteFile(filepath.Join(pd, name), []byte(text), 0600)
	}
	ps := testProxySpec()
	ps.Proxy.Backends = peers
	ps.Proxy.Monitor.Password = "native-monitor-password"
	ps.Proxy.Users = []myspec.ProxyUser{{Username: "app", Password: "native-app-password", RequireTLS: true}}
	adminAddress, clientAddress := nativeTestAddress(t), nativeTestAddress(t)
	t.Setenv("KARAKORAM_PROXYSQL_ADMIN_ADDR", adminAddress)
	t.Setenv("KARAKORAM_PROXYSQL_CLIENT_ADDR", clientAddress)
	conf, err := RenderProxyConfig(ps, pd, pd)
	if err != nil {
		t.Fatal(err)
	}
	conf = strings.ReplaceAll(conf, "datadir=", "errorlog="+strconv.Quote(filepath.Join(pd, "error.log"))+"\ndatadir=")
	os.WriteFile(filepath.Join(pd, "proxysql.cnf"), []byte(conf), 0600)
	proxy := exec.Command(proxyBin, "-f", "--initial", "-c", filepath.Join(pd, "proxysql.cnf"))
	if err = proxy.Start(); err != nil {
		t.Fatal(err)
	}
	proxyDone := make(chan error, 1)
	go func() { proxyDone <- proxy.Wait() }()
	t.Cleanup(func() {
		proxy.Process.Signal(syscall.SIGTERM)
		select {
		case <-proxyDone:
		case <-time.After(10 * time.Second):
			proxy.Process.Kill()
			<-proxyDone
		}
	})
	cfg := mysqldriver.NewConfig()
	cfg.User = ps.Proxy.Admin.Username
	cfg.Passwd = ps.Proxy.Admin.Password
	cfg.Net = "tcp"
	cfg.Addr = adminAddress
	cfg.Timeout = time.Second
	admin, _ := sql.Open("mysql", cfg.FormatDSN())
	defer admin.Close()
	nativeGroupPoll(t, 15*time.Second, "proxy admin", func() bool { return admin.Ping() == nil })
	if err = applyProxySQL(ctx, admin, ps); err != nil {
		t.Fatal("live apply:", err)
	}
	cfg = mysqldriver.NewConfig()
	cfg.User = "app"
	cfg.Passwd = "native-app-password"
	cfg.Net = "tcp"
	cfg.Addr = clientAddress
	cfg.DBName = "native_lab"
	cfg.Timeout = time.Second
	cfg.ReadTimeout = time.Second
	cfg.WriteTimeout = time.Second
	cfg.TLS = tlsConfig(x509Pool(ca), "127.0.0.1")
	connector, _ := mysqldriver.NewConnector(cfg)
	app := sql.OpenDB(connector)
	defer app.Close()
	app.SetMaxIdleConns(0)
	failures := 0
	write := func() bool {
		_, e := app.Exec("INSERT INTO writes VALUES ()")
		if e == nil {
			return true
		}
		failures++
		if failures%10 == 0 {
			t.Logf("proxied write failure: %v", e)
			for _, q := range []string{"SELECT hostgroup_id,hostname,port,status FROM runtime_mysql_servers", "SELECT hostname,port,error FROM monitor.mysql_server_group_replication_log ORDER BY time_start_us DESC LIMIT 3", "SELECT hostgroup,srv_host,srv_port,status,ConnERR FROM stats_mysql_connection_pool"} {
				rows, e := admin.Query(q)
				if e != nil {
					t.Log(q, e)
					continue
				}
				cols, _ := rows.Columns()
				for rows.Next() {
					v := make([]any, len(cols))
					ptr := make([]any, len(cols))
					for i := range v {
						ptr[i] = &v[i]
					}
					rows.Scan(ptr...)
					for i := range v {
						if b, ok := v[i].([]byte); ok {
							v[i] = string(b)
						}
					}
					t.Log(q, v)
				}
				rows.Close()
			}
			for _, n := range nodes {
				if n.cmd != nil {
					members, e := GroupMembers(ctx, n.db)
					t.Logf("members %s: %+v %v", n.s.Member, members, e)
				}
			}
		}
		return false
	}
	nativeGroupPoll(t, 25*time.Second, "initial proxied write", write)
	start = time.Now()
	nodes[0].stop(true)
	nativeGroupPoll(t, 65*time.Second, "write after primary failure", write)
	t.Logf("proxied write after primary kill: %s", time.Since(start))
	start = time.Now()
	if err = nodes[0].start(); err != nil {
		t.Fatal(err)
	}
	nativeGroupPoll(t, 45*time.Second, "automatic receipt-based rejoin", func() bool {
		var state string
		return nodes[0].db.QueryRow("SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state) == nil && state == "ONLINE"
	})
	t.Logf("receipt supervisor restart rejoin: %s", time.Since(start))
	// Leave the current primary alone, forcing it into minority fencing.
	survivor := -1
	for i, n := range nodes {
		var role string
		if n.db.QueryRow("SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&role) == nil && role == "PRIMARY" {
			survivor = i
			break
		}
	}
	if survivor < 0 {
		t.Fatal("no primary")
	}
	start = time.Now()
	for i, n := range nodes {
		if i != survivor {
			n.stop(true)
		}
	}
	nativeGroupPoll(t, 35*time.Second, "minority super_read_only", func() bool {
		var fenced int
		return nodes[survivor].db.QueryRow("SELECT @@super_read_only").Scan(&fenced) == nil && fenced == 1
	})
	t.Logf("minority fencing: %s", time.Since(start))
	if _, err = nodes[survivor].db.Exec("INSERT INTO native_lab.writes VALUES ()"); err == nil {
		t.Fatal("minority accepted write")
	}
}
