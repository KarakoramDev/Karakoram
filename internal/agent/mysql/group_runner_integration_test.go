//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// Every lifecycle mutation goes through the same pinned-CA, bearer-authenticated
// client as the control plane. Runner access is only used for SQL observations.
type groupRunnerAgent struct {
	t      *testing.T
	spec   myspec.Spec
	config agentapi.MemberConfig
	mu     sync.Mutex
	client *agentapi.Client
	runner *Runner
	cancel context.CancelFunc
	done   chan error
}

func (a *groupRunnerAgent) publish(ctx context.Context, desired []string) {
	a.t.Helper()
	raw, err := json.Marshal(a.spec)
	if err != nil {
		a.t.Fatal(err)
	}
	a.mu.Lock()
	a.config.EngineSpec = raw
	a.config.DesiredProcesses = desired
	a.config.ConfigVersion++
	revision := a.config.ConfigVersion
	a.mu.Unlock()
	wait := 60 * time.Second
	if a.spec.Galera != nil {
		wait = 4 * time.Minute // physical SST/prepare and graceful PXC stop
		a.t.Logf("%s publishing revision=%d desired=%v epoch=%d", a.spec.Member, revision, desired, a.spec.Galera.FormationEpoch)
	}
	if err = a.client.Reload(ctx); err != nil {
		var timedOut net.Error
		if a.spec.Galera == nil || ctx.Err() != nil || !errors.As(err, &timedOut) || !timedOut.Timeout() {
			a.t.Fatalf("%s revision=%d reload: %v", a.spec.Member, revision, err)
		}
		// Reload waits for Apply, unlike an asynchronous intent submission.
		// A transport deadline is not an acknowledgement; require the very
		// same submitted revision to complete through normal agent polling.
		a.t.Logf("%s revision=%d reload timed out; awaiting exact acknowledgement: %v", a.spec.Member, revision, err)
	}
	nativeGroupPoll(a.t, wait, fmt.Sprintf("%s applied config revision %d desired=%v", a.spec.Member, revision, desired), func() bool {
		s, e := a.client.Status(ctx)
		return e == nil && s.ConfigVersion == revision && s.Error == ""
	})
}
func (a *groupRunnerAgent) action(ctx context.Context, action string, req, out any) {
	a.t.Helper()
	if err := a.client.Engine(ctx, "mysql", action, req, out); err != nil {
		a.t.Fatalf("%s %s: %v", a.spec.Member, action, err)
	}
}
func (a *groupRunnerAgent) close() {
	if a.cancel == nil {
		return
	}
	a.cancel()
	// agent.Run must reap its supervised groups before returning. Do not let a
	// local timer abandon shutdown and run the older datadir cleanup while
	// mysqld is still using it, including when the test exits through Fatal.
	if err := <-a.done; err != nil {
		a.t.Errorf("%s shutdown: %v", a.spec.Member, err)
	}
	a.cancel = nil
}

// startNativeGroupRunnerAgent shares the production agent lifecycle with native
// backup tests; callers still form the group through bootstrap/clone/start APIs.
func startNativeGroupRunnerAgent(t *testing.T, ctx context.Context, s myspec.Spec, dir string) *groupRunnerAgent {
	t.Helper()
	pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	a := &groupRunnerAgent{t: t, spec: s, runner: New(), done: make(chan error, 1)}
	desired := []string{"mysqld"}
	if s.Role == myspec.RoleProxy {
		desired = []string{"proxysql"}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	a.config = agentapi.MemberConfig{Cluster: "group-runner", Member: s.Member, Engine: "mysql", Role: s.Role, EngineSpec: raw, DesiredProcesses: desired, ConfigVersion: 1, AgentTLS: agentapi.Certificate{CertPEM: s.ServerCert.CertPEM, KeyPEM: s.ServerCert.KeyPEM}}
	token := "native-group-token-" + s.Member
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/internal/agent/v1/config" || !agentapi.AuthenticateBearer(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		json.NewEncoder(w).Encode(a.config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	listen := nativeTestAddress(t)
	a.client, err = agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + listen, Token: token, CAPEM: []byte(s.CAPEM), EngineTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	t.Cleanup(func() { a.close(); cp.Close() })
	go func() {
		a.done <- agent.Run(runCtx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: s.CAPEM, Token: token, Cluster: a.config.Cluster, Member: s.Member, DataDir: filepath.Join(dir, s.Member), Listen: listen, Engine: "mysql", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, a.runner)
	}()
	nativeGroupPoll(t, 90*time.Second, "initial agent config "+s.Member, func() bool { st, e := a.client.Status(ctx); return e == nil && st.ConfigVersion == 1 && st.Error == "" })
	return a
}

// Prove the native MYSQL communication stack uses the configured recovery trust
// root, rather than merely negotiating encryption with any certificate.
func assertNativeGroupRejectsUntrustedPeer(t *testing.T, ctx context.Context, node *groupRunnerAgent) {
	t.Helper()
	db, err := node.runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, p := node.runner.state()
	untrustedCA, _ := coreTLS(t)
	caPath := filepath.Join(p.root, "untrusted-peer-ca.pem")
	if err = os.WriteFile(caPath, []byte(untrustedCA), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(caPath)
	if _, err = db.ExecContext(ctx, "SET GLOBAL group_replication_recovery_ssl_ca="+sqlLiteral(caPath)); err != nil {
		t.Fatal(err)
	}
	var activeCA, verifyIdentity string
	if err = db.QueryRowContext(ctx, "SELECT @@group_replication_recovery_ssl_ca, @@group_replication_recovery_ssl_verify_server_cert").Scan(&activeCA, &verifyIdentity); err != nil {
		t.Fatal(err)
	}
	if activeCA != caPath || verifyIdentity != "1" {
		t.Fatalf("unexpected native recovery trust: ca=%q verify=%q", activeCA, verifyIdentity)
	}
	probe, cancel := context.WithTimeout(ctx, 20*time.Second)
	err = node.client.Engine(probe, "mysql", myspec.ActionStartGroup, struct{}{}, nil)
	cancel()
	if err == nil {
		t.Fatal("Group Replication joined with an unrelated peer trust root")
	}
	log, err := os.ReadFile(filepath.Join(p.root, "mysql-error.log"))
	if err != nil {
		t.Fatal(err)
	}
	// The MYSQL stack reports a generic GCS TLS/connectivity failure rather
	// than exposing OpenSSL's certificate verification error in this log.
	if !strings.Contains(string(log), "failed to test connectivity to the local group communication engine") {
		t.Fatalf("failed join did not record native MYSQL communication rejection: %s", log)
	}
	var online int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid AND MEMBER_STATE='ONLINE'").Scan(&online); err != nil {
		t.Fatal(err)
	}
	if online != 0 {
		t.Fatal("untrusted peer admitted as ONLINE")
	}
	node.action(ctx, myspec.ActionStopGroup, struct{}{}, nil)
	if _, err = db.ExecContext(ctx, "SET GLOBAL group_replication_recovery_ssl_ca="+sqlLiteral(filepath.Join(p.root, "ca.pem"))); err != nil {
		t.Fatal(err)
	}
	t.Log("native Group Replication rejected an unrelated CA before trusted join")
}

func TestNativeGroupRunner(t *testing.T) {
	if os.Getenv("KARAKORAM_MYSQL_BIN_DIR") == "" || os.Getenv("KARAKORAM_PROXYSQL_BIN") == "" {
		t.Skip("set native MySQL and ProxySQL binary paths")
	}
	adminAddress, clientAddress := nativeTestAddress(t), nativeTestAddress(t)
	t.Setenv("KARAKORAM_PROXYSQL_ADMIN_ADDR", adminAddress)
	t.Setenv("KARAKORAM_PROXYSQL_CLIENT_ADDR", clientAddress)
	began := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ca, cert := coreTLS(t)
	peers := []myspec.Peer{{Name: "db1", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db2", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db3", Host: "127.0.0.1", Port: nativeTestPort(t)}}
	dir := nativeTestDir(t)
	nodes := make([]*groupRunnerAgent, 4)
	start := func(i int) *groupRunnerAgent {
		s := coreTestSpec()
		s.CAPEM, s.ServerCert, s.Topology, s.Ordinal, s.Peers = ca, &cert, myspec.TopologyGroup, i, peers
		if i < 3 {
			s.Member = peers[i].Name
			g := *testGroupSpec().GroupReplication
			g.Seeds, g.Bootstrap = peers, i == 0
			if i > 0 {
				donor := peers[0]
				g.CloneDonor = &donor
			}
			s.GroupReplication = &g
		} else {
			s.Member, s.Role, s.AllowInitialize = "proxy1", myspec.RoleProxy, false
			s.Proxy = &myspec.Proxy{Admin: myspec.Credentials{Username: "manager", Password: "native-proxy-password"}, Monitor: s.Monitor, Backends: peers, Users: []myspec.ProxyUser{{Username: s.Admin.Username, Password: s.Admin.Password, RequireTLS: true}}, MaxConnections: 30}
		}
		return startNativeGroupRunnerAgent(t, ctx, s, dir)
	}
	for i := range 3 {
		nodes[i] = start(i)
	}
	phase := time.Now()
	nodes[0].action(ctx, myspec.ActionBootstrap, myspec.GroupRequest{GroupName: nodes[0].spec.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}, nil)
	for i := 1; i < 3; i++ {
		nodes[i].action(ctx, myspec.ActionClone, myspec.CloneRequest{Receipt: fmt.Sprintf("runner-join-%d", i), Donor: peers[0], Version: nodes[i].spec.Version}, nil)
		if i == 1 {
			assertNativeGroupRejectsUntrustedPeer(t, ctx, nodes[i])
		}
		nodes[i].action(ctx, myspec.ActionStartGroup, struct{}{}, nil)
	}
	observe := func(a *groupRunnerAgent) (myspec.StatusResponse, error) {
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		var s myspec.StatusResponse
		err := a.client.Engine(probe, "mysql", myspec.ActionStatus, struct{}{}, &s)
		return s, err
	}
	online := func(want int) bool {
		count := 0
		for _, a := range nodes[:3] {
			if a.cancel == nil {
				continue
			}
			s, err := observe(a)
			if err == nil && s.MemberState == "ONLINE" && s.GroupMembers == want {
				count++
			}
		}
		return count == want
	}
	nativeGroupPoll(t, 40*time.Second, "three ONLINE production agents", func() bool { return online(3) })
	t.Logf("production bootstrap/clone/start-group: %s", time.Since(phase))
	primary := func() int {
		for i, a := range nodes[:3] {
			if a.cancel == nil {
				continue
			}
			s, err := observe(a)
			if err == nil && s.MemberRole == "PRIMARY" && s.MemberState == "ONLINE" {
				return i
			}
		}
		return -1
	}
	p := primary()
	if p < 0 {
		t.Fatal("no primary")
	}
	db, e := nodes[p].runner.LocalAccessDB(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{"CREATE DATABASE runner_data", "CREATE TABLE runner_data.writes(id INT PRIMARY KEY, writer VARCHAR(36) NOT NULL)"} {
		if _, e = db.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	nodes[3] = start(3)
	tlsName := "native-group-runner"
	if e = mysqldriver.RegisterTLSConfig(tlsName, tlsConfig(x509Pool(ca), "localhost")); e != nil {
		t.Fatal(e)
	}
	defer mysqldriver.DeregisterTLSConfig(tlsName)
	cfg := mysqldriver.NewConfig()
	cfg.User = nodes[0].spec.Admin.Username
	cfg.Passwd = nodes[0].spec.Admin.Password
	cfg.Net = "tcp"
	cfg.Addr = clientAddress
	cfg.TLSConfig = tlsName
	cfg.Timeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	app, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	write := func(id int) bool {
		_, e := app.ExecContext(ctx, "INSERT INTO runner_data.writes VALUES (?,@@server_uuid)", id)
		return e == nil
	}
	nativeGroupPoll(t, 30*time.Second, "initial proxied write", func() bool { return write(1) })
	var writer string
	if e = app.QueryRowContext(ctx, "SELECT writer FROM runner_data.writes WHERE id=1").Scan(&writer); e != nil {
		t.Fatal(e)
	}
	var before myspec.StatusResponse
	nodes[p].action(ctx, myspec.ActionStatus, struct{}{}, &before)
	if writer != before.ServerUUID {
		t.Fatalf("proxy writer %s != primary %s", writer, before.ServerUUID)
	}
	st, e := nodes[p].client.Status(ctx)
	if e != nil {
		t.Fatal(e)
	}
	pid := st.Processes["mysqld"].PID
	if pid <= 0 {
		t.Fatal("missing supervised mysqld PID")
	}
	// Observe survivors independently of the failed agent: its status endpoint
	// can legitimately spend its server-side deadline waiting for local SQL.
	// Serially probing it first would measure the observer, not failover.
	survivor := nodes[(p+1)%3]
	survivorDB, e := survivor.runner.LocalAccessDB(ctx)
	if e != nil {
		t.Fatal(e)
	}
	proxyDB, e := proxyAdmin(ctx, nodes[3].spec)
	if e != nil {
		t.Fatal(e)
	}
	defer proxyDB.Close()
	assertNativeMySQLOnlyProxy(t, proxyDB)
	type failoverTimes struct {
		restarted, expelled, elected, routed time.Duration
	}
	var timingMu sync.Mutex
	var timings failoverTimes
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	defer func() { stopMonitor(); <-monitorDone }()
	phase = time.Now()
	t.Logf("failover SIGKILL: %s (PID %d, UUID %s)", phase.UTC().Format(time.RFC3339Nano), pid, before.ServerUUID)
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
			probe, cancel := context.WithTimeout(monitorCtx, 300*time.Millisecond)
			var present, newPrimary int
			err := survivorDB.QueryRowContext(probe, "SELECT COUNT(IF(MEMBER_ID=?,1,NULL)),COUNT(IF(MEMBER_ID<>? AND MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE',1,NULL)) FROM performance_schema.replication_group_members", before.ServerUUID, before.ServerUUID).Scan(&present, &newPrimary)
			cancel()
			timingMu.Lock()
			if err == nil && present == 0 && timings.expelled == 0 {
				timings.expelled = time.Since(phase)
			}
			if err == nil && newPrimary > 0 && timings.elected == 0 {
				timings.elected = time.Since(phase)
			}
			timingMu.Unlock()
			probe, cancel = context.WithTimeout(monitorCtx, 300*time.Millisecond)
			var routed int
			// ProxySQL admin has no prepared-statement protocol; the port is
			// a harness-owned integer and is rendered into a plain COM_QUERY.
			err = proxyDB.QueryRowContext(probe, fmt.Sprintf("SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=10 AND status='ONLINE' AND port<>%d", peers[p].Port)).Scan(&routed)
			cancel()
			timingMu.Lock()
			if err == nil && routed > 0 && timings.routed == 0 {
				timings.routed = time.Since(phase)
			}
			timingMu.Unlock()
			probe, cancel = context.WithTimeout(monitorCtx, 300*time.Millisecond)
			status, err := nodes[p].client.Status(probe)
			cancel()
			timingMu.Lock()
			if err == nil && status.Processes["mysqld"].PID > 0 && status.Processes["mysqld"].PID != pid && timings.restarted == 0 {
				timings.restarted = time.Since(phase)
			}
			timingMu.Unlock()
		}
	}()
	if e = syscall.Kill(pid, syscall.SIGKILL); e != nil {
		t.Fatal(e)
	}
	nativeGroupPoll(t, 70*time.Second, "new primary and proxied write", func() bool {
		current := primary()
		return current >= 0 && current != p && write(2)
	})
	t.Logf("SIGKILL to new-primary proxied write: %s", time.Since(phase))
	nativeGroupPoll(t, 70*time.Second, "supervisor restart and watcher rejoin", func() bool {
		s, e := nodes[p].client.Status(ctx)
		return e == nil && s.Processes["mysqld"].PID > 0 && s.Processes["mysqld"].PID != pid && online(3)
	})
	t.Logf("SIGKILL to three ONLINE: %s", time.Since(phase))
	stopMonitor()
	<-monitorDone
	timingMu.Lock()
	t.Logf("failover breakdown from SIGKILL: supervisor restart=%s, old incarnation absent on survivor=%s, new PRIMARY=%s, ProxySQL writer move=%s", timings.restarted, timings.expelled, timings.elected, timings.routed)
	timingMu.Unlock()
	phase = time.Now()
	for _, a := range nodes[:3] {
		a.spec.Parameters = map[string]string{"max_connections": "35"}
		a.spec.GroupReplication.Bootstrap = false
		a.spec.GroupReplication.CloneDonor = nil
		a.spec.AllowInitialize = false
		a.publish(ctx, []string{"mysqld"})
	}
	if !online(3) {
		t.Fatal("revision disrupted group")
	}
	for _, a := range nodes[:3] {
		d, e := a.runner.LocalAccessDB(ctx)
		if e != nil {
			t.Fatal(e)
		}
		var max int
		if e = d.QueryRowContext(ctx, "SELECT @@max_connections").Scan(&max); e != nil || max != 35 {
			t.Fatalf("revision max_connections=%d: %v", max, e)
		}
	}
	t.Logf("live parameter revision: %s", time.Since(phase))
	// CP suspends through desired-process configuration, not STOP GR alone.
	phase = time.Now()
	for _, a := range nodes[:3] {
		a.publish(ctx, []string{})
	}
	sets := make([]recoveryGTIDs, 3)
	for i, a := range nodes[:3] {
		var out myspec.OfflineGTIDResponse
		a.action(ctx, myspec.ActionOfflineGTID, struct{}{}, &out)
		if !out.Stopped || !out.DataIdentity.Initialized || out.ServerUUID == "" {
			t.Fatalf("invalid offline observation: %+v", out)
		}
		sets[i], e = parseRecoveryGTIDs(out.GTIDExecuted)
		if e != nil {
			t.Fatal(e)
		}
	}
	// Same CP rule: choose the lexically first member whose normalized GTID set
	// contains every stopped member's set; incomparable histories fail closed.
	seed := -1
	for i, set := range sets {
		contains := true
		for _, other := range sets {
			contains = contains && set.contains(other)
		}
		if contains {
			seed = i
			break
		}
	}
	if seed < 0 {
		t.Fatal("offline histories have no superset")
	}
	for i, a := range nodes[:3] {
		a.spec.GroupReplication.FormationEpoch = 2
		a.spec.GroupReplication.Bootstrap = i == seed
		a.publish(ctx, []string{"mysqld"})
	}
	nodes[seed].action(ctx, myspec.ActionBootstrap, myspec.GroupRequest{GroupName: nodes[seed].spec.GroupReplication.GroupName, FormationEpoch: 2, Bootstrap: true}, nil)
	for i, a := range nodes[:3] {
		if i != seed {
			a.action(ctx, myspec.ActionStartGroup, struct{}{}, nil)
		}
	}
	nativeGroupPoll(t, 40*time.Second, "reformed group", func() bool { return online(3) })
	for _, a := range nodes[:3] {
		d, e := a.runner.LocalAccessDB(ctx)
		if e != nil {
			t.Fatal(e)
		}
		var count int
		if e = d.QueryRowContext(ctx, "SELECT COUNT(*) FROM runner_data.writes").Scan(&count); e != nil || count != 2 {
			t.Fatalf("reformed rows=%d: %v", count, e)
		}
	}
	t.Logf("CP stop/offline GTID/superset/epoch-2 reformation: %s (seed %s)", time.Since(phase), nodes[seed].spec.Member)
	p = primary()
	if p < 0 {
		t.Fatal("reformed primary missing")
	}
	// Observe the handoff while the old primary process still exists. Merely
	// observing an election after exit would not prove the shutdown hook ran.
	observer := (p + 1) % 3
	observed := make(chan bool, 1)
	oldStatus, e := nodes[p].client.Status(ctx)
	if e != nil {
		t.Fatal(e)
	}
	oldPID := oldStatus.Processes["mysqld"].PID
	phase = time.Now()
	go func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			d, e := nodes[observer].runner.LocalAccessDB(ctx)
			if e == nil {
				var uuid string
				e = d.QueryRowContext(ctx, "SELECT MEMBER_ID FROM performance_schema.replication_group_members WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'").Scan(&uuid)
				if e == nil && uuid != "" {
					id, _ := nodes[p].runner.GroupIdentity()
					if uuid != id.ServerUUID && syscall.Kill(oldPID, 0) == nil {
						observed <- true
						return
					}
				}
			}
			time.Sleep(time.Millisecond)
		}
		observed <- false
	}()
	nodes[p].close()
	if !<-observed {
		t.Fatal("did not observe primary handoff before process exit")
	}
	nativeGroupPoll(t, 30*time.Second, "graceful handoff proxied write", func() bool { return online(2) && write(3) })
	t.Logf("graceful primary handoff and proxied write: %s", time.Since(phase))
	t.Logf("production group runner total: %s", time.Since(began))
}
