//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/minio/minio-go/v7"
)

// All starts, crash restarts, SSTs, offline observations and routing changes run
// through production Runner and supervisor code, never a hand-rendered config.
func TestNativeGaleraRunner(t *testing.T) {
	if os.Getenv("KARAKORAM_MYSQL_BIN_DIR") == "" || os.Getenv("KARAKORAM_PROXYSQL_BIN") == "" || os.Getenv("KARAKORAM_MYSQL_XTRABACKUP_DIR") == "" || os.Getenv("KARAKORAM_MYSQL_MINIO_BIN") == "" {
		t.Skip("set native PXC, ProxySQL, XtraBackup and MinIO binary paths")
	}
	fixtures := nativeGaleraConfigs(t, "members.json")
	proxyAdminPort, proxyClient := nativeTestPort(t), nativeTestPort(t)
	addr := func(port int) string { return galeraAddress("127.0.0.1", port) }
	t.Setenv("KARAKORAM_PROXYSQL_ADMIN_ADDR", addr(proxyAdminPort))
	t.Setenv("KARAKORAM_PROXYSQL_CLIENT_ADDR", addr(proxyClient))
	var sqlPorts, galeraPorts, istPorts, sstPorts [3]int
	for i := range sqlPorts {
		sqlPorts[i], galeraPorts[i] = nativeTestPort(t), nativeTestPort(t)
		istPorts[i], sstPorts[i] = nativeTestPort(t), nativeTestPort(t)
	}
	began := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	trafficCert := *fixtures[0].spec.Galera.TrafficCert
	dir := nativeTestDir(t)
	peers, seeds := make([]myspec.Peer, 3), make([]myspec.Peer, 3)
	for i := range peers {
		peers[i] = myspec.Peer{Name: fixtures[i].spec.Member, Host: "127.0.0.1", Port: sqlPorts[i]}
		seeds[i] = myspec.Peer{Name: fixtures[i].spec.Member, Host: "127.0.0.1", Port: galeraPorts[i]}
	}
	nodes := make([]*groupRunnerAgent, 4)
	start := func(i int) *groupRunnerAgent {
		s := fixtures[i].spec
		s.Peers = peers
		if i < 3 {
			s.Galera.Peers = seeds
			s.Galera.GaleraPort = galeraPorts[i]
			s.Galera.ISTPort = istPorts[i]
			s.Galera.SSTPort = sstPorts[i]
		} else {
			s.Proxy.Backends = peers
		}
		return startNativeGaleraAgent(t, ctx, fixtures[i], s, dir)
	}
	nodes[0] = start(0)
	// Fail immediately on seed TLS/policy errors, before a joiner can spend
	// its entire SST deadline retrying an unusable traffic certificate.
	nativeGaleraTLSProof(t, ctx, nodes[:1], dir, trafficCert)
	nativeGaleraTrafficTLSProof(t, ctx, nodes[0])
	for i := 1; i < 3; i++ {
		nodes[i] = start(i)
	}
	healthy := func(size int) bool {
		for _, a := range nodes[:3] {
			var st myspec.GaleraStatusResponse
			if a.client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &st) != nil || !st.Eligible() || st.ClusterSize != size {
				return false
			}
		}
		return true
	}
	nativeGroupPoll(t, 45*time.Second, "three Primary/Synced SST members", func() bool { return healthy(3) })
	t.Logf("bootstrap + two encrypted SST joins: %s", time.Since(began))
	for _, a := range nodes[:3] {
		p := dataPaths(filepath.Join(dir, a.spec.Member))
		r, err := readGaleraIdentity(a.spec, p)
		if err != nil || !myspec.ValidUUID(r.StateUUID) {
			t.Fatalf("SST own identity was not durably adopted: %+v %v", r, err)
		}
		if _, err := guardInitialization(a.spec, p); err != nil {
			t.Fatalf("upstream SST UUID rejected on restart: %v", err)
		}
	}
	nativeGaleraTLSProof(t, ctx, nodes[:3], dir, trafficCert)
	nativeGaleraRejectUnauthorizedPeers(t, ctx, nodes[0])
	nativeSSTAdmissionProof(t, ctx, nodes[0].spec)
	formed := nativeGaleraConfigs(t, "formed-members.json")
	for _, a := range nodes[:3] {
		for _, exported := range formed {
			if exported.spec.Member == a.spec.Member {
				a.spec = nativeGaleraLocalConfig(exported.spec, a.spec)
				a.publish(ctx, []string{"mysqld"})
			}
		}
	}
	db, e := nodes[0].runner.LocalAccessDB(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{"CREATE DATABASE galera_runner", "CREATE TABLE galera_runner.writes(id INT PRIMARY KEY,writer_port INT NOT NULL)", "CREATE TABLE galera_runner.ist_backlog(id INT PRIMARY KEY,payload MEDIUMBLOB NOT NULL)"} {
		if _, e = db.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	for i, a := range nodes[:3] {
		db, e = a.runner.LocalAccessDB(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(?,@@port)", i+1); e != nil {
			t.Fatal(e)
		}
	}
	readAll := func(want int) bool {
		for _, a := range nodes[:3] {
			db, e := a.runner.LocalAccessDB(ctx)
			if e != nil {
				return false
			}
			var n int
			if db.QueryRowContext(ctx, "SELECT COUNT(*) FROM galera_runner.writes").Scan(&n) != nil || n != want {
				return false
			}
		}
		return true
	}
	nativeGroupPoll(t, 15*time.Second, "cross-node writes", func() bool { return readAll(3) })
	nativeGaleraDurabilityProof(t, ctx, nodes[:3])
	nativeGaleraGracefulISTProof(t, ctx, nodes[:3], dir)
	phase := time.Now()
	states, e := nodes[2].client.Status(ctx)
	if e != nil {
		t.Fatal(e)
	}
	livePID := states.Processes["mysqld"].PID
	// The preceding graceful IST proof legitimately ran supervised recovery;
	// a live reapply must not run it again.
	recoveryLog := filepath.Join(dataPaths(filepath.Join(dir, nodes[2].spec.Member)).root, "galera-supervisor-recover.log")
	recoveryBefore, recoveryBeforeErr := os.Stat(recoveryLog)
	if recoveryBeforeErr != nil && !os.IsNotExist(recoveryBeforeErr) {
		t.Fatal(recoveryBeforeErr)
	}
	nodes[2].publish(ctx, []string{"mysqld"})
	states, e = nodes[2].client.Status(ctx)
	if e != nil || states.Processes["mysqld"].PID != livePID {
		t.Fatalf("live config reapply restarted member: %+v %v", states, e)
	}
	recoveryAfter, recoveryAfterErr := os.Stat(recoveryLog)
	if recoveryAfterErr != nil && !os.IsNotExist(recoveryAfterErr) {
		t.Fatal(recoveryAfterErr)
	}
	if (recoveryBeforeErr != nil) != (recoveryAfterErr != nil) || recoveryAfterErr == nil && (!recoveryAfter.ModTime().Equal(recoveryBefore.ModTime()) || recoveryAfter.Size() != recoveryBefore.Size()) {
		t.Fatal("live config reapply ran offline recovery")
	}
	if e = nodes[2].client.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "restart"}); e != nil {
		t.Fatal(e)
	}
	nativeGroupPoll(t, 90*time.Second, "explicit stopped-to-starting recovery", func() bool {
		st, err := nodes[2].client.Status(ctx)
		return err == nil && st.Processes["mysqld"].PID != 0 && st.Processes["mysqld"].PID != livePID && healthy(3) && readAll(3)
	})
	nativeGaleraGTIDQuorumProof(t, ctx, nodes[:3], "explicit restart")
	errorLog := filepath.Join(dataPaths(filepath.Join(dir, nodes[2].spec.Member)).root, "mysql-error.log")
	for writeID := 4; writeID <= 5; writeID++ {
		states, e = nodes[2].client.Status(ctx)
		if e != nil {
			t.Fatal(e)
		}
		oldPID := states.Processes["mysqld"].PID
		if oldPID == 0 {
			t.Fatal("no supervised mysqld PID")
		}
		beforeLog, err := os.Stat(errorLog)
		if err != nil {
			t.Fatal(err)
		}
		logOffset := beforeLog.Size()
		if e = syscall.Kill(oldPID, syscall.SIGKILL); e != nil {
			t.Fatal(e)
		}
		db, e = nodes[0].runner.LocalAccessDB(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(?,@@port)", writeID); e != nil {
			t.Fatal(e)
		}
		nativeGroupPoll(t, 90*time.Second, "supervisor fresh recovery and IST rejoin", func() bool {
			st, err := nodes[2].client.Status(ctx)
			return err == nil && st.Processes["mysqld"].PID != 0 && st.Processes["mysqld"].PID != oldPID && healthy(3) && readAll(writeID)
		})
		raw, err := os.ReadFile(recoveryLog)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = parseRecoveredPosition(raw); err != nil {
			t.Fatalf("supervisor restart did not recover current redo: %v", err)
		}
		currentLog, err := os.ReadFile(errorLog)
		if err != nil || int64(len(currentLog)) < logOffset || !containsCompletedIST(currentLog[logOffset:]) {
			member := nodes[2].spec.Member
			nativeGaleraStartupDiagnostics(t, nodes[2].spec, "native-galera-token-"+member, filepath.Join(dir, member), filepath.Join(dir, member+"-agent.log"))
			t.Fatalf("crash cycle %d lacks completed writeset IST after missing-write propagation (or used physical SST): %v", writeID, err)
		}
		for _, line := range strings.Split(string(currentLog[logOffset:]), "\n") {
			if strings.Contains(line, "protocol") || strings.Contains(line, "Initialized wsrep sidno") {
				t.Logf("PXC IST protocol trace: cycle=%d %s", writeID-3, line)
			}
		}
		nativeGaleraGTIDQuorumProof(t, ctx, nodes[:3], fmt.Sprintf("completed IST cycle %d", writeID-3))
	}
	nativeGaleraTLSProof(t, ctx, nodes[:3], dir, trafficCert)
	t.Logf("live reapply + explicit restart + two SIGKILL/recovery/IST cycles: %s", time.Since(phase))
	phase = time.Now()
	// A full outage is CP-intended stop on every member, so the supervisor is
	// fenced before recovery observations are collected.
	for _, a := range nodes[:3] {
		a.publish(ctx, []string{})
	}
	outageBegan := time.Now()
	for _, a := range nodes[:3] {
		status, err := a.client.Status(ctx)
		if err != nil || status.Processes["mysqld"].PID != 0 {
			t.Fatalf("traffic rotation lacks stopped-member proof: %s %v", a.spec.Member, err)
		}
		local := a.spec.Peers[0]
		for _, peer := range a.spec.Peers {
			if peer.Name == a.spec.Member {
				local = peer
			}
		}
		conn, err := net.DialTimeout("tcp", galeraAddress(local.Host, local.Port), time.Second)
		if conn != nil {
			conn.Close()
			t.Fatal("SQL listener remained reachable during full-stop traffic rotation")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("SQL outage did not produce connection refusal: %v", err)
		}
	}
	recovered := make([]myspec.WSRepRecoverResponse, 3)
	best := 0
	for i, a := range nodes[:3] {
		p := dataPaths(filepath.Join(dir, a.spec.Member))
		id, err := readIdentity(p)
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(filepath.Join(p.data, "auto.cnf"))
		if err != nil {
			t.Fatal(err)
		}
		a.action(ctx, myspec.ActionWSRepRecover, struct{}{}, &recovered[i])
		after, err := os.ReadFile(filepath.Join(p.data, "auto.cnf"))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s recovery identity receipt=%s auto-before=%q auto-after=%q", a.spec.Member, id.ServerUUID, before, after)
		if !recovered[i].Stopped || recovered[i].UUID == "" {
			t.Fatal(recovered[i])
		}
		if i > 0 && recovered[i].UUID != recovered[0].UUID {
			t.Fatal("recovery UUID disagreement")
		}
		if recovered[i].Seqno > recovered[best].Seqno {
			best = i
		}
	}
	// Only the highest member gets the new epoch's bootstrap permission.
	recoveryEpoch := nodes[0].spec.Galera.FormationEpoch + 1
	retiredTraffic := trafficCert
	rotated := nativeGaleraConfigs(t, "rotated-members.json")
	for _, a := range nodes[:3] {
		for _, exported := range rotated {
			if exported.spec.Member == a.spec.Member {
				if exported.spec.ServerCert.CertPEM != a.spec.ServerCert.CertPEM || exported.spec.Galera.TrafficCAPEM == a.spec.Galera.TrafficCAPEM {
					t.Fatal("traffic rotation must replace dedicated trust without replacing SQL leaves")
				}
				a.spec = nativeGaleraLocalConfig(exported.spec, a.spec)
			}
		}
	}
	trafficCert = *nodes[0].spec.Galera.TrafficCert
	for i, a := range nodes[:3] {
		a.spec.Galera.FormationEpoch = recoveryEpoch
		a.spec.Galera.Bootstrap = i == best
		a.spec.Galera.StartPosition = ""
		if i == best {
			a.spec.Galera.StartPosition = fmt.Sprintf("%s:%d", recovered[i].UUID, recovered[i].Seqno)
		}
	}
	nodes[best].publish(ctx, []string{"mysqld"})
	nativeGaleraVerifiedSQL(t, ctx, nodes[best].spec)
	t.Logf("traffic CA/key rotation measured complete SQL unavailability until first verified encrypted SQL: %s", time.Since(outageBegan))
	for i, a := range nodes[:3] {
		if i != best {
			a.publish(ctx, []string{"mysqld"})
		}
	}
	nativeGroupPoll(t, 90*time.Second, "highest-position recovered cluster", func() bool { return healthy(3) && readAll(5) })
	// A consumed formation epoch cannot be bootstrapped again through its route.
	if e = nodes[best].client.Engine(ctx, "mysql", myspec.ActionGaleraBootstrap, myspec.GaleraBootstrapRequest{ClusterName: nodes[best].spec.Galera.ClusterName, FormationEpoch: recoveryEpoch, StartPosition: nodes[best].spec.Galera.StartPosition}, nil); e == nil {
		t.Fatal("repeated epoch bootstrap accepted")
	}
	t.Logf("full stop/recover/highest bootstrap/rejoin: %s positions=%+v", time.Since(phase), recovered)
	nativeGaleraGTIDQuorumProof(t, ctx, nodes[:3], "full-stop recovery")
	nativeGaleraTLSProof(t, ctx, nodes[:3], dir, trafficCert)
	nativeGaleraRejectUnauthorizedPeers(t, ctx, nodes[0], retiredTraffic)
	nodes[3] = start(3)
	proxyPeerPolicy := nativeTrafficPeerPolicy(t, nodes[0].spec, *nodes[3].spec.ServerCert)
	galeraPeer := galeraLocal(nodes[0].spec)
	if err := nativeTrafficPeerRejected(ctx, galeraAddress(galeraPeer.Host, nodes[0].spec.Galera.GaleraPort), proxyPeerPolicy, false); err != nil {
		t.Fatalf("SQL proxy credential was not explicitly rejected by Galera traffic trust: %v", err)
	}
	t.Log("actual CP-issued ProxySQL SQL-client credential rejected by dedicated Galera traffic trust")
	adminDB, e := proxyAdmin(ctx, nodes[3].spec)
	if e != nil {
		t.Fatal(e)
	}
	defer adminDB.Close()
	assertNativeMySQLOnlyProxy(t, adminDB)
	nativeGroupPoll(t, 30*time.Second, "exactly one Galera proxy writer", func() bool {
		var count int
		return adminDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=10 AND status='ONLINE'").Scan(&count) == nil && count == 1
	})
	tlsName := "native-galera-runner"
	if e = mysqldriver.RegisterTLSConfig(tlsName, tlsConfig(x509Pool(nodes[3].spec.CAPEM), nativeCertificateName(t, nodes[3].spec.ServerCert.CertPEM))); e != nil {
		t.Fatal(e)
	}
	defer mysqldriver.DeregisterTLSConfig(tlsName)
	cfg := mysqldriver.NewConfig()
	cfg.User = nodes[0].spec.Admin.Username
	cfg.Passwd = nodes[0].spec.Admin.Password
	cfg.Net = "tcp"
	cfg.Addr = addr(proxyClient)
	cfg.TLSConfig = tlsName
	cfg.Timeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	app, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	app.SetMaxOpenConns(1)
	app.SetMaxIdleConns(0)
	nativeGroupPoll(t, 40*time.Second, "Galera ProxySQL writer", func() bool {
		_, e := app.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(10,@@port)")
		return e == nil
	})
	var writerPort int
	db, e = nodes[0].runner.LocalAccessDB(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.QueryRowContext(ctx, "SELECT writer_port FROM galera_runner.writes WHERE id=10").Scan(&writerPort); e != nil {
		t.Fatal(e)
	}
	writer := -1
	for i, p := range peers {
		if p.Port == writerPort {
			writer = i
		}
	}
	if writer < 0 {
		t.Fatal(writerPort)
	}
	phase = time.Now()
	crashFenced := func(a *groupRunnerAgent) {
		t.Helper()
		if err := a.client.Maintenance(ctx, agentapi.Maintenance{Enabled: true}); err != nil {
			t.Fatal(err)
		}
		status, err := a.client.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Processes["mysqld"].PID <= 0 {
			t.Fatal("fault target has no owned live mysqld")
		}
		if err := syscall.Kill(status.Processes["mysqld"].PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		nativeGroupPoll(t, 15*time.Second, "crashed member held offline", func() bool {
			status, err := a.client.Status(ctx)
			return err == nil && status.Processes["mysqld"].PID == 0
		})
	}
	crashFenced(nodes[writer])
	// Each attempt uses a unique PK: never blindly retry a timeout's uncertain
	// commit. A success with another backend proves writer movement.
	id := 20
	nativeGroupPoll(t, 45*time.Second, "ProxySQL Galera writer failover", func() bool {
		id++
		_, e := app.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(?,@@port)", id)
		return e == nil
	})
	t.Logf("ProxySQL writer failover: %s", time.Since(phase))
	other := (writer + 1) % 3
	survivor := (writer + 2) % 3
	// Graceful Galera leaves shrink the component and can preserve a singleton
	// Primary. Two abrupt losses, with restart suppression, actually lose quorum.
	crashFenced(nodes[other])
	nativeGroupPoll(t, 40*time.Second, "minority is ineligible", func() bool {
		var status myspec.GaleraStatusResponse
		return nodes[survivor].client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status) == nil && status.ClusterStatus != "Primary" && !status.Ready && !status.Eligible()
	})
	db, e = nodes[survivor].runner.LocalAccessDB(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(1000,@@port)"); e == nil {
		t.Fatal("minority accepted a write")
	}
	if e = nodes[survivor].client.Engine(ctx, "mysql", myspec.ActionGaleraForcePrimary, myspec.GaleraForcePrimaryRequest{ClusterName: nodes[survivor].spec.Galera.ClusterName, FormationEpoch: recoveryEpoch}, nil); e == nil {
		t.Fatal("minority recovery accepted without operator confirmation")
	}
	nodes[survivor].action(ctx, myspec.ActionGaleraForcePrimary, myspec.GaleraForcePrimaryRequest{ClusterName: nodes[survivor].spec.Galera.ClusterName, FormationEpoch: recoveryEpoch, Confirmation: myspec.GaleraForcePrimaryConfirmation}, nil)
	nativeGroupPoll(t, 30*time.Second, "explicit fenced minority recovery", func() bool {
		var status myspec.GaleraStatusResponse
		return nodes[survivor].client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status) == nil && status.Eligible() && status.ClusterSize == 1
	})
	for _, a := range []*groupRunnerAgent{nodes[other], nodes[writer]} {
		if err := a.client.Maintenance(ctx, agentapi.Maintenance{Enabled: false}); err != nil {
			t.Fatal(err)
		}
	}
	nativeGroupPoll(t, 90*time.Second, "all members after proxy failover", func() bool { return healthy(3) })
	nativeGaleraGTIDQuorumProof(t, ctx, nodes[:3], "before physical backup")
	nativeGaleraRestoreProof(t, ctx, nodes[:3], dir)
	expanded := nativeGaleraAdditionProof(t, ctx, nodes[:3], nodes[3], dir)
	nativeGaleraISTFaultRecoveryProof(t, ctx, expanded, dir)
	t.Logf("production PXC agent total: %s", time.Since(began))
}

// Exercise the same asynchronous backup/restore and explicit prepare-join routes
// as the control plane, against a fresh native MinIO, without touching lab data.
func nativeGaleraRestoreProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, dir string) {
	t.Helper()
	began := time.Now()
	var credentials [32]byte
	if _, err := rand.Read(credentials[:]); err != nil {
		t.Fatal(err)
	}
	access, secret := hex.EncodeToString(credentials[:12]), hex.EncodeToString(credentials[12:])
	address, console := nativeTestAddress(t), nativeTestAddress(t)
	cmd := exec.CommandContext(ctx, os.Getenv("KARAKORAM_MYSQL_MINIO_BIN"), "server", "--address", address, "--console-address", console, nativeTestDir(t))
	cmd.Env = append(os.Environ(), "MINIO_ROOT_USER="+access, "MINIO_ROOT_PASSWORD="+secret)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	repo := myspec.Repository{Path: "pxc/runner", S3: myspec.S3{Endpoint: "http://" + address, Bucket: "pxc-native-backups", Region: "us-east-1", PathStyle: true, AccessKey: access, SecretKey: secret}}
	client, err := s3Client(repo)
	if err != nil {
		t.Fatal(err)
	}
	nativeGroupPoll(t, 30*time.Second, "PXC MinIO readiness", func() bool {
		return client.MakeBucket(ctx, repo.S3.Bucket, minio.MakeBucketOptions{Region: repo.S3.Region}) == nil
	})
	source := nodes[0]
	t.Log("PXC restore proof: configuring backup source")
	source.spec.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{RotateIntervalSeconds: 3600, ExpireSeconds: 604800}}
	source.publish(ctx, []string{"mysqld"})
	db, err := source.runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rows int
	var oldServerUUID string
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM galera_runner.writes").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&oldServerUUID); err != nil {
		t.Fatal(err)
	}
	var oldStatus myspec.GaleraStatusResponse
	source.action(ctx, myspec.ActionGaleraStatus, struct{}{}, &oldStatus)
	var status myspec.StatusResponse
	source.action(ctx, myspec.ActionStatus, struct{}{}, &status)
	if status.PhysicalIdentity == "" {
		t.Fatal("PXC source physical identity missing")
	}
	req := myspec.BackupRequest{Receipt: "pxc-full", Type: "full", Repository: repo, ObjectPrefix: repo.Path + "/full", PhysicalIdentity: status.PhysicalIdentity}
	var backup myspec.BackupResponse
	source.action(ctx, myspec.ActionBackup, req, &backup)
	nativeGroupPoll(t, 3*time.Minute, "PXC streamed physical backup", func() bool {
		source.action(ctx, myspec.ActionBackupInfo, map[string]string{"receipt": req.Receipt}, &backup)
		if backup.State == "failed" {
			t.Fatalf("PXC backup failed: %+v", backup)
		}
		return backup.State == "done"
	})
	if backup.Result == nil || backup.Result.GTIDSet == "" || backup.Result.ToLSN == 0 || backup.Result.PhysicalIdentity != status.PhysicalIdentity || backup.Result.ManifestDigest == "" {
		t.Fatalf("PXC backup metadata missing: %+v", backup)
	}
	t.Logf("PXC restore proof: full streamed backup complete, GTIDs=%s", backup.Result.GTIDSet)
	if _, err = db.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(2000,@@port)"); err != nil {
		t.Fatal(err)
	}
	newCluster := "9347c03140ab4a90b950ec3525ae4321"
	restoreReq := myspec.RestoreRequest{Receipt: "pxc-independent-history", Restore: myspec.Restore{Source: repo, ObjectPaths: []string{backup.Result.ObjectPrefix}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: backup.Result.ObjectPrefix, Digest: backup.Result.ManifestDigest, Size: backup.Result.ManifestSize}}, NewGroupName: newCluster}}
	tamperReq := restoreReq
	tamperReq.Receipt = "pxc-tamper-preflight"
	tamperReq.Restore.NewGroupName = "8347c03140ab4a90b950ec3525ae4321"
	session, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var beforeUUID, beforeGTID, afterUUID, afterGTID string
	if err = session.QueryRowContext(ctx, "SELECT @@server_uuid,@@gtid_executed").Scan(&beforeUUID, &beforeGTID); err != nil {
		t.Fatal(err)
	}
	nativePreflightRestore(t, func(action string, req, out any) {
		source.action(ctx, action, req, out)
	}, &tamperReq)
	_, sourcePaths := source.runner.state()
	preparedName, err := receiptPath(sourcePaths, "restore", tamperReq.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(preparedName+".staging", "0", "mysql.ibd"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err = file.ReadAt(first[:], 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	first[0] ^= 1
	_, err = file.WriteAt(first[:], 0)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("PXC staging tamper: write=%v close=%v", err, closeErr)
	}
	tamperReq.RecheckID = "physical-tamper-recheck"
	var rejected myspec.RestoreStatus
	source.action(ctx, myspec.ActionRestorePreflight, tamperReq, &rejected)
	nativeGroupPoll(t, 4*time.Minute, "PXC tampered prepared restore rejection", func() bool {
		source.action(ctx, myspec.ActionRestoreStatus, myspec.RestoreRequest{Receipt: tamperReq.Receipt}, &rejected)
		return rejected.State == "failed" || rejected.VerifiedRecheckID == tamperReq.RecheckID
	})
	if rejected.State != "failed" || rejected.Error != "replay_plan_changed" {
		t.Fatalf("PXC tampered physical plan accepted: %+v", rejected)
	}
	if err = session.QueryRowContext(ctx, "SELECT @@server_uuid,@@gtid_executed").Scan(&afterUUID, &afterGTID); err != nil || beforeUUID != afterUUID || beforeGTID != afterGTID {
		t.Fatalf("PXC failed preflight changed serving source: %v", err)
	}
	for _, member := range nodes {
		var serving myspec.GaleraStatusResponse
		member.action(ctx, myspec.ActionGaleraStatus, struct{}{}, &serving)
		if !serving.Eligible() {
			t.Fatalf("PXC source no longer Synced after failed preflight: %+v", serving)
		}
	}
	t.Log("PXC prepared physical tamper rejected before STOP; source still Synced")
	nativePreflightRestore(t, func(action string, req, out any) {
		source.action(ctx, action, req, out)
	}, &restoreReq)
	t.Log("PXC restore proof: complete physical/replay plan verified before source STOP")
	for _, a := range nodes {
		a.publish(ctx, []string{})
	}
	t.Log("PXC restore proof: all source members stopped; submitting independent-history restore")
	var restored myspec.RestoreStatus
	source.action(ctx, myspec.ActionRestore, restoreReq, &restored)
	nativeGroupPoll(t, 4*time.Minute, "PXC independent-history restore", func() bool {
		source.action(ctx, myspec.ActionRestoreStatus, myspec.RestoreRequest{Receipt: restoreReq.Receipt}, &restored)
		if restored.State == "failed" {
			raw, _ := os.ReadFile(filepath.Join(dataPaths(filepath.Join(dir, source.spec.Member)).root, "restore-error.log"))
			t.Fatalf("PXC restore failed: %+v log=%s", restored, raw)
		}
		return restored.State == "done"
	})
	wantGTID, err := parseRecoveryGTIDs(backup.Result.GTIDSet)
	if err != nil {
		t.Fatal(err)
	}
	gotGTID, err := parseRecoveryGTIDs(restored.GTIDExecuted)
	if err != nil || !gotGTID.contains(wantGTID) || !wantGTID.contains(gotGTID) {
		t.Fatalf("PXC restored GTIDs=%s backup=%s err=%v", restored.GTIDExecuted, backup.Result.GTIDSet, err)
	}
	if !myspec.ValidUUID(restored.ServerUUID) || restored.ServerUUID == oldServerUUID {
		t.Fatalf("restore did not replace server identity: before=%s after=%s", oldServerUUID, restored.ServerUUID)
	}
	t.Logf("PXC restore proof: seed restore complete, UUID=%s GTIDs=%s", restored.ServerUUID, restored.GTIDExecuted)
	restoreEpoch := source.spec.Galera.FormationEpoch + 1
	for i, a := range nodes {
		a.spec.Galera.ClusterName = newCluster
		a.spec.Galera.FormationEpoch = restoreEpoch
		a.spec.Galera.StartPosition = ""
		a.spec.Galera.Bootstrap = i == 0
		a.spec.AllowInitialize = false
		a.publish(ctx, []string{})
	}
	// Only the restored seed may form the new history; all other old datadirs
	// require the explicit, stopped, newer-epoch prepare-join authorization.
	source.publish(ctx, []string{"mysqld"})
	t.Log("PXC restore proof: restored seed Primary/Synced; resetting and SST-joining peers")
	for _, a := range nodes[1:] {
		prepare := myspec.GaleraBootstrapRequest{ClusterName: newCluster, FormationEpoch: restoreEpoch}
		a.action(ctx, myspec.ActionGaleraPrepareJoin, prepare, nil)
		a.action(ctx, myspec.ActionGaleraPrepareJoin, prepare, nil) // idempotent before join
		a.publish(ctx, []string{"mysqld"})
	}
	var newStateUUID string
	nativeGroupPoll(t, 3*time.Minute, "PXC restored seed and two new SST joins", func() bool {
		for _, a := range nodes {
			var st myspec.GaleraStatusResponse
			if a.client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &st) != nil || !st.Eligible() || st.ClusterSize != 3 || st.StateUUID == oldStatus.StateUUID {
				return false
			}
			if newStateUUID == "" {
				newStateUUID = st.StateUUID
			}
			if st.StateUUID != newStateUUID {
				return false
			}
		}
		return true
	})
	for _, a := range nodes {
		db, err := a.runner.LocalAccessDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var count, postBackup int
		var gtid string
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(id=2000),0) FROM galera_runner.writes").Scan(&count, &postBackup); err != nil || count != rows || postBackup != 0 {
			t.Fatalf("restored %s rows=%d want=%d post-backup=%d err=%v", a.spec.Member, count, rows, postBackup, err)
		}
		if err = db.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&gtid); err != nil {
			t.Fatal(err)
		}
		got, err := parseRecoveryGTIDs(gtid)
		if err != nil || !got.contains(wantGTID) {
			t.Fatalf("%s lost restored GTIDs: got=%s want=%s err=%v", a.spec.Member, gtid, backup.Result.GTIDSet, err)
		}
		identity, err := readGaleraIdentity(a.spec, dataPaths(filepath.Join(dir, a.spec.Member)))
		if err != nil || identity.StateUUID != newStateUUID || identity.ClusterName != newCluster {
			t.Fatalf("%s restored identity=%+v err=%v", a.spec.Member, identity, err)
		}
	}
	nativeGaleraTLSProof(t, ctx, nodes, dir, *nodes[0].spec.Galera.TrafficCert)
	t.Logf("production PXC full backup/restore/new history/prepare-join SST: %s GTIDs=%s old-state=%s new-state=%s", time.Since(began), backup.Result.GTIDSet, oldStatus.StateUUID, newStateUUID)
	nativeGaleraGTIDQuorumProof(t, ctx, nodes, "restore and peer SST")
}

// The SQL rows alone cannot certify asynchronous GTID continuity after IST.
// Exercise the same comparator used by production continuity admission. A
// missing comparison quorum (a member briefly not Synced/ready) is the
// production Unknown state, so observe again within a bounded window; only a
// compatible or diverged verdict ends the proof.
func nativeGaleraGTIDQuorumProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, phase string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		statuses := make(map[string]myspec.GaleraStatusResponse, len(nodes))
		for _, a := range nodes {
			var status myspec.GaleraStatusResponse
			if err := a.client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status); err != nil {
				t.Fatalf("%s GTID observation for %s: %v", phase, a.spec.Member, err)
			}
			statuses[a.spec.Member] = status
			t.Logf("GTID quorum observation: phase=%s member=%s state=%s eligible=%t cluster=%s local=%s ready=%t connected=%t GTIDs=%s", phase, a.spec.Member, status.StateUUID, status.Eligible(), status.ClusterStatus, status.LocalStateComment, status.Ready, status.Connected, status.GTIDExecuted)
		}
		err := myspec.CheckGTIDQuorum(statuses)
		if err == nil {
			t.Logf("GTID quorum guard: phase=%s histories compatible", phase)
			return
		}
		var conflict *myspec.GTIDQuorumError
		if errors.As(err, &conflict) && conflict.Diverged {
			t.Logf("GTID quorum guard: phase=%s upstream divergence detected; continuity-dependent admission rejected; a fresh full backup is required: %v", phase, err)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s lacks a GTID comparison quorum: %v", phase, err)
		}
		t.Logf("GTID quorum guard: phase=%s comparison unavailable, observing again: %v", phase, err)
		time.Sleep(2 * time.Second)
	}
}

func nativeGaleraDurabilityProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent) {
	t.Helper()
	for _, a := range nodes {
		db, err := a.runner.LocalAccessDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var syncBinlog, flush, replicaUpdates, ordered, simpleRecovery int
		var mode, consistency string
		err = db.QueryRowContext(ctx, "SELECT @@sync_binlog,@@innodb_flush_log_at_trx_commit,@@log_replica_updates,@@gtid_mode,@@enforce_gtid_consistency,@@binlog_order_commits,@@binlog_gtid_simple_recovery").Scan(&syncBinlog, &flush, &replicaUpdates, &mode, &consistency, &ordered, &simpleRecovery)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("PXC durability: member=%s sync_binlog=%d innodb_flush_log_at_trx_commit=%d log_replica_updates=%d gtid_mode=%s enforce_gtid_consistency=%s binlog_order_commits=%d binlog_gtid_simple_recovery=%d", a.spec.Member, syncBinlog, flush, replicaUpdates, mode, consistency, ordered, simpleRecovery)
		if syncBinlog != 1 || flush != 1 || replicaUpdates != 1 || mode != "ON" || consistency != "ON" || ordered != 1 {
			t.Fatal("native PXC is not using durable ordered GTID binary logging")
		}
		rows, err := db.QueryContext(ctx, "SHOW VARIABLES LIKE 'wsrep%gtid%'")
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var name, value string
			if err = rows.Scan(&name, &value); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			count++
			t.Logf("PXC GTID setting: member=%s %s=%s", a.spec.Member, name, value)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("PXC wsrep GTID variable count: member=%s count=%d", a.spec.Member, count)
	}
}

func nativeGaleraGracefulISTProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, dir string) {
	t.Helper()
	target := nodes[2]
	errorLog := filepath.Join(dataPaths(filepath.Join(dir, target.spec.Member)).root, "mysql-error.log")
	target.publish(ctx, []string{})
	before, err := os.Stat(errorLog)
	if err != nil {
		t.Fatal(err)
	}
	db, err := nodes[0].runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = nativeGaleraExec(ctx, db, "INSERT INTO galera_runner.ist_backlog VALUES(30,'graceful-missing-write')"); err != nil {
		t.Fatal(err)
	}
	target.publish(ctx, []string{"mysqld"})
	nativeGroupPoll(t, 90*time.Second, "gracefully stopped member completes missing-write IST", func() bool {
		var status myspec.GaleraStatusResponse
		if target.client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status) != nil || !status.Eligible() || status.ClusterSize != 3 {
			return false
		}
		db, err := target.runner.LocalAccessDB(ctx)
		if err != nil {
			return false
		}
		var payload string
		return db.QueryRowContext(ctx, "SELECT payload FROM galera_runner.ist_backlog WHERE id=30").Scan(&payload) == nil && payload == "graceful-missing-write"
	})
	current, err := os.ReadFile(errorLog)
	if err != nil || int64(len(current)) < before.Size() || !containsCompletedIST(current[before.Size():]) {
		t.Fatalf("graceful missing-write rejoin lacks completed IST evidence: %v", err)
	}
	nativeGaleraGTIDQuorumProof(t, ctx, nodes, "graceful missing-write IST")
}
