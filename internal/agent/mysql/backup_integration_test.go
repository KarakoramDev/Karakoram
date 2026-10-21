//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
	"github.com/minio/minio-go/v7"
)

type nativeBackupAgent struct {
	t        *testing.T
	runner   *Runner
	client   *http.Client
	url, dir string
	cancel   context.CancelFunc
	done     chan error
	once     sync.Once
}

func startBackupAgent(t *testing.T, s myspec.Spec, stopped bool) *nativeBackupAgent {
	t.Helper()
	dir := nativeTestDir(t)
	ca, cert := coreTLS(t)
	s.CAPEM = ca
	s.ServerCert = &cert
	raw, _ := json.Marshal(s)
	desired := []string{"mysqld"}
	if stopped {
		desired = []string{}
	}
	config := agentapi.MemberConfig{Cluster: "backup-integration", Member: s.Member, Role: s.Role, Engine: "mysql", EngineSpec: raw, DesiredProcesses: desired, ConfigVersion: 1, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(config) }))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 30 * time.Second}
	listen := nativeTestAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	a := &nativeBackupAgent{t: t, runner: New(), client: client, url: "https://" + listen, dir: dir, cancel: cancel, done: make(chan error, 1)}
	go func() {
		a.done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: "backup-native-token", Cluster: config.Cluster, Member: s.Member, DataDir: dir, Listen: listen, Engine: "mysql", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, a.runner)
	}()
	t.Cleanup(func() { a.close(); client.CloseIdleConnections(); cp.Close(); os.RemoveAll(dir) })
	nativeGroupPoll(t, 60*time.Second, "backup agent configuration", func() bool {
		a.runner.mu.RLock()
		ready := a.runner.current.Member == s.Member && a.runner.node != nil
		a.runner.mu.RUnlock()
		if !ready {
			return false
		}
		if stopped {
			_, err := os.Stat(dataPaths(dir).config)
			return err == nil
		}
		_, err := a.runner.LocalAccessDB(context.Background())
		return err == nil
	})
	return a
}
func (a *nativeBackupAgent) close() {
	a.once.Do(func() {
		a.cancel()
		select {
		case err := <-a.done:
			if err != nil {
				a.t.Errorf("agent shutdown: %v", err)
			}
		case <-time.After(65 * time.Second):
			a.t.Error("agent shutdown timeout")
		}
	})
}
func (a *nativeBackupAgent) request(action string, value, out any) int {
	a.t.Helper()
	b, _ := json.Marshal(value)
	req, _ := http.NewRequest("POST", a.url+action, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer backup-native-token")
	resp, err := a.client.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		a.t.Logf("%s HTTP %d: %s", action, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode == 200 && out != nil {
		if err = json.Unmarshal(body, out); err != nil {
			a.t.Fatalf("%s: %s: %v", action, body, err)
		}
	}
	return resp.StatusCode
}
func (a *nativeBackupAgent) backup(req myspec.BackupRequest) myspec.BackupResult {
	a.t.Helper()
	start := time.Now()
	var out myspec.BackupResponse
	if code := a.request("/v1/mysql/backup", req, &out); code != 200 {
		a.t.Fatalf("backup HTTP %d", code)
	}
	nativeGroupPoll(a.t, 2*time.Minute, "physical backup receipt", func() bool {
		a.request("/v1/mysql/backup-info", map[string]string{"receipt": req.Receipt}, &out)
		if out.State == "failed" {
			a.t.Fatalf("backup failed: %+v", out)
		}
		return out.State == "done"
	})
	a.t.Logf("%s backup: %s", req.Type, time.Since(start))
	if out.Result == nil {
		a.t.Fatal("backup result missing")
	}
	if out.Result.PhysicalIdentity == "" || req.PhysicalIdentity != "" && out.Result.PhysicalIdentity != req.PhysicalIdentity {
		a.t.Fatal("backup result lost source physical identity")
	}
	return *out.Result
}

func startNativeBackupRepository(t *testing.T, ctx context.Context) (myspec.Repository, *minio.Client) {
	t.Helper()
	minioBin := os.Getenv("KARAKORAM_MYSQL_MINIO_BIN")
	if minioBin == "" {
		t.Skip("set KARAKORAM_MYSQL_MINIO_BIN to a native MinIO binary")
	}
	data := nativeTestDir(t)
	var credentials [32]byte
	if _, err := rand.Read(credentials[:]); err != nil {
		t.Fatal(err)
	}
	access := hex.EncodeToString(credentials[:12])
	secret := hex.EncodeToString(credentials[12:])
	minioAddress, consoleAddress := nativeTestAddress(t), nativeTestAddress(t)
	cmd := exec.CommandContext(ctx, minioBin, "server", "--address", minioAddress, "--console-address", consoleAddress, data)
	cmd.Env = append(os.Environ(), "MINIO_ROOT_USER="+access, "MINIO_ROOT_PASSWORD="+secret)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	repo := myspec.Repository{Path: "mysql/integration", S3: myspec.S3{Endpoint: "http://" + minioAddress, Bucket: "native-backups", Region: "us-east-1", PathStyle: true, AccessKey: access, SecretKey: secret}}
	c, err := s3Client(repo)
	if err != nil {
		t.Fatal(err)
	}
	nativeGroupPoll(t, 30*time.Second, "MinIO readiness", func() bool {
		return c.MakeBucket(ctx, repo.S3.Bucket, minio.MakeBucketOptions{Region: repo.S3.Region}) == nil
	})
	return repo, c
}
func TestNativeMySQLBackupRestorePITR(t *testing.T) {
	if os.Getenv("KARAKORAM_MYSQL_BIN_DIR") == "" || os.Getenv("KARAKORAM_MYSQL_XTRABACKUP_DIR") == "" {
		t.Skip("set native MySQL and XtraBackup integration paths")
	}
	t.Setenv("KARAKORAM_MYSQL_PORT", fmt.Sprint(nativeTestPort(t)))
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	repo, c := startNativeBackupRepository(t, ctx)
	var err error
	s := coreTestSpec()
	s.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 3600, ExpireSeconds: 604800}}
	source := startBackupAgent(t, s, false)
	db, err := source.runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execSQL := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	execSQL("CREATE DATABASE recovery")
	execSQL("CREATE TABLE recovery.rows_test(id INT PRIMARY KEY)")
	execSQL("CREATE USER 'recovery_app'@'%' IDENTIFIED BY 'backup-secondary-password' REQUIRE SSL")
	execSQL("ALTER USER 'recovery_app'@'%' IDENTIFIED BY 'backup-current-password' RETAIN CURRENT PASSWORD")
	execSQL("CREATE USER 'recovery_dropped'@'%' IDENTIFIED BY 'backup-dropped-password' REQUIRE SSL")
	execSQL("INSERT INTO recovery.rows_test VALUES(1)")
	var sourceStatus myspec.StatusResponse
	if code := source.request("/v1/mysql/status", struct{}{}, &sourceStatus); code != http.StatusOK {
		t.Fatalf("source status HTTP %d", code)
	}
	if sourceStatus.PhysicalIdentity == "" {
		t.Fatal("source physical identity missing")
	}
	full := source.backup(myspec.BackupRequest{Receipt: "full", Type: "full", Repository: repo, ObjectPrefix: repo.Path + "/full", PhysicalIdentity: sourceStatus.PhysicalIdentity})
	execSQL("INSERT INTO recovery.rows_test VALUES(2)")
	inc := source.backup(myspec.BackupRequest{Receipt: "incremental", Type: "incremental", BaseLSN: full.ToLSN, Repository: repo, ObjectPrefix: repo.Path + "/incremental", PhysicalIdentity: full.PhysicalIdentity})
	execSQL("ALTER USER 'recovery_app'@'%' IDENTIFIED BY 'rotated-after-backup-password'")
	execSQL("DROP USER 'recovery_dropped'@'%'")
	// Both commits fall within one second; the target is read from the original
	// commit timestamp in the archived events, not wall-clock statement start.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second).Add(30 * time.Millisecond)))
	execSQL("INSERT INTO recovery.rows_test VALUES(3)")
	var targetGTID string
	if err = db.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&targetGTID); err != nil {
		t.Fatal(err)
	}
	execSQL("INSERT INTO recovery.rows_test VALUES(4)")
	archiveStart := time.Now()
	if err = source.runner.archiveBinlogs(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("binlog archive: %s", time.Since(archiveStart))
	targetSet, _ := parseRecoveryGTIDs(targetGTID)
	backupSet, _ := parseRecoveryGTIDs(inc.GTIDSet)
	var targetMicros, afterMicros int64
	sidecars := 0
	for obj := range c.ListObjects(ctx, repo.S3.Bucket, minio.ListObjectsOptions{Prefix: repo.Path + "/binlogs/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		o, err := c.GetObject(ctx, repo.S3.Bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var m binlogSidecar
		if err = json.NewDecoder(o).Decode(&m); err != nil {
			t.Fatal(err)
		}
		o.Close()
		sidecars++
		file := filepath.Join(t.TempDir(), m.File)
		if err = downloadFile(ctx, c, repo, strings.TrimSuffix(obj.Key, ".json"), file); err != nil {
			t.Fatal(err)
		}
		hash, size, err := hashFile(file)
		if err != nil || hash != m.SHA256 || size != m.Size {
			t.Fatalf("sidecar sha256 mismatch: %v", err)
		}
		for _, tx := range m.Transactions {
			g, err := parseRecoveryGTIDs(tx.GTID)
			if err != nil {
				t.Fatal(err)
			}
			if !backupSet.contains(g) {
				if targetSet.contains(g) {
					if tx.OriginalCommitTimestamp > targetMicros {
						targetMicros = tx.OriginalCommitTimestamp
					}
				} else {
					afterMicros = tx.OriginalCommitTimestamp
				}
			}
		}
	}
	if sidecars == 0 || targetMicros == 0 || afterMicros == 0 || targetMicros/1000000 != afterMicros/1000000 || targetMicros >= afterMicros {
		t.Fatalf("same-second commits missing: target=%d after=%d sidecars=%d", targetMicros, afterMicros, sidecars)
	}
	// Restore is refused while the supervisor owns a live mysqld.
	manifests := []myspec.BackupManifestRef{{ObjectPrefix: full.ObjectPrefix, Digest: full.ManifestDigest, Size: full.ManifestSize}, {ObjectPrefix: inc.ObjectPrefix, Digest: inc.ManifestDigest, Size: inc.ManifestSize}}
	invalid := myspec.RestoreRequest{Receipt: "live-refused", Restore: myspec.Restore{Source: repo, ObjectPaths: []string{full.ObjectPrefix, inc.ObjectPrefix}, ObjectManifests: manifests}}
	nativePreflightRestore(t, func(action string, req, out any) {
		if code := source.request("/v1/mysql/"+action, req, out); code != http.StatusOK {
			t.Fatalf("%s HTTP %d", action, code)
		}
	}, &invalid)
	if code := source.request("/v1/mysql/restore", invalid, nil); code == 200 {
		t.Fatal("live restore accepted")
	}
	source.close()
	for _, tc := range []struct {
		name, gtid, stamp string
		rows              int
	}{{"latest", "", "", 2}, {"gtid", targetGTID, "", 3}, {"time", "", time.UnixMicro(targetMicros).UTC().Format(time.RFC3339Nano), 3}} {
		t.Run(tc.name, func(t *testing.T) {
			began := time.Now()
			targetSpec := s
			targetSpec.Backup = nil
			targetSpec.Agent.Password = "target-agent-password"
			targetSpec.Superuser.Password = "target-root-password"
			a := startBackupAgent(t, targetSpec, true)
			request := myspec.RestoreRequest{Receipt: tc.name, Restore: myspec.Restore{Source: repo, ObjectPaths: []string{full.ObjectPrefix, inc.ObjectPrefix}, ObjectManifests: manifests, TargetGTID: tc.gtid, TargetTime: tc.stamp}}
			request.Restore.RecoveryUsers = []myspec.RecoveryUser{{Name: "recovery_app", Current: "backup-current-password", Secondary: "backup-secondary-password"}, {Name: "recovery_dropped", Current: "backup-dropped-password"}}
			nativePreflightRestore(t, func(action string, req, out any) {
				if code := a.request("/v1/mysql/"+action, req, out); code != http.StatusOK {
					t.Fatalf("%s HTTP %d", action, code)
				}
			}, &request)
			var out myspec.RestoreStatus
			if code := a.request("/v1/mysql/restore", request, &out); code != 200 {
				t.Fatalf("restore HTTP %d", code)
			}
			nativeGroupPoll(t, 3*time.Minute, "restore final receipt", func() bool {
				a.request("/v1/mysql/restore-status", map[string]string{"receipt": tc.name}, &out)
				if out.State == "failed" {
					log, _ := os.ReadFile(filepath.Join(dataPaths(a.dir).root, "restore-error.log"))
					t.Fatalf("restore failed: %+v log=%s", out, log)
				}
				return out.State == "done"
			})
			expected := targetGTID
			if tc.name == "latest" {
				expected = inc.GTIDSet
			}
			want, _ := parseRecoveryGTIDs(expected)
			got, err := parseRecoveryGTIDs(out.GTIDExecuted)
			if err != nil || !got.contains(want) || !want.contains(got) {
				t.Fatalf("restored GTIDs=%s want=%s err=%v", out.GTIDExecuted, expected, err)
			}
			if out.Stage != "done" || !myspec.ValidUUID(out.ServerUUID) {
				t.Fatalf("restore identity: %+v", out)
			}
			// The explicit API start releases the stop intent; target credentials must
			// authenticate after the protected startup re-key, unlike source credentials.
			if code := a.request("/v1/processes/mysqld/start", struct{}{}, nil); code != 200 {
				t.Fatalf("start HTTP %d", code)
			}
			var restored *sql.DB
			nativeGroupPoll(t, 45*time.Second, "restored server", func() bool { var e error; restored, e = a.runner.LocalAccessDB(ctx); return e == nil })
			var count int
			if err = restored.QueryRowContext(ctx, "SELECT COUNT(*) FROM recovery.rows_test").Scan(&count); err != nil || count != tc.rows {
				t.Fatalf("rows=%d expected=%d err=%v", count, tc.rows, err)
			}
			var actual string
			if err = restored.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&actual); err != nil {
				t.Fatal(err)
			}
			got, _ = parseRecoveryGTIDs(actual)
			if !got.contains(want) || !want.contains(got) {
				t.Fatalf("live GTIDs=%s expected=%s", actual, expected)
			}
			var port, dropped int
			if err = restored.QueryRowContext(ctx, "SELECT @@port").Scan(&port); err != nil {
				t.Fatal(err)
			}
			if err = restored.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.user WHERE User='recovery_dropped' AND Host='%'").Scan(&dropped); err != nil {
				t.Fatal(err)
			}
			expectedDropped := 0
			if tc.name == "latest" {
				expectedDropped = 1
			}
			if dropped != expectedDropped || len(out.RecoveredUsers) != 1+expectedDropped {
				t.Fatal("PITR-dropped account resurrected or account result incomplete", dropped, out.RecoveredUsers)
			}
			for _, password := range []string{"backup-current-password", "backup-secondary-password", "rotated-after-backup-password"} {
				cfg := mysqldriver.NewConfig()
				cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.TLSConfig = "recovery_app", password, "tcp", fmt.Sprintf("127.0.0.1:%d", port), "skip-verify"
				cfg.Timeout = 3 * time.Second
				connection, e := sql.Open("mysql", cfg.FormatDSN())
				if e != nil {
					t.Fatal(e)
				}
				e = connection.PingContext(ctx)
				connection.Close()
				if password == "rotated-after-backup-password" {
					if e == nil {
						t.Fatal("replayed password survived sealed credential recovery")
					}
				} else if e != nil {
					t.Fatal("backup-era managed password did not authenticate", e)
				}
			}
			var repeat myspec.RestoreStatus
			if code := a.request("/v1/mysql/restore-status", map[string]string{"receipt": tc.name}, &repeat); code != 200 || repeat.GTIDExecuted != out.GTIDExecuted {
				t.Fatal("durable status changed")
			}
			a.close()
			t.Logf("%s restore: %s (%d rows, GTIDs %s)", tc.name, time.Since(began), count, actual)
		})
	}
	t.Logf("native backup/restore/PITR total: %s; verified %d sidecar SHA256 values", time.Since(start), sidecars)
}

func TestNativeMySQLFreshFullBaselinePITR(t *testing.T) {
	testNativeFreshFullBaselinePITR(t, false)
}

func TestNativeGroupFreshFullBaselinePITR(t *testing.T) {
	testNativeFreshFullBaselinePITR(t, true)
}

// A replaced physical source is a valid way to retain transactions absent from
// a fresh full's GTID set. Reusing a SID with shifted counters would instead
// require deliberately divergent/reset GTID history, covered by planner tests.
func testNativeFreshFullBaselinePITR(t *testing.T, group bool) {
	if os.Getenv("KARAKORAM_MYSQL_BIN_DIR") == "" || os.Getenv("KARAKORAM_MYSQL_XTRABACKUP_DIR") == "" {
		t.Skip("set native MySQL and XtraBackup integration paths")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	repo, c := startNativeBackupRepository(t, ctx)
	backupConfig := &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true, RotateIntervalSeconds: 3600, ExpireSeconds: 604800}}
	t.Setenv("KARAKORAM_MYSQL_PORT", fmt.Sprint(nativeTestPort(t)))
	oldSpec := coreTestSpec()
	oldSpec.Backup = backupConfig
	old := startBackupAgent(t, oldSpec, false)
	oldDB, err := old.runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"CREATE DATABASE stale_prebaseline", "CREATE TABLE stale_prebaseline.must_not_replay(id INT PRIMARY KEY)", "INSERT INTO stale_prebaseline.must_not_replay VALUES(99)"} {
		if _, err = oldDB.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	var oldGTID, oldUUID string
	if err = oldDB.QueryRowContext(ctx, "SELECT @@gtid_executed, @@server_uuid").Scan(&oldGTID, &oldUUID); err != nil {
		t.Fatal(err)
	}
	if err = old.runner.archiveBinlogs(ctx); err != nil {
		t.Fatal(err)
	}
	retained := map[string]string{}
	for obj := range c.ListObjects(ctx, repo.S3.Bucket, minio.ListObjectsOptions{Prefix: repo.Path + "/binlogs/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		retained[obj.Key] = obj.ETag
	}
	if len(retained) == 0 {
		t.Fatal("prebaseline archives missing")
	}
	old.close()

	t.Setenv("KARAKORAM_MYSQL_PORT", fmt.Sprint(nativeTestPort(t)))
	s := coreTestSpec()
	s.Backup = backupConfig
	var runner *Runner
	var call func(string, any, any)
	var recheck func(myspec.RestoreRequest) (myspec.RestoreStatus, error)
	var closeSource func()
	var members []*groupRunnerAgent
	if group {
		ca, cert := coreTLS(t)
		peers := []myspec.Peer{{Name: "db1", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db2", Host: "127.0.0.1", Port: nativeTestPort(t)}, {Name: "db3", Host: "127.0.0.1", Port: nativeTestPort(t)}}
		dir := nativeTestDir(t)
		for i := range 3 {
			spec := s
			spec.CAPEM, spec.ServerCert, spec.Topology, spec.Ordinal, spec.Peers, spec.Member = ca, &cert, myspec.TopologyGroup, i, peers, peers[i].Name
			g := *testGroupSpec().GroupReplication
			g.Seeds, g.Bootstrap = peers, i == 0
			if i > 0 {
				donor := peers[0]
				g.CloneDonor = &donor
			}
			spec.GroupReplication = &g
			members = append(members, startNativeGroupRunnerAgent(t, ctx, spec, dir))
		}
		members[0].action(ctx, myspec.ActionBootstrap, myspec.GroupRequest{GroupName: members[0].spec.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}, nil)
		for i := 1; i < 3; i++ {
			members[i].action(ctx, myspec.ActionClone, myspec.CloneRequest{Receipt: fmt.Sprintf("backup-join-%d", i), Donor: peers[0], Version: members[i].spec.Version}, nil)
			members[i].action(ctx, myspec.ActionStartGroup, struct{}{}, nil)
		}
		var primary *groupRunnerAgent
		nativeGroupPoll(t, 60*time.Second, "three ONLINE backup sources with exactly one primary", func() bool {
			primaries := 0
			for _, a := range members {
				var status myspec.StatusResponse
				if err := a.client.Engine(ctx, "mysql", myspec.ActionStatus, struct{}{}, &status); err != nil || status.MemberState != "ONLINE" || status.GroupMembers != 3 {
					return false
				}
				if status.MemberRole == "PRIMARY" {
					primary = a
					primaries++
				}
			}
			return primaries == 1
		})
		s, runner = primary.spec, primary.runner
		call = func(action string, req, out any) { primary.action(ctx, action, req, out) }
		recheck = func(req myspec.RestoreRequest) (myspec.RestoreStatus, error) {
			var out myspec.RestoreStatus
			req.RecheckID = fmt.Sprintf("recheck-%d", time.Now().UnixNano())
			err := primary.client.Engine(ctx, "mysql", "restore-preflight", req, &out)
			if err != nil {
				return out, err
			}
			nativeGroupPoll(t, 4*time.Minute, "prepared restore recheck", func() bool {
				err = primary.client.Engine(ctx, "mysql", myspec.ActionRestoreStatus, myspec.RestoreRequest{Receipt: req.Receipt}, &out)
				return err != nil || out.State == "failed" || out.State == "prepared" && out.VerifiedRecheckID == req.RecheckID
			})
			return out, err
		}
		closeSource = func() {
			for i := len(members) - 1; i >= 0; i-- {
				members[i].close()
			}
		}
	} else {
		a := startBackupAgent(t, s, false)
		runner = a.runner
		call = func(action string, req, out any) {
			if code := a.request("/v1/mysql/"+action, req, out); code != http.StatusOK {
				t.Fatalf("%s HTTP %d", action, code)
			}
		}
		recheck = func(req myspec.RestoreRequest) (myspec.RestoreStatus, error) {
			var out myspec.RestoreStatus
			req.RecheckID = fmt.Sprintf("recheck-%d", time.Now().UnixNano())
			code := a.request("/v1/mysql/restore-preflight", req, &out)
			if code != http.StatusOK {
				return out, fmt.Errorf("preflight recheck HTTP %d", code)
			}
			nativeGroupPoll(t, 4*time.Minute, "prepared restore recheck", func() bool {
				code = a.request("/v1/mysql/restore-status", myspec.RestoreRequest{Receipt: req.Receipt}, &out)
				return code != http.StatusOK || out.State == "failed" || out.State == "prepared" && out.VerifiedRecheckID == req.RecheckID
			})
			if code != http.StatusOK {
				return out, fmt.Errorf("recheck status HTTP %d", code)
			}
			return out, nil
		}
		closeSource = a.close
	}
	db, err := runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execute := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	execute("CREATE DATABASE fresh_baseline")
	execute("CREATE TABLE fresh_baseline.rows_test(id INT PRIMARY KEY)")
	execute("INSERT INTO fresh_baseline.rows_test VALUES(1)")
	// Observe actual replicated writes on every member before XtraBackup.
	sourceUUIDs := map[string]bool{oldUUID: true}
	for _, a := range members {
		nativeGroupPoll(t, 30*time.Second, "baseline row replicated to "+a.spec.Member, func() bool {
			memberDB, err := a.runner.LocalAccessDB(ctx)
			if err != nil {
				return false
			}
			var rows int
			return memberDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM fresh_baseline.rows_test WHERE id=1").Scan(&rows) == nil && rows == 1
		})
		var memberStatus myspec.StatusResponse
		a.action(ctx, myspec.ActionStatus, struct{}{}, &memberStatus)
		if !myspec.ValidUUID(memberStatus.ServerUUID) || sourceUUIDs[memberStatus.ServerUUID] {
			t.Fatalf("source member identity not independent: %+v", memberStatus)
		}
		sourceUUIDs[memberStatus.ServerUUID] = true
	}
	// Also retain archives from this source before its fresh full.
	if err = runner.archiveBinlogs(ctx); err != nil {
		t.Fatal(err)
	}
	// Keep a real transaction before the snapshot position in the same file
	// that will also contain the post-full replay transactions.
	execute("CREATE TABLE fresh_baseline.pre_full_marker(id INT PRIMARY KEY)")
	var status myspec.StatusResponse
	call("status", struct{}{}, &status)
	sourceUUIDs[status.ServerUUID] = true
	req := myspec.BackupRequest{Receipt: "fresh-full", Type: "full", Repository: repo, ObjectPrefix: repo.Path + "/fresh-full", PhysicalIdentity: status.PhysicalIdentity}
	var receipt myspec.BackupResponse
	call("backup", req, &receipt)
	nativeGroupPoll(t, 2*time.Minute, "fresh full receipt", func() bool {
		call("backup-info", map[string]string{"receipt": req.Receipt}, &receipt)
		if receipt.State == "failed" {
			t.Fatalf("fresh full failed: %+v", receipt)
		}
		return receipt.State == "done"
	})
	if receipt.Result == nil {
		t.Fatal("fresh full result missing")
	}
	full := *receipt.Result
	if full.SourceUUID != status.ServerUUID || full.SourceUUID == oldUUID || full.BinlogFile == "" || full.BinlogPosition <= 4 || full.PhysicalIdentity != status.PhysicalIdentity || full.PhysicalIdentity == "" {
		t.Fatalf("fresh full provenance missing: %+v source=%+v", full, status)
	}
	base, err := parseRecoveryGTIDs(full.GTIDSet)
	if err != nil {
		t.Fatal(err)
	}
	oldSet, err := parseRecoveryGTIDs(oldGTID)
	if err != nil || base.contains(oldSet) {
		t.Fatalf("old history unexpectedly present in fresh full: old=%s full=%s err=%v", oldGTID, full.GTIDSet, err)
	}
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second).Add(30 * time.Millisecond)))
	execute("INSERT INTO fresh_baseline.rows_test VALUES(2)")
	var targetGTID, latestGTID string
	if err = db.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&targetGTID); err != nil {
		t.Fatal(err)
	}
	execute("INSERT INTO fresh_baseline.rows_test VALUES(3)")
	if err = db.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&latestGTID); err != nil {
		t.Fatal(err)
	}
	if err = runner.archiveBinlogs(ctx); err != nil {
		t.Fatal(err)
	}
	targetSet, err := parseRecoveryGTIDs(targetGTID)
	if err != nil {
		t.Fatal(err)
	}
	var targetMicros, afterMicros int64
	for obj := range c.ListObjects(ctx, repo.S3.Bucket, minio.ListObjectsOptions{Prefix: repo.Path + "/binlogs/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		if etag, ok := retained[obj.Key]; ok {
			if obj.ETag != etag {
				t.Fatalf("prebaseline archive overwritten: %s", obj.Key)
			}
			delete(retained, obj.Key)
		}
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		o, err := c.GetObject(ctx, repo.S3.Bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var sidecar binlogSidecar
		err = json.NewDecoder(o).Decode(&sidecar)
		o.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, tx := range sidecar.Transactions {
			g, err := parseRecoveryGTIDs(tx.GTID)
			if err != nil {
				t.Fatal(err)
			}
			if base.contains(g) || oldSet.contains(g) {
				continue
			}
			if targetSet.contains(g) {
				if tx.OriginalCommitTimestamp > targetMicros {
					targetMicros = tx.OriginalCommitTimestamp
				}
			} else if tx.OriginalCommitTimestamp > afterMicros {
				afterMicros = tx.OriginalCommitTimestamp
			}
		}
	}
	if len(retained) != 0 || targetMicros == 0 || afterMicros <= targetMicros || targetMicros/1000000 != afterMicros/1000000 {
		t.Fatalf("retention/same-second proof failed: retainedMissing=%v target=%d after=%d", retained, targetMicros, afterMicros)
	}
	assertSnapshotPlan := func(runner *Runner, receipt string) {
		t.Helper()
		_, paths := runner.state()
		name, err := receiptPath(paths, "restore", receipt)
		if err != nil {
			t.Fatal(err)
		}
		saved, err := loadRestoreReceipt(name)
		if err != nil {
			t.Fatal(err)
		}
		if saved.Replay == nil || len(saved.Replay.Logs.Files) == 0 || saved.Replay.Logs.Files[0].ObjectKey != archiveObject(repo, full.SourceUUID, full.BinlogFile) || saved.Replay.Logs.StartPosition != full.BinlogPosition {
			t.Fatalf("frozen plan does not anchor actual snapshot file/position: %+v", saved.Replay)
		}
	}
	// A fully verified frozen plan must be available while the original
	// source is still serving the same SQL session, before any STOP or swap.
	session, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var beforeID, afterID uint64
	var beforeUUID, afterUUID, beforeGTID, afterGTID string
	if err = session.QueryRowContext(ctx, "SELECT CONNECTION_ID(),@@server_uuid,@@gtid_executed").Scan(&beforeID, &beforeUUID, &beforeGTID); err != nil {
		t.Fatal(err)
	}
	preflight := myspec.RestoreRequest{Receipt: "source-preflight", Restore: myspec.Restore{Source: repo, ObjectPaths: []string{full.ObjectPrefix}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: full.ObjectPrefix, Digest: full.ManifestDigest, Size: full.ManifestSize}}, TargetGTID: latestGTID}}
	if group {
		preflight.Restore.NewGroupName = "9347c031-40ab-4a90-b950-ec3525ae4321"
	}
	prepared := nativePreflightRestore(t, call, &preflight)
	assertSnapshotPlan(runner, preflight.Receipt)
	if err = session.QueryRowContext(ctx, "SELECT CONNECTION_ID(),@@server_uuid,@@gtid_executed").Scan(&afterID, &afterUUID, &afterGTID); err != nil || beforeID != afterID || beforeUUID != afterUUID || beforeGTID != afterGTID {
		t.Fatalf("preflight stopped or mutated serving source: %v", err)
	}
	if confirmed, err := recheck(preflight); err != nil || confirmed.State != "prepared" || confirmed.PlanDigest != prepared.PlanDigest {
		t.Fatalf("untampered pinned plan failed pre-STOP recheck: %+v err=%v", confirmed, err)
	}
	if prepared.PlanDigest != preflight.PlanDigest {
		t.Fatal("prepared digest was not pinned by the restore request")
	}
	_, sourcePaths := runner.state()
	preparedName, err := receiptPath(sourcePaths, "restore", preflight.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	physical := filepath.Join(preparedName+".staging", "0", "mysql.ibd")
	file, err := os.OpenFile(physical, os.O_RDWR, 0)
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
		t.Fatalf("staging tamper setup: write=%v close=%v", err, closeErr)
	}
	if rejected, err := recheck(preflight); err == nil && rejected.State != "failed" {
		t.Fatalf("tampered prepared physical bytes passed pre-STOP recheck: %+v", rejected)
	}
	if err = session.QueryRowContext(ctx, "SELECT CONNECTION_ID(),@@server_uuid,@@gtid_executed").Scan(&afterID, &afterUUID, &afterGTID); err != nil || beforeID != afterID || beforeUUID != afterUUID || beforeGTID != afterGTID {
		t.Fatalf("failed integrity preflight stopped or changed serving source: %v", err)
	}
	t.Log("prepared physical tamper rejected before STOP; original SQL session and GTIDs unchanged")
	closeSource()
	for _, tc := range []struct {
		name, gtid, stamp, expected, rows string
	}{
		{"latest", latestGTID, "", latestGTID, "1,2,3"},
		{"gtid", targetGTID, "", targetGTID, "1,2"},
		{"time", "", time.UnixMicro(targetMicros).UTC().Format(time.RFC3339Nano), targetGTID, "1,2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targetSpec := s
			targetSpec.Backup = nil
			targetSpec.Agent.Password = "fresh-target-agent-password"
			targetSpec.Superuser.Password = "fresh-target-root-password"
			a := startBackupAgent(t, targetSpec, true)
			var out myspec.RestoreStatus
			request := myspec.RestoreRequest{Receipt: tc.name, Restore: myspec.Restore{Source: repo, ObjectPaths: []string{full.ObjectPrefix}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: full.ObjectPrefix, Digest: full.ManifestDigest, Size: full.ManifestSize}}, TargetGTID: tc.gtid, TargetTime: tc.stamp}}
			if group {
				request.Restore.NewGroupName = "9347c031-40ab-4a90-b950-ec3525ae4321"
			}
			nativePreflightRestore(t, func(action string, req, out any) {
				if code := a.request("/v1/mysql/"+action, req, out); code != http.StatusOK {
					t.Fatalf("%s HTTP %d", action, code)
				}
			}, &request)
			if code := a.request("/v1/mysql/restore", request, &out); code != http.StatusOK {
				t.Fatalf("restore HTTP %d", code)
			}
			nativeGroupPoll(t, 3*time.Minute, "fresh full restore receipt", func() bool {
				a.request("/v1/mysql/restore-status", map[string]string{"receipt": tc.name}, &out)
				if out.State == "failed" {
					log, _ := os.ReadFile(filepath.Join(dataPaths(a.dir).root, "restore-error.log"))
					t.Fatalf("restore failed: %+v log=%s", out, log)
				}
				return out.State == "done"
			})
			assertSnapshotPlan(a.runner, tc.name)
			want, err := parseRecoveryGTIDs(tc.expected)
			if err != nil {
				t.Fatal(err)
			}
			got, err := parseRecoveryGTIDs(out.GTIDExecuted)
			if err != nil || !got.contains(want) || !want.contains(got) || out.Stage != "done" || !myspec.ValidUUID(out.ServerUUID) || sourceUUIDs[out.ServerUUID] {
				t.Fatalf("restore identity/GTIDs: %+v want=%s err=%v", out, tc.expected, err)
			}
			if code := a.request("/v1/processes/mysqld/start", struct{}{}, nil); code != http.StatusOK {
				t.Fatalf("restored start HTTP %d", code)
			}
			var restored *sql.DB
			nativeGroupPoll(t, 45*time.Second, "fresh restored server", func() bool {
				var err error
				restored, err = a.runner.LocalAccessDB(ctx)
				return err == nil
			})
			var rows, actual string
			var stale int
			if err := restored.QueryRowContext(ctx, "SELECT GROUP_CONCAT(id ORDER BY id) FROM fresh_baseline.rows_test").Scan(&rows); err != nil || rows != tc.rows {
				t.Fatalf("restored rows=%s want=%s err=%v", rows, tc.rows, err)
			}
			if err := restored.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='stale_prebaseline'").Scan(&stale); err != nil || stale != 0 {
				t.Fatalf("prebaseline transaction replayed: schemas=%d err=%v", stale, err)
			}
			if err := restored.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&actual); err != nil {
				t.Fatal(err)
			}
			got, err = parseRecoveryGTIDs(actual)
			if err != nil || !got.contains(want) || !want.contains(got) {
				t.Fatalf("live GTIDs=%s want=%s err=%v", actual, tc.expected, err)
			}
			a.close()
		})
	}
}

func nativePreflightRestore(t *testing.T, call func(string, any, any), request *myspec.RestoreRequest) myspec.RestoreStatus {
	t.Helper()
	var prepared myspec.RestoreStatus
	call("restore-preflight", *request, &prepared)
	nativeGroupPoll(t, 4*time.Minute, "verified physical/replay preflight", func() bool {
		call("restore-status", map[string]string{"receipt": request.Receipt}, &prepared)
		if prepared.State == "failed" {
			t.Fatalf("preflight failed before source stop: %+v", prepared)
		}
		return prepared.State == "prepared" && prepared.Stage == "ready"
	})
	if prepared.PlanDigest == "" {
		t.Fatal("prepared physical/replay plan has no digest")
	}
	request.PlanDigest = prepared.PlanDigest
	return prepared
}
