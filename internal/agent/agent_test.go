// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"gopkg.in/yaml.v3"
)

func memberConfig(t *testing.T) agentapi.MemberConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "member"}, DNSNames: []string{"member"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert := agentapi.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))}
	return agentapi.MemberConfig{Cluster: "demo", Member: "member", Role: agentapi.RoleReplicaSet, ReplSetName: "rs0", Keyfile: strings.Repeat("a", 40), TLS: agentapi.TLSConfig{Mode: "disabled"}, AgentTLS: cert, SystemUsers: map[string]agentapi.Credentials{"backup": {Username: "backup", Password: "p@ss:word"}}, DesiredProcesses: []string{"mongod"}, Maintenance: true}
}
func TestRenderConfig(t *testing.T) {
	c := memberConfig(t)
	c.WiredTigerCacheSizeGB = 0.5
	c.SlowOpThresholdMS = 250
	c.EngineConfig = "storage:\n  journal:\n    commitIntervalMs: 100\nsetParameter:\n  ttlMonitorSleepSecs: 30\n"
	data, err := RenderConfig(c, "/data")
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	netConfig := cfg["net"].(map[string]any)
	if netConfig["bindIpAll"] != true || netConfig["ipv6"] != true {
		t.Fatalf("MongoDB must listen on both IPv4 and IPv6: %v", netConfig)
	}
	storage := cfg["storage"].(map[string]any)
	if storage["dbPath"] != "/data/db" {
		t.Fatal(storage)
	}
	if !strings.Contains(string(data), "cacheSizeGB: 0.5") || !strings.Contains(string(data), "slowOpThresholdMs: 250") {
		t.Fatal(string(data))
	}
	for _, override := range []string{"net:\n  port: 28017", "net:\n  ipv6: false", "security:\n  keyFile: /evil", "storage: null", "net:\n  tls:\n    mode: disabled", "replication:\n  replSetName: wrong", "processManagement:\n  fork: true", "systemLog:\n  path: /tmp/mongo.log", "operationProfiling:\n  slowOpThresholdMs: 5", "storage:\n  wiredTiger:\n    engineConfig:\n      cacheSizeGB: 3", "net.port: 1"} {
		c.EngineConfig = override
		if _, err = RenderConfig(c, "/data"); err == nil {
			t.Errorf("accepted managed override %s", override)
		}
	}
	c.EngineConfig = ""
	c.Role = agentapi.RoleConfigServer
	data, err = RenderConfig(c, "/data")
	if err != nil || !strings.Contains(string(data), "clusterRole: configsvr") {
		t.Fatal(string(data), err)
	}
	c.Role = agentapi.RoleReplicaSet
	c.ShardServer = true
	data, err = RenderConfig(c, "/data")
	if err != nil || !strings.Contains(string(data), "clusterRole: shardsvr") {
		t.Fatal(string(data), err)
	}
	c.Role = agentapi.RoleRouter
	c.ConfigDB = "cfg/c0:27017,c1:27017"
	data, err = RenderConfig(c, "/data")
	if err != nil || !strings.Contains(string(data), "configDB: cfg/") || strings.Contains(string(data), "dbPath") {
		t.Fatal(string(data), err)
	}
	c.TLS = agentapi.TLSConfig{Mode: "requireTLS"}
	data, err = RenderConfig(c, "/data")
	if err != nil || !strings.Contains(string(data), "certificateKeyFile: /data/karakoram/mongodb.pem") {
		t.Fatal(string(data), err)
	}
}
func TestAtomicFilesAndEffectiveChanges(t *testing.T) {
	dir := t.TempDir()
	c := memberConfig(t)
	a := &nodeAgent{o: Options{Cluster: c.Cluster, Member: c.Member, DataDir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, sup: newSupervisor(io.Discard)}
	changes, err := a.apply(context.Background(), c)
	if err != nil || len(changes) != 2 {
		t.Fatal(changes, err)
	}
	c.ConfigVersion++
	changes, err = a.apply(context.Background(), c)
	if err != nil || len(changes) != 0 {
		t.Fatal(changes, err)
	}
	for path, mode := range map[string]os.FileMode{"keyfile": 0400, "mongod.conf": 0600, "config.json": 0600} {
		stat, err := os.Stat(filepath.Join(dir, "karakoram", path))
		if err != nil || stat.Mode().Perm() != mode {
			t.Fatal(path, stat, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cached agentapi.MemberConfig
	if err = json.Unmarshal(data, &cached); err != nil || cached.ConfigVersion != c.ConfigVersion {
		t.Fatal(err, cached)
	}
}
func buildFake(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	source := `package main
import("fmt";"os";"os/signal";"syscall";"time")
func main(){mode:="";if len(os.Args)>1{mode=os.Args[1]};fmt.Println("ready");if mode=="crash"{time.Sleep(15*time.Millisecond);os.Exit(7)};if mode=="clean"{time.Sleep(15*time.Millisecond);return};if os.Getenv("PBM_MONGODB_URI")!=""{fmt.Println("Started logical restore.\nWaiting to finish........\n{\"name\":\"2024-01-05T11:12:59.612594853Z\"}");return};ch:=make(chan os.Signal,1);signal.Notify(ch,syscall.SIGTERM);<-ch;fmt.Println("terminated")}
`
	path := filepath.Join(dir, "fake.go")
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "fake")
	if output, err := exec.Command("go", "build", "-o", binary, path).CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	return binary
}
func awaitState(t *testing.T, s *supervisor, condition func(agentapi.ProcessStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		states, _ := s.snapshot()
		if condition(states["mongod"]) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	states, _ := s.snapshot()
	t.Fatal("process state deadline", states)
}
func TestSupervisor(t *testing.T) {
	binary := buildFake(t)
	t.Run("shutdown_and_logs", func(t *testing.T) {
		s := newSupervisor(io.Discard)
		s.configure("mongod", processSpec{binary: binary})
		if err := s.action(context.Background(), "mongod", "start"); err != nil {
			t.Fatal(err)
		}
		awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.State == "running" })
		time.Sleep(50 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		lines := s.logTail("mongod", 100)
		if len(lines) < 2 || lines[len(lines)-1].Message != "terminated" {
			t.Fatal(lines)
		}
	})
	for _, mode := range []string{"crash", "clean"} {
		t.Run(mode, func(t *testing.T) {
			s := newSupervisor(io.Discard)
			s.backoff = 20 * time.Millisecond
			s.configure("mongod", processSpec{binary: binary, args: []string{mode}})
			if err := s.action(context.Background(), "mongod", "start"); err != nil {
				t.Fatal(err)
			}
			awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.Restarts > 0 })
			s.setMaintenance(true)
			awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID == 0 })
			before, _ := s.snapshot()
			time.Sleep(150 * time.Millisecond)
			after, _ := s.snapshot()
			if after["mongod"].Restarts != before["mongod"].Restarts || after["mongod"].PID != 0 {
				t.Fatal(after)
			}
			if err := s.action(context.Background(), "mongod", "start"); err != nil {
				t.Fatal(err)
			}
			awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID == 0 })
			if err := s.shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPBMArgumentsAndJSON(t *testing.T) {
	for _, r := range []agentapi.PBMRequest{{Command: "status"}, {Command: "list", Args: []string{"--restore"}}, {Command: "backup", Args: []string{"--type=physical"}}, {Command: "restore", Args: []string{"snapshot", "--yes", "--wait"}}, {Command: "config", Args: []string{"--set", "pitr.enabled=true", "--force-resync"}}, {Command: "delete-pitr", Args: []string{"--all", "--yes"}}} {
		args, err := pbmArgs(r, "")
		if err != nil || args[len(args)-1] != "--out=json" {
			t.Fatal(args, err)
		}
		if r.Command == "backup" && strings.Contains(strings.Join(args, " "), "--wait") {
			t.Fatal("injected wait")
		}
	}
	for _, r := range []agentapi.PBMRequest{{Command: "version"}, {Command: "backup", Args: []string{"--uri=mongodb://evil"}}, {Command: "status", Args: []string{"--out=yaml"}}, {Command: "config", Args: []string{"--file=/etc/passwd"}}, {Command: "restore", Args: []string{"--config", "/tmp/evil"}}} {
		if _, err := pbmArgs(r, ""); err == nil {
			t.Fatal("accepted", r)
		}
	}
	args, err := pbmArgs(agentapi.PBMRequest{Command: "describe-restore", Args: []string{"restore", "--config"}}, "/secure/temp")
	if err != nil || !strings.Contains(strings.Join(args, " "), "--config /secure/temp") {
		t.Fatal(args, err)
	}
	samples := []string{`{"snapshots":[{"name":"2024-01-05T11:10:54Z","status":"done","restoreTo":1704453059}],"pitr":{"on":false,"ranges":[]}}`, "Started logical restore.\nWaiting to finish........\n{\"name\":\"2024-01-05T11:12:59.612594853Z\"}", `{"name":"restore","replsets":[{"name":"rs0","nodes":[{"status":"done"}]}]}`}
	for _, sample := range samples {
		parsed := lastJSONObject([]byte(sample))
		if !json.Valid(parsed) {
			t.Fatal(string(parsed))
		}
		if strings.HasPrefix(sample, "{") && string(parsed) != sample {
			t.Fatal("selected nested object", string(parsed))
		}
	}
	uri, err := backupURI(memberConfig(t), "/data")
	if err != nil || strings.Contains(uri, "directConnection") || !strings.Contains(uri, "p%40ss%3Aword") {
		t.Fatal(uri, err)
	}
	tlsConfig := memberConfig(t)
	tlsConfig.TLS.Mode = "requireTLS"
	uri, err = backupURI(tlsConfig, "/data")
	if err != nil || !strings.Contains(uri, "tls=true") || !strings.Contains(uri, "tlsCAFile=%2Fdata%2Fkarakoram%2Fca.pem") || strings.Contains(uri, "directConnection") {
		t.Fatal(uri, err)
	}
}
func TestHTTPAuthAndMetricsRegistry(t *testing.T) {
	a := &nodeAgent{o: Options{Token: "secret", DataDir: t.TempDir()}, sup: newSupervisor(io.Discard), started: time.Now()}
	for _, token := range []string{"", "Bearer wrong"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
		r.Header.Set("Authorization", token)
		w := httptest.NewRecorder()
		a.handler().ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatal(w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "karakoram_mongodb_up 0") {
		t.Fatal(w.Code, w.Body.String())
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(scrapeCollector{values: []metricValue{{name: "karakoram_mongodb_documents_total", counter: true, labels: map[string]string{"operation": "inserted"}}, {name: "karakoram_mongodb_documents_total", counter: true, labels: map[string]string{"operation": "updated"}}}})
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
}
func TestOptionsFromEnv(t *testing.T) {
	for name, value := range map[string]string{"URL": "https://cp:9443", "INTERNAL_CA": "pem", "AGENT_TOKEN": "token", "CLUSTER": "demo", "MEMBER": "m0", "DATA_DIR": "/data"} {
		t.Setenv("KARAKORAM_"+name, value)
	}
	o, err := OptionsFromEnv()
	if err != nil || o.Listen != ":9443" {
		t.Fatal(o, err)
	}
	t.Setenv("KARAKORAM_MEMBER", "")
	if _, err := OptionsFromEnv(); err == nil {
		t.Fatal("accepted missing member")
	}
}

func TestPBMDurableReceipt(t *testing.T) {
	binary := buildFake(t)
	dir := t.TempDir()
	pbm := filepath.Join(filepath.Dir(binary), "pbm")
	if err := os.Symlink(binary, pbm); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Dir(binary)+string(os.PathListSeparator)+os.Getenv("PATH"))
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "karakoram"), 0700); err != nil {
		t.Fatal(err)
	}
	a := &nodeAgent{o: Options{DataDir: dataDir}, c: memberConfig(t)}
	request := agentapi.PBMRequest{Command: "restore", Args: []string{"snapshot", "--yes"}, RequestID: "operation-123"}
	first, err := a.pbm(context.Background(), request)
	if err != nil || first.ExitCode != 0 || !strings.Contains(string(first.Output), "2024-01-05T11:12:59") {
		t.Fatal(first, err)
	}
	// Losing the HTTP response or restarting the agent cannot launch a duplicate.
	if err := os.Remove(pbm); err != nil {
		t.Fatal(err)
	}
	restarted := &nodeAgent{o: a.o, c: a.c}
	second, err := restarted.pbm(context.Background(), request)
	if err != nil || string(second.Output) != string(first.Output) {
		t.Fatal(second, err)
	}
	request.Args = []string{"different"}
	if _, err = restarted.pbm(context.Background(), request); err == nil {
		t.Fatal("accepted reused request ID")
	}
}

func TestMetricBSONDocuments(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"connections": bson.M{"current": int32(3)}, "wiredTiger": bson.M{"cache": bson.M{"bytes currently in the cache": int64(1024)}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded bson.M
	if err = bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if number(field(decoded, "connections", "current")) != 3 || number(field(decoded, "wiredTiger", "cache", "bytes currently in the cache")) != 1024 {
		t.Fatal(decoded)
	}
}

func TestDurableMaintenanceSurvivesPullAndRestart(t *testing.T) {
	dir := t.TempDir()
	config := memberConfig(t)
	config.Maintenance = false
	a := &nodeAgent{o: Options{Cluster: config.Cluster, Member: config.Member, DataDir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, sup: newSupervisor(io.Discard), c: config}
	if err := a.maintenance(true); err != nil {
		t.Fatal(err)
	}
	if _, err := a.apply(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, maintenance := a.sup.snapshot(); !maintenance {
		t.Fatal("pull released explicit maintenance")
	}
	controls, err := loadControls(dir)
	if err != nil || !controls.Maintenance {
		t.Fatal(controls, err)
	}
	restarted := &nodeAgent{o: a.o, sup: newSupervisor(io.Discard), controls: controls}
	if _, err := restarted.apply(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	states, maintenance := restarted.sup.snapshot()
	if !maintenance || states["mongod"].PID != 0 {
		t.Fatal("restart released maintenance", states, maintenance)
	}
	if err := restarted.maintenance(false); err != nil {
		t.Fatal(err)
	}
	controls, err = loadControls(dir)
	if err != nil || controls.Maintenance {
		t.Fatal("explicit release not persisted", controls, err)
	}
}

func TestDurableProcessStopSurvivesPullAndRestart(t *testing.T) {
	config := memberConfig(t)
	config.Role = agentapi.RoleRouter
	config.ConfigDB = "cfg/config:27017"
	config.Maintenance = false
	config.DesiredProcesses = []string{"mongos"}
	a := &nodeAgent{o: Options{Cluster: config.Cluster, Member: config.Member, DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, sup: newSupervisor(io.Discard), c: config}
	a.sup.configure("mongos", processSpec{binary: "mongos"})
	if err := a.processAction(context.Background(), "mongos", "stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.apply(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	controls, err := loadControls(a.o.DataDir)
	if err != nil || !controls.Stopped["mongos"] {
		t.Fatal(controls, err)
	}
	restarted := &nodeAgent{o: a.o, sup: newSupervisor(io.Discard), controls: controls}
	if _, err := restarted.apply(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	states, _ := restarted.sup.snapshot()
	if states["mongos"].State != "stopped" {
		t.Fatal("pull/restart lost stop hold", states)
	}
	binary := buildFake(t)
	restarted.sup.configure("mongos", processSpec{binary: binary})
	if err := restarted.processAction(context.Background(), "mongos", "start"); err != nil {
		t.Fatal(err)
	}
	defer restarted.sup.shutdown(context.Background())
	controls, err = loadControls(a.o.DataDir)
	if err != nil || controls.Stopped["mongos"] {
		t.Fatal("start did not release hold", controls, err)
	}
	states, _ = restarted.sup.snapshot()
	if states["mongos"].State != "running" {
		t.Fatal(states)
	}
}

func TestExplicitEmptyDesiredProcesses(t *testing.T) {
	c := memberConfig(t)
	c.Maintenance = false
	c.DesiredProcesses = []string{}
	a := &nodeAgent{o: Options{Cluster: c.Cluster, Member: c.Member, DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, sup: newSupervisor(io.Discard)}
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	states, _ := a.sup.snapshot()
	if states["mongod"].State != "stopped" {
		t.Fatal("empty desired list started mongod", states)
	}
}
