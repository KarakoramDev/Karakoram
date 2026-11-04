// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package openbao

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	openbaoagent "github.com/KarakoramDev/Karakoram/internal/agent/openbao"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type nativeCPLog struct {
	mu sync.Mutex
	bytes.Buffer
}

func (l *nativeCPLog) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Buffer.Write(b)
}
func (l *nativeCPLog) snapshot() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Clone(l.Bytes())
}

type nativeCPMember struct {
	name, dir, token string
	ports            [5]int
	cancel           context.CancelFunc
	done             chan error
	logs             nativeCPLog
}
type nativeCP struct {
	*fakeInfra
	t                           *testing.T
	p                           *Provider
	rc                          *engine.Context
	cp                          *httptest.Server
	binary, plugins, internalCA string
	mu                          sync.Mutex
	published                   *domain.Cluster
	nodes                       map[string]*nativeCPMember
	generations                 map[string]int64
}

// OS ephemeral allocation can reuse a just-closed port before its eventual
// native listener starts. Never assign the same number twice in this process.
var nativePorts = struct {
	sync.Mutex
	used map[int]bool
}{used: map[int]bool{}}

func nativePort(t *testing.T) int {
	t.Helper()
	nativePorts.Lock()
	defer nativePorts.Unlock()
	for {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		// Keep rejected allocations bound until a fresh number is selected.
		defer l.Close()
		port := l.Addr().(*net.TCPAddr).Port
		if !nativePorts.used[port] {
			nativePorts.used[port] = true
			return port
		}
	}
}
func (h *nativeCP) node(name string) *nativeCPMember {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.nodes[name]
	if n == nil {
		n = &nativeCPMember{name: name}
		for i := range n.ports {
			n.ports[i] = nativePort(h.t)
		}
		h.nodes[name] = n
	}
	return n
}
func (h *nativeCP) publish(t *testing.T) {
	t.Helper()
	if h.generations == nil {
		h.generations = map[string]int64{}
	}
	for _, m := range state(t, h.rc).Members {
		if old := h.generations[m.Name]; old != 0 && old != m.ProcessGeneration {
			h.log.add("generation:" + m.Name)
		}
		h.generations[m.Name] = m.ProcessGeneration
	}
	raw, e := json.Marshal(h.rc.Cluster)
	if e != nil {
		t.Fatal(e)
	}
	var c domain.Cluster
	if e = json.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	c.Internal = bytes.Clone(h.rc.Cluster.Internal)
	h.mu.Lock()
	h.published = &c
	h.mu.Unlock()
	if e = h.p.options.Store.(*store.Store).UpdateClusterState(t.Context(), h.rc.Cluster); e != nil {
		t.Fatal(e)
	}
}
func (h *nativeCP) config(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/internal/agent/v1/config" {
		http.NotFound(w, r)
		return
	}
	token, e := h.p.options.Store.(*store.Store).FindAgentTokenByHash(r.Context(), secrets.Hash(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if e != nil || token == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	h.mu.Lock()
	c := h.published
	h.mu.Unlock()
	rc := *h.rc
	rc.Cluster = c
	cfg, e := h.p.MemberConfig(r.Context(), &rc, token.Member)
	if e != nil {
		http.Error(w, "configuration unavailable", http.StatusServiceUnavailable)
		return
	}
	var s baospec.MemberSpec
	if e = json.Unmarshal(cfg.EngineSpec, &s); e != nil {
		http.Error(w, "invalid spec", 500)
		return
	}
	n := h.node(token.Member)
	s.BinaryPath = h.binary
	s.ListenAddress = "127.0.0.1"
	s.APIPort = n.ports[0]
	s.ClusterPort = n.ports[1]
	s.MetricsPort = n.ports[2]
	s.RecoveryPort = n.ports[4]
	s.APIAddr = fmt.Sprintf("https://127.0.0.1:%d", n.ports[0])
	s.ClusterAddr = fmt.Sprintf("https://127.0.0.1:%d", n.ports[1])
	s.PluginDirectory = h.plugins
	for i, j := range s.Storage.RetryJoin {
		u, e := url.Parse(j.LeaderAPIAddr)
		if e != nil {
			http.Error(w, "invalid join", 500)
			return
		}
		other := h.node(u.Hostname())
		s.Storage.RetryJoin[i].LeaderAPIAddr = fmt.Sprintf("https://127.0.0.1:%d", other.ports[0])
		s.Storage.RetryJoin[i].TLSServerName = other.name
	}
	cfg.EngineSpec, _ = json.Marshal(s)
	_ = json.NewEncoder(w).Encode(cfg)
}
func (h *nativeCP) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	name, port, e := net.SplitHostPort(address)
	if e != nil {
		return nil, e
	}
	h.mu.Lock()
	n := h.nodes[name]
	h.mu.Unlock()
	if n != nil {
		if port == "8200" {
			port = strconv.Itoa(n.ports[0])
		}
		if port == strconv.Itoa(baospec.RecoveryPort) {
			port = strconv.Itoa(n.ports[4])
		}
		address = net.JoinHostPort("127.0.0.1", port)
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
func (h *nativeCP) Dial(ctx context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	n := h.node(name)
	token, e := h.rc.Secrets.GetAgentToken(ctx, h.rc.Cluster.Ref(), name)
	if e != nil {
		return nil, e
	}
	ca, e := secret(ctx, h.rc, "tls/ca.crt")
	if e != nil {
		return nil, e
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: fmt.Sprintf("https://%s:%d", name, n.ports[3]), Token: token.Token, CAPEM: ca, DialContext: h.DialContext, Timeout: 3 * time.Second, EngineTimeout: 15 * time.Second})
}
func (h *nativeCP) start(ctx context.Context, name string) error {
	n := h.node(name)
	if n.cancel != nil {
		return nil
	}
	service := h.services[name]
	if service == nil {
		return fmt.Errorf("runtime %s absent", name)
	}
	n.token = service.Ownership["KARAKORAM_AGENT_TOKEN"]
	if n.token == "" {
		return fmt.Errorf("runtime token absent")
	}
	if n.dir == "" {
		dir, e := os.MkdirTemp("../../../", ".openbao-cp-member-")
		if e != nil {
			return e
		}
		n.dir, e = filepath.Abs(dir)
		if e != nil {
			return e
		}
	}
	h.publish(h.t)
	runctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan error, 1)
	go func() {
		n.done <- agent.Run(runctx, agent.Options{ControlPlaneURL: h.cp.URL, InternalCAPEM: h.internalCA, Token: n.token, Cluster: h.rc.Cluster.Name, Member: name, DataDir: n.dir, Listen: fmt.Sprintf("127.0.0.1:%d", n.ports[3]), Engine: baospec.Engine, Logger: slog.New(slog.NewTextHandler(&n.logs, nil))}, openbaoagent.New())
	}()
	return nil
}
func (h *nativeCP) stop(name string) error {
	n := h.node(name)
	if n.cancel == nil {
		return nil
	}
	n.cancel()
	n.cancel = nil
	select {
	case e := <-n.done:
		return e
	case <-time.After(40 * time.Second):
		return fmt.Errorf("agent %s shutdown deadline", name)
	}
}
func (h *nativeCP) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, e := h.fakeInfra.Deploy(ctx, name, d)
	if e != nil {
		return op, e
	}
	return op, h.start(ctx, name)
}
func (h *nativeCP) Start(ctx context.Context, name string) (infra.Op, error) {
	op, e := h.op("start", name)
	if e == nil {
		e = h.start(ctx, name)
	}
	return op, e
}
func (h *nativeCP) Stop(_ context.Context, name string) (infra.Op, error) {
	op, e := h.op("stop", name)
	if e == nil {
		e = h.stop(name)
	}
	return op, e
}
func (h *nativeCP) Restart(ctx context.Context, name string) (infra.Op, error) {
	if e := h.stop(name); e != nil {
		return infra.Op{}, e
	}
	return h.Start(ctx, name)
}
func (h *nativeCP) DeleteService(ctx context.Context, name string) (infra.Op, error) {
	for member, n := range h.nodes {
		if name == member {
			if e := h.stop(member); e != nil {
				return infra.Op{}, e
			}
		}
		if name == member+"v" {
			if e := os.RemoveAll(n.dir); e != nil {
				return infra.Op{}, e
			}
		}
	}
	return h.fakeInfra.DeleteService(ctx, name)
}
func (h *nativeCP) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	s := h.services[name]
	if s == nil {
		return infra.Op{}, nil
	}
	if s.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	return h.DeleteService(ctx, name)
}

func nativePlugin(t *testing.T, binary string) string {
	t.Helper()
	dir, e := os.MkdirTemp("../../../", ".openbao-cp-plugins-")
	if e != nil {
		t.Fatal(e)
	}
	dir, _ = filepath.Abs(dir)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if path := os.Getenv("KARAKORAM_OPENBAO_MONGODB_PLUGIN"); path != "" {
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, memberPlugins(Options{})[0].Command), b, 0755); e != nil {
			t.Fatal(e)
		}
		return dir
	}
	f, e := os.Open(filepath.Join(filepath.Dir(binary), "openbao-plugin-database-mongodb_linux_amd64_v1.tar.gz"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if hdr.Typeflag != tar.TypeReg || !strings.Contains(filepath.Base(hdr.Name), "openbao-plugin-database-mongodb") {
			continue
		}
		b, e := io.ReadAll(tr)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, memberPlugins(Options{})[0].Command), b, 0755); e != nil {
			t.Fatal(e)
		}
		return dir
	}
	t.Fatal("MongoDB plugin missing from archive")
	return ""
}
func newNativeCP(t *testing.T, binary string) *nativeCP {
	p, rc, f, _ := setup(t)
	ca, e := pki.NewCA("native-control-plane", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	crt, key, e := ca.Issue(pki.CertRequest{CommonName: "localhost", DNSNames: []string{"localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true, Validity: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	pair, e := tls.X509KeyPair(crt, key)
	if e != nil {
		t.Fatal(e)
	}
	h := &nativeCP{fakeInfra: f, t: t, p: p, rc: rc, binary: binary, plugins: nativePlugin(t, binary), internalCA: string(ca.CertPEM), nodes: map[string]*nativeCPMember{}}
	f.agents = nil
	h.cp = httptest.NewUnstartedServer(http.HandlerFunc(h.config))
	h.cp.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	h.cp.StartTLS()
	p.options.ControlPlaneURL = h.cp.URL
	p.options.InternalCAPEM = string(ca.CertPEM)
	rc.Backend = h
	rc.Agents = h
	rc.Dialer = h
	t.Cleanup(func() {
		for name, n := range h.nodes {
			if e := h.stop(name); e != nil {
				t.Error(e)
			}
			_ = os.RemoveAll(n.dir)
		}
		h.cp.Close()
	})
	return h
}
func (h *nativeCP) loop(t *testing.T, what string, fn func() (bool, error)) {
	t.Helper()
	end := time.Now().Add(100 * time.Second)
	var last error
	for time.Now().Before(end) {
		h.publish(t)
		done, e := fn()
		h.publish(t)
		if e != nil {
			last = e
		} else if done {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	for name, n := range h.nodes {
		t.Logf("%s logs:\n%s", name, n.logs.snapshot())
	}
	t.Fatalf("%s deadline: %v state=%s", what, last, h.rc.Cluster.Internal)
}
func (h *nativeCP) ready(t *testing.T) {
	t.Helper()
	h.loop(t, "Ready", func() (bool, error) {
		r, e := h.p.Sync(t.Context(), h.rc)
		if e != nil {
			return false, e
		}
		if r.ConvergenceError != nil {
			return false, r.ConvergenceError
		}
		s := state(t, h.rc)
		if s.Phase != "ready" || h.rc.Cluster.Status.ObservedGeneration != h.rc.Cluster.Generation {
			return false, nil
		}
		for _, m := range s.Members {
			if !m.Healthy || !m.Voter || m.Sealed {
				return false, nil
			}
		}
		return true, nil
	})
}
func (h *nativeCP) operation(t *testing.T, kind, payload string) {
	t.Helper()
	op := &domain.Operation{Kind: kind, Payload: json.RawMessage(payload)}
	h.loop(t, kind, func() (bool, error) {
		r, e := h.p.RunOperation(t.Context(), h.rc, op)
		if e == nil {
			op.Checkpoint = r.Checkpoint
		}
		return r.Done, e
	})
}
func (h *nativeCP) root(t *testing.T) *baoapi.Client {
	t.Helper()
	keys, e := h.p.RevealUnsealKeys(t.Context(), h.rc)
	if e != nil {
		t.Fatal(e)
	}
	c, _, e := h.p.activeClient(t.Context(), h.rc, state(t, h.rc), keys.InitialRootToken)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func (h *nativeCP) scan(t *testing.T, values []string) {
	t.Helper()
	for _, n := range h.nodes {
		check := func(b []byte) {
			for _, v := range values {
				if v != "" && bytes.Contains(b, []byte(v)) {
					t.Fatal("plaintext initialization material on member", n.name)
				}
			}
		}
		check(n.logs.snapshot())
		client, err := h.Dial(t.Context(), h.rc.Cluster.Ref(), n.name)
		if err != nil {
			t.Fatal(err)
		}
		logs, err := client.Logs(t.Context(), baospec.Process, 10000)
		client.Close()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(logs)
		if err != nil {
			t.Fatal(err)
		}
		check(raw)
		initializations := 0
		for _, line := range logs.Lines {
			if strings.Contains(line.Message, "security barrier initialized") {
				initializations++
			}
		}
		expected := 0
		if n.name == state(t, h.rc).InitOwner {
			expected = 1
		}
		if initializations != expected {
			t.Fatalf("%s sys/init count: got %d, want %d", n.name, initializations, expected)
		}
		e := filepath.WalkDir(n.dir, func(path string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				return nil
			}
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			check(b)
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
}

func TestNativeControlPlane(t *testing.T) {
	binary := os.Getenv("KARAKORAM_OPENBAO_BIN")
	if binary == "" {
		t.Skip("set KARAKORAM_OPENBAO_BIN to an installed OpenBao binary")
	}
	h := newNativeCP(t, binary)
	scenario := func(name string, fn func(*testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}
	scenario("create", func(t *testing.T) {
		h.ready(t)
		s := state(t, h.rc)
		if len(s.Members) != 3 || s.InitOwner != "demom0" {
			t.Fatal("formation", s)
		}
		keys, e := h.p.RevealUnsealKeys(t.Context(), h.rc)
		if e != nil {
			t.Fatal(e)
		}
		if len(keys.Shares) != 5 || keys.Threshold != 3 || keys.InitialRootToken == "" {
			t.Fatal("invalid reveal metadata")
		}
		if e = h.root(t).Do(t.Context(), "GET", "auth/token/lookup-self", nil, nil); e != nil {
			t.Fatal(e)
		}
		if _, e = secret(t.Context(), h.rc, SecretInitKey); !missing(e) {
			t.Fatal("receipt key not acknowledged", e)
		}
		for _, n := range h.nodes {
			if _, e = os.Stat(filepath.Join(n.dir, "karakoram/openbao/init-receipt.enc")); !os.IsNotExist(e) {
				t.Fatal("receipt not deleted", e)
			}
		}
		st, e := h.p.Status(t.Context(), h.rc)
		if e != nil {
			t.Fatal(e)
		}
		roles := map[string]int{}
		for _, m := range st.Members {
			roles[m.Role]++
		}
		if roles["primary"] != 1 || roles["secondary"] != 2 {
			t.Fatal(roles)
		}
		h.scan(t, append(slices.Clone(keys.Shares), keys.InitialRootToken))
	})
	scenario("child_restart", func(t *testing.T) {
		a, e := h.Dial(t.Context(), h.rc.Cluster.Ref(), "demom1")
		if e != nil {
			t.Fatal(e)
		}
		defer a.Close()
		s, e := a.Status(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		pid := s.Processes[baospec.Process].PID
		if pid <= 0 {
			t.Fatal("missing child pid")
		}
		if e = syscall.Kill(pid, syscall.SIGKILL); e != nil {
			t.Fatal(e)
		}
		time.Sleep(500 * time.Millisecond)
		h.ready(t)
		s, e = a.Status(t.Context())
		if e != nil || s.Processes[baospec.Process].PID == pid {
			t.Fatal("child not replaced", e)
		}
	})
	scenario("rolling_restart", func(t *testing.T) {
		before := state(t, h.rc)
		active := ""
		for _, m := range before.Members {
			if m.Active {
				active = m.Name
			}
		}
		h.log.clear()
		h.operation(t, domain.OperationRestart, `{}`)
		h.ready(t)
		for _, m := range state(t, h.rc).Members {
			if m.ProcessGeneration != before.find(m.Name).ProcessGeneration+1 {
				t.Fatal("member not rolled", m.Name)
			}
		}
		var order []string
		for _, call := range h.log.snapshot() {
			if strings.HasPrefix(call, "generation:") {
				order = append(order, strings.TrimPrefix(call, "generation:"))
			}
		}
		if len(order) != 3 || order[2] != active {
			t.Fatalf("standby-first/active-last restart order: %v (active %s)", order, active)
		}
	})
	scenario("scale", func(t *testing.T) {
		h.log.clear()
		for _, count := range []int{5, 3} {
			h.rc.Cluster.Spec.Components.Engine.Replicas = count
			h.rc.Cluster.Generation++
			h.ready(t)
			if len(state(t, h.rc).Members) != count {
				t.Fatal("wrong member count")
			}
			cfg, e := h.root(t).RaftConfiguration(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			if len(cfg) != count {
				t.Fatal("raft configuration count", cfg)
			}
		}
		calls := h.log.snapshot()
		first := slices.Index(calls, "infra:demom4:delete")
		second := slices.Index(calls, "infra:demom3:delete")
		if first < 0 || second <= first {
			t.Fatalf("scale-down not highest first: %v", calls)
		}
		for _, name := range []string{"demom4", "demom3"} {
			n := h.node(name)
			if n.cancel != nil {
				t.Fatal("retired agent running", name)
			}
			if _, err := os.Stat(n.dir); !os.IsNotExist(err) {
				t.Fatal("retired volume remains", name, err)
			}
		}
	})
	scenario("replace_standby", func(t *testing.T) {
		old := ""
		for _, m := range state(t, h.rc).Members {
			if !m.Active {
				old = m.Name
				break
			}
		}
		h.operation(t, domain.OperationReplaceMember, fmt.Sprintf(`{"member":%q}`, old))
		h.ready(t)
		if state(t, h.rc).find(old) != nil {
			t.Fatal("old member remains")
		}
	})
	scenario("snapshot_restore", func(t *testing.T) { h.snapshot(t) })
	scenario("postgresql_credentials", func(t *testing.T) { nativeControlPlanePostgres(t, h.p, h.rc, func() { h.ready(t) }) })
	scenario("delete", func(t *testing.T) {
		h.loop(t, "cleanup", func() (bool, error) { return h.p.Cleanup(t.Context(), h.rc, domain.DeleteOptions{KeepVolumes: false}) })
		for name, n := range h.nodes {
			if n.cancel != nil {
				t.Fatal("agent remains", name)
			}
			if _, e := os.Stat(n.dir); !os.IsNotExist(e) {
				t.Fatal("member directory remains", name, e)
			}
		}
		if len(h.services) != 0 {
			t.Fatal("services remain")
		}
		for _, key := range []string{SecretUnseal, SecretManagementToken, SecretInitKey, SecretConnections, "tls/ca.crt", "tls/ca.key"} {
			if _, e := secret(t.Context(), h.rc, key); !missing(e) {
				t.Fatal("secret remains", key, e)
			}
		}
	})
}

func (h *nativeCP) snapshot(t *testing.T) {
	t.Helper()
	s3 := &snapshotFakeS3{objects: map[string][]byte{}}
	server := httptest.NewServer(s3)
	defer server.Close()
	original := snapshotS3Client
	defer func() { snapshotS3Client = original }()
	snapshotS3Client = func(s domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
		u, e := url.Parse(s.EndpointURL)
		if e != nil {
			return nil, nil, e
		}
		client, e := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: server.Client().Transport})
		return client, func() {}, e
	}
	st := &snapshotTestStore{ClusterStore: h.rc.Store, storage: domain.BackupStorage{Namespace: h.rc.Cluster.Ref().Namespace, Name: "s3", Bucket: "bucket", Prefix: "native", EndpointURL: server.URL, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, backup: &domain.Backup{Namespace: h.rc.Cluster.Ref().Namespace, Cluster: h.rc.Cluster.Name, Name: "snap", Storage: "s3", Type: "raft-snapshot", Provider: "openbao"}}
	h.rc.Store = st
	c := h.root(t)
	if e := c.Do(t.Context(), "POST", "sys/mounts/native", map[string]any{"type": "kv"}, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.Do(t.Context(), "POST", "native/before", map[string]string{"value": "preserved"}, nil); e != nil {
		t.Fatal(e)
	}
	h.operation(t, domain.OperationBackup, `{"backup":"snap"}`)
	if st.backup.Phase != domain.PhaseSucceeded || st.backup.Size == 0 {
		t.Fatal("backup incomplete")
	}
	if e := c.Do(t.Context(), "POST", "native/after", map[string]string{"value": "removed"}, nil); e != nil {
		t.Fatal(e)
	}
	st.restore = &domain.Restore{Namespace: h.rc.Cluster.Ref().Namespace, Cluster: h.rc.Cluster.Name, Name: "restore", Source: domain.RestoreSource{Backup: "snap", SourceCluster: h.rc.Cluster.Name}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	h.loop(t, "restore through applied POST", func() (bool, error) {
		r, err := h.p.RunOperation(t.Context(), h.rc, op)
		if err != nil {
			return false, err
		}
		if r.Step == "unseal" {
			return true, nil // Lose the successful native POST's checkpoint.
		}
		op.Checkpoint = r.Checkpoint
		return false, nil
	})
	// This write occurs after real OpenBao applied the snapshot. Replaying the
	// import would erase it, even though a naturally expired token says nothing
	// about whether that import happened.
	if e := h.root(t).Do(t.Context(), "POST", "native/post-apply", map[string]string{"value": "not-replayed"}, nil); e != nil {
		t.Fatal(e)
	}
	h.p = New(h.p.options)
	h.loop(t, "restore after lost POST checkpoint", func() (bool, error) {
		r, err := h.p.RunOperation(t.Context(), h.rc, op)
		if err == nil {
			op.Checkpoint = r.Checkpoint
		}
		return r.Done, err
	})
	h.ready(t)
	c = h.root(t)
	var out struct {
		Data struct {
			Value string `json:"value"`
		} `json:"data"`
	}
	if e := c.Do(t.Context(), "GET", "native/before", nil, &out); e != nil {
		t.Fatal(e)
	}
	if out.Data.Value != "preserved" {
		t.Fatal("pre-snapshot secret changed during restore")
	}
	if e := c.Do(t.Context(), "GET", "native/after", nil, nil); !baoapi.IsStatus(e, http.StatusNotFound) {
		t.Fatal("post-snapshot secret was not removed", e)
	}
	if e := c.Do(t.Context(), "GET", "native/post-apply", nil, &out); e != nil || out.Data.Value != "not-replayed" {
		t.Fatal("lost acknowledgement caused a second real snapshot import", e)
	}
	token, e := secret(t.Context(), h.rc, SecretManagementToken)
	if e != nil {
		t.Fatal(e)
	}
	client, _, e := h.p.activeClient(t.Context(), h.rc, state(t, h.rc), string(token))
	if e != nil {
		t.Fatal(e)
	}
	if e = client.Do(t.Context(), "GET", "auth/token/lookup-self", nil, nil); e != nil {
		t.Fatal(e)
	}
}
