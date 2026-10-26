//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/nomad"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

// This private fixture belongs exclusively to this test. Its ordinary CP must
// be stopped: the test replaces only its authenticated configuration listener,
// while the real Core, agents, scheduler and native applications do all work.
// No credentials or configuration contents are included in test diagnostics.
type nativeStorageFixtureConfig struct {
	StateDB, MasterKeyFile, CAFile           string
	ServerCertFile, ServerKeyFile            string
	Listen, ControlPlaneURL, MemberDNSSuffix string
	ArtifactFile, ArtifactSHA256             string
	Ref                                      domain.ClusterRef
	Target                                   infra.Target
	// infra.Target deliberately excludes its token from JSON.
	TargetToken string `json:"targetToken"`
}

type nativeStorageArtifact struct{ path string }

func (a nativeStorageArtifact) Open(context.Context) (io.ReadCloser, error) { return os.Open(a.path) }

type nativeStorageAgents struct {
	st   *store.Store
	base string
}

func (a nativeStorageAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	token, err := a.st.GetAgentToken(ctx, ref, member)
	if err != nil || token == nil {
		return nil, errors.New("native member token unavailable")
	}
	ca, err := a.st.GetClusterSecret(ctx, ref, "tls/ca.crt")
	if err != nil {
		return nil, errors.New("native member CA unavailable")
	}
	dial := func(ctx context.Context, network, logical string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(logical)
		if err != nil {
			return nil, err
		}
		if a.base != "" && net.ParseIP(host) == nil {
			host += "." + a.base
		}
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(host, port))
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + member + ":9443", CAPEM: ca, Token: token.Token, DialContext: dial,
		DialInstanceContext: func(ctx context.Context, network, logical, host string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(logical)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(host, port))
		}})
}

type nativeStorageReceipt struct {
	Version, ServiceID, ArtifactHash  string
	Runtime                           infra.RuntimeSpec
	Requests, Accepted, NativeDeploys int
	Epoch                             int64
	Outcome                           infra.OpState
	NativeOp                          *infra.Op
	Heal, InventoryError              bool
}

// Only deployment submission/outcome and a Storage inventory read are fake.
// Everything else is delegated to the actual native scheduler backend. The
// selected upload is a SHA-bound real agent archive and a frozen RuntimeSpec.
// Accepted is the synthetic platform count; NativeDeploys is the real count.
type nativeStorageSeam struct {
	infra.Backend
	path, member string
	r            nativeStorageReceipt
}

func (b *nativeStorageSeam) persist() error {
	raw, err := json.Marshal(b.r)
	if err != nil {
		return err
	}
	return os.WriteFile(b.path, raw, 0600)
}

func (b *nativeStorageSeam) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name != b.member {
		return b.Backend.Deploy(ctx, name, d)
	}
	if err := d.Validate(); err != nil {
		return infra.Op{}, err
	}
	service, err := b.Backend.GetService(ctx, name)
	if err != nil || service == nil || service.ID == "" {
		return infra.Op{}, errors.New("native deployment target unavailable")
	}
	if b.r.ServiceID == "" {
		b.r.ServiceID = service.ID
	}
	if b.r.ServiceID != service.ID {
		return infra.Op{}, errors.New("native deployment target replaced within selected attempt")
	}
	data, err := io.ReadAll(d.Artifact.Reader)
	if err != nil {
		return infra.Op{}, err
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if hash != b.r.ArtifactHash {
		return infra.Op{}, errors.New("native archive identity changed")
	}
	if len(b.r.Runtime.Args) == 0 {
		b.r.Runtime = d.Runtime
	} else if !reflect.DeepEqual(b.r.Runtime, d.Runtime) {
		return infra.Op{}, errors.New("selected native runtime changed")
	}
	if d.RetryEpoch > b.r.Epoch {
		b.r.Epoch = d.RetryEpoch
		b.r.Accepted = 0
	}
	if b.r.Accepted >= 4 {
		return infra.Op{}, &infra.DeploymentExhaustedError{Service: name, Version: b.r.Version, Attempts: 4, LastError: "DEPLOY_FAILED after runtime preparation"}
	}
	b.r.Requests++
	if b.r.Outcome.Status != "RETRY_REQUIRED" {
		b.r.Accepted++
	}
	if b.r.Heal {
		if b.r.NativeDeploys != 0 {
			return infra.Op{}, errors.New("duplicate accepted native mutation")
		}
		// Persist the pre-send identity before the genuine scheduler mutation.
		b.r.NativeDeploys++
		if err := b.persist(); err != nil {
			return infra.Op{}, err
		}
		d.Artifact.Reader = bytes.NewReader(data)
		op, err := b.Backend.Deploy(ctx, name, d)
		if err != nil {
			return infra.Op{}, err
		}
		b.r.NativeOp = &op
	}
	if err := b.persist(); err != nil {
		return infra.Op{}, err
	}
	return infra.Op{ID: "version:" + b.r.Version, ServiceID: b.r.ServiceID, Kind: "deploy"}, nil
}

func (b *nativeStorageSeam) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID != "version:"+b.r.Version {
		return b.Backend.OpStatus(ctx, op)
	}
	if op.ServiceID != b.r.ServiceID {
		return infra.OpState{}, errors.New("selected native service changed")
	}
	if b.r.NativeOp != nil {
		return b.Backend.OpStatus(ctx, *b.r.NativeOp)
	}
	return b.r.Outcome, nil
}

func (b *nativeStorageSeam) Containers(ctx context.Context, member string) ([]infra.Container, error) {
	if member == b.member && b.r.InventoryError {
		return nil, infra.ErrUnavailable
	}
	return b.Backend.Containers(ctx, member)
}

// The real manager owns Sync, fresh observations, generation acknowledgement
// and publication. This gate only serializes test-owned receipt changes between
// worker cycles; it never supplies a native result or skips an admitted Sync.
type nativeStorageSteppedProvider struct {
	*Provider
	entered chan struct{}
	permit  chan struct{}
	results chan error
}

func (p *nativeStorageSteppedProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	select {
	case p.entered <- struct{}{}:
	case <-ctx.Done():
		return engine.Result{}, ctx.Err()
	}
	select {
	case <-p.permit:
	case <-ctx.Done():
		return engine.Result{}, ctx.Err()
	}
	result, err := p.Provider.Sync(ctx, rc)
	p.results <- err
	return result, err
}

type nativeStorageNamespaces struct {
	namespace string
	backend   infra.Backend
	dialer    engine.ContextDialer
	agentURL  string
}

func (n nativeStorageNamespaces) Backend(namespace string) (infra.Backend, error) {
	if domain.NormalizedNamespace(namespace) != n.namespace {
		return nil, errors.New("native worker namespace outside owned fixture")
	}
	return n.backend, nil
}
func (n nativeStorageNamespaces) Dialer(string) engine.ContextDialer { return n.dialer }
func (n nativeStorageNamespaces) AgentURL(string) string             { return n.agentURL }

// Retain actual manager errors for assertions without printing remote messages,
// configuration, tokens or SQL connection material.
type nativeStorageWorkerLog struct {
	mu      sync.Mutex
	failure error
}

func (*nativeStorageWorkerLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}
func (l *nativeStorageWorkerLog) Handle(_ context.Context, record slog.Record) error {
	if record.Level < slog.LevelError {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "error" {
			if err, ok := attr.Value.Any().(error); ok {
				l.failure = errors.Join(l.failure, err)
			}
		}
		return true
	})
	return nil
}
func (l *nativeStorageWorkerLog) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *nativeStorageWorkerLog) WithGroup(string) slog.Handler      { return l }
func (l *nativeStorageWorkerLog) take() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.failure
	l.failure = nil
	return err
}

type nativeStorageFixture struct {
	t                  *testing.T
	ctx                context.Context
	cfg                nativeStorageFixtureConfig
	sealer             *secrets.Sealer
	ca                 []byte
	st                 *store.Store
	p                  *Provider
	rc                 *engine.Context
	b                  *nativeStorageSeam
	mu                 sync.RWMutex // configuration requests versus store reconstruction
	server             *http.Server
	serviceID, version string
	baseline           map[string]string
	bucket             string
	payload            []byte
	payloadHash        [sha256.Size]byte
	backing            sbspec.Storage
	s3Object           string
	objectID           string
	worker             *nativeStorageSteppedProvider
	workerLog          *nativeStorageWorkerLog
	cancelWorker       context.CancelFunc
	workerDone         chan error
}

func nativeStorageMust(t *testing.T, action string, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(action + " failed (sensitive diagnostics withheld)")
	}
}

func openNativeStorageFixture(t *testing.T, ctx context.Context) *nativeStorageFixture {
	t.Helper()
	path := os.Getenv("KARAKORAM_NATIVE_STORAGE_FIXTURE")
	if path == "" {
		t.Skip("requires an exclusive native standard HA3 strict-sync fixture with Auth/REST replicas=2; set KARAKORAM_NATIVE_STORAGE_FIXTURE")
	}
	if os.Getenv("KARAKORAM_NATIVE_STORAGE_EXCLUSIVE") != "1" {
		t.Fatal("configured native fixture requires exclusive ownership and a stopped ordinary reconciler")
	}
	f := &nativeStorageFixture{t: t, ctx: ctx}
	raw, err := os.ReadFile(path)
	nativeStorageMust(t, "read private fixture", err)
	nativeStorageMust(t, "decode private fixture", json.Unmarshal(raw, &f.cfg))
	if strings.TrimSpace(f.cfg.TargetToken) == "" {
		t.Fatal("configured native fixture requires a private top-level targetToken")
	}
	f.cfg.Target.Token = f.cfg.TargetToken
	// Match TargetFromEnv: unset optional public addresses are omitted, not
	// submitted as invalid empty IP literals. Nonempty values remain subject
	// to the real factory's address-family validation.
	for _, key := range []string{"publicIPv4", "publicIPv6"} {
		if f.cfg.Target.Config[key] == "" {
			delete(f.cfg.Target.Config, key)
		}
	}
	if f.cfg.Target.Kind != "nomad" {
		t.Fatal("configured native fixture must use a disposable native Nomad target")
	}
	// Never open a shared CP database or put acceptance state on shared /tmp.
	home, err := os.UserHomeDir()
	nativeStorageMust(t, "resolve owned cache", err)
	root := filepath.Join(home, ".cache", "karakoram-native-acceptance") + string(os.PathSeparator)
	db, err := filepath.Abs(f.cfg.StateDB)
	nativeStorageMust(t, "resolve private state", err)
	if !strings.HasPrefix(db, root) {
		t.Fatal("dedicated fixture state must be under the owned acceptance cache")
	}
	key, err := os.ReadFile(f.cfg.MasterKeyFile)
	nativeStorageMust(t, "read private master key", err)
	f.sealer, err = secrets.New(strings.TrimSpace(string(key)))
	nativeStorageMust(t, "load private sealer", err)
	f.ca, err = os.ReadFile(f.cfg.CAFile)
	nativeStorageMust(t, "read pinned CA", err)
	f.reopen()
	t.Cleanup(func() {
		f.t = t
		f.stopWorker()
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.st != nil {
			_ = f.st.Close()
		}
	})
	if f.b.r.Version != "" || f.b.r.Requests != 0 {
		t.Fatal("native acceptance requires a fresh owned seam receipt; preserve the prior fixture evidence")
	}
	f.startConfigServer()
	if f.rc.Cluster.Spec.Provider != "supabase" || f.rc.Cluster.Spec.Topology.Type != "ha" || f.rc.Cluster.Spec.Components.Engine.Replicas != 3 {
		t.Fatal("requires supported Supabase HA3, not singleton")
	}
	o, err := ParseOptions(f.rc.Cluster.Spec.EngineOptions)
	nativeStorageMust(t, "parse native options", err)
	if o.SynchronousMode != "strict" {
		t.Fatal("requires strict synchronous replication")
	}
	for _, c := range []string{"auth", "rest"} {
		if f.rc.Cluster.Spec.Components.Extra[c].Replicas != 2 {
			t.Fatal("requires two native Auth and REST replicas")
		}
	}
	for _, c := range []string{"gateway", "storage", "realtime", "meta", "studio", "proxy"} {
		if f.rc.Cluster.Spec.Components.Extra[c].Replicas < 1 {
			t.Fatal("requires the complete standard native component set")
		}
	}
	if f.rc.Cluster.Deleting {
		t.Fatal("fixture is deleting")
	}
	s, err := loadState(f.rc.Cluster)
	nativeStorageMust(t, "load native checkpoint", err)
	m := s.Members["storage"]
	if s.Pending != nil || m == nil || (m.Stage != engine.ComponentImported && m.Stage != engine.ComponentJoined) || m.ServiceID == "" {
		t.Fatal("requires genuine initial Storage import or healthy Joined native fixture")
	}
	sp, err := f.p.desiredSpec(ctx, f.rc, m)
	nativeStorageMust(t, "derive initial pinned native target", err)
	if sp.Version != "1.81.0" || sp.Topology != "ha" || len(sp.DataPeers) != 3 {
		t.Fatal("initial Storage target must be pinned native 1.81.0 on HA3")
	}
	f.serviceID, f.version = m.ServiceID, sp.Version
	f.baseline = f.identities()
	core, err := f.p.Core.Status(ctx, f.rc)
	nativeStorageMust(t, "observe native HA3 before Storage startup", err)
	if core.Phase != domain.PhaseReady {
		t.Fatal("native HA3 core is not healthy before initial Storage deployment")
	}
	for _, name := range []string{"auth", "rest"} {
		member := s.Members[name]
		if member == nil || member.Stage != engine.ComponentJoined || member.Replicas != 2 {
			t.Fatal("native Auth/REST two-replica checkpoint is not joined")
		}
		health, err := componentStatus(ctx, f.rc, member)
		nativeStorageMust(t, "observe real native Auth/REST replicas", err)
		if !health.Ready {
			t.Fatal("native Auth/REST replicas are not healthy")
		}
	}
	if m.Stage == engine.ComponentJoined {
		f.assertReady()
	}
	t.Cleanup(func() {
		f.t = t
		f.stopWorker()
		if f.bucket == "" {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		f.ctx, f.t = cleanup, t
		if err := f.cleanupRetainedObject(cleanup); err != nil {
			t.Error("retained native probe cleanup failed: " + err.Error())
		}
	})
	f.startWorker()
	return f
}

func (f *nativeStorageFixture) reopen() {
	f.t.Helper()
	f.stopWorker()
	f.mu.Lock()
	defer func() {
		f.mu.Unlock()
		if f.server != nil {
			f.startWorker()
		}
		if f.bucket != "" {
			f.roundtrip()
		}
	}()
	if f.st != nil {
		nativeStorageMust(f.t, "close durable CP", f.st.Close())
	}
	st, err := store.Open(f.cfg.StateDB, f.sealer)
	nativeStorageMust(f.t, "reconstruct durable CP", err)
	f.st = st
	cluster, err := st.GetCluster(f.ctx, f.cfg.Ref)
	nativeStorageMust(f.t, "reload durable cluster", err)
	factory := nomad.NewFactory()
	backend, err := factory.New(f.cfg.Target)
	nativeStorageMust(f.t, "construct native backend", err)
	f.b = &nativeStorageSeam{Backend: backend, member: cluster.Name + codes["storage"], path: f.cfg.StateDB + ".storage-receipt"}
	if raw, err := os.ReadFile(f.b.path); err == nil {
		nativeStorageMust(f.t, "reconstruct provider seam receipt", json.Unmarshal(raw, &f.b.r))
	} else if !os.IsNotExist(err) {
		nativeStorageMust(f.t, "read provider seam receipt", err)
	}
	f.p = New(postgres.Options{Store: st, ControlPlaneURL: f.cfg.ControlPlaneURL, InternalCAPEM: string(f.ca), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	f.rc = f.context(cluster, f.b)
}

func (f *nativeStorageFixture) context(cluster *domain.Cluster, backend infra.Backend) *engine.Context {
	return &engine.Context{Cluster: cluster, Namespace: cluster.Namespace, Backend: backend, Store: f.st, Secrets: f.st,
		Agents: nativeStorageAgents{st: f.st, base: f.cfg.MemberDNSSuffix}, AgentURL: f.cfg.ControlPlaneURL,
		Artifact: nativeStorageArtifact{path: f.cfg.ArtifactFile}, Dialer: &net.Dialer{}, Clock: engine.RealClock{}, ObservationEpoch: rand.Text(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func (f *nativeStorageFixture) startWorker() {
	f.t.Helper()
	if f.worker != nil {
		f.t.Fatal("native publication worker already owns the fixture")
	}
	clusters, err := f.st.ListClusters(f.ctx, "")
	nativeStorageMust(f.t, "read exclusive worker scope", err)
	if len(clusters) != 1 || clusters[0].Ref().Key() != f.cfg.Ref.Key() {
		f.t.Fatal("native publication worker requires exactly its one owned cluster")
	}
	operations, err := f.st.ListOperations(f.ctx, f.cfg.Ref, domain.PhasePending, domain.PhaseRunning)
	nativeStorageMust(f.t, "read active native operations", err)
	for _, operation := range operations {
		if !domain.IsConvergenceOperation(operation.Kind) {
			f.t.Fatal("native fixture has an active imperative operation")
		}
	}
	f.worker = &nativeStorageSteppedProvider{Provider: f.p, entered: make(chan struct{}), permit: make(chan struct{}), results: make(chan error, 1)}
	f.workerLog = &nativeStorageWorkerLog{}
	registry, err := engine.NewRegistry(f.worker)
	nativeStorageMust(f.t, "register actual native provider", err)
	managerContext := *f.rc
	managerContext.Logger = slog.New(f.workerLog)
	manager, err := reconciler.New(reconciler.Options{
		Store: f.st, Registry: registry, Context: managerContext,
		Namespaces:     nativeStorageNamespaces{namespace: domain.NormalizedNamespace(f.cfg.Ref.Namespace), backend: f.b, dialer: f.rc.Dialer, agentURL: f.cfg.ControlPlaneURL},
		ResyncInterval: time.Second, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond,
	})
	nativeStorageMust(f.t, "construct real native publication manager", err)
	ctx, cancel := context.WithCancel(f.ctx)
	f.cancelWorker = cancel
	f.workerDone = make(chan error, 1)
	done := f.workerDone
	go func() {
		done <- manager.Run(ctx)
		close(done)
	}()
	f.awaitWorker()
	f.reloadPublished()
}

func (f *nativeStorageFixture) awaitWorker() {
	f.t.Helper()
	select {
	case <-f.worker.entered:
	case <-f.workerDone:
		f.t.Fatal("native publication manager exited before the next worker barrier")
	case <-f.ctx.Done():
		f.t.Fatal("native publication manager deadline exceeded")
	}
}

func (f *nativeStorageFixture) stopWorker() {
	if f.cancelWorker == nil {
		return
	}
	f.cancelWorker()
	if err := <-f.workerDone; err != nil {
		f.t.Error("native publication manager shutdown failed (sensitive diagnostics withheld)")
	}
	f.cancelWorker, f.worker, f.workerDone = nil, nil, nil
}

func (f *nativeStorageFixture) reloadPublished() {
	f.t.Helper()
	cluster, err := f.st.GetCluster(f.ctx, f.cfg.Ref)
	nativeStorageMust(f.t, "read real worker-published native state", err)
	f.rc.Cluster = cluster
}

// Publish the actual inventory observation without accepting another deployment.
// A pending handle can be polled normally. After terminal failure, reconstruct
// the manager: its real pre-Sync availability publication runs before the gate.
func (f *nativeStorageFixture) observeInventory() {
	f.t.Helper()
	before := f.b.r
	if f.state().Pending != nil {
		nativeStorageMust(f.t, "publish pending native inventory observation", f.step())
	} else {
		if f.rc.Cluster.Status.Phase != domain.PhaseDegraded {
			f.t.Fatal("terminal inventory observation requires the actual degraded worker checkpoint")
		}
		f.reopen()
	}
	if f.b.r.Requests != before.Requests || f.b.r.Accepted != before.Accepted || f.b.r.NativeDeploys != before.NativeDeploys {
		f.t.Fatal("inventory observation accepted a duplicate deployment mutation")
	}
}

func (f *nativeStorageFixture) startConfigServer() {
	f.t.Helper()
	cert, err := tls.LoadX509KeyPair(f.cfg.ServerCertFile, f.cfg.ServerKeyFile)
	nativeStorageMust(f.t, "load original CP listener identity", err)
	listener, err := tls.Listen("tcp", f.cfg.Listen, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	nativeStorageMust(f.t, "take exclusive CP configuration listener", err)
	f.server = &http.Server{ReadHeaderTimeout: 10 * time.Second, ErrorLog: slog.NewLogLogger(slog.NewTextHandler(io.Discard, nil), slog.LevelError), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet || r.URL.Path != "/internal/agent/v1/config" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.mu.RLock()
		defer f.mu.RUnlock()
		auth := strings.Fields(r.Header.Get("Authorization"))
		if len(auth) != 2 || !strings.EqualFold(auth[0], "Bearer") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		token, err := f.st.FindAgentTokenByHash(r.Context(), secrets.Hash(auth[1]))
		if err != nil || token.Namespace != f.cfg.Ref.Namespace || token.Cluster != f.cfg.Ref.Name {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		cluster, err := f.st.GetCluster(r.Context(), f.cfg.Ref)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		p := New(postgres.Options{Store: f.st, ControlPlaneURL: f.cfg.ControlPlaneURL, InternalCAPEM: string(f.ca)})
		cfg, err := p.MemberConfig(r.Context(), f.context(cluster, f.b.Backend), token.Member)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	})}
	go func() { _ = f.server.Serve(listener) }()
	f.t.Cleanup(func() { _ = f.server.Close() })
}

func (f *nativeStorageFixture) state() *componentState {
	f.t.Helper()
	s, err := loadState(f.rc.Cluster)
	nativeStorageMust(f.t, "read component checkpoint", err)
	return s
}

func (f *nativeStorageFixture) persist() {
	f.t.Helper()
	nativeStorageMust(f.t, "persist provider seam checkpoint", f.b.persist())
}

func (f *nativeStorageFixture) step() error {
	f.t.Helper()
	if f.worker == nil {
		f.t.Fatal("native publication worker is not running")
	}
	_ = f.workerLog.take()
	select {
	case f.worker.permit <- struct{}{}:
	case <-f.workerDone:
		f.t.Fatal("native publication worker exited before Sync")
	case <-f.ctx.Done():
		f.t.Fatal("native publication worker Sync deadline exceeded")
	}
	// The next Sync entry follows the previous cycle's real observation,
	// generation normalization, durable publication and operation tracking.
	f.awaitWorker()
	syncErr := <-f.worker.results
	f.reloadPublished()
	if err := f.workerLog.take(); err != nil {
		return err
	}
	return syncErr
}

func (f *nativeStorageFixture) status() domain.ClusterStatus {
	f.t.Helper()
	f.reloadPublished()
	return f.rc.Cluster.Status
}

func (f *nativeStorageFixture) identities() map[string]string {
	f.t.Helper()
	ids := map[string]string{}
	// The standard fixture must have three genuinely distinct native PG members.
	for _, member := range []string{f.rc.Cluster.Name + "p0", f.rc.Cluster.Name + "p1", f.rc.Cluster.Name + "p2", f.rc.Cluster.Name + codes["storage"]} {
		s, err := f.b.Backend.GetService(f.ctx, member)
		nativeStorageMust(f.t, "observe native service identity", err)
		if s == nil || s.ID == "" {
			f.t.Fatal("native service identity missing")
		}
		ids[member] = s.ID
	}
	return ids
}

func (f *nativeStorageFixture) assertTarget() {
	f.t.Helper()
	if !reflect.DeepEqual(f.baseline, f.identities()) {
		f.t.Fatal("native service identity changed across boundary")
	}
	m := f.state().Members["storage"]
	if m == nil || m.ServiceID != f.serviceID || m.Version != f.version {
		f.t.Fatal("reconcile target no longer names the original pinned native Storage")
	}
	sp, err := f.p.desiredSpec(f.ctx, f.rc, m)
	nativeStorageMust(f.t, "derive real native Storage configuration", err)
	if sp.Component != "storage" || sp.Version != "1.81.0" || sp.Topology != "ha" || len(sp.DataPeers) != 3 {
		f.t.Fatal("native Storage spec detached from HA3 fixture")
	}
	if f.bucket != "" && !reflect.DeepEqual(sp.Storage, f.backing) {
		f.t.Fatal("native target no longer uses the original retained-object backing identity")
	}
}

func (f *nativeStorageFixture) assertReady() {
	f.t.Helper()
	status := f.status()
	if status.Phase != domain.PhaseReady || status.ObservedGeneration != f.rc.Cluster.Generation {
		f.t.Fatal("native fixture is not current-generation Ready")
	}
	if f.worker != nil && (status.ObservationEpoch != f.rc.ObservationEpoch || status.ObservedAt.IsZero()) {
		f.t.Fatal("native Ready observation was not freshly published by the reconstructed manager")
	}
	for _, c := range status.Conditions {
		if (c.Type == "Ready" || c.Type == "ComponentsReady") && c.Status != "True" {
			f.t.Fatal("native health condition is not Ready")
		}
	}
	f.assertTarget()
}

func (f *nativeStorageFixture) begin(outcome infra.OpState) {
	f.t.Helper()
	// A Joined runtime cannot be redeployed by re-arming its retry epoch.
	// Exercise the real supported spec lifecycle: remove it completely, then
	// request a new native Storage import. Never edit internal stage/receipts.
	m := f.state().Members["storage"]
	if m != nil && m.Stage == engine.ComponentJoined {
		f.assertReady()
		if f.bucket != "" {
			f.roundtrip()
		}
		oldID := m.ServiceID
		f.setStorageReplicas(0)
		for {
			nativeStorageMust(f.t, "converge native Storage removal", f.step())
			service, err := f.b.Backend.GetService(f.ctx, f.b.member)
			if !errors.Is(err, infra.ErrNotFound) {
				nativeStorageMust(f.t, "observe native Storage removal", err)
			}
			if f.state().Members["storage"] == nil && service == nil {
				status := f.status()
				if status.Phase == domain.PhaseReady && status.ObservedGeneration == f.rc.Cluster.Generation {
					break
				}
			}
			select {
			case <-f.ctx.Done():
				f.t.Fatal("native Storage removal deadline exceeded")
			case <-time.After(time.Second):
			}
		}
		if f.bucket != "" {
			f.assertBackingObject()
		}
		f.setStorageReplicas(1)
		f.serviceID = ""
		if f.baseline[f.b.member] != oldID {
			f.t.Fatal("previous native target identity changed before removal")
		}
	} else if m == nil || m.Stage != engine.ComponentImported || f.rc.Cluster.Status.ObservedGeneration >= f.rc.Cluster.Generation {
		f.t.Fatal("initial Storage checkpoint is not an unresolved genuine import")
	}
	f.b.r = nativeStorageReceipt{Version: "native-storage-" + strings.ToLower(rand.Text()), ArtifactHash: f.cfg.ArtifactSHA256, Epoch: f.rc.Cluster.Spec.RetryEpoch, Outcome: outcome}
	f.persist()
	f.untilPending()
}

func (f *nativeStorageFixture) setStorageReplicas(replicas int) {
	f.t.Helper()
	old := f.rc.Cluster
	raw, err := json.Marshal(old.Spec)
	nativeStorageMust(f.t, "copy actual desired spec", err)
	next := *old
	next.Spec = domain.ClusterSpec{}
	nativeStorageMust(f.t, "decode actual desired spec", json.Unmarshal(raw, &next.Spec))
	storage := next.Spec.Components.Extra["storage"]
	storage.Replicas = replicas
	next.Spec.Components.Extra["storage"] = storage
	nativeStorageMust(f.t, "validate supported Storage lifecycle", f.p.Validate(f.ctx, old, &next))
	nativeStorageMust(f.t, "persist real Storage lifecycle spec", f.st.UpdateClusterSpec(f.ctx, f.cfg.Ref, next.Spec))
	updated, err := f.st.GetCluster(f.ctx, f.cfg.Ref)
	nativeStorageMust(f.t, "read persisted lifecycle mutation", err)
	if updated.Generation != old.Generation+1 || updated.Status.ObservedGeneration != old.Status.ObservedGeneration || updated.Status.Phase != old.Status.Phase {
		f.t.Fatal("real spec mutation did not preserve the genuine observed checkpoint")
	}
	f.reopen()
}

func (f *nativeStorageFixture) untilPending() {
	f.t.Helper()
	for range 100 {
		nativeStorageMust(f.t, "reconcile actual Storage import/deployment", f.step())
		s := f.state()
		if s.Pending != nil && s.Pending.Component == "storage" && s.Pending.Action == "deploy" {
			if f.serviceID == "" {
				if f.b.r.ServiceID == "" || f.b.r.ServiceID == f.baseline[f.b.member] {
					f.t.Fatal("supported lifecycle did not create a new native target")
				}
				f.serviceID = f.b.r.ServiceID
				f.baseline[f.b.member] = f.serviceID
			}
			f.assertTarget()
			return
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal("native deployment checkpoint deadline exceeded")
		case <-time.After(time.Second):
		}
	}
	f.t.Fatal("actual Core/component reconciliation did not submit selected Storage")
}

func (f *nativeStorageFixture) diagnose(reason, phase string) {
	f.t.Helper()
	status := f.status()
	if status.Phase == domain.PhaseReady || status.ObservedGeneration == f.rc.Cluster.Generation {
		f.t.Fatal("unresolved deployment falsely published current-generation Ready")
	}
	for _, typ := range []string{"Ready", "ComponentsReady"} {
		found := false
		for _, c := range status.Conditions {
			if c.Type == typ {
				found = c.Status == "False" && c.Reason == reason && strings.Contains(c.Message, "version:"+f.b.r.Version) && strings.Contains(c.Message, f.serviceID) && strings.Contains(c.Message, phase)
				if f.b.r.InventoryError {
					found = found && strings.Contains(c.Message, "inventory")
				}
			}
		}
		if !found {
			f.t.Fatal("precise deployment diagnosis was lost")
		}
	}
	f.assertTarget()
	if f.bucket != "" {
		f.roundtrip()
	}
}

func (f *nativeStorageFixture) heal() {
	f.t.Helper()
	// Retire an existing synthetic pending handle before the real same-target
	// native submission. Never report success on a synthetic deployment alone.
	if f.state().Pending != nil {
		f.b.r.Outcome = infra.OpState{Done: true, Failed: true, Status: "RETRY_REQUIRED"}
		nativeStorageMust(f.t, "persist unsubmitted recovery classification", f.b.persist())
		_ = f.step()
	}
	f.b.r.Heal = true
	f.b.r.InventoryError = false
	f.b.r.Outcome = infra.OpState{Status: "PREPARING_RUNTIME"}
	f.persist()
	for {
		_ = f.step()
		status := f.status()
		if status.Phase == domain.PhaseReady && status.ObservedGeneration == f.rc.Cluster.Generation {
			break
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal("native recovery did not reach current-generation Ready")
		case <-time.After(time.Second):
		}
	}
	if f.b.r.NativeDeploys != 1 || f.b.r.NativeOp == nil {
		f.t.Fatal("recovery did not execute exactly one genuine native deployment")
	}
	f.assertReady()
	f.roundtrip()
	f.reopen()
	f.assertReady()
}

func (f *nativeStorageFixture) roundtrip() {
	m := f.state().Members["storage"]
	if f.bucket != "" && (m == nil || m.Stage != engine.ComponentJoined) {
		// The actual native frontend is absent during import/failure. Check
		// its original acknowledged data directly, never fake an HTTP read.
		f.assertBackingObject()
		return
	}
	f.objects()
	if f.bucket != "" {
		f.assertBackingObject()
	}
}

// Keep one acknowledged object across every subsequent failure, CP reopen and
// actual native redeployment. Only the parent's final bounded cleanup deletes it.
func (f *nativeStorageFixture) objects() {
	f.t.Helper()
	f.assertTarget()
	m := f.state().Members["storage"]
	sp, err := f.p.desiredSpec(f.ctx, f.rc, m)
	nativeStorageMust(f.t, "load sealed Storage probe credentials", err)
	if f.bucket == "" {
		f.backing = sp.Storage
	}
	instances, err := f.b.Backend.Containers(f.ctx, m.Name)
	nativeStorageMust(f.t, "read actual native Storage inventory", err)
	if len(instances) != m.Replicas || len(instances) == 0 {
		f.t.Fatal("native Storage inventory does not match target")
	}
	client, err := f.rc.Agents.Dial(f.ctx, f.rc.Cluster.Ref(), m.Name)
	nativeStorageMust(f.t, "dial actual Storage member", err)
	defer client.Close()
	for _, instance := range instances {
		host := instance.IPv4
		if host == "" {
			host = instance.Hostname
		}
		transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return client.DialContext(ctx, network, net.JoinHostPort(host, "5000"))
		}}
		httpClient := &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		request := func(method, path string, body []byte) (int, []byte) {
			req, err := http.NewRequestWithContext(f.ctx, method, "http://native-storage"+path, bytes.NewReader(body))
			nativeStorageMust(f.t, "construct native object request", err)
			req.Header.Set("Authorization", "Bearer "+sp.ServiceRoleKey)
			req.Header.Set("Content-Type", "application/json")
			res, err := httpClient.Do(req)
			nativeStorageMust(f.t, "native Storage request", err)
			defer res.Body.Close()
			data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			nativeStorageMust(f.t, "read native Storage response", err)
			return res.StatusCode, data
		}
		if f.bucket == "" {
			f.bucket = "native-" + strings.ToLower(rand.Text())
			f.payload = []byte("native-storage-boundary-" + rand.Text())
			f.payloadHash = sha256.Sum256(f.payload)
			body, _ := json.Marshal(map[string]any{"id": f.bucket, "name": f.bucket, "public": false})
			code, _ := request("POST", "/bucket", body)
			if code != 200 && code != 201 {
				f.t.Fatal("native Storage bucket creation failed")
			}
			code, _ = request("POST", "/object/"+f.bucket+"/probe.json", f.payload)
			if code != 200 {
				f.t.Fatal("native Storage object upload failed")
			}
			conn, closeConn, err := f.metadataConnection(f.ctx)
			nativeStorageMust(f.t, "connect native object metadata", err)
			err = conn.QueryRow(f.ctx, "SELECT id::text FROM storage.objects WHERE bucket_id=$1 AND name=$2", f.bucket, "probe.json").Scan(&f.objectID)
			closeConn()
			nativeStorageMust(f.t, "record exact acknowledged object metadata", err)
		}
		code, got := request("GET", "/object/authenticated/"+f.bucket+"/probe.json", nil)
		if code != 200 || !bytes.Equal(got, f.payload) || sha256.Sum256(got) != f.payloadHash {
			f.t.Fatal("retained native Storage object bytes/hash changed")
		}
		transport.CloseIdleConnections()
	}
}

func (f *nativeStorageFixture) assertBackingObject() {
	f.t.Helper()
	if f.bucket == "" {
		return
	}
	s := f.backing
	client, closeClient, err := backupstorage.NewClient(domain.BackupStorage{Type: "s3", EndpointURL: s.Endpoint, Bucket: s.Bucket, Region: s.Region, ForcePathStyle: s.PathStyle, VerifyTLS: s.TLS},
		domain.StorageCredentials{AccessKeyID: s.AccessKey, SecretAccessKey: s.SecretKey})
	nativeStorageMust(f.t, "open real retained-object backing store", err)
	defer closeClient()
	if f.s3Object == "" {
		// The registered backing bucket belongs exclusively to this fixture.
		// Discover the exact upstream key once; never invent a release-specific
		// key layout or substitute a newly written preservation control.
		for object := range client.ListObjects(f.ctx, s.Bucket, minio.ListObjectsOptions{Recursive: true}) {
			nativeStorageMust(f.t, "locate acknowledged native object", object.Err)
			if strings.Contains(object.Key, f.bucket) && strings.Contains(object.Key, "probe.json") {
				if f.s3Object != "" {
					f.t.Fatal("acknowledged native object backing identity is ambiguous")
				}
				f.s3Object = object.Key
			}
		}
		if f.s3Object == "" {
			f.t.Fatal("acknowledged native object backing identity missing")
		}
	}
	object, err := client.GetObject(f.ctx, s.Bucket, f.s3Object, minio.GetObjectOptions{})
	nativeStorageMust(f.t, "read exact retained native backing object", err)
	defer object.Close()
	got, err := io.ReadAll(io.LimitReader(object, int64(len(f.payload)+1)))
	nativeStorageMust(f.t, "read retained native object bytes", err)
	if !bytes.Equal(got, f.payload) || sha256.Sum256(got) != f.payloadHash {
		f.t.Fatal("acknowledged native object backing bytes/hash changed")
	}
}

// Discover the actual native writer and use its CA-pinned PostgreSQL transport.
// This is only for recording/deleting this probe's exact owned metadata.
func (f *nativeStorageFixture) metadataConnection(ctx context.Context) (*pgx.Conn, func(), error) {
	password, err := f.st.GetClusterSecret(ctx, f.cfg.Ref, "users/postgres")
	if err != nil {
		return nil, nil, err
	}
	ca, err := f.st.GetClusterSecret(ctx, f.cfg.Ref, "tls/ca.crt")
	if err != nil {
		return nil, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("native database CA unavailable")
	}
	for _, member := range f.p.Core.DataMembers(f.rc.Cluster) {
		a, err := f.rc.Agents.Dial(ctx, f.cfg.Ref, member)
		if err != nil {
			continue
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		var status pgspec.StatusResponse
		err = a.Engine(probe, "postgres", "status", struct{}{}, &status)
		cancel()
		if err != nil || !status.Running || (status.Role != "primary" && status.Role != "standalone") {
			a.Close()
			continue
		}
		cfg, err := pgx.ParseConfig("user=postgres dbname=postgres sslmode=disable")
		if err != nil {
			a.Close()
			return nil, nil, err
		}
		cfg.Host, cfg.Port, cfg.Password = member, 5432, string(password)
		cfg.ConnectTimeout = 3 * time.Second
		cfg.DialFunc = a.DialContext
		cfg.Fallbacks = nil
		config, err := f.p.Core.MemberConfig(ctx, f.rc, member)
		if err != nil {
			a.Close()
			return nil, nil, err
		}
		var spec pgspec.Spec
		if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
			a.Close()
			return nil, nil, err
		}
		if spec.Postgres != nil && spec.Postgres.RequireTLS {
			cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: member}
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			a.Close()
			return nil, nil, err
		}
		return conn, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = conn.Close(closeCtx)
			a.Close()
		}, nil
	}
	return nil, nil, errors.New("native database writer unavailable")
}

// Attempt the normal native API cleanup first. Absence/failure of the frontend
// is expected after a failed removal/recreation; the exact-key/row path below
// remains independently usable without a component state or agent token.
func (f *nativeStorageFixture) cleanupHTTP(ctx context.Context) error {
	instances, err := f.b.Backend.Containers(ctx, f.b.member)
	if err != nil || len(instances) == 0 {
		return errors.New("native frontend unavailable")
	}
	a, err := f.rc.Agents.Dial(ctx, f.cfg.Ref, f.b.member)
	if err != nil {
		return err
	}
	defer a.Close()
	credentials, err := loadCredentials(ctx, f.rc)
	if err != nil {
		return err
	}
	host := instances[0].IPv4
	if host == "" {
		host = instances[0].Hostname
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return a.DialContext(ctx, network, net.JoinHostPort(host, "5000"))
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(path string, body []byte) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://native-storage"+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+credentials.Keys.ServiceRoleKey)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return errors.New("native cleanup API refused")
		}
		return nil
	}
	body, _ := json.Marshal(map[string]any{"prefixes": []string{"probe.json"}})
	if err = request("/object/"+url.PathEscape(f.bucket), body); err != nil {
		return err
	}
	return request("/bucket/"+url.PathEscape(f.bucket), nil)
}

func (f *nativeStorageFixture) cleanupRetainedObject(ctx context.Context) error {
	httpCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	httpErr := f.cleanupHTTP(httpCtx)
	cancel()
	// Exact, recorded identities only. Never empty/list-delete a backing prefix
	// or delete arbitrary bucket rows to make a failed cleanup look successful.
	if f.s3Object == "" || f.objectID == "" {
		return errors.New("recorded exact cleanup identities unavailable")
	}
	s := f.backing
	client, closeClient, err := backupstorage.NewClient(domain.BackupStorage{Type: "s3", EndpointURL: s.Endpoint, Bucket: s.Bucket, Region: s.Region, ForcePathStyle: s.PathStyle, VerifyTLS: s.TLS},
		domain.StorageCredentials{AccessKeyID: s.AccessKey, SecretAccessKey: s.SecretKey})
	if err != nil {
		return errors.New("exact backing cleanup client unavailable")
	}
	defer closeClient()
	if err = client.RemoveObject(ctx, s.Bucket, f.s3Object, minio.RemoveObjectOptions{}); err != nil {
		return errors.New("exact backing object deletion failed")
	}
	if _, err = client.StatObject(ctx, s.Bucket, f.s3Object, minio.StatObjectOptions{}); err == nil {
		return errors.New("exact backing object remains after cleanup")
	} else {
		code := minio.ToErrorResponse(err).Code
		if code != "NoSuchKey" && code != "NoSuchObject" && code != "NotFound" {
			return errors.New("exact backing object absence could not be verified")
		}
	}
	conn, closeConn, err := f.metadataConnection(ctx)
	if err != nil {
		return errors.New("exact native metadata cleanup connection unavailable")
	}
	defer closeConn()
	const metadataRemainsQuery = "SELECT EXISTS(SELECT 1 FROM storage.objects WHERE id::text=$1 OR bucket_id=$2) OR EXISTS(SELECT 1 FROM storage.buckets WHERE id=$2)"
	tx, err := conn.Begin(ctx)
	if err != nil {
		return errors.New("exact metadata cleanup transaction failed")
	}
	defer tx.Rollback(ctx)
	if httpErr == nil {
		var remains bool
		if err = tx.QueryRow(ctx, metadataRemainsQuery, f.objectID, f.bucket).Scan(&remains); err != nil {
			return errors.New("exact metadata cleanup absence could not be verified")
		}
		if !remains {
			if err = tx.Commit(ctx); err != nil {
				return errors.New("exact metadata cleanup commit failed")
			}
			return nil
		}
	}
	// Native Storage protects even zero-row DELETE statements. Enable its
	// supported maintenance guard only inside this exact-owned-ID transaction.
	if _, err = tx.Exec(ctx, "SET LOCAL storage.allow_delete_query = 'true'"); err != nil {
		return errors.New("exact metadata cleanup permission setup failed")
	}
	if _, err = tx.Exec(ctx, "DELETE FROM storage.objects WHERE id::text=$1 AND bucket_id=$2 AND name=$3", f.objectID, f.bucket, "probe.json"); err != nil {
		return errors.New("exact owned object metadata deletion failed")
	}
	if _, err = tx.Exec(ctx, "DELETE FROM storage.buckets WHERE id=$1 AND name=$1 AND NOT EXISTS (SELECT 1 FROM storage.objects WHERE bucket_id=$1)", f.bucket); err != nil {
		return errors.New("exact owned bucket metadata deletion failed")
	}
	var remains bool
	if err = tx.QueryRow(ctx, metadataRemainsQuery, f.objectID, f.bucket).Scan(&remains); err != nil {
		return errors.New("exact metadata cleanup absence could not be verified")
	}
	if remains {
		return errors.New("owned metadata remains; refusing broader cleanup")
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("exact metadata cleanup commit failed")
	}
	return nil
}

// Compile-time assertions keep the fixture's native dependency boundaries clear.
var _ engine.ArtifactSource = nativeStorageArtifact{}
var _ engine.AgentDialer = nativeStorageAgents{}
var _ infra.Backend = (*nativeStorageSeam)(nil)
