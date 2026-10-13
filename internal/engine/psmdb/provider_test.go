// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

type fakeInfra struct {
	services    map[string]*infra.Service
	mutations   int
	done        bool
	failed      bool
	deployments []infra.Deployment
	imports     [][]infra.ServiceSpec
	publicPorts []infra.PublicPortRoute
	exposed     []string
	unexposed   []string
}

func (f *fakeInfra) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return f.publicPorts, nil
}

func (f *fakeInfra) Name() string { return "test" }
func (f *fakeInfra) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv6: "2001:db8::1"}, nil
}
func (f *fakeInfra) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	f.mutations++
	f.imports = append(f.imports, specs)
	for _, s := range specs {
		f.services[s.Hostname] = &infra.Service{Hostname: s.Hostname, ID: s.Hostname, Type: s.Type, Resources: s.Resources, Ownership: s.Env}
	}
	return infra.Op{ID: "import"}, nil
}
func (f *fakeInfra) GetService(_ context.Context, n string) (*infra.Service, error) {
	return f.services[n], nil
}
func (f *fakeInfra) ListServices(context.Context, string) ([]infra.Service, error) {
	out := []infra.Service{}
	for _, v := range f.services {
		out = append(out, *v)
	}
	return out, nil
}
func (f *fakeInfra) DeleteService(_ context.Context, n string) (infra.Op, error) {
	f.mutations++
	delete(f.services, n)
	return infra.Op{ID: "delete"}, nil
}
func (f *fakeInfra) Deploy(_ context.Context, _ string, d infra.Deployment) (infra.Op, error) {
	f.mutations++
	if _, e := io.ReadAll(d.Artifact); e != nil {
		return infra.Op{}, e
	}
	d.Artifact = nil
	f.deployments = append(f.deployments, d)
	return infra.Op{ID: "deploy"}, nil
}
func (*fakeInfra) SetEnvSecrets(context.Context, string, map[string]string) error { return nil }
func (f *fakeInfra) Start(context.Context, string) (infra.Op, error) {
	f.mutations++
	return infra.Op{ID: "start"}, nil
}
func (f *fakeInfra) Stop(context.Context, string) (infra.Op, error) {
	f.mutations++
	return infra.Op{ID: "stop"}, nil
}
func (f *fakeInfra) Restart(context.Context, string) (infra.Op, error) {
	f.mutations++
	return infra.Op{ID: "restart"}, nil
}
func (f *fakeInfra) SetResources(context.Context, string, infra.Resources) (infra.Op, error) {
	f.mutations++
	return infra.Op{ID: "resources"}, nil
}
func (f *fakeInfra) SetContainerCount(context.Context, string, int, int) (infra.Op, error) {
	f.mutations++
	return infra.Op{ID: "containers"}, nil
}
func (f *fakeInfra) EnsurePublicPort(_ context.Context, name string, _ infra.PublicPort) error {
	f.exposed = append(f.exposed, name)
	return nil
}
func (f *fakeInfra) DeletePublicPorts(_ context.Context, name string) error {
	f.unexposed = append(f.unexposed, name)
	return nil
}
func (*fakeInfra) Containers(context.Context, string) ([]infra.Container, error) {
	return []infra.Container{{Status: "ACTIVE"}}, nil
}
func (*fakeInfra) Logs(context.Context, string, infra.LogQuery) ([]infra.LogLine, error) {
	return nil, nil
}
func (f *fakeInfra) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: f.done, Failed: f.failed, Message: "failed operation"}, nil
}

type fakeArtifact struct{}

func (fakeArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type testDialer struct {
	url string
	ca  []byte
}

func (d testDialer) Dial(context.Context, string, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test-token", CAPEM: d.ca})
}
func setup(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *int) {
	t.Helper()
	sealer, e := secrets.New(strings.Repeat("x", 32))
	if e != nil {
		t.Fatal(e)
	}
	st, e := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := New(Options{Store: st, InternalCAPEM: "control-plane-ca"})
	c := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Replicas = 1
	if e = st.CreateCluster(context.Background(), c); e != nil {
		t.Fatal(e)
	}
	boot := new(int)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing bearer authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "mongos": {State: "running"}}, EngineVersion: "8.0.32-14", ConfigVersion: 1, HostFingerprint: "host-a"})
		case "/v1/bootstrap":
			*boot++
			var req agentapi.BootstrapRequest
			if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
				t.Error(e)
			}
			if req.ReplSetName != "rs0" || len(req.Members) != 1 || req.UserAdmin.Username != "userAdmin" {
				t.Errorf("bootstrap request %+v", req)
			}
			_ = json.NewEncoder(w).Encode(agentapi.BootstrapResponse{Initialized: true, UserCreated: true})
		case "/v1/reload", "/v1/processes/mongos/restart":
			_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		default:
			http.Error(w, "not found", 404)
		}
	}))
	t.Cleanup(server.Close)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	f := &fakeInfra{services: map[string]*infra.Service{}, done: true}
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: f, Agents: testDialer{server.URL, cert}, Artifact: fakeArtifact{}}
	return p, rc, f, boot
}
func TestCatalogAndValidation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	if e := p.Validate(ctx, nil, rc.Cluster); e != nil {
		t.Fatal(e)
	}
	info := p.Describe()
	if len(info.Versions) < 5 || len(info.UpgradePaths) < 3 {
		t.Fatal(info)
	}
	for _, v := range info.Versions {
		if strings.HasPrefix(v.Version, "8.3") {
			t.Fatal("unverified rapid release in supported catalog")
		}
	}
	for _, test := range []struct {
		name   string
		change func(*domain.Cluster)
	}{{"name", func(c *domain.Cluster) { c.Name = "bad-name" }}, {"even", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 2 }}, {"public", func(c *domain.Cluster) { c.Spec.Exposure.Public = true }}, {"CIDR", func(c *domain.Cluster) { c.Spec.Exposure.Allowlist = []string{"not-cidr"} }}, {"bounds", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MaxRAM = 0.1 }}, {"fractional CPU", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinCPU = 0.25 }}, {"managed config", func(c *domain.Cluster) { c.Spec.EngineConfig = "security:\n  authorization: disabled\n" }}, {"storage", func(c *domain.Cluster) { c.Spec.Backup.Storage = "missing" }}} {
		t.Run(test.name, func(t *testing.T) {
			c := *rc.Cluster
			test.change(&c)
			if e := p.Validate(ctx, nil, &c); !errors.Is(e, engine.ErrInvalid) {
				t.Fatalf("expected invalid, got %v", e)
			}
		})
	}
	old := *rc.Cluster
	c := old
	c.Spec.Components.Engine.Resources.MaxDisk = 4
	if e := p.Validate(ctx, &old, &c); e == nil {
		t.Fatal("disk shrink accepted")
	}
	c = old
	c.Spec.Version = "7.0.43-23"
	if e := p.Validate(ctx, &old, &c); e == nil {
		t.Fatal("downgrade accepted")
	}
	for _, test := range []struct {
		a, b    string
		allowed bool
	}{{"6.0.29-23", "8.0.32-14", false}, {"7.0.40-22", "7.0.43-23", true}, {"7.0.43-23", "8.0.29-13", true}, {"8.0.32-14", "8.0.29-13", false}} {
		if upgrade(test.a, test.b) != test.allowed {
			t.Fatalf("upgrade %s -> %s", test.a, test.b)
		}
	}
}
func TestPlanNamingMonotonicAndShards(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Name = "abcdefghijkl"
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 2}
	p.ApplyDefaults(&rc.Cluster.Spec)
	s, _ := load(rc.Cluster)
	if !plan(rc.Cluster, s) {
		t.Fatal("no plan")
	}
	if len(s.Members) != 6 {
		t.Fatal(len(s.Members))
	}
	for _, m := range s.Members {
		if len(m.Name) > 25 || len(m.Volume) > 25 {
			t.Fatal(m.Name)
		}
	}
	first := s.setMembers("rs0")[0]
	removeRecord(s, first.Name)
	next := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, 0)
	if next.Index <= first.Index || next.Name == first.Name {
		t.Fatal("reused member index")
	}
	if plan(rc.Cluster, s) {
		t.Fatal("stable plan changed")
	}
}
func TestZeropsYAMLAndConfig(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	m := s.Members[0]
	if e := p.ensureSecrets(context.Background(), rc, s); e != nil {
		t.Fatal(e)
	}
	if e := save(rc.Cluster, s); e != nil {
		t.Fatal(e)
	}
	text, e := p.zeropsYAML(rc.Cluster, m, "8.0.29-13")
	if e != nil {
		t.Fatal(e)
	}
	var doc map[string]any
	if e = yaml.Unmarshal([]byte(text), &doc); e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"temporaryShutdown: true", "/var/www/karakoram agent readiness", "failureTimeout: 300s", "retryPeriod: 10s", "ubuntu@24.04", "hostname: demom0v", "PERCONA_TELEMETRY_DISABLE=1", "--scheme https", "percona-server-mongodb-server=8.0.29-13.noble", "percona-backup-mongodb=2.16.0-1.noble", "KARAKORAM_URL", "./karakoram agent"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %s", want, text)
		}
	}
	cfg, e := p.MemberConfig(context.Background(), rc, m.Name)
	if e != nil {
		t.Fatal(e)
	}
	if cfg.WiredTigerCacheSizeGB != 0.25 || cfg.PBMEnabled || len(cfg.SystemUsers) != 2 || cfg.Keyfile == "" {
		t.Fatal(cfg)
	}
	block, _ := pem.Decode([]byte(cfg.TLS.CertPEM))
	cert, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	if e = cert.VerifyHostname("127.0.0.1"); e != nil {
		t.Fatal(e)
	}
	if e = cert.VerifyHostname("2001:db8::1"); e != nil {
		t.Fatal(e)
	}
	before := cfg.ConfigVersion
	s.ConfigVersion++
	s.TLSMode = "preferTLS"
	rc.Cluster.Spec.EngineConfig = "operationProfiling:\n  slowOpThresholdMs: 200\n"
	_ = save(rc.Cluster, s)
	cfg, e = p.MemberConfig(context.Background(), rc, m.Name)
	if e != nil || cfg.ConfigVersion != before || cfg.TLS.Mode != "requireTLS" || cfg.EngineConfig != "" {
		t.Fatal("configuration escaped rolling gate", cfg, e)
	}
}
func TestSyncInitialStateMachineAndPending(t *testing.T) {
	p, rc, f, boot := setup(t)
	ctx := context.Background()
	for range 20 {
		before := f.mutations
		if _, e := p.Sync(ctx, rc); e != nil {
			t.Fatal(e)
		}
		if f.mutations-before > 1 {
			t.Fatal("multiple infrastructure mutations in a call")
		}
		s, _ := load(rc.Cluster)
		if s.Pending != nil && s.Pending.Action == "import" {
			f.done = false
			before := f.mutations
			if _, e := p.Sync(ctx, rc); e != nil {
				t.Fatal(e)
			}
			if f.mutations != before {
				t.Fatal("advanced past pending import")
			}
			f.done = true
		}
		if s.Sets["rs0"].Stage == "users" {
			if *boot != 1 || len(f.imports) != 1 || len(f.deployments) != 1 {
				t.Fatal(*boot, len(f.imports), len(f.deployments))
			}
			token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Name, "demom0")
			if e != nil || token.Hash != secrets.Hash(token.Token) {
				t.Fatal(token, e)
			}
			if f.imports[0][1].ZeropsYAML != "" {
				t.Fatal("inline YAML would trigger empty-artifact build")
			}
			return
		}
	}
	t.Fatal("bootstrap did not complete")
}
func TestPendingFailureAndUnknownInternal(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	s.UID = "pending-fixture"
	plan(rc.Cluster, s)
	s.Pending = &pending{Op: infra.Op{ID: "failure"}, Member: s.Members[0].Name, Action: "deploy"}
	rc.Cluster.Internal = json.RawMessage(`{"pbmCleanup":{"step":"observe"}}`)
	_ = save(rc.Cluster, s)
	f.failed = true
	if _, e := p.Sync(context.Background(), rc); e == nil {
		t.Fatal("failed process ignored")
	}
	if !strings.Contains(string(rc.Cluster.Internal), "pbmCleanup") {
		t.Fatal("lost peer state")
	}
}
func TestCleanupRetainsAndResumes(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	s.UID = "retention-fixture"
	plan(rc.Cluster, s)
	_ = p.ensureSecrets(ctx, rc, s)
	_ = save(rc.Cluster, s)
	m := s.Members[0]
	m.ServiceID, m.VolumeID = m.Name, m.Volume
	_ = save(rc.Cluster, s)
	f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name}
	f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume}
	for range 12 {
		done, e := p.Cleanup(ctx, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if e != nil {
			t.Fatal(e)
		}
		if done {
			if f.services[m.Volume] == nil || f.services[m.Name] != nil || rc.Cluster.Status.Phase != domain.PhaseSuspended {
				t.Fatal("retention failed")
			}
			if _, e = p.Sync(ctx, rc); e != nil {
				t.Fatal(e)
			}
			s, _ := load(rc.Cluster)
			if s.Suspended || s.Members[0].Stage != "planned" {
				t.Fatal("resume not planned")
			}
			return
		}
	}
	t.Fatal("cleanup did not complete")
}
func TestProjectPortReservations(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	a, e := p.allocatePublicPort(context.Background(), rc, s, s.Members[0])
	if e != nil || a != 27017 {
		t.Fatal(a, e)
	}
	other := *rc.Cluster
	other.Name = "other"
	other.Internal = nil
	otherRC := *rc
	otherRC.Cluster = &other
	ss, _ := load(&other)
	plan(&other, ss)
	b, e := p.allocatePublicPort(context.Background(), &otherRC, ss, ss.Members[0])
	if e != nil || b != 27018 {
		t.Fatal(b, e)
	}
	again, e := p.allocatePublicPort(context.Background(), rc, s, s.Members[0])
	if e != nil || again != a {
		t.Fatal(again, e)
	}
}
func TestPlacementWarnsHonestly(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	plan(rc.Cluster, s)
	_ = save(rc.Cluster, s)
	v, e := p.Placement(context.Background(), rc)
	if e != nil {
		t.Fatal(e)
	}
	if len(v.Groups["host-a"]) != 3 || !strings.Contains(v.Warning, "co-located") || !strings.Contains(v.Warning, "does not guarantee") {
		t.Fatal(v)
	}
}
func TestMemberLogsBeforeDeploymentIsConflict(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	_ = save(rc.Cluster, s)
	if _, e := p.MemberLogs(context.Background(), rc, s.Members[0].Name, "mongod", 10); !errors.Is(e, engine.ErrConflict) {
		t.Fatalf("planned member logs: %v", e)
	}
	if _, e := p.MemberLogs(context.Background(), rc, "missing", "mongod", 10); !errors.Is(e, engine.ErrNotFound) {
		t.Fatalf("unknown member logs: %v", e)
	}
}
func TestSystemRoleHiddenFromRoleAPI(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	if _, e := p.GetDatabaseRole(ctx, rc, "admin", "pbmAnyAction"); !errors.Is(e, engine.ErrNotFound) {
		t.Fatalf("get: %v", e)
	}
	if e := p.CreateDatabaseRole(ctx, rc, domain.DatabaseRole{Database: "admin", Name: "pbmAnyAction"}); !errors.Is(e, engine.ErrInvalid) {
		t.Fatalf("create: %v", e)
	}
	if e := p.UpdateDatabaseRole(ctx, rc, domain.DatabaseRole{Database: "admin", Name: "pbmAnyAction"}); !errors.Is(e, engine.ErrNotFound) {
		t.Fatalf("update: %v", e)
	}
	if e := p.DeleteDatabaseRole(ctx, rc, "admin", "pbmAnyAction"); !errors.Is(e, engine.ErrNotFound) {
		t.Fatalf("delete: %v", e)
	}
}

func TestRestoreDesiredProcessHolds(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	m := s.add(rc.Cluster.Name, "router", agentapi.RoleRouter, -1)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := setInternal(rc.Cluster, "pbmMaintenance", map[string]bool{m.Name: true}); err != nil {
		t.Fatal(err)
	}
	if err := setInternal(rc.Cluster, "pbmStopped", map[string][]string{m.Name: {"mongos"}}); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(ctx, rc, m.Name)
	if err != nil || !cfg.Maintenance || cfg.DesiredProcesses == nil || len(cfg.DesiredProcesses) != 0 {
		t.Fatal(cfg, err)
	}
}

func TestRestartOperationCheckpoint(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	m := s.add(rc.Cluster.Name, "router", agentapi.RoleRouter, -1)
	m.Stage, m.Version = "active", rc.Cluster.Spec.Version
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "restart-op", Kind: domain.OperationRestart}
	for range 4 {
		result, err := p.RunOperation(ctx, rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			var checkpoint operationState
			_ = json.Unmarshal(result.Checkpoint, &checkpoint)
			if len(checkpoint.Done) != 1 || checkpoint.Done[0] != m.Name {
				t.Fatal(checkpoint)
			}
			return
		}
	}
	t.Fatal("rolling router restart did not finish")
}

func TestCleanupDeletesNodeSecrets(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	s.UID = "delete-fixture"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	_ = save(rc.Cluster, s)
	m := s.Members[0]
	m.ServiceID, m.VolumeID = m.Name, m.Volume
	_ = save(rc.Cluster, s)
	f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name}
	f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume}
	for range 12 {
		done, err := p.Cleanup(ctx, rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			continue
		}
		if len(f.services) != 0 {
			t.Fatal(f.services)
		}
		for _, key := range []string{"tls/ca.key", "users/backup", "members/" + m.Name + "/users/backup", "tls/members/" + m.Name + ".pem"} {
			if _, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Name, key); !missing(err) {
				t.Fatalf("secret %s survived: %v", key, err)
			}
		}
		if _, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Name, m.Name); !missing(err) {
			t.Fatal("agent token survived", err)
		}
		return
	}
	t.Fatal("cleanup did not complete")
}

func TestCleanupFailedProvisioningWithUnusedBackupStorage(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	s.UID = "failed-provisioning"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = "never-configured"
	m := s.Members[0]
	m.ServiceID, m.VolumeID = m.Name, m.Volume
	f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name}
	f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume}
	s.Pending = &pending{Op: infra.Op{ID: "failed-deploy"}, Action: "deploy", Member: m.Name}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	f.failed = true
	if done, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err != nil || done {
		t.Fatalf("failed provisioning must not block deletion: done=%t err=%v", done, err)
	}
	f.failed = false
	for range 12 {
		done, err := p.Cleanup(ctx, rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if len(f.services) != 0 {
				t.Fatalf("services survived cleanup: %v", f.services)
			}
			return
		}
	}
	t.Fatal("failed provisioning cleanup did not complete")
}

func TestCleanupFailedDeletionRemainsAnError(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	s.Pending = &pending{Op: infra.Op{ID: "failed-delete"}, Action: "cleanup-runtime"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	f.failed = true
	if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{}); err == nil {
		t.Fatal("failed deletion must remain visible")
	}
}

func TestHighestIndexRemovalPlan(t *testing.T) {
	_, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	plan(rc.Cluster, s)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	plan(rc.Cluster, s)
	toRemove := removingMembers(s)
	if len(toRemove) != 4 || toRemove[0].Index != 4 || toRemove[3].Index != 1 {
		t.Fatal(toRemove)
	}
	if plan(rc.Cluster, s) {
		t.Fatal("removing members caused new replacements")
	}
}

func TestAlternateStoragePrefixAndPITRDisabled(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "alt", Type: "s3", Bucket: "bucket", Prefix: "custom", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	s.PBMPrefix = "karakoram/demo-uid"
	plan(rc.Cluster, s)
	env, err := p.pbmEnvForStorage(ctx, rc, s, "alt")
	if err != nil || env.Storage.Name != "alt" || env.Prefix != "custom/karakoram/demo-uid" || env.Credentials.SecretAccessKey != "secret" {
		t.Fatal(env, err)
	}
	if len(s.UsedStorages) != 1 || s.UsedStorages[0] != "alt" {
		t.Fatal(s.UsedStorages)
	}
	windows, err := p.PITRWindow(ctx, rc)
	if err != nil || len(windows) != 0 {
		t.Fatal(windows, err)
	}
}

func TestMajorUpgradeRequiresObservedFCV(t *testing.T) {
	p, rc, _, _ := setup(t)
	old := *rc.Cluster
	old.Spec.Version = "7.0.43-23"
	if err := p.Validate(context.Background(), &old, rc.Cluster); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("major upgrade accepted without observed FCV", err)
	}
	old.Status.FCV = "7.0"
	if err := p.Validate(context.Background(), &old, rc.Cluster); err != nil {
		t.Fatal(err)
	}
}

type allocatedInfra struct{ *fakeInfra }

func (*allocatedInfra) Containers(context.Context, string) ([]infra.Container, error) {
	return []infra.Container{{Status: "ACTIVE", Resources: &domain.ObservedResources{CPUCores: 1, MemoryGB: 1, DiskGB: 1}}}, nil
}

func TestStatusSeparatesFreshPlatformAutoscalingFromAllocation(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Backend = &allocatedInfra{fakeInfra: f}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "observed-autoscaling"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	name := s.Members[0].Name
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, bounds := range []domain.Resources{
		{CPUMode: "SHARED", MinCPU: 2, MaxCPU: 2, MinRAM: 0.5, MaxRAM: 0.5, MinDisk: 1, MaxDisk: 3},
		{CPUMode: "DEDICATED", MinCPU: 3, MaxCPU: 3, MinRAM: 2, MaxRAM: 2, MinDisk: 1, MaxDisk: 3},
	} {
		f.services[name] = &infra.Service{ID: name, Hostname: name, Resources: bounds}
		status, err := p.Status(context.Background(), rc)
		if err != nil || len(status.Members) != 1 {
			t.Fatal(status, err)
		}
		m := status.Members[0]
		if m.Autoscaling == nil || *m.Autoscaling != bounds {
			t.Fatalf("autoscaling must read current platform config, not desired/applied state: %+v", m)
		}
		if m.Resources == nil || m.Resources.CPUCores != 1 || m.Resources.MemoryGB != 1 {
			t.Fatal("configuration was confused with allocated hardware", m)
		}
	}
	f.services[name].Resources = domain.Resources{}
	status, err := p.Status(context.Background(), rc)
	if err != nil || status.Members[0].Autoscaling != nil {
		t.Fatal("unknown platform configuration must remain unknown", status, err)
	}
}
