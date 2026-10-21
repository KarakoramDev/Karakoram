// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/rand"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeProvider struct{ info *domain.ProviderInfo }

func (*fakeProvider) Name() string { return "fake" }
func (p *fakeProvider) Describe() domain.ProviderInfo {
	if p.info != nil {
		return *p.info
	}
	return domain.ProviderInfo{Name: "fake", DisplayName: "Fake engine", Operations: []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, domain.OperationRaiseFCV, domain.OperationRestart, domain.OperationReplaceMember, domain.OperationRotateSystemUser, domain.OperationRotateTLS}, BackupTypes: []string{"logical", "physical"}, Processes: []string{"mongod", "mongos", "pbm-agent"}, Versions: []domain.VersionInfo{{Version: "8.0", Major: "8.0", Default: true, Status: "supported"}}, Topologies: []domain.TopologyInfo{{Type: "replicaSet", Components: map[string]domain.ComponentInfo{"engine": {Defaults: domain.ComponentSpec{Replicas: 3}, MinReplicas: 1, MaxReplicas: 7}}}, {Type: "sharded", Components: map[string]domain.ComponentInfo{"engine": {Defaults: domain.ComponentSpec{Replicas: 3}}, "configServer": {Defaults: domain.ComponentSpec{Replicas: 1}}, "router": {Defaults: domain.ComponentSpec{Replicas: 1}}}}}}
}
func (*fakeProvider) ApplyDefaults(s *domain.ClusterSpec) {
	if s.Provider == "" {
		s.Provider = "fake"
	}
	if s.Version == "" {
		s.Version = "8.0"
	}
	if s.Topology.Type == "" {
		s.Topology.Type = "replicaSet"
	}
	if s.Components.Engine.Replicas == 0 {
		s.Components.Engine.Replicas = 3
	}
	if s.Components.Engine.Resources.CPUMode == "" {
		s.Components.Engine.Resources = domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}
	}
	for name, component := range s.Components.Extra {
		if component.Resources.CPUMode == "" {
			component.Resources = s.Components.Engine.Resources
			s.Components.Extra[name] = component
		}
	}
	if s.Exposure.IPType == "" {
		s.Exposure.IPType = "IPV6"
	}
	if s.Backup.PITR.OplogSpanMinutes == 0 {
		s.Backup.PITR.OplogSpanMinutes = 10
	}
	if s.TLS.Mode == "" {
		s.TLS.Mode = "requireTLS"
	}
	if s.TLS.CA == "" {
		s.TLS.CA = "selfSigned"
	}
	if s.Components.Engine.Resources.CPUMode == "" {
		s.Components.Engine.Resources = domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 1, MaxRAM: 2, MinDisk: 5, MaxDisk: 10}
	}
	if s.Topology.Type == "sharded" {
		if s.Topology.Shards == 0 {
			s.Topology.Shards = 1
		}
		if s.Components.ConfigServer.Replicas == 0 {
			s.Components.ConfigServer.Replicas = 1
		}
		if s.Components.Router.Replicas == 0 {
			s.Components.Router.Replicas = 1
		}
		s.Components.ConfigServer.Resources = s.Components.Engine.Resources
		s.Components.Router.Resources = s.Components.Engine.Resources
	}
}
func (*fakeProvider) Validate(_ context.Context, _ *domain.Cluster, c *domain.Cluster) error {
	if c.Spec.Version == "invalid" || c.Name == "" {
		return engine.ErrInvalid
	}
	return nil
}
func (*fakeProvider) Sync(context.Context, *engine.Context) (engine.Result, error) {
	return engine.Result{}, nil
}
func (*fakeProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	return rc.Cluster.Status, nil
}
func (*fakeProvider) RunOperation(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error) {
	return engine.OpResult{Done: true}, nil
}
func (*fakeProvider) Cleanup(context.Context, *engine.Context, domain.DeleteOptions) (bool, error) {
	return true, nil
}
func (*fakeProvider) MemberConfig(_ context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	if rc.Backend == nil {
		return agentapi.MemberConfig{}, engine.ErrInvalid
	}
	return agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: member, ConfigVersion: rc.Cluster.Generation}, nil
}
func (*fakeProvider) Connection(context.Context, *engine.Context) (domain.ConnectionDetails, error) {
	return domain.ConnectionDetails{Connection: domain.Connection{Hosts: []string{"prodm0:27017"}, URI: "mongodb://prodm0"}, Credentials: []domain.ConnectionCredential{{Username: "clusterAdmin", Password: "private-test-password", Database: "admin"}}}, nil
}
func (*fakeProvider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	return []domain.SystemUser{{Name: "clusterAdmin", Roles: []domain.RoleRef{{DB: "admin", Role: "clusterAdmin"}}}}, nil
}
func (*fakeProvider) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return []domain.DatabaseUser{{Name: "app", Database: "app", Roles: []domain.RoleRef{{DB: "app", Role: "readWrite"}}}}, nil
}
func (*fakeProvider) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return &domain.DatabaseUser{Name: "app", Database: "app"}, nil
}
func (*fakeProvider) CreateDatabaseUser(_ context.Context, _ *engine.Context, _ domain.DatabaseUser, password string) (string, error) {
	if password == "" {
		password = "generated-test-password"
	}
	return password, nil
}
func (*fakeProvider) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return nil
}
func (*fakeProvider) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return nil
}
func (*fakeProvider) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return []domain.DatabaseRole{{Name: "custom", Database: "app"}}, nil
}
func (*fakeProvider) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return &domain.DatabaseRole{Name: "custom", Database: "app"}, nil
}
func (*fakeProvider) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return nil
}
func (*fakeProvider) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return nil
}
func (*fakeProvider) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return nil
}
func (*fakeProvider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{Enabled: true, Mode: "full"}, nil
}
func (*fakeProvider) SetBalancer(context.Context, *engine.Context, bool) error { return nil }
func (*fakeProvider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return []domain.TimeWindow{{Start: time.Now().Add(-time.Hour), End: time.Now()}}, nil
}
func (*fakeProvider) Placement(context.Context, *engine.Context) (domain.Placement, error) {
	return domain.Placement{Warning: "Best effort", Groups: map[string][]string{"one": {"prodm0"}}}, nil
}
func (*fakeProvider) MemberLogs(context.Context, *engine.Context, string, string, int) ([]domain.MemberLogLine, error) {
	return []domain.MemberLogLine{{Timestamp: time.Now(), Process: "mongod", Message: "healthy"}}, nil
}

type fakeBackend struct{}

func (fakeBackend) Name() string { return "fake" }
func (fakeBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Logs: true, ScopedTokens: true, DedicatedCPU: true, HorizontalScaling: true}
}
func (fakeBackend) Layout() infra.Layout {
	return infra.Layout{AppDir: "/var/www", VolumeRoot: "/mnt", User: "zerops"}
}
func (fakeBackend) DeleteServiceByID(context.Context, string, string) (infra.Op, error) {
	return infra.Op{}, nil
}
func (fakeBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: "test"}, nil
}
func (fakeBackend) ImportServices(context.Context, []infra.ServiceSpec) (infra.Op, error) {
	return infra.Op{}, nil
}
func (fakeBackend) GetService(context.Context, string) (*infra.Service, error)    { return nil, nil }
func (fakeBackend) ListServices(context.Context, string) ([]infra.Service, error) { return nil, nil }
func (fakeBackend) DeleteService(context.Context, string) (infra.Op, error)       { return infra.Op{}, nil }
func (fakeBackend) Deploy(context.Context, string, infra.Deployment) (infra.Op, error) {
	return infra.Op{}, nil
}
func (fakeBackend) SetEnvSecrets(context.Context, string, map[string]string) error { return nil }
func (fakeBackend) SetEnvVars(context.Context, string, map[string]string) error    { return nil }
func (fakeBackend) Start(context.Context, string) (infra.Op, error)                { return infra.Op{}, nil }
func (fakeBackend) Stop(context.Context, string) (infra.Op, error)                 { return infra.Op{}, nil }
func (fakeBackend) Restart(context.Context, string) (infra.Op, error)              { return infra.Op{}, nil }
func (fakeBackend) SetResources(context.Context, string, infra.Resources) (infra.Op, error) {
	return infra.Op{}, nil
}
func (fakeBackend) SetContainerCount(context.Context, string, int, int) (infra.Op, error) {
	return infra.Op{}, nil
}
func (fakeBackend) EnsurePublicPort(context.Context, string, infra.PublicPort) error { return nil }
func (fakeBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) { return nil, nil }
func (fakeBackend) DeletePublicPorts(context.Context, string) error                  { return nil }
func (fakeBackend) Containers(context.Context, string) ([]infra.Container, error)    { return nil, nil }
func (fakeBackend) Logs(context.Context, string, infra.LogQuery) ([]infra.LogLine, error) {
	return nil, nil
}
func (fakeBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: true}, nil
}

type triggerSpy struct {
	mu    sync.Mutex
	names []string
}

func (t *triggerSpy) Trigger(ref domain.ClusterRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.names = append(t.names, ref.Key())
}
func testServer(t *testing.T) (*Server, *secrets.Sealer, string) {
	t.Helper()
	return testServerWithDatabase(t, filepath.Join(t.TempDir(), "test.db"))
}

func testServerWithDatabase(t *testing.T, path string) (*Server, *secrets.Sealer, string) {
	t.Helper()
	ctx := context.Background()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	a, err := auth.New(auth.Options{Store: st, Sealer: sealer, PublicURL: "http://localhost"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.CreateAccount(ctx, domain.Account{Name: "admin", Roles: []string{"role:admin"}}, "admin-test-password"); err != nil {
		t.Fatal(err)
	}
	policy, err := rbac.New(st)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := engine.NewRegistry(&fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	hub := events.NewHub(128)
	rc := engine.Context{Backend: fakeBackend{}, Store: st, Secrets: st, Events: events.NewRecorder(st, hub), Clock: engine.RealClock{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ObservationEpoch: rand.Text()}
	for _, name := range []string{"prod", "dev"} {
		c := domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: "fake", Version: "8.0", Topology: domain.Topology{Type: "replicaSet"}}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: name + "m0", Set: "rs0", Health: true}}}}
		(&fakeProvider{}).ApplyDefaults(&c.Spec)
		if err = st.CreateCluster(ctx, &c); err != nil {
			t.Fatal(err)
		}
	}
	collector := monitoring.New(monitoring.Options{Store: st})
	s, err := New(Options{Store: st, Auth: a, RBAC: policy, Registry: registry, Context: rc, Trigger: &triggerSpy{}, Hub: hub, Monitoring: collector, PublicURL: "https://localhost", Logger: rc.Logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	token, _, err := a.CreateToken(ctx, "admin", "testing", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	registerConformance(t, s)
	return s, sealer, token
}

var _ infra.Backend = fakeBackend{}
var _ engine.Provider = (*fakeProvider)(nil)
var _ engine.DataPlane = (*fakeProvider)(nil)
var _ engine.AgentConfigSource = (*fakeProvider)(nil)
