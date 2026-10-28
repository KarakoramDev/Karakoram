// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type testArtifact struct{}

func (testArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type testAgents struct {
	url string
	ca  []byte
}

func (a testAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: "token"})
}

type agentState struct {
	config              agentapi.MemberConfig
	spec                neo4jspec.Spec
	password            string
	rotations, restarts int
	failPasswordReply   bool
	credentialMismatch  bool
	userCode            int
	users               map[string]neo4jspec.User
	userPasswords       map[string]string
	adminErrorCode      int
	recoveryReceipt     string
	recoveryPassword    string
	recoveries          int
	failRecoveryReply   bool
	failRecoveryJob     bool
}

func setup(t *testing.T) (*Provider, *engine.Context, *fake.Backend, *agentState) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	p := New(Options{ControlPlaneURL: "https://control.example.test"})
	c := &domain.Cluster{Name: "demo", Namespace: domain.DefaultNamespace, Generation: 1}
	p.ApplyDefaults(&c.Spec)
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	b := fake.New(fake.Options{})
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: b, Artifact: testArtifact{}}
	a := &agentState{users: map[string]neo4jspec.User{}, userPasswords: map[string]string{}}
	pull := func() {
		s, _ := load(c)
		if s.Member == nil {
			return
		}
		cfg, e := p.MemberConfig(t.Context(), rc, s.Member.Name)
		if e != nil {
			return
		}
		a.config = cfg
		json.Unmarshal(cfg.EngineSpec, &a.spec)
		if a.password == "" {
			a.password = a.spec.AdminPassword
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/reload":
			pull()
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Version: "agent-version", Commit: "agent-commit", Build: "agent-build"}, ConfigVersion: a.config.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"neo4j": {State: "running"}}})
		case "/v1/neo4j/readiness":
			json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: a.config.ConfigVersion > 0, ConfigVersion: a.config.ConfigVersion, Version: a.spec.Version, Edition: "community", AppliedRestartToken: a.spec.RestartToken, CredentialMismatch: a.credentialMismatch || a.password != a.spec.AdminPassword})
		case "/v1/processes/neo4j/restart", "/v1/processes":
			a.restarts++
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/neo4j/admin-password":
			if a.adminErrorCode != 0 {
				http.Error(w, "administrator action unavailable", a.adminErrorCode)
				return
			}
			var req neo4jspec.AdminPasswordRequest
			json.NewDecoder(r.Body).Decode(&req)
			if a.password != req.Next {
				if a.password != req.Current {
					http.Error(w, "Neo4j administrator credentials do not authenticate", http.StatusConflict)
					return
				}
				a.password = req.Next
				a.rotations++
			}
			if a.failPasswordReply {
				a.failPasswordReply = false
				http.Error(w, "interrupted", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(struct{}{})
		case "/v1/neo4j/recover-admin":
			var req neo4jspec.RecoverAdminRequest
			json.NewDecoder(r.Body).Decode(&req)
			if a.recoveryReceipt == "" {
				a.recoveryReceipt = req.Receipt
				a.recoveryPassword = req.AdminPassword
				a.recoveries++
				json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobRunning})
				return
			}
			if req.Receipt != a.recoveryReceipt || req.AdminPassword != a.recoveryPassword {
				http.Error(w, "receipt conflict", http.StatusConflict)
				return
			}
			if a.failRecoveryJob {
				json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobFailed, Message: "password=do-not-leak"})
				return
			}
			a.password = req.AdminPassword
			if a.failRecoveryReply {
				a.failRecoveryReply = false
				http.Error(w, "password=do-not-leak", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobDone})
		case "/v1/neo4j/users":
			if a.userCode != 0 {
				http.Error(w, "password=do-not-leak", a.userCode)
				return
			}
			var req neo4jspec.UserRequest
			json.NewDecoder(r.Body).Decode(&req)
			if req.Op == neo4jspec.UserCreate || req.Op == neo4jspec.UserPassword {
				a.userPasswords[req.Name] = req.Password
			}
			out := neo4jspec.UserResponse{}
			switch req.Op {
			case neo4jspec.UserList:
				for _, u := range a.users {
					out.Users = append(out.Users, u)
				}
			case neo4jspec.UserCreate:
				if _, ok := a.users[req.Name]; ok {
					http.Error(w, "exists", http.StatusConflict)
					return
				}
				a.users[req.Name] = neo4jspec.User{Name: req.Name}
			case neo4jspec.UserGet:
				u, ok := a.users[req.Name]
				if !ok {
					http.Error(w, "missing", 404)
					return
				}
				out.User = &u
			case neo4jspec.UserDelete:
				delete(a.users, req.Name)
			}
			json.NewEncoder(w).Encode(out)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, b, a
}
func converge(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	for range 50 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if s.Phase == domain.PhaseReady {
			rc.Cluster.Status = s
			return
		}
	}
	t.Fatal("singleton did not become ready")
}
func TestMemoryMB(t *testing.T) {
	for _, tc := range []struct {
		ram        float64
		heap, page int
	}{{1, 384, 256}, {2, 768, 512}, {4, 1600, 1024}, {16, 6528, 4096}, {64, 26176, 16384}} {
		h, p := MemoryMB(tc.ram)
		if h != tc.heap || p != tc.page {
			t.Fatalf("%g GiB: %d/%d", tc.ram, h, p)
		}
	}
}
func TestDescribe(t *testing.T) {
	info := New(Options{}).Describe()
	if info.Name != "neo4j" || info.DisplayName != "Neo4j Community" || len(info.Topologies) != 1 || info.Topologies[0].Type != "single" || len(info.UpgradePaths) != 1 || !info.UpgradePaths[0].RequiresBackup {
		t.Fatal(info)
	}
	if !reflect.DeepEqual(info.BackupTypes, []string{"dump"}) {
		t.Fatal(info.BackupTypes)
	}
	if len(New(Options{}).Metrics()) != 10 {
		t.Fatal("metrics catalog")
	}
}
func TestValidation(t *testing.T) {
	p := New(Options{})
	base := domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&base.Spec)
	if err := p.Validate(t.Context(), nil, &base); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Cluster)
	}{
		{"replicas", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 2 }},
		{"topology", func(c *domain.Cluster) { c.Spec.Topology.Type = "ha" }},
		{"shards", func(c *domain.Cluster) { c.Spec.Topology.Shards = 1 }},
		{"router", func(c *domain.Cluster) { c.Spec.Components.Router.Replicas = 1 }},
		{"config", func(c *domain.Cluster) { c.Spec.EngineConfig = "server.bolt.enabled=false" }},
		{"extra", func(c *domain.Cluster) { c.Spec.Components.Extra = map[string]domain.ComponentSpec{"proxy": {}} }},
		{"version", func(c *domain.Cluster) { c.Spec.Version = "2025.1" }},
		{"ram", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinRAM = .5 }},
		{"unknown", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"extra":1}`) }},
		{"managed", func(c *domain.Cluster) {
			c.Spec.EngineOptions = json.RawMessage(`{"settings":{"server.bolt.enabled":"false"}}`)
		}},
		{"heap-small", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"heapMB":255}`) }},
		{"page-small", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"pageCacheMB":63}`) }},
		{"memory-sum", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"heapMB":1536,"pageCacheMB":512}`) }},
		{"schedule", func(c *domain.Cluster) { c.Spec.Backup.Schedules = []domain.BackupSchedule{{Enabled: true}} }},
		{"tls", func(c *domain.Cluster) { c.Spec.TLS.Mode = "disabled" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			if err := p.Validate(t.Context(), nil, &c); !errors.Is(err, engine.ErrInvalid) {
				t.Fatal("accepted invalid spec", err)
			}
		})
	}
	n := base
	n.Spec.Version = "2026.09.0"
	if err := p.Validate(t.Context(), &base, &n); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("downgrade accepted", err)
	}
	if err := p.Validate(t.Context(), &n, &base); err != nil {
		t.Fatal(err)
	}
}
func TestCreateMemberConfigAndStatus(t *testing.T) {
	p, rc, b, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec neo4jspec.Spec
	json.Unmarshal(cfg.EngineSpec, &spec)
	if spec.HeapMB != 768 || spec.PageCacheMB != 512 || spec.Install.Neo4jHome != "/opt/karakoram/neo4j-community-2026.10.0/neo4j-community-2026.10.0" || spec.AdminPassword == "" || spec.RestartToken == "" || cfg.Volume == nil || !cfg.Volume.Initialized {
		t.Fatal("incomplete member config", cfg.Volume)
	}
	runtime, ok := b.Deployed(s.Member.Name)
	if !ok || len(runtime.Ports) != 3 || len(runtime.Prepare) != 3 {
		t.Fatal("runtime preparation", runtime)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(status)
	if strings.Contains(string(raw), spec.AdminPassword) || strings.Contains(string(rc.Cluster.Internal), spec.AdminPassword) {
		t.Fatal("secret escaped")
	}
	conn, err := p.Connection(t.Context(), rc)
	if err != nil || conn.Credentials[0].Password != spec.AdminPassword || !strings.HasPrefix(conn.Connection.URI, "neo4j+s://") {
		t.Fatal(conn, err)
	}
	s.Member.Offline = true
	save(rc.Cluster, s)
	cfg, err = p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil || cfg.DesiredProcesses == nil || len(cfg.DesiredProcesses) != 0 || !cfg.Maintenance {
		t.Fatal("offline intent", err)
	}
}
func runOp(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation) {
	t.Helper()
	for range 20 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
		if r.Done {
			return
		}
	}
	t.Fatal("operation did not complete")
}
func TestRotatePasswordCrashRetry(t *testing.T) {
	p, rc, _, a := setup(t)
	converge(t, p, rc)
	old, _ := secret(t.Context(), rc, "users/neo4j")
	op := &domain.Operation{ID: "rotate", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"neo4j"}`)}
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = r.Checkpoint
	a.failPasswordReply = true
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("missing injected interruption")
	}
	unchanged, _ := secret(t.Context(), rc, "users/neo4j")
	if string(unchanged) != string(old) {
		t.Fatal("secret committed before acknowledgement")
	}
	p = New(Options{})
	runOp(t, p, rc, op)
	next, _ := secret(t.Context(), rc, "users/neo4j")
	if string(next) == string(old) || string(next) != a.password || a.rotations != 1 {
		t.Fatal("rotation was not idempotent")
	}
	runOp(t, p, rc, op)
}
func TestRotateTLSUpgradeAndMismatch(t *testing.T) {
	p, rc, b, a := setup(t)
	rc.Cluster.Spec.Version = "2026.09.0"
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	old, _ := secret(t.Context(), rc, "tls/servers/"+s.Member.Name+".crt")
	runOp(t, p, rc, &domain.Operation{ID: "tls", Kind: domain.OperationRotateTLS})
	next, _ := secret(t.Context(), rc, "tls/servers/"+s.Member.Name+".crt")
	if reflect.DeepEqual(old, next) {
		t.Fatal("leaf not rotated")
	}
	rc.Cluster.Spec.Version = defaultVersion
	rc.Cluster.Generation++
	converge(t, p, rc)
	runtime, _ := b.Deployed(s.Member.Name)
	if len(runtime.Prepare) != 4 {
		t.Fatal("upgrade did not prepare both versions")
	}
	a.credentialMismatch = true
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase == domain.PhaseReady || len(status.Conditions) == 0 || status.Conditions[0].Type != "CredentialMismatch" {
		t.Fatal(status, err)
	}
}
func TestUsersAndErrorMapping(t *testing.T) {
	p, rc, _, a := setup(t)
	converge(t, p, rc)
	pw, err := p.CreateUser(t.Context(), rc, "alice", "")
	if err != nil || pw == "" {
		t.Fatal(err)
	}
	u, err := p.GetUser(t.Context(), rc, "alice")
	if err != nil || u.Name != "alice" {
		t.Fatal(u, err)
	}
	if _, err = p.CreateUser(t.Context(), rc, "alice", "password"); !errors.Is(err, engine.ErrConflict) {
		t.Fatal(err)
	}
	for _, name := range []string{"neo4j", "kk_admin", "a/b", "1alice"} {
		if _, err = p.CreateUser(t.Context(), rc, name, ""); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(name, err)
		}
	}
	for _, tc := range []struct {
		code int
		want error
	}{{400, engine.ErrInvalid}, {404, engine.ErrNotFound}, {409, engine.ErrConflict}} {
		a.userCode = tc.code
		_, err = p.GetUser(t.Context(), rc, "alice")
		if !errors.Is(err, tc.want) || strings.Contains(err.Error(), "do-not-leak") {
			t.Fatal(err)
		}
	}
	a.userCode = 0
	if err = p.DeleteUser(t.Context(), rc, "alice"); err != nil {
		t.Fatal(err)
	}
}
func TestCleanup(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "retain"}[keep], func(t *testing.T) {
			p, rc, b, _ := setup(t)
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			done := false
			for range 20 {
				var err error
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("cleanup incomplete")
			}
			runtime, _ := b.GetService(t.Context(), s.Member.Name)
			volume, _ := b.GetService(t.Context(), s.Member.Volume)
			if runtime != nil || (volume != nil) != keep {
				t.Fatal("incorrect retention")
			}
		})
	}
}
func TestSingleAdmissionBootstrap(t *testing.T) {
	p, rc, b, _ := setup(t)
	admission := &fake.Admission{Backend: b, Limit: 1, Peers: []string{"demon0"}}
	rc.Backend = admission
	for range 50 {
		admission.Advance()
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady {
			service, err := b.GetService(t.Context(), "demon0")
			if err != nil || service.Kind != infra.KindRuntime || admission.Peak > 1 || len(admission.Submitted) != 1 || !admission.Ready() {
				t.Fatal("admission did not converge", service, err)
			}
			return
		}
	}
	t.Fatal("admission bootstrap did not converge")
}
