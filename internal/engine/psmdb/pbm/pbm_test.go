// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const sampleList = `{"snapshots":[{"name":"2024-01-05T11:10:54Z","status":"done","restoreTo":1704453059,"type":"logical"}],"pitr":{"on":false,"ranges":[{"start":1704453060,"end":1704453137}]}}`
const sampleStatus = `{"backups":{"snapshot":[{"name":"2024-01-05T11:10:54Z","size":43178,"status":"done","restoreTo":1704453059,"type":"logical"}]},"running":{}}`
const sampleDescribe = `{"name":"2024-01-05T11:10:54Z","type":"logical","status":"done","size":43178,"last_write_ts":1704453059,"last_write_time":"2024-01-05T11:10:59Z","start":"2024-01-05T11:10:55Z","finish":"2024-01-05T11:11:01Z"}`

type testDialer struct {
	url string
	ca  []byte
}

func (d testDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test", CAPEM: d.ca})
}

type fakeHooks struct {
	reset     []string
	balancers []bool
}

func (h *fakeHooks) ResetSystemUsers(_ context.Context, s string) error {
	h.reset = append(h.reset, s)
	return nil
}
func (h *fakeHooks) SetBalancer(_ context.Context, b bool) error {
	h.balancers = append(h.balancers, b)
	return nil
}

type fakeEvents struct{ events []domain.Event }

func (e *fakeEvents) Record(_ context.Context, v domain.Event) error {
	e.events = append(e.events, v)
	return nil
}

type fakeAgent struct {
	mu                sync.Mutex
	requests          []agentapi.PBMRequest
	actions           []string
	receipts          map[string]agentapi.PBMResponse
	starts            int
	deleted           bool
	pitrDeleted       bool
	failRestore       bool
	restorePolling    int
	requireConfig     bool
	configured        bool
	activeBackup      bool
	cancelledBackup   bool
	backupCancelCalls int
}

func (f *fakeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	enc := json.NewEncoder(w)
	switch {
	case r.URL.Path == "/v1/status":
		_ = enc.Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
	case r.URL.Path == "/v1/pbm":
		var req agentapi.PBMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.requests = append(f.requests, req)
		if req.RequestID != "" {
			if cached, ok := f.receipts[req.RequestID]; ok {
				_ = enc.Encode(cached)
				return
			}
		}
		data := `{}`
		switch req.Command {
		case "config":
			if len(req.Config) > 0 {
				f.configured = true
			}
		case "list":
			if f.requireConfig && !f.configured {
				_ = enc.Encode(agentapi.PBMResponse{ExitCode: 1, Stderr: "configuration is absent"})
				return
			}
			data = sampleList
			if f.deleted {
				data = `{"snapshots":[],"pitr":{"ranges":[]}}`
			} else if f.pitrDeleted {
				data = `{"snapshots":[{"name":"2024-01-05T11:10:54Z","status":"done","type":"logical"}],"pitr":{"ranges":[]}}`
			}
		case "status":
			data = sampleStatus
		case "backup":
			f.starts++
			data = `{"name":"2024-01-05T11:10:54Z"}`
		case "restore":
			f.starts++
			raw, _ := json.Marshal("Started logical restore.\nWaiting to finish...\n{\"name\":\"2024-01-05T11:12:26.296561123Z\"}\n")
			data = string(raw)
		case "describe-backup":
			data = sampleDescribe
			if f.activeBackup {
				data = `{"name":"2024-01-05T11:10:54Z","type":"logical","status":"running"}`
				if f.cancelledBackup {
					data = `{"name":"2024-01-05T11:10:54Z","type":"logical","status":"cancelled","error":"Backup cancelled"}`
				}
			}
		case "cancel-backup":
			f.cancelledBackup = true
			f.backupCancelCalls++
		case "describe-restore":
			f.restorePolling++
			data = `{"name":"2024-01-05T11:12:26.296561123Z","status":"done","finish":"2024-01-05T11:16:13Z"}`
			if f.failRestore {
				data = `{"status":"error","error":"restore failed"}`
			}
		case "delete-backup":
			f.deleted = true
		case "delete-pitr":
			f.pitrDeleted = true
		}
		response := agentapi.PBMResponse{Output: json.RawMessage(data)}
		if req.RequestID != "" {
			f.receipts[req.RequestID] = response
		}
		_ = enc.Encode(response)
	default:
		f.actions = append(f.actions, r.URL.Path)
		_ = enc.Encode(agentapi.ActionResponse{})
	}
}

type routerBackend struct {
	infra.Backend
	agent *fakeAgent
	done  bool
}

func (b *routerBackend) Stop(_ context.Context, name string) (infra.Op, error) {
	b.agent.actions = append(b.agent.actions, "/service/"+name+"/stop")
	return infra.Op{ID: "stop"}, nil
}
func (b *routerBackend) Start(_ context.Context, name string) (infra.Op, error) {
	b.agent.actions = append(b.agent.actions, "/service/"+name+"/start")
	return infra.Op{ID: "start"}, nil
}
func (b *routerBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: b.done}, nil
}

func setup(t *testing.T) (Env, *store.Store, *fakeAgent, *fakeHooks) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fake := &fakeAgent{receipts: map[string]agentapi.PBMResponse{}}
	server := httptest.NewTLSServer(fake)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	hooks := &fakeHooks{}
	cluster := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: "psmdb"}, Internal: json.RawMessage(`{"uid":"target-id","pbmPrefix":"karakoram/target-target-id"}`)}
	if err := st.SetClusterSecret(context.Background(), domain.ClusterRef{Name: "target"}, "users/userAdmin", []byte("snapshot-password")); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackupStorage(context.Background(), &domain.BackupStorage{Name: "storage", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
		t.Fatal(err)
	}
	env := Env{RC: &engine.Context{Cluster: cluster, Store: st, Secrets: st, Agents: testDialer{server.URL, ca}, Events: &fakeEvents{}}, Nodes: []Node{{Member: "m0", ReplSet: "rs0"}, {Member: "m1", ReplSet: "rs0"}}, Storage: domain.BackupStorage{Name: "storage", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", ForcePathStyle: true, VerifyTLS: true}, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}, Prefix: "karakoram/target-target-id", PITR: domain.PITRConfig{OplogSpanMinutes: 1}, Hooks: hooks}
	env.RC.Backend = &routerBackend{agent: fake, done: true}
	return env, st, fake, hooks
}

// drive simulates losing each completed call before its checkpoint is committed. The next call resumes the last durable checkpoint.
func drive(t *testing.T, env Env, op *domain.Operation, run func(context.Context, Env, *domain.Operation) (engine.OpResult, error), wantFailure bool) engine.OpResult {
	t.Helper()
	for range 100 {
		discarded, err := run(context.Background(), env, op)
		if err != nil && !(wantFailure && discarded.Done) {
			t.Fatal(err)
		}
		// Round-trip persisted JSON, including a fresh Operation allocation as after a control-plane restart.
		raw, _ := json.Marshal(op)
		var resumed domain.Operation
		if err := json.Unmarshal(raw, &resumed); err != nil {
			t.Fatal(err)
		}
		got, err := run(context.Background(), env, &resumed)
		if err != nil && !(wantFailure && got.Done) {
			t.Fatal(err)
		}
		*op = resumed
		op.Checkpoint = append(json.RawMessage(nil), got.Checkpoint...)
		if got.Done {
			return got
		}
	}
	t.Fatal("operation did not terminate")
	return engine.OpResult{}
}

func TestBackupCrashResume(t *testing.T) {
	env, st, fake, _ := setup(t)
	b := &domain.Backup{Name: "manual", Cluster: "target", Storage: "storage", Type: "logical"}
	if err := st.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "backup-op", Payload: json.RawMessage(`{"backup":"manual"}`)}
	drive(t, env, op, RunBackup, false)
	got, err := st.GetBackup(context.Background(), domain.ClusterRef{Name: "target"}, "manual")
	if err != nil {
		t.Fatal(err)
	}
	if fake.starts != 1 || got.Phase != domain.PhaseSucceeded || got.Size != 43178 || got.LastWriteTime == nil || got.CompletedAt == nil {
		t.Fatalf("starts=%d backup=%+v", fake.starts, got)
	}
}

func TestInventoryPreservesBackupCompletionTime(t *testing.T) {
	finished := time.Date(2024, 1, 5, 13, 31, 18, 0, time.UTC)
	for _, status := range []string{"done", "error", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			b := domain.Backup{CompletedAt: &finished}
			applySnapshot(&b, snapshot{Status: status}, finished.Add(time.Minute))
			if b.CompletedAt == nil || !b.CompletedAt.Equal(finished) {
				t.Fatalf("inventory changed recorded completion time: %v", b.CompletedAt)
			}
			b.CompletedAt = nil
			applySnapshot(&b, snapshot{Status: status}, finished)
			if b.CompletedAt == nil || !b.CompletedAt.Equal(finished) {
				t.Fatal("first observed completion was not recorded")
			}
			actual := finished.Add(-time.Second)
			applySnapshot(&b, snapshot{Status: status, Finish: &actual}, finished.Add(time.Minute))
			if !b.CompletedAt.Equal(actual) {
				t.Fatal("PBM's actual finish must replace the observation fallback")
			}
		})
	}
}

func TestDeleteCrashResume(t *testing.T) {
	env, st, _, _ := setup(t)
	b := &domain.Backup{Name: "manual", Cluster: "target", PBMName: "2024-01-05T11:10:54Z", Type: "logical"}
	if err := st.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "delete-op", Payload: json.RawMessage(`{"backup":"manual"}`)}
	drive(t, env, op, RunDeleteBackup, false)
	if _, err := st.GetBackup(context.Background(), domain.ClusterRef{Name: "target"}, "manual"); err == nil {
		t.Fatal("backup record retained")
	}
}

func TestRestoreCrashResume(t *testing.T) {
	for _, mode := range []string{"logical", "physical", "pitr", "clone", "sharded", "failed"} {
		t.Run(mode, func(t *testing.T) {
			env, st, fake, hooks := setup(t)
			env.PITR.Enabled = true
			env.RC.Cluster.Spec.Backup.PITR.Enabled = true
			kind, source := "logical", "target"
			if mode == "physical" {
				kind = "physical"
			}
			if mode == "sharded" {
				env.Sharded = true
				env.Nodes[0].ReplSet = "cfg"
				env.Routers = []string{"router"}
			}
			if mode == "clone" {
				source = "source"
				c := &domain.Cluster{Name: source, Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "source-storage"}}, Internal: json.RawMessage(`{"uid":"source-id","pbmPrefix":"source-prefix"}`)}
				if err := st.CreateCluster(context.Background(), c); err != nil {
					t.Fatal(err)
				}
				storage := env.Storage
				storage.Name, storage.Prefix, storage.Credentials = "source-storage", "shared-source-base", env.Credentials
				if err := st.CreateBackupStorage(context.Background(), &storage); err != nil {
					t.Fatal(err)
				}
			}
			last := time.Unix(1704453059, 0).UTC()
			b := &domain.Backup{Name: "base", Cluster: source, Storage: "storage", Type: kind, PBMName: "2024-01-05T11:10:54Z", Phase: domain.PhaseSucceeded, LastWriteTime: &last}
			if mode == "clone" {
				b.Storage = "source-storage"
			}
			if err := st.CreateBackup(context.Background(), b); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{Backup: "base"}}
			if mode == "clone" {
				r.Source.SourceCluster = source
			}
			if mode == "pitr" {
				target := time.Unix(1704453100, 0).UTC()
				r.Source.Backup = ""
				r.Source.PITRTime = &target
			}
			if err := st.CreateRestore(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			fake.failRestore = mode == "failed"
			op := &domain.Operation{ID: "restore-op", Payload: json.RawMessage(`{"restore":"restore"}`)}
			res := drive(t, env, op, RunRestore, mode == "failed")
			got, err := st.GetRestore(context.Background(), domain.ClusterRef{Name: "target"}, "restore")
			if err != nil {
				t.Fatal(err)
			}
			want := domain.PhaseSucceeded
			if mode == "failed" {
				want = domain.PhaseFailed
			}
			if fake.starts != 1 || got.Phase != want || got.CompletedAt == nil {
				t.Fatalf("starts=%d restore=%+v", fake.starts, got)
			}
			var cp checkpoint
			_ = json.Unmarshal(res.Checkpoint, &cp)
			if cp.FreshBase != (mode != "failed") {
				t.Fatalf("fresh base checkpoint: %s", res.Checkpoint)
			}
			if mode == "physical" {
				configPoll := false
				for _, req := range fake.requests {
					if req.Command == "describe-restore" && len(req.Config) > 0 && strings.Contains(strings.Join(req.Args, " "), "--config") {
						configPoll = true
					}
				}
				if !configPoll {
					t.Fatal("physical progress lacked offline storage config")
				}
				if len(fake.actions) == 0 {
					t.Fatal("physical lifecycle actions absent")
				}
			}
			if mode == "clone" {
				if len(hooks.reset) == 0 || hooks.reset[0] != b.RecoveryIdentity {
					t.Fatal(hooks.reset)
				}
				var configs []string
				for _, req := range fake.requests {
					if len(req.Config) != 0 {
						var cfg string
						_ = json.Unmarshal(req.Config, &cfg)
						configs = append(configs, cfg)
					}
				}
				if !strings.Contains(configs[0], "prefix: shared-source-base/source-prefix") || !strings.Contains(configs[len(configs)-1], "prefix: karakoram/target-target-id") {
					t.Fatal(configs)
				}
			}
			if mode == "sharded" && (len(hooks.balancers) == 0 || hooks.balancers[0] || !hooks.balancers[len(hooks.balancers)-1]) {
				t.Fatal(hooks.balancers)
			}
		})
	}
}

func TestInventoryAdoptsAndMarksMissing(t *testing.T) {
	env, st, fake, _ := setup(t)
	summary, ranges, err := SyncInventory(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	all, err := st.ListBackups(context.Background(), domain.ClusterRef{Name: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Size != 43178 || summary.LastBackup == nil || summary.PITRWindow == nil || len(ranges) != 1 || ranges[0].Start.Unix() != 1704453060 {
		t.Fatalf("backups=%+v status=%+v ranges=%+v", all, summary, ranges)
	}
	fake.deleted = true
	if _, _, err := SyncInventory(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetBackup(context.Background(), domain.ClusterRef{Name: "target"}, all[0].Name)
	if got.Phase != domain.PhaseFailed || !strings.Contains(got.Error, "missing") {
		t.Fatal(got)
	}
}

func TestEnsureConfigHashAndPITRGate(t *testing.T) {
	env, _, fake, _ := setup(t)
	env.PITR.Enabled = true
	fake.deleted = true
	hash, err := EnsureConfig(context.Background(), env, "")
	if err != nil {
		t.Fatal(err)
	}
	count := len(fake.requests)
	again, err := EnsureConfig(context.Background(), env, hash)
	if err != nil || again != hash {
		t.Fatal(again, err)
	}
	for _, req := range fake.requests[count:] {
		if req.Command == "config" {
			t.Fatal("same hash reapplied")
		}
	}
	var cfg string
	for _, req := range fake.requests {
		if req.Command == "config" {
			_ = json.Unmarshal(req.Config, &cfg)
		}
	}
	if !strings.Contains(cfg, "enabled: false") {
		t.Fatal(cfg)
	}
	fake.deleted = false
	enabled, err := EnsureConfig(context.Background(), env, hash)
	if err != nil || enabled == hash {
		t.Fatal(enabled, err)
	}
}

func TestDeleteAllPreservesProviderInternal(t *testing.T) {
	env, _, fake, _ := setup(t)
	for range 40 {
		res, err := DeleteAll(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		if res.Done {
			if !fake.deleted || !fake.pitrDeleted || !strings.Contains(string(env.RC.Cluster.Internal), "target-id") {
				t.Fatal(string(env.RC.Cluster.Internal))
			}
			return
		}
	}
	t.Fatal("cleanup did not finish")
}

func TestDecodeLast(t *testing.T) {
	var v map[string]any
	if err := decodeLast([]byte("progress {broken\n{\"name\":\"one\",\"nested\":{\"a\":1}}\n{\"name\":\"last\"}"), &v); err != nil || v["name"] != "last" {
		t.Fatal(v, err)
	}
	if _, err := payloadName(&domain.Operation{Payload: json.RawMessage(`{}`)}, "backup"); err == nil {
		t.Fatal("missing payload accepted")
	}
	if !alreadyGone(fmt.Errorf("backup not found")) {
		t.Fatal("already-gone classification")
	}
}
