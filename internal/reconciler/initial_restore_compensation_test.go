// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestInitialMongoFailureAndCancellationDrainRunningCompensation(t *testing.T) {
	for _, scenario := range []string{"error", "partlyDone", "cancelled", "safe-cancel", "retired-create"} {
		t.Run(scenario, func(t *testing.T) {
			p := psmdb.New(psmdb.Options{})
			m, st := newTestManager(t, p)
			backend := &cleanupBackend{Backend: fake.New(fake.Options{})}
			m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
			starts, maintenanceOff, configs, polls, nativeStarts := 0, 0, 0, 0, 0
			agent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/status":
					json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
					return
				case "/v1/processes/mongod/start", "/v1/processes/pbm-agent/start":
					starts++
				case "/v1/maintenance":
					maintenanceOff++
				case "/v1/pbm":
					var req agentapi.PBMRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						return
					}
					switch req.Command {
					case "restore":
						nativeStarts++
					case "config":
						configs++
					case "describe-restore":
						polls++
						status := scenario
						if status == "retired-create" {
							status = "error"
						}
						json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{"status":"` + status + `","error":"native recovery interrupted"}`)})
						return
					}
					json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{"running":{},"snapshots":[]}`)})
					return
				}
				json.NewEncoder(w).Encode(agentapi.ActionResponse{})
			}))
			t.Cleanup(agent.Close)
			m.opts.Context.Agents = cleanupAgentDialer{agent.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agent.Certificate().Raw})}
			if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "storage", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: "source", Storage: "storage", Provider: "psmdb", Phase: domain.PhaseSucceeded, PBMName: "source-snapshot"}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb", DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}, Internal: json.RawMessage(`{"uid":"incarnation","phase":"restore","cloneQueued":true,"pbmHash":"previous","pbmPrefix":"karakoram/demo","members":[{"name":"m0","set":"rs0","role":"mongod","stage":"active"},{"name":"router","role":"mongos","stage":"active"}],"pbmMaintenance":{"m0":true},"pbmStopped":{"router":["mongos"]}}`)}
			c.Spec.Backup.Storage = "storage"
			if scenario == "safe-cancel" {
				var state map[string]json.RawMessage
				if err := json.Unmarshal(c.Internal, &state); err != nil {
					t.Fatal(err)
				}
				delete(state, "pbmMaintenance")
				delete(state, "pbmStopped")
				var err error
				c.Internal, err = json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
			}
			create := &domain.Operation{ID: "initial-create", Kind: domain.OperationCreate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
				t.Fatal(err)
			}
			step := "poll"
			if scenario == "safe-cancel" {
				step = "configure-source"
			}
			checkpoint := map[string]any{"step": step, "member": "m0", "physical": true, "pbmName": "native-restore"}
			if scenario == "safe-cancel" {
				delete(checkpoint, "pbmName")
			}
			raw, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: c.Ref().Key() + "-incarnation-initial-restore", Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Step: step, Checkpoint: raw, Payload: json.RawMessage(`{"restore":"initial-restore"}`), CancelRequested: scenario == "safe-cancel"}
			restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}, Phase: domain.PhaseRunning}
			if err := st.EnqueueRestore(t.Context(), restore, op); err != nil {
				t.Fatal(err)
			}
			terminal := false
			sawFailedWhileRunning := false
			for attempt := range 60 {
				_, _ = m.reconcile(t.Context(), c.Ref())
				current, err := st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				target, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if target.Status.Phase == domain.PhaseReady {
					t.Fatalf("failed initial restore became Ready: %+v", target.Status)
				}
				for _, condition := range target.Status.Conditions {
					if condition.Type == "Ready" && condition.Status == "True" {
						t.Fatalf("failed initial restore advertises Ready: %+v", target.Status)
					}
				}
				if current.Phase == domain.PhaseRunning && target.Status.Phase == domain.PhaseFailed {
					sawFailedWhileRunning = true
				}
				if scenario == "retired-create" && attempt == 0 {
					tracker, err := st.GetOperation(t.Context(), create.ID)
					if err != nil {
						t.Fatal(err)
					}
					tracker.Phase, tracker.Message = domain.PhaseFailed, "create deadline exceeded while native compensation remains running"
					if err := st.UpdateOperation(t.Context(), tracker); err != nil {
						t.Fatal(err)
					}
				}
				if current.Terminal() {
					expected := domain.PhaseFailed
					if scenario == "safe-cancel" {
						expected = domain.PhaseCancelled
					}
					if current.Phase != expected || current.CompletedAt == nil {
						t.Fatalf("incorrect compensation terminal outcome: %+v", current)
					}
					terminal = true
					break
				}
			}
			if !terminal || !sawFailedWhileRunning {
				t.Fatalf("failure observation starved compensation: terminal=%v failed-running=%v starts=%d configs=%d polls=%d", terminal, sawFailedWhileRunning, starts, configs, polls)
			}
			if nativeStarts != 0 {
				t.Fatal("compensation replayed native restore initiation")
			}
			if scenario != "safe-cancel" && (starts < 2 || maintenanceOff == 0 || backend.starts == 0 || configs == 0) {
				t.Fatalf("native process/maintenance/router cleanup incomplete: starts=%d maintenance=%d routers=%d configs=%d", starts, maintenanceOff, backend.starts, configs)
			}
			if scenario == "safe-cancel" && (configs == 0 || polls != 0) {
				t.Fatalf("safe pre-start cancellation did not finish without native restore: configs=%d polls=%d", configs, polls)
			}
			target, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Maintenance map[string]bool     `json:"pbmMaintenance"`
				Stopped     map[string][]string `json:"pbmStopped"`
			}
			if err := json.Unmarshal(target.Internal, &state); err != nil {
				t.Fatal(err)
			}
			if target.Status.Phase != domain.PhaseFailed || state.Maintenance["m0"] || len(state.Stopped["router"]) != 0 {
				t.Fatalf("terminal failed restore left holds/readiness wrong: status=%+v state=%+v", target.Status, state)
			}
			if !strings.Contains(target.Status.Message, op.ID) {
				t.Fatalf("failure lost owning operation: %+v", target.Status)
			}
		})
	}
}

func TestInitialPGTerminalQueueFailureDrainsFrozenRecoveryJournal(t *testing.T) {
	for _, name := range []string{"postgres", "supabase"} {
		t.Run(name, func(t *testing.T) {
			var provider engine.Provider = postgres.New(postgres.Options{})
			if name == "supabase" {
				provider = supabase.New(postgres.Options{})
			}
			m, st := newTestManager(t, provider)
			m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{})}
			reloads, nativeStarts := 0, 0
			agent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/reload":
					reloads++
				case "/v1/postgres/restore":
					nativeStarts++
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
					return
				case "/v1/postgres/status":
					_ = json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", Version: "17.11.1", BootstrapGeneration: 1})
					return
				}
				_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{})
			}))
			t.Cleanup(agent.Close)
			m.opts.Context.Agents = cleanupAgentDialer{agent.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agent.Certificate().Raw})}
			if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: "source", Storage: "archive", Provider: name, Type: "full", SourceVersion: "17.11.1", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source"}}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, Version: "17.11.1", Topology: domain.Topology{Type: "single"}, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
			create := &domain.Operation{ID: "create", Kind: domain.OperationCreate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}, Phase: domain.PhaseRunning}
			op := &domain.Operation{ID: "native", Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"initial-restore"}`), Checkpoint: json.RawMessage(`{"step":"publish-access"}`)}
			if err := st.EnqueueRestore(t.Context(), r, op); err != nil {
				t.Fatal(err)
			}
			journal, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			root := map[string]any{
				"postgres": map[string]any{
					"uid": "incarnation", "topology": "single", "formed": true, "bootstrapGeneration": 1,
					"members": []map[string]any{{"name": "targetp0", "role": "data", "stage": "joined", "topology": "single", "version": "17.11.1"}},
					// Unrelated pending infrastructure must not preempt the
					// frozen native journal after the queue owner retires.
					"pending":         map[string]any{"member": "proxy", "action": "deploy", "op": map[string]any{"id": "unplaceable"}},
					"restoreRecovery": json.RawMessage(journal),
				},
				"postgresBackup": map[string]any{"initialQueued": true, "cloneComplete": true},
			}
			c.Internal, err = json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			op.Phase, op.Message = domain.PhaseFailed, "native queue deadline; frozen recovery journal retained"
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				_, _ = m.reconcile(t.Context(), c.Ref())
			}
			stored, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			pending, err := provider.(engine.InitialDataSourceRecoveryPolicy).InitialDataSourceRecoveryPending(stored)
			if err != nil || pending || reloads == 0 || nativeStarts != 0 {
				t.Fatalf("terminal queue failure stranded/replayed native recovery: pending=%v reloads=%d starts=%d err=%v state=%s", pending, reloads, nativeStarts, err, stored.Internal)
			}
			if stored.Status.Phase != domain.PhaseFailed {
				t.Fatalf("completed compensation made an aborted create Ready: %+v", stored.Status)
			}
			current, err := st.GetOperation(t.Context(), op.ID)
			if err != nil || current.Phase != domain.PhaseFailed {
				t.Fatalf("recovery rewrote terminal operation history: %+v %v", current, err)
			}
			before := reloads
			_, _ = m.reconcile(t.Context(), c.Ref())
			if reloads != before {
				t.Fatal("ordinary convergence resumed after the frozen recovery journal drained")
			}
		})
	}
}
