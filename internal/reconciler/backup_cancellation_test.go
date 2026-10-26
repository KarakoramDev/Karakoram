// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestBackupCancellationTerminatesNativeRunningOrOrphanedPoll(t *testing.T) {
	for _, native := range []string{"running", "stuck", "done", "status-unavailable"} {
		t.Run(native, func(t *testing.T) {
			m, st := newTestManager(t, &psmdb.Provider{})
			m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{})}
			cancelled, starts, cancels := false, 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				enc := json.NewEncoder(w)
				if r.URL.Path == "/v1/status" {
					_ = enc.Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
					return
				}
				if r.URL.Path != "/v1/pbm" {
					_ = enc.Encode(agentapi.ActionResponse{})
					return
				}
				var request agentapi.PBMRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				output := `{}`
				switch request.Command {
				case "describe-backup":
					status := "running"
					if native == "done" {
						status = "done"
					} else if cancelled {
						status = "cancelled"
					}
					output = `{"name":"native-backup","type":"logical","status":"` + status + `"}`
				case "status":
					if native == "status-unavailable" {
						http.Error(w, "temporary status failure", http.StatusServiceUnavailable)
						return
					}
					output = `{"backups":{"snapshot":[]}}`
					if native == "stuck" {
						output = `{"running":{"type":"backup","name":"native-backup"},"backups":{"snapshot":[{"name":"native-backup","status":"error","error":"Backup stuck at running stage"}]}}`
					}
				case "cancel-backup":
					cancels++
					cancelled = true
					if request.RequestID != "cancel-backup/cancel" {
						t.Error("lost durable cancellation identity", request.RequestID)
					}
				case "backup":
					starts++
				}
				_ = enc.Encode(agentapi.PBMResponse{Output: json.RawMessage(output)})
			}))
			t.Cleanup(server.Close)
			m.opts.Context.Agents = cleanupAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}, Internal: json.RawMessage(`{"phase":"ready","pbmPrefix":"karakoram/demo","members":[{"name":"m0","set":"rs0","role":"mongod","stage":"active"}]}`)}
			c.Spec.Backup.Storage = "storage"
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "storage", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "manual", Cluster: c.Name, Storage: "storage", Type: "logical", Phase: domain.PhaseRunning, PBMName: "native-backup"}
			if err := st.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "cancel-backup", Cluster: c.Name, Kind: domain.OperationBackup, Phase: domain.PhaseRunning, Step: "poll", CancelRequested: true, Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"poll","member":"m0","pbmName":"native-backup"}`)}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			for attempt := range 4 {
				delay, err := m.reconcile(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				current, err := st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.Terminal() {
					if current.Phase != domain.PhaseCancelled || current.CompletedAt == nil {
						t.Fatal("cancellation did not terminate consistently", current)
					}
					break
				}
				if attempt == 3 || delay > 5*time.Second {
					t.Fatal("accepted cancellation remained polling", current, delay)
				}
			}
			stored, err := st.GetBackup(t.Context(), c.Ref(), b.Name)
			if err != nil {
				t.Fatal(err)
			}
			expected := domain.PhaseFailed
			if native == "done" {
				expected = domain.PhaseSucceeded
			}
			if stored.Phase != expected || stored.CompletedAt == nil {
				t.Fatal("native inventory inconsistent", stored)
			}
			expectedCancels := 0
			if native == "running" || native == "status-unavailable" {
				expectedCancels = 1
			}
			if cancels != expectedCancels || starts != 0 {
				t.Fatal("wrong native mutations", cancels, starts)
			}
		})
	}
}

func TestAcceptedCancellationDispatchesBeforeFurtherImperativeWork(t *testing.T) {
	for _, kind := range []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, domain.OperationRestart, domain.OperationReplaceMember} {
		t.Run(kind, func(t *testing.T) {
			p := lifecycleProvider()
			p.invalid = true // Any ordinary RunOperation would fail, not cancel.
			m, st := newTestManager(t, p)
			createOperationCluster(t, st)
			op := &domain.Operation{Cluster: "demo", Kind: kind, Phase: domain.PhaseRunning, Step: "poll", CancelRequested: true}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(t.Context(), op.ID)
			if err != nil || got.Phase != domain.PhaseCancelled || got.CompletedAt == nil || got.Step != "cancelled" {
				t.Fatal("accepted cancellation ran ordinary work or failed to terminate", got, err)
			}
		})
	}
}
