// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestInitialDataSourceRequiresDurableCompletionAfterGenerationChange(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
	rc.Cluster.Generation++
	bs := backupState{InitialQueued: true, CloneComplete: true}
	if err := saveBackupState(rc.Cluster, bs); err != nil {
		t.Fatal(err)
	}
	// A simulated control-plane restart must not interpret the early repository
	// cutover marker, or a healthy preexisting database, as completed restoration.
	p = New(p.options)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("healthy empty database became Ready after amendment/restart")
	}
	found := false
	for _, c := range status.Conditions {
		found = found || c.Type == "Ready" && c.Status == "False" && c.Reason == "RestoreIncomplete"
	}
	if !found {
		t.Fatalf("missing restore readiness gate: %+v", status)
	}
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: "create cancelled before restoration"})
	status, err = p.Status(t.Context(), rc)
	if status.Phase != domain.PhaseFailed {
		t.Fatalf("cancelled initial restore not Failed: %+v %v", status, err)
	}
	bs.InitialApplied = true
	if err := saveBackupState(rc.Cluster, bs); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if status.Phase != domain.PhaseFailed {
		t.Fatalf("late native success cleared an aborted create: %+v %v", status, err)
	}
	// A non-aborted attempt is released only by positive completion.
	rc.Cluster.Status.Conditions = nil
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("completed restore not released: %+v %v", status, err)
	}
}

func TestInitialClonePrimaryRequiresNativeBootstrapReceipt(t *testing.T) {
	for _, provider := range []string{"postgres", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, s := backupFixture(t, "single")
			rc.Cluster.Spec.Provider = provider
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
			rc.Cluster.Generation = 2
			b := &domain.Backup{Namespace: "default", Cluster: "source", Name: "base", Storage: "archive", Provider: provider, Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, RecoveryIdentity: "snapshot", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source-old"}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Namespace: "default", Cluster: rc.Cluster.Name, Name: "initial-restore", Phase: domain.PhaseRunning, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}}
			if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			receipt := 0
			rekeys := 0
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", BootstrapGeneration: receipt})
				case "/v1/postgres/rekey":
					rekeys++
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				case "/v1/reload":
					if strings.HasSuffix(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "px") {
						t.Error("unplaced proxy blocked data restore publication")
					}
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				default:
					t.Errorf("unexpected native call %s", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			op := &domain.Operation{ID: "initial-native", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"initial-restore"}`)}
			cp := backupCheckpoint{Step: "clone-primary", Member: s.data()[0].Name}
			result, err := p.runRestore(t.Context(), rc, op, cp)
			if err != nil {
				t.Fatal(err)
			}
			var pending backupCheckpoint
			if err = json.Unmarshal(result.Checkpoint, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.Step != "clone-primary" || rekeys != 0 {
				t.Fatalf("process health bypassed native bootstrap: %+v rekeys=%d", pending, rekeys)
			}
			// Reconstruct the core and resume the same checkpoint with a positive
			// agent receipt. No desired-generation transition may bypass this boundary.
			p = New(p.options)
			p.restoreSetPresent = func(_ context.Context, _ *domain.BackupStorage, _ string) (bool, error) { return true, nil }
			receipt = s.BootstrapGeneration
			result, err = p.runRestore(t.Context(), rc, op, pending)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(result.Checkpoint, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.Step != "clone-cutover" || rekeys != 1 {
				t.Fatalf("native receipt not consumed: %+v rekeys=%d", pending, rekeys)
			}
			applied, err := p.InitialDataSourceApplied(rc.Cluster)
			if err != nil || applied {
				t.Fatalf("primary receipt alone prematurely completed restore: %v %v", applied, err)
			}
			// The final publication/completion boundary must not require a
			// never-placed proxy. Proxy readiness remains independently gated.
			for _, member := range s.Members {
				if member.Role == "proxy" {
					member.Stage = "planned"
				}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Backup.Storage = ""
			cp.Step = "publish-access"
			result, err = p.runRestore(t.Context(), rc, op, cp)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(result.Checkpoint, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.Step != "fresh-base" {
				t.Fatal("unplaced proxy prevented final data publication", pending)
			}
			r.Error = "temporary native polling diagnostic"
			if err := rc.Store.UpdateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			result, err = p.runRestore(t.Context(), rc, op, pending)
			if err != nil || !result.Done {
				t.Fatalf("positive native recovery did not complete: %+v %v", result, err)
			}
			completed, readErr := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
			if readErr != nil || completed.Phase != domain.PhaseSucceeded || completed.Error != "" {
				t.Fatalf("positive completion retained retry diagnostic: %+v %v", completed, readErr)
			}
			if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
				t.Fatalf("missing durable final completion marker: %v %v", applied, err)
			}
		})
	}
}

func TestLegacyUnprovenSourceKeepsRuntimeConvergenceAndExposureAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-runtime.db")
	p, rc, backend := setupWithStatePath(t, "single", path)
	converge(t, p, rc)
	services := map[string]string{}
	for name, service := range backend.services {
		services[name] = service.ID
	}
	st := rc.Store.(*store.Store)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "missing", Backup: "missing"}
	if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	message := "Historical create operation legacy-create has no restore receipt; delete this target and recreate from the preserved source"
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "LegacyRestoreUnproven", Message: message})
	if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	stored, err := reopened.GetCluster(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	// Preserve the fake agent's shared cluster pointer while replacing all
	// authoritative state from the closed/reopened controller database.
	*rc.Cluster = *stored
	rc.Store, rc.Secrets = reopened, reopened
	p = New(p.options)
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("legacy proof gap stopped ordinary runtime work: %v", err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady {
			t.Fatal("unproven source became Ready")
		}
		if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			break
		}
	}
	if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatal("legacy proof gap stopped configuration convergence")
	}
	for name, id := range services {
		service := backend.services[name]
		if service == nil || service.ID != id {
			t.Fatalf("readiness hold changed data-plane service registration %s", name)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Bootstrap == nil || spec.Bootstrap.Method == "restore" || spec.Bootstrap.AllowInitDB {
		t.Fatalf("legacy unknown data received destructive bootstrap authorization: %+v", spec.Bootstrap)
	}
	if _, err := reopened.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("legacy restore was replayed/enqueued: %v", err)
	}
	if err := reopened.EnqueueBackup(t.Context(), &domain.Backup{Name: "scheduled", Cluster: rc.Cluster.Name, Type: "full"}, &domain.Operation{Cluster: rc.Cluster.Name, Kind: domain.OperationBackup}); err != nil {
		t.Fatalf("legacy readiness hold blocked own backup scheduling: %v", err)
	}
}

func TestInitialCloneJoinsDataWhileProxyDeploymentPending(t *testing.T) {
	for _, provider := range []string{"postgres", "supabase"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			p.restoreSetPresent = func(context.Context, *domain.BackupStorage, string) (bool, error) { return true, nil }
			s := converge(t, p, rc)
			backend := rc.Backend.(*fakeBackend)
			storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Provider = provider
			origin := &domain.Cluster{Name: "source", Spec: rc.Cluster.Spec}
			if err := rc.Store.(*store.Store).CreateCluster(t.Context(), origin); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Namespace: "default", Cluster: origin.Name, Name: "base", Storage: "archive", Provider: provider, Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, RecoveryIdentity: "snapshot", Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/source-old"}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			st := rc.Store.(*store.Store)
			started := time.Now()
			create := &domain.Operation{ID: "live-create", Cluster: rc.Cluster.Name, Kind: domain.OperationCreate, Phase: domain.PhaseRunning, Step: "Creating", TargetGeneration: rc.Cluster.Generation + 1, StartedAt: &started}
			if err := st.EnqueueOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: origin.Name, Backup: b.Name}
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			current, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation = current.Generation
			if err := st.ClaimInitialDataSource(store.WithClusterSnapshot(t.Context(), rc.Cluster), rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
			s.Formed = false
			s.data()[0].Stage = "deployed"
			s.find("demopx").Stage = "deploying"
			s.find("demopx").DeployOp = &infra.Op{ID: "waiting-deployment"}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := saveBackupState(rc.Cluster, backupState{CloneCredentials: true, ManagedRolesHeld: true}); err != nil {
				t.Fatal(err)
			}
			rc.Backend = blockedDeploymentBackend{backend}
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			r, err := st.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore")
			if err != nil {
				t.Fatalf("DATA stayed behind proxy deployment barrier: %v", err)
			}
			op, err := st.GetOperation(t.Context(), r.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			spec := rc.Cluster.Spec
			proxy := spec.Components.Extra["proxy"]
			proxy.Replicas = 0
			spec.Components.Extra = map[string]domain.ComponentSpec{"proxy": proxy}
			patch := &domain.Operation{Kind: domain.OperationUpdate}
			if err := st.UpdateClusterSpecWithSecrets(store.WithOperation(t.Context(), patch), rc.Cluster.Ref(), spec, nil); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("claimed initial recovery admitted PATCH: %v", err)
			}
			installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
				// Status samples agent connectivity independently of native recovery.
				if req.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{})
					return
				}
				if strings.HasSuffix(strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "), "px") {
					if req.URL.Path == "/v1/postgres/status" {
						http.Error(w, "proxy has no allocation", http.StatusServiceUnavailable)
						return
					}
					t.Errorf("recovery contacted unplaced proxy: %s", req.URL.Path)
				}
				switch req.URL.Path {
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", BootstrapGeneration: s.BootstrapGeneration})
				case "/v1/postgres/rekey", "/v1/postgres/finish-restore", "/v1/reload", "/v1/postgres/access":
					json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				default:
					t.Errorf("unexpected recovery request: %s", req.URL.Path)
					http.NotFound(w, req)
				}
			})
			done := false
			for range 30 {
				result, err := p.runBackupOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatalf("restore %s: %v", result.Step, err)
				}
				op.Checkpoint = result.Checkpoint
				if result.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("initial recovery never completed")
			}
			r, err = st.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
			if err != nil || r.Phase != domain.PhaseSucceeded || r.CompletedAt == nil {
				t.Fatalf("initial Restore did not succeed: %+v %v", r, err)
			}
			op.Phase, op.CompletedAt = domain.PhaseSucceeded, r.CompletedAt
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase == domain.PhaseReady {
				t.Fatalf("unplaced desired proxy incorrectly became Ready: %+v %v", status, err)
			}
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
				t.Fatalf("missing native completion: %v %v", applied, err)
			}
			tracker, err := st.GetOperation(t.Context(), create.ID)
			if err != nil || tracker.Phase != domain.PhaseRunning {
				t.Fatalf("recovery incorrectly required retiring create tracker: %+v %v", tracker, err)
			}
			patch = &domain.Operation{Kind: domain.OperationUpdate}
			if err := st.UpdateClusterSpecWithSecrets(store.WithOperation(t.Context(), patch), rc.Cluster.Ref(), spec, nil); err != nil {
				t.Fatalf("completed recovery still fences corrective PATCH: %v", err)
			}
			if patch.ID == "" || patch.ID == create.ID || patch.Kind != domain.OperationUpdate {
				t.Fatalf("post-completion PATCH was not an ordinary update: %+v", patch)
			}
		})
	}
}
