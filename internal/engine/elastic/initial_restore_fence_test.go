// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestInitialSnapshotRestoreRequirementSurvivesRestartAndGeneration(t *testing.T) {
	_, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) { t.Errorf("unexpected native request: %s", r.URL.Path) })
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "snapshot-one"}
	s.Phase, s.InitialRestore = "ready", "initial-restore"
	st.restore = domain.Restore{Name: "initial-restore", Phase: domain.PhaseRunning}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// Recreate both provider and serialized cluster, as after a CP restart. An
	// unrelated desired generation must not waive the native restore requirement.
	raw, err := json.Marshal(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var restarted domain.Cluster
	if err = json.Unmarshal(raw, &restarted); err != nil {
		t.Fatal(err)
	}
	restarted.Internal = append(json.RawMessage(nil), rc.Cluster.Internal...)
	restarted.Generation++
	restarted.Status.ObservedGeneration = restarted.Generation
	rc.Cluster = &restarted
	p := New(ProviderOptions{})
	status := domain.ClusterStatus{Phase: domain.PhaseReady}
	if err = engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status); err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("healthy processes bypassed pending restore after restart")
	}
	if err = observeInitialRestore(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.InitialRestore != "initial-restore" {
		t.Fatal("pending restore lost requirement")
	}
	st.restore.Phase = domain.PhaseSucceeded
	if err = putSecret(t.Context(), rc, "operations/initial-restore-"+s.UID+"/restore-submission", []byte(`{"repository":"archive","snapshot":"snapshot-one","completed":true}`)); err != nil {
		t.Fatal(err)
	}
	if err = observeInitialRestore(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
		t.Fatalf("successful durable restore not applied: %v %v", applied, err)
	}
}

func TestInitialSnapshotCancelledOrFailedNeverResumesOrBecomesReady(t *testing.T) {
	for _, terminal := range []domain.Phase{domain.PhaseCancelled, domain.PhaseFailed} {
		t.Run(string(terminal), func(t *testing.T) {
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("aborted restore issued native request: %s", r.URL.Path)
			})
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "snapshot-one"}
			s.InitialRestore = "initial-restore"
			st.restore = domain.Restore{Name: "initial-restore", Phase: terminal, Error: "restore interrupted"}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("terminal restore not fenced: %v", err)
			}
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			if err := engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status); err == nil || status.Phase != domain.PhaseFailed || !strings.Contains(status.Message, "restore interrupted") {
				t.Fatalf("aborted restore status: %+v %v", status, err)
			}
			out, err := p.runRestore(t.Context(), rc, s, &domain.Operation{ID: "restore-op"}, operationState{}, "initial-restore")
			if err == nil || !out.Done {
				t.Fatalf("aborted native restore resumed: %+v %v", out, err)
			}
		})
	}
}

func TestInitialSnapshotPendingRestoreRefusesSupersede(t *testing.T) {
	_, rc, _, _ := coreSetup(t, 1)
	st := rc.Store.(*store.Store)
	backup := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "snapshot-one", Provider: "elasticsearch", Type: "snapshot", SourceVersion: rc.Cluster.Spec.Version, Phase: domain.PhaseSucceeded}
	if err := st.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	target := &domain.Cluster{Name: "snaptarget", Spec: rc.Cluster.Spec}
	target.Spec.DataSource = &domain.DataSource{Cluster: rc.Cluster.Name, Backup: "snapshot-one"}
	if err := st.CreateCluster(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if err := st.ClaimInitialDataSource(t.Context(), target.Ref()); err != nil {
		t.Fatal(err)
	}
	spec := target.Spec
	spec.Components.Engine.Resources.MinRAM++
	if err := st.UpdateClusterSpec(t.Context(), target.Ref(), spec); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("pending snapshot restore was superseded: %v", err)
	}
}

func TestInitialSnapshotNativeAbortBecomesFailed(t *testing.T) {
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_restore") {
			opsJSON(w, map[string]any{"snapshot": map[string]any{"shards": map[string]int{"total": 2, "successful": 1, "failed": 1}}})
			return
		}
		opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
	})
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "snapshot-one"}
	s.InitialRestore, s.RestoreActive = "initial-restore", true
	st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", "9.5.5"
	st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
	st.restore = domain.Restore{Name: "initial-restore", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhaseRunning}
	out, err := p.runRestore(t.Context(), rc, s, &domain.Operation{ID: "native-abort"}, operationState{Stage: "submit-receipted"}, "initial-restore")
	if !out.Done || !errors.Is(err, engine.ErrInvalid) || st.restore.Phase != domain.PhaseFailed {
		t.Fatalf("native abort not terminal: %+v %v %+v", out, err, st.restore)
	}
	if err = observeInitialRestore(t.Context(), rc, s); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("native abort did not preserve restore requirement: %v", err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status := domain.ClusterStatus{Phase: domain.PhaseReady}
	if err = engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status); err == nil || status.Phase != domain.PhaseFailed {
		t.Fatalf("native abort became ready: %+v %v", status, err)
	}
}

// Reopen real durable state rather than only rebuilding in-memory structs:
// completed prepatch markers remain valid, while accepted-only submissions do
// not become a completion marker even if the old restore row says Succeeded.
func TestInitialSnapshotUpgradeReopensDurableCompletionEvidence(t *testing.T) {
	for _, mode := range []string{"legacy-done", "accepted-only", "in-flight"} {
		t.Run(mode, func(t *testing.T) {
			sealer, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "state.db")
			st, err := store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			source := &domain.Cluster{Name: "source"}
			if err = st.CreateCluster(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			backup := &domain.Backup{Cluster: source.Name, Name: "snapshot", Provider: "elasticsearch", Type: "snapshot", Phase: domain.PhaseSucceeded}
			if err = st.CreateBackup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			target := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: "elasticsearch", DataSource: &domain.DataSource{Cluster: source.Name, Backup: backup.Name}}}
			s := &state{UID: "upgrade", Phase: "ready", InitialRestore: "initial-restore"}
			if mode == "legacy-done" {
				s.InitialRestore = "done"
			}
			if err = save(target, s); err != nil {
				t.Fatal(err)
			}
			if err = st.CreateCluster(t.Context(), target); err != nil {
				t.Fatal(err)
			}
			restore := &domain.Restore{Cluster: target.Name, Name: "initial-restore", Phase: domain.PhaseRunning, Source: domain.RestoreSource{SourceCluster: source.Name, Backup: backup.Name}}
			if mode == "accepted-only" {
				restore.Phase = domain.PhaseSucceeded
			}
			if err = st.CreateRestore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			key := "operations/initial-restore-upgrade/restore-submission"
			if err = st.SetClusterSecret(t.Context(), target.Ref(), key, []byte(`{"repository":"archive","snapshot":"snapshot","accepted":true}`)); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			target, err = st.GetCluster(t.Context(), target.Ref())
			if err != nil {
				t.Fatal(err)
			}
			s, err = load(target)
			if err != nil {
				t.Fatal(err)
			}
			rc := &engine.Context{Cluster: target, Store: st, Secrets: st}
			p := New(ProviderOptions{Store: st})
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			if err = engine.ApplyInitialDataSourceStatus(p, target, &status); err != nil {
				t.Fatal(err)
			}
			if mode == "legacy-done" {
				if status.Phase != domain.PhaseReady {
					t.Fatalf("prepatch done marker lost readiness: %+v", status)
				}
				if err = observeInitialRestore(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				return
			}
			if status.Phase == domain.PhaseReady {
				t.Fatal("accepted-only restore became ready across reopen")
			}
			err = observeInitialRestore(t.Context(), rc, s)
			if mode == "accepted-only" {
				if err == nil || s.InitialRestore == "done" {
					t.Fatal("legacy succeeded row promoted accepted-only receipt", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The native restore finishes after upgrade. Its explicit completion
			// receipt, not an applied generation or process probe, releases Ready.
			restore.Phase = domain.PhaseSucceeded
			if err = st.UpdateRestore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			if err = st.SetClusterSecret(t.Context(), target.Ref(), key, []byte(`{"repository":"archive","snapshot":"snapshot","completed":true}`)); err != nil {
				t.Fatal(err)
			}
			if err = observeInitialRestore(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err = save(target, s); err != nil {
				t.Fatal(err)
			}
			if err = st.UpdateClusterState(t.Context(), target); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			target, err = st.GetCluster(t.Context(), target.Ref())
			if err != nil {
				t.Fatal(err)
			}
			status = domain.ClusterStatus{Phase: domain.PhaseReady}
			if err = engine.ApplyInitialDataSourceStatus(p, target, &status); err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("positive native completion not durable: %+v %v", status, err)
			}
		})
	}
}

type unprovenSnapshotStore struct {
	engine.ClusterStore
	sourceReads, enqueues int
}

func (s *unprovenSnapshotStore) GetBackup(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	if ref.Name == "source" {
		s.sourceReads++
		return nil, errors.New("legacy source must not be resolved during an unproven restore hold")
	}
	return s.ClusterStore.GetBackup(ctx, ref, name)
}

func (s *unprovenSnapshotStore) EnqueueRestore(context.Context, *domain.Restore, *domain.Operation) error {
	s.enqueues++
	return errors.New("legacy initial restore must not be replayed")
}

func TestInitialSnapshotUnprovenReopenKeepsRuntimeAndExposureConverging(t *testing.T) {
	p, rc, backend, _ := coreSetup(t, 1)
	coreConverge(t, p, rc, backend)
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	source := &domain.Cluster{Name: "source"}
	if err = st.CreateCluster(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if err = st.CreateBackup(t.Context(), &domain.Backup{Cluster: source.Name, Name: "snapshot", Storage: "sourcearchive", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: "snapshot"}
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
	rc.Cluster.Status.ObservedGeneration = 0
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "LegacyRestoreUnproven", Message: "Historical create was superseded; native restore completion is unproven"})
	if err = st.CreateCluster(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	held := &unprovenSnapshotStore{ClusterStore: st}
	rc.Store = held
	p = New(ProviderOptions{Store: held})
	converged := false
	for range 300 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil || result.ConvergenceError != nil {
			t.Fatalf("legacy hold blocked ordinary reconciliation: %+v %v", result, err)
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Phase == "restore" && len(backend.public) > 0 {
			if s.InitialRestore != "" || s.RestoreActive {
				t.Fatal("legacy hold started a restore", s.InitialRestore)
			}
			converged = true
			break
		}
	}
	if !converged {
		t.Fatal("legacy hold blocked public exposure/runtime convergence")
	}
	if held.sourceReads != 0 || held.enqueues != 0 {
		t.Fatalf("legacy hold resolved/replayed source: reads=%d enqueues=%d", held.sourceReads, held.enqueues)
	}
	if rc.Cluster.Status.ObservedGeneration != 0 {
		t.Fatal("unproven initial restore marked generation applied")
	}
	if _, err = st.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("legacy hold created restore inventory", err)
	}
	// Own backup storage references remain active; only the historical
	// dataSource bootstrap dependency is suppressed.
	if err = st.CreateBackup(t.Context(), &domain.Backup{Cluster: rc.Cluster.Name, Name: "own", Storage: "ownarchive", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := p.storageReferences(t.Context(), rc, s)
	if err != nil || !refs[rc.Cluster.Ref().Namespace+"/ownarchive"] || refs[source.Ref().Namespace+"/sourcearchive"] {
		t.Fatalf("hold changed own backup references: %v %v", refs, err)
	}
	status, _ := p.Status(t.Context(), rc)
	if status.Phase == domain.PhaseReady {
		t.Fatal("legacy unproven hold became Ready")
	}
}

func TestInitialSnapshotTransientRecoveryErrorRetriesToPositiveCompletion(t *testing.T) {
	recoveryPolls, submissions := 0, 0
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_restore"):
			submissions++
			opsJSON(w, map[string]any{"snapshot": map[string]any{"shards": map[string]int{"total": 1, "successful": 1, "failed": 0}}})
		case r.URL.Path == "/_cat/recovery":
			recoveryPolls++
			if recoveryPolls == 1 {
				http.Error(w, "transient recovery observation unavailable", http.StatusServiceUnavailable)
				return
			}
			opsJSON(w, []any{})
		case r.URL.Path == "/_cluster/health":
			opsJSON(w, esapi.Health{Status: "green", NumberOfDataNodes: 1})
		case r.URL.Path == "/_security/_authenticate":
			opsJSON(w, map[string]string{"username": "elastic/kibana"})
		default:
			opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
		}
	})
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: st.backup.Name}
	s.Phase, s.InitialRestore, s.RestoreActive = "restore", "initial-restore", true
	st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", rc.Cluster.Spec.Version
	st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
	st.restore = domain.Restore{Name: "initial-restore", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhaseRunning}
	op := &domain.Operation{ID: "initial-restore-" + s.UID, Kind: domain.OperationRestore}
	out, err := p.runRestore(t.Context(), rc, s, op, operationState{Stage: "submit-receipted"}, "initial-restore")
	if err != nil || out.Done || opsCheckpoint(t, out).Stage != "recover" {
		t.Fatalf("native completion submission failed: %+v %v", out, err)
	}
	out, err = p.runRestore(t.Context(), rc, s, op, opsCheckpoint(t, out), "initial-restore")
	if err == nil || out.Done || st.restore.Phase != domain.PhaseRunning || st.restore.Error == "" || st.restore.CompletedAt != nil {
		t.Fatalf("transient poll was not retained as a retry diagnostic: %+v %v %+v", out, err, st.restore)
	}
	cp := opsCheckpoint(t, out)
	if cp.Stage != "recover" || cp.Failure != "" {
		t.Fatalf("retryable native observation became a terminal checkpoint: %+v", cp)
	}
	if err = observeInitialRestore(t.Context(), rc, s); err != nil || s.InitialRestore != "initial-restore" {
		t.Fatalf("Running restore diagnostic was interpreted as abort: %s %v", s.InitialRestore, err)
	}
	done := false
	for range 12 {
		out, err = p.runRestore(t.Context(), rc, s, op, cp, "initial-restore")
		if err != nil {
			t.Fatal(err)
		}
		cp = opsCheckpoint(t, out)
		if out.Done {
			done = true
			break
		}
	}
	if !done || st.restore.Phase != domain.PhaseSucceeded || st.restore.Error != "" || st.restore.CompletedAt == nil || s.InitialRestore != "done" || s.RestoreActive {
		t.Fatalf("native restore did not recover from transient diagnostic: %+v %+v", st.restore, cp)
	}
	if recoveryPolls != 2 || submissions != 1 {
		t.Fatalf("poll retry resubmitted restore: polls=%d submissions=%d", recoveryPolls, submissions)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
		t.Fatalf("positive completion failed to release durable requirement: applied=%v err=%v", applied, err)
	}
}
