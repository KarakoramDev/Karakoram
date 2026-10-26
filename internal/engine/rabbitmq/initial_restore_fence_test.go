// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestDefinitionsCloneRequirementSurvivesRestartAndGeneration(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "target", Backup: "daily"}
	f.importStatus = 503
	if done, err := f.p.cloneStep(t.Context(), f.rc); err == nil || done {
		t.Fatalf("pending import: done=%v err=%v", done, err)
	}
	if !f.store.claimed {
		t.Fatal("native import started without durable claim")
	}
	raw, err := json.Marshal(f.rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var restarted domain.Cluster
	if err = json.Unmarshal(raw, &restarted); err != nil {
		t.Fatal(err)
	}
	restarted.Internal = append(json.RawMessage(nil), f.rc.Cluster.Internal...)
	restarted.Generation++
	restarted.Status.ObservedGeneration = restarted.Generation
	f.rc.Cluster = &restarted
	p := New(Options{})
	status := domain.ClusterStatus{Phase: domain.PhaseReady}
	if err = engine.ApplyInitialDataSourceStatus(p, f.rc.Cluster, &status); err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("healthy broker bypassed pending definitions restore after restart")
	}
	f.importStatus = 0
	if done, err := p.cloneStep(t.Context(), f.rc); err != nil || !done {
		t.Fatalf("resume import: done=%v err=%v", done, err)
	}
	if applied, err := p.InitialDataSourceApplied(f.rc.Cluster); err != nil || !applied {
		t.Fatalf("native import completion not durable: applied=%v err=%v", applied, err)
	}
}

func TestDefinitionsCloneCancelledBeforeClaimNeverImportsOrBecomesReady(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "target", Backup: "daily"}
	// The atomic store claim rejects a terminal create operation; no native
	// request is allowed after that decision, even on a fresh provider instance.
	f.store.claimError = engine.ErrConflict
	if done, err := f.p.cloneStep(t.Context(), f.rc); !errors.Is(err, engine.ErrConflict) || done {
		t.Fatalf("cancelled clone resumed: done=%v err=%v", done, err)
	}
	if f.imports != 0 {
		t.Fatalf("cancelled clone imported %d times", f.imports)
	}
	f.rc.Cluster.Status.Conditions = []domain.Condition{{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: "Initial restore create-op was cancelled before native completion"}}
	status := domain.ClusterStatus{Phase: domain.PhaseReady}
	if err := engine.ApplyInitialDataSourceStatus(f.p, f.rc.Cluster, &status); err == nil {
		t.Fatal("cancelled restore must report its terminal error")
	}
	if status.Phase != domain.PhaseFailed || status.Message != f.rc.Cluster.Status.Conditions[0].Message {
		t.Fatalf("cancelled clone status: %+v", status)
	}
}

func TestDefinitionsClonePendingRestoreRefusesSupersede(t *testing.T) {
	_, rc, _, _ := setup(t)
	st := rc.Store.(*store.Store)
	backup := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "daily", Provider: "rabbitmq", Type: "definitions", SourceVersion: rc.Cluster.Spec.Version, Phase: domain.PhaseSucceeded}
	if err := st.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	target := &domain.Cluster{Name: "clonetarget", Spec: rc.Cluster.Spec}
	target.Spec.DataSource = &domain.DataSource{Cluster: rc.Cluster.Name, Backup: "daily"}
	if err := st.CreateCluster(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if err := st.ClaimInitialDataSource(t.Context(), target.Ref()); err != nil {
		t.Fatal(err)
	}
	spec := target.Spec
	spec.Components.Engine.Resources.MinRAM++
	if err := st.UpdateClusterSpec(t.Context(), target.Ref(), spec); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("pending definitions clone was superseded: %v", err)
	}
}

func TestDefinitionsCloneUpgradeReopensDurableImport(t *testing.T) {
	for _, imported := range []bool{true, false} {
		name := "pending-import"
		if imported {
			name = "legacy-imported"
		}
		t.Run(name, func(t *testing.T) {
			f := newDefinitionsFixture(t)
			seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
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
			f.store.backup.Cluster = source.Name
			if err = st.CreateBackup(t.Context(), f.store.backup); err != nil {
				t.Fatal(err)
			}
			f.rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: f.store.backup.Name}
			s, err := loadState(f.rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.CloneImported = imported
			if err = s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err = st.CreateCluster(t.Context(), f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			f.store.ClusterStore = st
			if !imported {
				f.importStatus = 503
				if done, err := f.p.cloneStep(t.Context(), f.rc); err == nil || done {
					t.Fatalf("pending native import: done=%v err=%v", done, err)
				}
				if !f.store.claimed || f.imports != 1 {
					t.Fatalf("initial native import not durably claimed: claimed=%v imports=%d", f.store.claimed, f.imports)
				}
				if err = st.UpdateClusterState(t.Context(), f.rc.Cluster); err != nil {
					t.Fatal(err)
				}
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			f.store.ClusterStore = st
			f.rc.Cluster, err = st.GetCluster(t.Context(), f.rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			f.p = New(Options{})
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			if err = engine.ApplyInitialDataSourceStatus(f.p, f.rc.Cluster, &status); err != nil {
				t.Fatal(err)
			}
			if (status.Phase == domain.PhaseReady) != imported {
				t.Fatalf("durable clone receipt readiness=%s imported=%v", status.Phase, imported)
			}
			if imported {
				// Completed prepatch clones need neither native replay nor source
				// backup inventory after upgrade.
				f.store.backup = nil
			} else {
				changed := f.rc.Cluster.Spec
				changed.Components.Engine.Resources.MinRAM++
				if err = st.UpdateClusterSpec(t.Context(), f.rc.Cluster.Ref(), changed); !errors.Is(err, store.ErrConflict) {
					t.Fatalf("reopen lost pending native claim: %v", err)
				}
			}
			f.importStatus = 0
			if done, err := f.p.cloneStep(t.Context(), f.rc); err != nil || !done {
				t.Fatalf("upgraded clone completion: done=%v err=%v", done, err)
			}
			wantImports := 0
			if !imported {
				wantImports = 2
			}
			if f.imports != wantImports {
				t.Fatalf("native imports=%d want=%d", f.imports, wantImports)
			}
			if err = st.UpdateClusterState(t.Context(), f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			target, err := st.GetCluster(t.Context(), f.rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			status = domain.ClusterStatus{Phase: domain.PhaseReady}
			if err = engine.ApplyInitialDataSourceStatus(f.p, target, &status); err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("completed clone lost readiness across reopen: %+v %v", status, err)
			}
		})
	}
}

type unprovenDefinitionsStore struct {
	engine.ClusterStore
	sourceReads, claims int
}

func (s *unprovenDefinitionsStore) GetBackup(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	if ref.Name == "source" {
		s.sourceReads++
		return nil, errors.New("legacy definitions source must not be resolved")
	}
	return s.ClusterStore.GetBackup(ctx, ref, name)
}

func (s *unprovenDefinitionsStore) ClaimInitialDataSource(context.Context, domain.ClusterRef) error {
	s.claims++
	return errors.New("legacy definitions import must never be replayed")
}

func TestDefinitionsUnprovenReopenKeepsRuntimeAndExposureConverging(t *testing.T) {
	p, rc, backend, _ := setup(t)
	ready(t, p, rc)
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
	if err = st.CreateBackup(t.Context(), &domain.Backup{Cluster: source.Name, Name: "definitions", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: "definitions"}
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
	rc.Cluster.Status.ObservedGeneration = 0
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "LegacyRestoreUnproven", Message: "Historical create was superseded; definitions import completion is unproven"})
	if err = st.CreateCluster(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if err = st.CreateBackup(t.Context(), &domain.Backup{Cluster: rc.Cluster.Name, Name: "own", Phase: domain.PhaseSucceeded}); err != nil {
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
	held := &unprovenDefinitionsStore{ClusterStore: st}
	rc.Store = held
	p = New(Options{Store: held, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
	converged := false
	for range 300 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil || result.ConvergenceError != nil {
			t.Fatalf("legacy hold blocked normal reconciliation: %+v %v", result, err)
		}
		s, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Phase == "restore" && len(backend.publicPorts) > 0 {
			if s.CloneImported {
				t.Fatal("unproven clone was marked imported")
			}
			converged = true
			break
		}
	}
	if !converged {
		t.Fatal("legacy hold blocked runtime/public exposure convergence")
	}
	if done, err := p.cloneStep(t.Context(), rc); done || err != nil {
		t.Fatalf("direct clone bypassed unproven hold: done=%v err=%v", done, err)
	}
	if held.sourceReads != 0 || held.claims != 0 {
		t.Fatalf("legacy hold resolved/replayed source: reads=%d claims=%d", held.sourceReads, held.claims)
	}
	if rc.Cluster.Status.ObservedGeneration != 0 {
		t.Fatal("unproven initial import marked generation applied")
	}
	if _, err = st.GetBackup(t.Context(), rc.Cluster.Ref(), "own"); err != nil {
		t.Fatal("ordinary own backup inventory lost during hold", err)
	}
	status, _ := p.Status(t.Context(), rc)
	if status.Phase == domain.PhaseReady {
		t.Fatal("legacy unproven definitions hold became Ready")
	}
}
