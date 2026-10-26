// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestInitialRestorePendingSurvivesGenerationAndRestart(t *testing.T) {
	for _, source := range []string{"backup", "clone", "pitr"} {
		t.Run(source, func(t *testing.T) {
			p, rc, _, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			stamp := time.Now().UTC()
			origin := *rc.Cluster
			origin.Name = "source"
			origin.Spec.DataSource = nil
			if err := rc.Store.(*store.Store).CreateCluster(t.Context(), &origin); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{
				Namespace: origin.Ref().Namespace, Name: "base", Cluster: origin.Name,
				Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded,
				PBMName: "source-snapshot",
			}); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
			if source == "clone" {
				rc.Cluster.Spec.DataSource.Backup = ""
			}
			if source == "pitr" {
				rc.Cluster.Spec.DataSource.PITRTime = &stamp
			}
			complete, err := reconcileInitialRestore(t.Context(), rc, s)
			if err != nil || complete || !s.CloneQueued {
				t.Fatalf("queued=%v complete=%v err=%v", s.CloneQueued, complete, err)
			}
			id := initialRestoreID(rc.Cluster, s)
			// A replacement desired generation must not retire or re-queue the native
			// restore. Persist/reload the controller's only completion requirement.
			rc.Cluster.Generation += 2
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			st := rc.Store.(*store.Store)
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			reloaded, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			reloaded.Spec.DataSource = rc.Cluster.Spec.DataSource
			reloaded.Generation = rc.Cluster.Generation
			rc.Cluster = reloaded
			p = New(p.options)
			s = retryState(t, rc)
			if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || applied {
				t.Fatalf("applied=%v err=%v", applied, err)
			}
			if complete, err := reconcileInitialRestore(t.Context(), rc, s); err != nil || complete {
				t.Fatalf("complete=%v err=%v", complete, err)
			}
			op, err := rc.Store.GetOperation(t.Context(), id)
			if err != nil || op.Phase != domain.PhasePending {
				t.Fatalf("operation=%+v err=%v", op, err)
			}
			for _, terminal := range []domain.Phase{domain.PhaseCancelled, domain.PhaseFailed} {
				op.Phase, op.Message = terminal, "interrupted initial restore"
				if err := st.UpdateOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
				if complete, err := reconcileInitialRestore(t.Context(), rc, s); err != nil || complete {
					t.Fatalf("complete=%v err=%v", complete, err)
				}
				if rc.Cluster.Status.Phase != domain.PhaseFailed || !strings.Contains(rc.Cluster.Status.Message, id) {
					t.Fatalf("status=%+v", rc.Cluster.Status)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
					t.Fatal("terminal initial restore became applied")
				}
			}
		})
	}
}

func TestInitialRestoreNeedsNativeCompletionEvidence(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Backup: "base"}
	op := &domain.Operation{ID: initialRestoreID(rc.Cluster, s), Kind: domain.OperationRestore}
	for _, checkpoint := range []string{`{"step":"poll","pbmName":"native"}`, `{"step":"done","pbmName":"native"}`, `{"step":"done","pbmName":"native","restoreNativeSucceeded":true,"failure":"cancelled"}`} {
		if err := recordInitialRestore(rc.Cluster, s, op, engine.OpResult{Done: true, Checkpoint: json.RawMessage(checkpoint)}, nil); err != nil {
			t.Fatal(err)
		}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
			t.Fatalf("accepted %s", checkpoint)
		}
	}
	checkpoint := json.RawMessage(`{"step":"done","pbmName":"native","restoreNativeSucceeded":true}`)
	// A late successful checkpoint cannot clear a genuinely cancelled target.
	if err := recordInitialRestore(rc.Cluster, s, op, engine.OpResult{Done: true, Checkpoint: checkpoint}, nil); err != nil {
		t.Fatal(err)
	}
	if s.InitialRestore.Completed {
		t.Fatal("late success cleared failed initial restore")
	}
	// A separate fresh target may acquire positive native evidence.
	s.InitialRestore = nil
	if err := recordInitialRestore(rc.Cluster, s, op, engine.OpResult{Done: true, Checkpoint: checkpoint}, nil); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	if applied, err := New(p.options).InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
}

func TestInitialRestoreCancellationBeforeStartFailsTarget(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Backup: "base"}
	op := &domain.Operation{ID: initialRestoreID(rc.Cluster, s), Kind: domain.OperationRestore}
	result, err := p.CancelOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if rc.Cluster.Status.Phase != domain.PhaseFailed {
		t.Fatalf("status=%+v", rc.Cluster.Status)
	}
	if applied, err := p.InitialDataSourceApplied(rc.Cluster); err == nil || applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	s = retryState(t, rc)
	if s.InitialRestore == nil || !strings.Contains(s.InitialRestore.Failure, op.ID) || phase(s) != domain.PhaseFailed {
		t.Fatalf("state=%+v", s)
	}
}
