// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// legacyInitialRestoreFixture persists only legacy pre-marker fields, then closes and
// reopens SQLite. No live backend or liveness observation supplies any evidence.
func legacyInitialRestoreFixture(t *testing.T) (*Provider, *engine.Context, *domain.Operation, *domain.Restore, func() *domain.Operation) {
	t.Helper()
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
	p := New(Options{Store: st})
	origin := &domain.Cluster{Name: "source"}
	p.ApplyDefaults(&origin.Spec)
	if err := st.CreateCluster(t.Context(), origin); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: origin.Name, Storage: "archive", Type: "logical", PBMName: "source-snapshot", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	target := &domain.Cluster{Name: "target"}
	p.ApplyDefaults(&target.Spec)
	target.Spec.DataSource = &domain.DataSource{Cluster: origin.Name, Backup: "base"}
	create := &domain.Operation{ID: "legacy-create", Kind: domain.OperationCreate}
	if err := st.CreateCluster(store.WithOperation(t.Context(), create), target); err != nil {
		t.Fatal(err)
	}
	s := &internalState{UID: "legacy-target", CloneQueued: true, Phase: "ready"}
	if err := save(target, s); err != nil {
		t.Fatal(err)
	}
	target.Status.Phase = domain.PhaseReady
	target.Status.ObservedGeneration = target.Generation
	if err := st.UpdateClusterState(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: initialRestoreID(target, s), Cluster: target.Name, Kind: domain.OperationRestore, Phase: domain.PhasePending, Payload: json.RawMessage(`{"restore":"initial-restore"}`)}
	r := &domain.Restore{Name: "initial-restore", Cluster: target.Name, Source: domain.RestoreSource{SourceCluster: origin.Name, Backup: "base"}, Phase: domain.PhasePending}
	if err := st.EnqueueRestore(t.Context(), r, op); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	r.Phase, r.CompletedAt = domain.PhaseSucceeded, &stamp
	op.Phase, op.Checkpoint = domain.PhaseSucceeded, json.RawMessage(`{"step":"done","pbmName":"legacy-native"}`)
	rc := &engine.Context{Store: st, Cluster: target}
	reopen := func() *domain.Operation {
		t.Helper()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		st, err = store.Open(path, sealer)
		if err != nil {
			t.Fatal(err)
		}
		rc.Store = st
		rc.Cluster, err = st.GetCluster(t.Context(), target.Ref())
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := st.GetOperation(t.Context(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		return loaded
	}
	return p, rc, op, r, reopen
}

func TestLegacyInitialRestoreMigratesAfterStoreReopen(t *testing.T) {
	p, rc, op, r, reopen := legacyInitialRestoreFixture(t)
	st := rc.Store.(*store.Store)
	if err := st.UpdateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	create, err := st.GetOperation(t.Context(), "legacy-create")
	if err != nil {
		t.Fatal(err)
	}
	create.Phase, create.Message = domain.PhaseCancelled, "Superseded by generation 2"
	if err := st.UpdateOperation(t.Context(), create); err != nil {
		t.Fatal(err)
	}
	reopen()
	if err := p.MigrateInitialDataSource(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	reopen()
	s := retryState(t, rc)
	if complete, err := reconcileInitialRestore(t.Context(), rc, s); err != nil || !complete {
		t.Fatalf("complete=%v err=%v", complete, err)
	}
	if s.InitialRestore.NativeName != "legacy-native" {
		t.Fatalf("receipt=%+v", s.InitialRestore)
	}
}

func TestLegacyInitialRestoreRejectsAmbiguousReceiptsAfterReopen(t *testing.T) {
	for _, scenario := range []string{"missing-native", "pending-native", "failed-checkpoint", "cancelled-checkpoint", "uncertain", "failed-op", "cancelled-op", "cancel-request", "failed-restore", "successful-restore-with-error", "missing-completion", "wrong-source", "wrong-operation", "wrong-payload"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, op, r, reopen := legacyInitialRestoreFixture(t)
			switch scenario {
			case "missing-native":
				op.Checkpoint = json.RawMessage(`{"step":"done"}`)
			case "pending-native":
				op.Checkpoint = json.RawMessage(`{"step":"poll","pbmName":"legacy-native"}`)
			case "failed-checkpoint":
				op.Checkpoint = json.RawMessage(`{"step":"done","pbmName":"legacy-native","failure":"native restore failed"}`)
			case "cancelled-checkpoint":
				op.Checkpoint = json.RawMessage(`{"step":"done","pbmName":"legacy-native","cancelling":true}`)
			case "uncertain":
				op.Checkpoint = json.RawMessage(`{"step":"done","pbmName":"legacy-native","restoreConsistencyUncertain":true}`)
			case "failed-op":
				op.Phase = domain.PhaseFailed
			case "cancelled-op":
				op.Phase = domain.PhaseCancelled
			case "cancel-request":
				op.CancelRequested = true
			case "failed-restore":
				r.Phase, r.Error = domain.PhaseFailed, "native failure"
			case "successful-restore-with-error":
				r.Error = "completion receipt is not clean"
			case "missing-completion":
				r.CompletedAt = nil
			case "wrong-source":
				r.Source.Backup = "another-backup"
			case "wrong-operation":
				r.OperationID = "another-operation"
			case "wrong-payload":
				op.Payload = json.RawMessage(`{"restore":"another-restore"}`)
			}
			st := rc.Store.(*store.Store)
			if err := st.UpdateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			reopen()
			if err := p.MigrateInitialDataSource(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
				t.Fatal("ambiguous receipt migrated")
			}
			s := retryState(t, rc)
			if complete, err := reconcileInitialRestore(t.Context(), rc, s); err != nil || complete {
				t.Fatalf("complete=%v err=%v", complete, err)
			}
			if rc.Cluster.Status.Phase != domain.PhaseFailed {
				t.Fatalf("status=%+v", rc.Cluster.Status)
			}
		})
	}
}

func TestLegacyInFlightRecoveryRetainsNativeSuccessAfterReopen(t *testing.T) {
	for _, step := range []string{"mongod-start", "configure-target", "target-inventory", "complete"} {
		t.Run(step, func(t *testing.T) {
			p, rc, op, r, reopen := legacyInitialRestoreFixture(t)
			op.Phase = domain.PhaseRunning
			op.Checkpoint = json.RawMessage(`{"step":"` + step + `","pbmName":"legacy-native"}`)
			r.Phase, r.CompletedAt = domain.PhaseRunning, nil
			// A controller retry diagnostic is not a native terminal outcome.
			// The persisted positive recovery checkpoint must remain usable.
			r.Error = "temporary source inventory connection failure"
			st := rc.Store.(*store.Store)
			if err := st.UpdateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			op = reopen()
			r, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := pbm.MigrateRestoreCheckpoint(op, r)
			if err != nil || raw == nil {
				t.Fatalf("checkpoint=%s err=%v", raw, err)
			}
			// Persist the migrated native receipt while recovery remains incomplete.
			op.Checkpoint = raw
			if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			op = reopen()
			if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
				t.Fatal("unfinished recovery became applied")
			}
			var cp map[string]any
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			cp["step"] = "complete"
			// Exercise automatic PBM migration at the terminal legacy recovery step,
			// rather than relying on this test's earlier explicit migration.
			delete(cp, "restoreNativeSucceeded")
			op.Checkpoint, err = json.Marshal(cp)
			if err != nil {
				t.Fatal(err)
			}
			result, err := pbm.RunRestore(t.Context(), pbm.Env{RC: rc}, op)
			if err != nil || !result.Done {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			outcome, err := pbm.ReadRestoreOutcome(result.Checkpoint)
			if err != nil || !outcome.RecoveredSuccessfully {
				t.Fatalf("outcome=%+v err=%v", outcome, err)
			}
			completed, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore")
			if err != nil || completed.Phase != domain.PhaseSucceeded || completed.Error != "" {
				t.Fatalf("completion did not clear retry diagnostic: restore=%+v err=%v", completed, err)
			}
			// Crash after the Restore/checkpoint finished but before marker persistence:
			// completed receipt migration must reconstruct the marker after reopening.
			op.Phase, op.Checkpoint = domain.PhaseSucceeded, result.Checkpoint
			if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			reopen()
			if err := p.MigrateInitialDataSource(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if applied, err := p.InitialDataSourceApplied(rc.Cluster); err != nil || !applied {
				t.Fatalf("applied=%v err=%v", applied, err)
			}
		})
	}
}

func TestLegacyInFlightAmbiguousEvidenceStaysUnprovenAfterReopen(t *testing.T) {
	for _, checkpoint := range []string{
		`{"step":"poll","pbmName":"legacy-native"}`,
		`{"step":"configure-source","pbmName":"legacy-native"}`,
		`{"step":"configure-target"}`,
		`{"step":"configure-target","pbmName":"legacy-native","failure":"native failed"}`,
		`{"step":"configure-target","pbmName":"legacy-native","cancelling":true}`,
		`{"step":"configure-target","pbmName":"legacy-native","restoreConsistencyUncertain":true}`,
	} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, op, r, reopen := legacyInitialRestoreFixture(t)
			op.Phase, op.Checkpoint = domain.PhaseRunning, json.RawMessage(checkpoint)
			r.Phase, r.CompletedAt = domain.PhaseRunning, nil
			r.Error = "temporary source inventory connection failure"
			st := rc.Store.(*store.Store)
			if err := st.UpdateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			op = reopen()
			r, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := pbm.MigrateRestoreCheckpoint(op, r)
			if err != nil || raw != nil {
				t.Fatalf("checkpoint=%s err=%v", raw, err)
			}
			if err := p.MigrateInitialDataSource(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
				t.Fatal("ambiguous in-flight restore became applied")
			}
		})
	}
}
