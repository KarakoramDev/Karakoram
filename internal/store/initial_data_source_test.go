// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func initialAdmissionFixture(t *testing.T, s *Store) (*domain.Cluster, *domain.Operation) {
	t.Helper()
	if err := s.CreateCluster(context.Background(), &domain.Cluster{Name: "source"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBackup(context.Background(), &domain.Backup{Name: "daily", Cluster: "source", Phase: domain.PhaseSucceeded, PBMName: "native"}); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "clone", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17", DataSource: &domain.DataSource{Cluster: "source", Backup: "daily"}}}
	op := &domain.Operation{Kind: domain.OperationCreate}
	if err := s.CreateCluster(WithOperation(context.Background(), op), c); err != nil {
		t.Fatal(err)
	}
	return c, op
}

func TestInitialRestoreAmendmentPreservesCreate(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, create := initialAdmissionFixture(t, s)
	c.Spec.Components.Engine.Replicas = 3
	amendment := &domain.Operation{Kind: domain.OperationUpdate}
	if err := s.UpdateClusterSpec(WithOperation(ctx, amendment), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, create.ID)
	if err != nil || got.ID != amendment.ID || got.Kind != domain.OperationCreate || got.Phase != domain.PhasePending || got.TargetGeneration != 2 {
		t.Fatalf("create=%+v amendment=%+v err=%v", got, amendment, err)
	}
	current, err := s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if current.Spec.DataSource.Backup != "daily" || current.Status.Phase == domain.PhaseReady {
		t.Fatalf("restore source/readiness changed: %+v", current)
	}
	ops, err := s.ListOperations(ctx, c.Ref())
	if err != nil || len(ops) != 1 {
		t.Fatalf("ops=%+v err=%v", ops, err)
	}
	var n int
	if err := s.reader.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE json_extract(data,'$.operation')=? AND json_extract(data,'$.reason')='InitialRestoreAmended'`, create.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("audit=%d err=%v", n, err)
	}
	create.Phase = domain.PhaseSucceeded
	if err := s.UpdateOperation(ctx, create); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale convergence overwrote amended generation: %v", err)
	}
}

func TestInitialRestoreRejectsNonPlaceabilityAndAllowsDelete(t *testing.T) {
	for _, field := range []string{"source", "version", "topology", "credentials", "restart"} {
		t.Run(field, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c, op := initialAdmissionFixture(t, s)
			var err error
			switch field {
			case "source":
				d := *c.Spec.DataSource
				d.Backup = "other"
				c.Spec.DataSource = &d
			case "version":
				c.Spec.Version = "18"
			case "topology":
				c.Spec.Topology.Type = "other"
			case "credentials":
				err = s.UpdateClusterSpecWithSecrets(ctx, c.Ref(), c.Spec, map[string][]byte{"password": []byte("new-password")})
			case "restart":
				err = s.EnqueueOperation(ctx, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart})
			}
			if field != "credentials" && field != "restart" {
				err = s.UpdateClusterSpec(WithOperation(ctx, &domain.Operation{Kind: domain.OperationUpdate}), c.Ref(), c.Spec)
			}
			if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), op.ID) {
				t.Fatalf("err=%v op=%s", err, op.ID)
			}
			if err = s.MarkClusterDeleting(WithOperation(ctx, &domain.Operation{Kind: domain.OperationDelete}), c.Ref(), domain.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitialRestoreClaimPersistsAcrossReopen(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	c, op := initialAdmissionFixture(t, s)
	if err := s.ClaimInitialDataSource(WithClusterSnapshot(ctx, c), c.Ref()); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	c.Spec.Components.Engine.Replicas = 3
	err = reopened.UpdateClusterSpec(WithOperation(ctx, &domain.Operation{Kind: domain.OperationUpdate}), c.Ref(), c.Spec)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), op.ID) {
		t.Fatalf("err=%v", err)
	}
}

func TestInitialRestoreClaimSerializesWithAmendment(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, op := initialAdmissionFixture(t, s)
	c.Spec.Components.Engine.Replicas = 3
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var claimErr, amendErr error
	go func() {
		defer wg.Done()
		<-start
		claimErr = s.ClaimInitialDataSource(WithClusterSnapshot(ctx, c), c.Ref())
	}()
	go func() {
		defer wg.Done()
		<-start
		amendErr = s.UpdateClusterSpec(WithOperation(ctx, &domain.Operation{Kind: domain.OperationUpdate}), c.Ref(), c.Spec)
	}()
	close(start)
	wg.Wait()
	if (claimErr == nil) == (amendErr == nil) {
		t.Fatalf("claim=%v amendment=%v; exactly one stale-snapshot contestant must win", claimErr, amendErr)
	}
	if amendErr != nil && (!errors.Is(amendErr, ErrConflict) || !strings.Contains(amendErr.Error(), op.ID)) {
		t.Fatal(amendErr)
	}
	if claimErr != nil && !errors.Is(claimErr, ErrConflict) {
		t.Fatal(claimErr)
	}
}

func TestInitialRestoreNativeClaimClosesAmendmentWindow(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, create := initialAdmissionFixture(t, s)
	restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}}
	native := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
	if err := s.EnqueueRestore(ctx, restore, native); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimOperation(ctx, c.Ref())
	if err != nil || claimed.ID != native.ID {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	c.Spec.Components.Engine.Replicas = 3
	err = s.UpdateClusterSpec(WithOperation(ctx, &domain.Operation{Kind: domain.OperationUpdate}), c.Ref(), c.Spec)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), create.ID) {
		t.Fatalf("error=%v", err)
	}
}

func TestInitialRestoreSourceFenceSurvivesCancelledCreate(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c, create := initialAdmissionFixture(t, s)
	create.Phase = domain.PhaseCancelled
	if err := s.UpdateOperation(ctx, create); err != nil {
		t.Fatal(err)
	}
	b, err := s.GetBackup(ctx, domain.ClusterRef{Name: "source"}, "daily")
	if err != nil {
		t.Fatal(err)
	}
	err = s.EnqueueBackupDeletion(ctx, b, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup})
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), create.ID) {
		t.Fatalf("error=%v", err)
	}
	if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueBackupDeletion(ctx, b, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup}); err != nil {
		t.Fatal(err)
	}
}

func TestInitialRestoreSourceFenceWithoutCreateTracker(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: "source"}); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Name: "daily", Cluster: "source", Phase: domain.PhaseSucceeded, PBMName: "native"}
	if err := s.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	target := &domain.Cluster{Name: "legacyclone", Spec: domain.ClusterSpec{DataSource: &domain.DataSource{Cluster: "source", Backup: "daily"}}}
	if err := s.CreateCluster(ctx, target); err != nil {
		t.Fatal(err)
	}
	remove := func() error {
		return s.EnqueueBackupDeletion(ctx, backup, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup})
	}
	if err := remove(); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "legacyclone initial restore") {
		t.Fatalf("untracked data source lost its durable fence: %v", err)
	}
	target.Internal = []byte(`{"postgresBackup":{"cloneComplete":true}}`)
	if err := s.UpdateClusterState(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := remove(); !errors.Is(err, ErrConflict) {
		t.Fatalf("early CloneComplete lifted source fence: %v", err)
	}
	target.Internal = []byte(`{"postgresBackup":{"initialApplied":true}}`)
	if err := s.UpdateClusterState(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := remove(); err != nil {
		t.Fatalf("native completion did not release source: %v", err)
	}
}

func TestLegacySupersededCreateCanClaimOnlyMatchingLiveRestoreAfterReopen(t *testing.T) {
	for _, scenario := range []string{"superseded", "explicit-cancel", "native-cancel", "wrong-source"} {
		t.Run(scenario, func(t *testing.T) {
			s, path := testStore(t)
			ctx := t.Context()
			c, create := initialAdmissionFixture(t, s)
			restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}}
			native := &domain.Operation{ID: "legacy-native", Cluster: c.Name, Kind: domain.OperationRestore}
			if err := s.EnqueueRestore(ctx, restore, native); err != nil {
				t.Fatal(err)
			}
			create.Phase, create.Message = domain.PhaseCancelled, "Superseded by operation old-patch (update)"
			if scenario == "explicit-cancel" {
				create.Message = "Create cancelled by operator"
			}
			if err := s.UpdateOperation(ctx, create); err != nil {
				t.Fatal(err)
			}
			if scenario == "native-cancel" {
				native.CancelRequested = true
				if err := s.UpdateOperation(ctx, native); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "wrong-source" {
				restore.Source.Backup = "other"
				if err := s.UpdateRestore(ctx, restore); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			key, err := secrets.New(strings.Repeat("m", 32))
			if err != nil {
				t.Fatal(err)
			}
			s, err = Open(path, key)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			claimed, err := s.ClaimOperation(ctx, c.Ref())
			if scenario == "superseded" {
				if err != nil || claimed.ID != native.ID || claimed.Phase != domain.PhaseRunning {
					t.Fatalf("legacy native recovery cannot finish: %+v %v", claimed, err)
				}
			} else if !errors.Is(err, ErrConflict) {
				t.Fatalf("unrelated/cancelled restore bypassed claim fence: %+v %v", claimed, err)
			}
			c.Spec.Components.Engine.Resources.MinRAM++
			if err := s.UpdateClusterSpec(ctx, c.Ref(), c.Spec); !errors.Is(err, ErrConflict) {
				t.Fatalf("legacy native claim reopened mutation admission: %v", err)
			}
		})
	}
}

func TestGenuineInitialAbortKeepsSourceFenceDespiteLatePositiveMarker(t *testing.T) {
	s, _ := testStore(t)
	c, create := initialAdmissionFixture(t, s)
	c.Internal = []byte(`{"postgresBackup":{"initialApplied":true}}`)
	c.Status.InitialDataSource = &domain.InitialDataSourceLifecycle{AbortMessage: "Native restore was genuinely aborted before late completion"}
	if err := s.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(t.Context(), c.Ref(), domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
		t.Fatal(err)
	}
	var err error
	c, err = s.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SuspendCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetCluster(t.Context(), c.Ref())
	if err != nil || stored.Status.InitialDataSource == nil || stored.Status.InitialDataSource.AbortMessage == "" {
		t.Fatalf("health condition rewrite lost abort evidence: %+v %v", stored, err)
	}
	backup, err := s.GetBackup(t.Context(), domain.ClusterRef{Name: "source"}, "daily")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueBackupDeletion(t.Context(), backup, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup}); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), create.ID) {
		t.Fatalf("late completion released aborted source dependency: %v", err)
	}
	c.Spec.Components.Engine.Resources.MinRAM++
	if err := s.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("late completion reopened aborted-target edits: %v", err)
	}
}

func TestInitialBootstrapClaimSurvivesTrackerBudgetFailureAndReopen(t *testing.T) {
	for _, step := range []string{domain.InitialRestoreTrackerDeadline, domain.InitialRestoreTrackerRetryBudget, "validated-legacy-budget"} {
		t.Run(step, func(t *testing.T) {
			s, path := testStore(t)
			ctx := t.Context()
			c, create := initialAdmissionFixture(t, s)
			if _, err := s.GetInitialDataSourceClaim(ctx, c.Ref()); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing receipt inferred a claim: %v", err)
			}
			if err := s.ClaimInitialDataSource(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			original, err := s.GetInitialDataSourceClaim(ctx, c.Ref())
			if err != nil || original.Operation != create.ID || original.ClusterIncarnation != create.ClusterIncarnation || original.ClaimedAt.IsZero() {
				t.Fatalf("invalid bootstrap receipt: %+v %v", original, err)
			}
			completed := time.Now().UTC()
			create.Phase, create.Step, create.CompletedAt = domain.PhaseFailed, step, &completed
			if err := s.UpdateOperation(ctx, create); err != nil {
				t.Fatal(err)
			}
			if step == "validated-legacy-budget" {
				c.Status.InitialDataSource = &domain.InitialDataSourceLifecycle{TrackingFailure: create.ID}
				if err := s.UpdateClusterState(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			key, err := secrets.New(strings.Repeat("m", 32))
			if err != nil {
				t.Fatal(err)
			}
			s, err = Open(path, key)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err := s.ClaimInitialDataSource(ctx, c.Ref()); err != nil {
				t.Fatalf("durably claimed bootstrap aborted after budget failure: %v", err)
			}
			repeated, err := s.GetInitialDataSourceClaim(ctx, c.Ref())
			if err != nil || !repeated.ClaimedAt.Equal(original.ClaimedAt) || repeated.Operation != original.Operation {
				t.Fatalf("repeat claim rewrote receipt: %+v %v", repeated, err)
			}
			c.Spec.Components.Engine.Replicas++
			if err := s.UpdateClusterSpec(ctx, c.Ref(), c.Spec); !errors.Is(err, ErrConflict) {
				t.Fatalf("continuation reopened edits: %v", err)
			}
			backup, err := s.GetBackup(ctx, domain.ClusterRef{Name: "source"}, "daily")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.EnqueueBackupDeletion(ctx, backup, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup}); !errors.Is(err, ErrConflict) {
				t.Fatalf("continuation released source early: %v", err)
			}
			restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}}
			native := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
			if err := s.EnqueueRestore(ctx, restore, native); err != nil {
				t.Fatalf("preclaimed native restore could not enqueue after budget failure: %v", err)
			}
			native, err = s.ClaimOperation(ctx, c.Ref())
			if err != nil {
				t.Fatalf("preclaimed native restore could not continue: %v", err)
			}
			repeated, err = s.GetInitialDataSourceClaim(ctx, c.Ref())
			if err != nil || !repeated.ClaimedAt.Equal(original.ClaimedAt) {
				t.Fatalf("native claim rewrote original bootstrap time: %+v %v", repeated, err)
			}
			current, err := s.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			current.Internal = []byte(`{"postgresBackup":{"initialApplied":true}}`)
			finished := time.Now().UTC()
			native.Phase, native.CompletedAt = domain.PhaseSucceeded, &finished
			restore.Phase, restore.CompletedAt = domain.PhaseSucceeded, &finished
			if err := s.UpdateRestore(ctx, restore); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateOperationAndClusterState(ctx, native, current); err != nil {
				t.Fatal(err)
			}
			current.Spec.Version = "18"
			if err := s.UpdateClusterSpec(ctx, current.Ref(), current.Spec); err != nil {
				t.Fatalf("positive completion did not release mutation fence: %v", err)
			}
			if err := s.EnqueueBackupDeletion(ctx, backup, &domain.Operation{Cluster: "source", Kind: domain.OperationDeleteBackup}); err != nil {
				t.Fatalf("positive completion did not release source: %v", err)
			}
			tracker, err := s.GetOperation(ctx, create.ID)
			if err != nil || tracker.Phase != domain.PhaseFailed {
				t.Fatalf("native success rewrote failed tracker: %+v %v", tracker, err)
			}
		})
	}
}

func TestInitialBootstrapClaimNeverReopensGenuineAbortOrUnclaimedFailure(t *testing.T) {
	for _, scenario := range []string{"before-claim", "unclaimed-tracking-cache", "cancelled", "cancel-requested", "ordinary-failure", "aborted", "late-claim", "wrong-owner", "deleting"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c, create := initialAdmissionFixture(t, s)
			var claim *domain.InitialDataSourceClaim
			if scenario != "before-claim" && scenario != "unclaimed-tracking-cache" {
				if err := s.ClaimInitialDataSource(ctx, c.Ref()); err != nil {
					t.Fatal(err)
				}
				var err error
				claim, err = s.GetInitialDataSourceClaim(ctx, c.Ref())
				if err != nil {
					t.Fatal(err)
				}
			}
			// The native record may already be queued, but Pending is not proof
			// of a durable native claim.
			restore := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "daily"}}
			native := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
			if err := s.EnqueueRestore(ctx, restore, native); err != nil {
				t.Fatal(err)
			}
			completed := time.Now().UTC()
			create.Phase, create.Step, create.CompletedAt = domain.PhaseFailed, domain.InitialRestoreTrackerDeadline, &completed
			switch scenario {
			case "cancelled":
				create.Phase = domain.PhaseCancelled
			case "cancel-requested":
				create.CancelRequested = true
			case "ordinary-failure":
				create.Step = domain.InitialRestoreCreateFailure
			case "late-claim":
				completed = claim.ClaimedAt.Add(-time.Second)
			case "aborted":
				c.Status.InitialDataSource = &domain.InitialDataSourceLifecycle{TrackingFailure: create.ID, AbortMessage: "operator aborted native recovery"}
			case "unclaimed-tracking-cache":
				c.Status.InitialDataSource = &domain.InitialDataSourceLifecycle{TrackingFailure: create.ID}
			case "wrong-owner":
				claim.Operation = "another-create"
				tx, err := s.writer.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := put(ctx, tx, "initial-data-source", c.Ref().Key(), claim.ClusterIncarnation, claim, "", "", "", false); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.UpdateOperation(ctx, create); err != nil {
				t.Fatal(err)
			}
			if c.Status.InitialDataSource != nil {
				if err := s.UpdateClusterState(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "deleting" {
				if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ClaimInitialDataSource(ctx, c.Ref()); !errors.Is(err, ErrConflict) {
				t.Fatalf("invalid existing/new claim continued: %v", err)
			}
			_, err := s.ClaimOperation(ctx, c.Ref())
			if scenario == "deleting" {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("deleted target retained native work: %v", err)
				}
			} else if !errors.Is(err, ErrConflict) {
				t.Fatalf("native work crossed abort/no-claim fence: %v", err)
			}
			if err := s.EnqueueRestore(ctx, &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: restore.Source}, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}); !errors.Is(err, ErrConflict) {
				t.Fatalf("new native enqueue crossed abort/no-claim fence: %v", err)
			}
		})
	}
}

func TestInitialDataSourceClaimGetterScopesIncarnationAndReadsLegacyReceipt(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c, create := initialAdmissionFixture(t, s)
	at := time.Now().UTC()
	legacy := struct {
		Operation string    `json:"operation"`
		ClaimedAt time.Time `json:"claimedAt"`
	}{create.ID, at}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := put(ctx, tx, "initial-data-source", c.Ref().Key(), "old-incarnation", legacy, "", "", "", false); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetInitialDataSourceClaim(ctx, c.Ref()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old incarnation counted as current native claim: %v", err)
	}
	tx, err = s.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := put(ctx, tx, "initial-data-source", c.Ref().Key(), create.ClusterIncarnation, legacy, "", "", "", false); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	claim, err := s.GetInitialDataSourceClaim(ctx, c.Ref())
	if err != nil || claim.ClusterIncarnation != create.ClusterIncarnation || claim.Operation != create.ID || !claim.ClaimedAt.Equal(at) {
		t.Fatalf("legacy receipt identity/time changed: %+v %v", claim, err)
	}
	if err := s.ClaimInitialDataSource(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	claim, err = s.GetInitialDataSourceClaim(ctx, c.Ref())
	if err != nil || !claim.ClaimedAt.Equal(at) {
		t.Fatalf("legacy repeat claim replaced original time: %+v %v", claim, err)
	}
}

func TestInitialMutationPreflightUsesAuthoritativeFenceErrorWithoutWrites(t *testing.T) {
	s, _ := testStore(t)
	c, owner := initialAdmissionFixture(t, s)
	for _, claimed := range []bool{false, true} {
		if claimed {
			if err := s.ClaimInitialDataSource(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
		}
		err := s.CheckInitialDataSourceMutation(t.Context(), c.Ref())
		var preflight *domain.ResourceError
		if !errors.As(err, &preflight) || preflight.Resource != "operation" || preflight.Name != owner.ID || preflight.Reason != domain.ReasonInvalidState {
			t.Fatalf("claimed=%v preflight=%+v err=%v", claimed, preflight, err)
		}
		var authoritative *domain.ResourceError
		err = s.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationRotateSystemUser})
		if !errors.As(err, &authoritative) || authoritative.Message != preflight.Message || authoritative.Name != preflight.Name || authoritative.Reason != preflight.Reason {
			t.Fatalf("preflight drifted from writer admission: preflight=%+v writer=%+v err=%v", preflight, authoritative, err)
		}
		ops, err := s.ListOperations(t.Context(), c.Ref())
		if err != nil || len(ops) != 1 || ops[0].ID != owner.ID {
			t.Fatalf("failed action/preflight created work: %+v %v", ops, err)
		}
	}
	c.Internal = []byte(`{"postgresBackup":{"initialApplied":true}}`)
	if err := s.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckInitialDataSourceMutation(t.Context(), c.Ref()); err != nil {
		t.Fatalf("completed source remained fenced: %v", err)
	}
	plain := &domain.Cluster{Name: "plain"}
	if err := s.CreateCluster(t.Context(), plain); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckInitialDataSourceMutation(t.Context(), plain.Ref()); err != nil {
		t.Fatalf("ordinary cluster received initial-source fence: %v", err)
	}
}
