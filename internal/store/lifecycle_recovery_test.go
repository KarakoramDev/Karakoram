// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestLateEnqueueAfterDeletionIsRejected(t *testing.T) {
	for _, kind := range []string{"operation", "backup", "restore"} {
		for _, state := range []string{"deleting", "absent", "recreated", "suspended"} {
			t.Run(kind+"/"+state, func(t *testing.T) {
				s, _ := testStore(t)
				ctx := context.Background()
				c := &domain.Cluster{Name: "demo"}
				if err := s.CreateCluster(ctx, c); err != nil {
					t.Fatal(err)
				}
				stale := WithClusterSnapshot(ctx, c)
				if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: state == "suspended"}); err != nil {
					t.Fatal(err)
				}
				if state == "absent" || state == "recreated" {
					if err := s.DeleteCluster(ctx, c.Ref()); err != nil {
						t.Fatal(err)
					}
				}
				if state == "recreated" {
					if err := s.CreateCluster(ctx, &domain.Cluster{Name: c.Name}); err != nil {
						t.Fatal(err)
					}
				}
				if state == "suspended" {
					current, err := s.GetCluster(ctx, c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					if err = s.SuspendCluster(ctx, current); err != nil {
						t.Fatal(err)
					}
				}
				op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart}
				var err error
				switch kind {
				case "operation":
					err = s.EnqueueOperation(stale, op)
				case "backup":
					op.Kind = domain.OperationBackup
					err = s.EnqueueBackup(stale, &domain.Backup{Cluster: c.Name, Name: "daily"}, op)
				case "restore":
					op.Kind = domain.OperationRestore
					err = s.EnqueueRestore(stale, &domain.Restore{Cluster: c.Name, Name: "recover"}, op)
				}
				if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
					t.Fatalf("accepted stale %s: %v", kind, err)
				}
				if _, err = s.GetOperation(ctx, op.ID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("left operation: %v", err)
				}
				if items, err := s.ListBackups(ctx, c.Ref()); err != nil || len(items) != 0 {
					t.Fatalf("left backup: %v %v", items, err)
				}
				if items, err := s.ListRestores(ctx, c.Ref()); err != nil || len(items) != 0 {
					t.Fatalf("left restore: %v %v", items, err)
				}
				if _, err = s.ClaimOperation(ctx, c.Ref()); !errors.Is(err, ErrNotFound) {
					t.Fatalf("stale work claimable: %v", err)
				}
			})
		}
	}
}

func TestStaleSuspendCannotCompleteNewDelete(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	old, err := s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	full := &domain.Operation{Kind: domain.OperationDelete}
	if err = s.MarkClusterDeleting(WithOperation(ctx, full), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = s.SuspendCluster(ctx, old); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale suspension accepted: %v", err)
	}
	current, err := s.GetCluster(ctx, c.Ref())
	if err != nil || !current.Deleting || current.DeleteOptions.KeepVolumes {
		t.Fatalf("lost delete: %+v %v", current, err)
	}
	op, err := s.GetOperation(ctx, full.ID)
	if err != nil || op.Terminal() {
		t.Fatalf("premature completion: %+v %v", op, err)
	}
	if err = s.StartClusterCleanup(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true}); !errors.Is(err, ErrConflict) {
		t.Fatalf("promised retention after full delete: %v", err)
	}
	if err = s.DeleteCluster(WithClusterSnapshot(ctx, current), c.Ref()); err != nil {
		t.Fatal(err)
	}
	op, err = s.GetOperation(ctx, full.ID)
	if err != nil || op.Phase != domain.PhaseSucceeded {
		t.Fatal(op, err)
	}
}

func TestCleanupStartFencesSupersededSnapshotsAndSurvivesRestart(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	stale, err := s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
		t.Fatal("queued deletion could not be superseded", err)
	}
	if err = s.StartClusterCleanup(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatal("superseded cleanup was allowed to start", err)
	}
	if err = s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetCluster(ctx, c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StartClusterCleanup(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
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
	if err = reopened.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true}); !errors.Is(err, ErrConflict) {
		t.Fatal("restart forgot volume cleanup", err)
	}
	if err = reopened.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepBackups: true}); !errors.Is(err, ErrConflict) {
		t.Fatal("restart forgot backup cleanup", err)
	}
	if err = reopened.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal("identical cleanup intent was rejected", err)
	}
}

func TestFailedBackupDeletionCanBeRetried(t *testing.T) {
	for _, outcome := range []domain.Phase{domain.PhaseFailed, domain.PhaseCancelled} {
		t.Run(string(outcome), func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c := &domain.Cluster{Name: "demo"}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "daily", Cluster: c.Name, PBMName: "immutable-target", Phase: domain.PhaseSucceeded}
			if err := s.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
			if err := s.EnqueueBackupDeletion(ctx, b, op); err != nil {
				t.Fatal(err)
			}
			if outcome == domain.PhaseCancelled {
				if _, err := s.RequestOperationCancellation(ctx, op.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				stamp := time.Now()
				op.Phase = outcome
				op.Message = "temporary dependency failure"
				op.CompletedAt = &stamp
				if err := s.UpdateOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
			}
			b, err := s.GetBackup(ctx, c.Ref(), b.Name)
			if err != nil || b.Phase == domain.PhaseDeleting || b.PBMName != "immutable-target" {
				t.Fatalf("wedged inventory: %+v %v", b, err)
			}
			retry := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
			if err = s.EnqueueBackupDeletion(ctx, b, retry); err != nil {
				t.Fatalf("cannot retry: %v", err)
			}
			if retry.ID == op.ID {
				t.Fatal("reused old operation")
			}
		})
	}
}

func TestConcurrentSpecEditsRejectStaleSnapshot(t *testing.T) {
	for _, secrets := range []bool{false, true} {
		t.Run(map[bool]string{false: "spec", true: "secrets"}[secrets], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Version: "7"}}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			stale := WithClusterSnapshot(ctx, c)
			first := c.Spec
			first.Version = "8"
			if err := s.UpdateClusterSpec(stale, c.Ref(), first); err != nil {
				t.Fatal(err)
			}
			second := c.Spec
			second.Version = "9"
			var values map[string][]byte
			if secrets {
				values = map[string][]byte{"tls/user.crt": []byte("certificate")}
			}
			if err := s.UpdateClusterSpecWithSecrets(stale, c.Ref(), second, values); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale edit accepted: %v", err)
			}
			got, err := s.GetCluster(ctx, c.Ref())
			if err != nil || got.Spec.Version != "8" || got.Generation != 2 {
				t.Fatal(got, err)
			}
			if secrets {
				if _, err = s.GetClusterSecret(ctx, c.Ref(), "tls/user.crt"); !errors.Is(err, ErrNotFound) {
					t.Fatalf("stale secret committed: %v", err)
				}
			}
		})
	}
}

func TestEventualDeletionCorrectsFailedTracking(t *testing.T) {
	for _, suspend := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[suspend], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			c := &domain.Cluster{Name: "demo"}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Kind: domain.OperationDelete}
			if err := s.MarkClusterDeleting(WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{KeepVolumes: suspend}); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now()
			op.Phase = domain.PhaseFailed
			op.Message = "retry budget exhausted"
			op.CompletedAt = &stamp
			if err := s.UpdateOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			current, err := s.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if suspend {
				err = s.SuspendCluster(ctx, current)
			} else {
				err = s.DeleteCluster(ctx, c.Ref())
			}
			if err != nil {
				t.Fatal(err)
			}
			done, err := s.GetOperation(ctx, op.ID)
			if err != nil || done.Phase != domain.PhaseSucceeded {
				t.Fatalf("stale failure: %+v %v", done, err)
			}
		})
	}
}

func TestLifecycleRecoveryMigrationRepairsHistoricState(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	deletion := &domain.Operation{Kind: domain.OperationDelete}
	if err := s.MarkClusterDeleting(WithOperation(ctx, deletion), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCluster(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE records SET phase='Failed',data=json_set(data,'$.phase','Failed') WHERE kind='operation' AND key=?", deletion.ID); err != nil {
		t.Fatal(err)
	}
	live := &domain.Cluster{Name: "live"}
	if err := s.CreateCluster(ctx, live); err != nil {
		t.Fatal(err)
	}
	pending := &domain.Operation{Kind: domain.OperationDelete}
	if err := s.MarkClusterDeleting(WithOperation(ctx, pending), live.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now()
	pending.Phase, pending.CompletedAt = domain.PhaseFailed, &stamp
	if err := s.UpdateOperation(ctx, pending); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Name: "daily", Cluster: live.Name, Location: "s3://archive/daily", Phase: domain.PhaseSucceeded}
	if err := s.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	remove := &domain.Operation{Cluster: live.Name, Kind: domain.OperationDeleteBackup}
	if err := s.EnqueueBackupDeletion(ctx, backup, remove); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE records SET phase='Failed',data=json_set(data,'$.phase','Failed') WHERE kind='operation' AND key=?", remove.ID); err != nil {
		t.Fatal(err)
	}
	script, err := migrations.ReadFile("migrations/010_lifecycle_recovery.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.writer.ExecContext(ctx, string(script)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, deletion.ID)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.CompletedAt == nil {
		t.Fatal(got, err)
	}
	got, err = s.GetOperation(ctx, pending.ID)
	if err != nil || got.Phase != domain.PhaseFailed {
		t.Fatal("unproven deletion succeeded", got, err)
	}
	backup, err = s.GetBackup(ctx, live.Ref(), backup.Name)
	if err != nil || backup.Phase != domain.PhaseFailed || backup.Location == "" {
		t.Fatal(backup, err)
	}
	retry := &domain.Operation{Cluster: live.Name, Kind: domain.OperationDeleteBackup}
	if err = s.EnqueueBackupDeletion(ctx, backup, retry); err != nil || retry.Terminal() {
		t.Fatal("remote cleanup was skipped", retry, err)
	}
}

func TestStaleBackupDeletionCannotTargetRecreatedCluster(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	stale := WithClusterSnapshot(ctx, c)
	if err := s.DeleteCluster(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: c.Name}); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Cluster: c.Name, Name: "daily", Location: "s3://archive/new-target", Phase: domain.PhaseSucceeded}
	if err := s.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
	if err := s.EnqueueBackupDeletion(stale, b, op); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale deletion accepted: %v", err)
	}
	current, err := s.GetBackup(ctx, c.Ref(), b.Name)
	if err != nil || current.Phase != domain.PhaseSucceeded || current.Location != b.Location {
		t.Fatal(current, err)
	}
}

func TestClaimBackupDeletionSkipsUnrelatedRuntimeWork(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	restart := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
	cleanup := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
	for _, op := range []*domain.Operation{restart, cleanup} {
		if err := s.EnqueueOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := s.ClaimBackupDeletion(ctx, c.Ref())
	if err != nil || claimed.ID != cleanup.ID || claimed.Phase != domain.PhaseRunning {
		t.Fatal(claimed, err)
	}
	current, err := s.GetOperation(ctx, restart.ID)
	if err != nil || current.Attempts != 0 {
		t.Fatal("filtered claim touched runtime work", current, err)
	}
}

func TestDeletionCompletionUsesIncarnationNotWallClockOrder(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "identified", true: "legacy"}[legacy], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			c := &domain.Cluster{Name: "demo"}
			if err := s.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			// Creation was audited while the host clock was ahead; deletion
			// is admitted after correction, not after a new incarnation.
			future := time.Now().Add(24 * time.Hour).UnixNano()
			if _, err := s.writer.ExecContext(ctx, "UPDATE clusters SET created=? WHERE name=?", future, c.Name); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Kind: domain.OperationDelete, CreatedAt: time.Now().Add(-time.Hour)}
			if err := s.MarkClusterDeleting(WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			op.Phase = domain.PhaseFailed
			if legacy {
				op.ClusterIncarnation = ""
			}
			if err := s.UpdateOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			script, err := migrations.ReadFile("migrations/010_lifecycle_recovery.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.writer.ExecContext(ctx, string(script)); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetOperation(ctx, op.ID)
			if err != nil || got.Phase != domain.PhaseFailed {
				t.Fatal("wall-clock order invented completed cleanup", got, err)
			}
			if err = s.DeleteCluster(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err = s.GetOperation(ctx, op.ID)
			if err != nil || got.Phase != domain.PhaseSucceeded {
				t.Fatal("actual removal did not complete matching cleanup", got, err)
			}
		})
	}
}

func TestLifecycleMigrationRecognizesExplicitlyReplacedIncarnation(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationDelete}
	if err := s.MarkClusterDeleting(WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCluster(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE records SET phase='Failed',data=json_set(data,'$.phase','Failed') WHERE kind='operation' AND key=?", op.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: c.Name}); err != nil {
		t.Fatal(err)
	}
	// The replacement's timestamp is deliberately earlier, not later.
	if _, err := s.writer.ExecContext(ctx, "UPDATE clusters SET created=? WHERE name=?", c.CreatedAt.Add(-time.Hour).UnixNano(), c.Name); err != nil {
		t.Fatal(err)
	}
	script, err := migrations.ReadFile("migrations/010_lifecycle_recovery.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.writer.ExecContext(ctx, string(script)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseSucceeded {
		t.Fatal("replacement was confused with the original target", got, err)
	}
}
