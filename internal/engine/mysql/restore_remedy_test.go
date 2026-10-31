// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func blockedRestoreRemedyFixture(t *testing.T, phase domain.Phase) (*Core, *engine.Context, *domain.Operation) {
	t.Helper()
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	op := &domain.Operation{ID: "old-blocked-restore", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: phase}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	cp := backupCheckpoint{Step: "certify-blocked", GTIDCertification: &pxcRestoreCertification{Negotiated: true, Phase: "blocked", CleanupComplete: true}}
	if err := persistRestore(rc, op, cp); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	s.RestoreGTIDBlocked = "PXC restored GTID mappings disagree"
	s.GTIDIncidents = []gtidIncident{{DetectedAt: time.Now(), Audited: true}}
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	return p, rc, op
}

func TestPXCTerminalBlockedRestoreRemedyAdmission(t *testing.T) {
	for _, scenario := range []string{"terminal", "running", "pending", "native-recovery", "cleanup-pending", "fence-owned", "unclean"} {
		t.Run(scenario, func(t *testing.T) {
			phase := domain.PhaseFailed
			if scenario == "running" {
				phase = domain.PhaseRunning
			}
			if scenario == "pending" {
				phase = domain.PhasePending
			}
			p, rc, op := blockedRestoreRemedyFixture(t, phase)
			bs, _ := loadBackupState(rc.Cluster)
			var cp backupCheckpoint
			if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "native-recovery":
				cp.Step = "reform"
				cp.GTIDCertification.RecoveryCleanupPending = true
			case "cleanup-pending":
				cp.GTIDCertification.CleanupPending = true
			case "fence-owned":
				cp.GTIDCertification.FencesOwned = true
			case "unclean":
				cp.GTIDCertification.CleanupComplete = false
			}
			if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if err := persistRestore(rc, op, cp); err != nil {
				t.Fatal(err)
			}
			ordinary, err := p.CanClaimOperation(rc.Cluster)
			if err != nil || ordinary {
				t.Fatalf("ordinary maintenance admitted: %t %v", ordinary, err)
			}
			kind, err := p.RecoveryOperationKind(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if scenario == "terminal" {
				want = domain.OperationRestore
			}
			if kind != want {
				t.Fatalf("kind=%q want=%q", kind, want)
			}
		})
	}
}

func TestPXCTerminalBlockedRestoreHandoffPreservesIncident(t *testing.T) {
	p, rc, old := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
	next := &domain.Operation{ID: "new-restore", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"missing-source"}`)}
	if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	// A missing/unresolved new source cannot consume the old recovery owner.
	if _, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{}); err == nil {
		t.Fatal("missing source accepted")
	}
	bs, _ := loadBackupState(rc.Cluster)
	if bs.RestoreOperation.ID != old.ID {
		t.Fatal("unresolved request replaced recovery owner")
	}
	cp := backupCheckpoint{Step: "preflight", Member: "seed", Source: &restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Backup: "new-full"}, Request: &myspec.Restore{BackupID: "new-full"}}
	pending := *next
	pending.Phase = domain.PhasePending
	if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), &pending); err != nil {
		t.Fatal(err)
	}
	if err := handoffBlockedRestore(t.Context(), rc, next, &cp); err == nil {
		t.Fatal("pending request acquired recovery ownership")
	}
	if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if err := handoffBlockedRestore(t.Context(), rc, next, &cp); err != nil {
		t.Fatal(err)
	}
	bs, _ = loadBackupState(rc.Cluster)
	var fresh backupCheckpoint
	if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &fresh); err != nil {
		t.Fatal(err)
	}
	if bs.RestoreOperation.ID != next.ID || fresh.Step != "preflight" || fresh.GTIDCertification != nil {
		t.Fatal("new request inherited old certification")
	}
	// Re-entry must negotiate a fresh certification, never reuse the blocked
	// owner's success flags or acknowledge its incident. Hydration may then fail
	// because this unit fixture deliberately has no physical backup inventory.
	_, _ = p.runRestore(t.Context(), rc, next, backupCheckpoint{})
	bs, _ = loadBackupState(rc.Cluster)
	if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.GTIDCertification == nil || !fresh.GTIDCertification.Negotiated || fresh.GTIDCertification.Certified {
		t.Fatal("new restore did not establish fresh certification ownership")
	}
	s, _ := load(rc.Cluster)
	if err := loadGTIDJournal(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("handoff cleared real divergence")
	}
	retained, err := rc.Store.GetOperation(t.Context(), old.ID)
	if err != nil || retained.Phase != domain.PhaseFailed {
		t.Fatal("old terminal record lost", err)
	}
}

func assertPXCRestoreOnlyAdmission(t *testing.T, p *Core, rc *engine.Context) {
	t.Helper()
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatalf("unresolved certification admitted ordinary operations: %t %v", allowed, err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenanceAllowed(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("unresolved certification admitted native maintenance: %v", err)
	}
	if kind, err := p.RecoveryOperationKind(t.Context(), rc); err != nil || kind != domain.OperationRestore {
		t.Fatalf("fresh explicit restore remedy was not available: kind=%q error=%v", kind, err)
	}
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("failed remedy acknowledged or erased the original GTID incident")
	}
}

func TestPXCRestoreRemedyRejectedPreflightRestoresExactBlockedOwner(t *testing.T) {
	for _, step := range []string{"preflight", "preflight-poll", "fence", "fence-recheck", "fence-recheck-poll"} {
		t.Run(step, func(t *testing.T) {
			p, rc, old := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
			st := rc.Store.(*store.Store)
			before, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			original := before.RestoreOperation
			// Queue history need not contain the latest native checkpoint.
			// Rollback must restore the actual saved blocked owner, not this.
			old.Step, old.Checkpoint = "preflight", json.RawMessage(`{"step":"preflight"}`)
			if err := st.UpdateOperation(t.Context(), old); err != nil {
				t.Fatal(err)
			}
			next := &domain.Operation{ID: "replacement-preflight", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}
			if err := st.EnqueueOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			cp := backupCheckpoint{Step: "preflight", Member: "seed", Source: &restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Backup: "new-full"}, Request: &myspec.Restore{BackupID: "new-full"}}
			if err := handoffBlockedRestore(t.Context(), rc, next, &cp); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cp.PreviousBlockedRestore, original) {
				t.Fatal("handoff did not retain the exact prior blocked checkpoint")
			}
			// A real source/preflight failure after handoff must not drop the
			// old unresolved-certification admission state during cleanup.
			if _, err := p.runRestore(t.Context(), rc, next, cp); err == nil {
				t.Fatal("missing physical source was accepted")
			}
			saved, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(saved.RestoreOperation.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.PreviousBlockedRestore == nil {
				t.Fatal("fresh certification negotiation lost the previous owner")
			}
			cp.Step = step
			if err := persistRestore(rc, next, cp); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now()
			next.FailureCleanupStartedAt = &stamp
			next.Checkpoint = backupResult(cp, false).Checkpoint
			if err := st.UpdateOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			result, err := p.CancelOperation(t.Context(), rc, next)
			if err != nil || !result.Done {
				t.Fatalf("untouched replacement cleanup failed: %+v %v", result, err)
			}
			restored, err := loadBackupState(rc.Cluster)
			if err != nil || !reflect.DeepEqual(restored.RestoreOperation, original) {
				t.Fatalf("preflight cleanup did not restore the exact native owner: %+v %v", restored.RestoreOperation, err)
			}
			next.Phase, next.Step, next.CompletedAt = domain.PhaseFailed, result.Step, &stamp
			if err := st.UpdateOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			assertPXCRestoreOnlyAdmission(t, p, rc)
			for i, kind := range p.Describe().Operations {
				if err := st.EnqueueOperation(t.Context(), &domain.Operation{
					ID: "queued-" + kind, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: kind,
					CreatedAt: stamp.Add(time.Duration(i) * time.Second),
				}); err != nil {
					t.Fatal(err)
				}
			}
			kind, err := p.RecoveryOperationKind(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := st.ClaimRecoveryOperation(t.Context(), rc.Cluster.Ref(), kind)
			if err != nil || claimed.Kind != domain.OperationRestore {
				t.Fatalf("restore-only selector claimed arbitrary maintenance: %+v %v", claimed, err)
			}
			for _, candidate := range p.Describe().Operations {
				if candidate == domain.OperationRestore {
					continue
				}
				queued, err := st.GetOperation(t.Context(), "queued-"+candidate)
				if err != nil || queued.Phase != domain.PhasePending || queued.Attempts != 0 {
					t.Fatalf("blocked kind %s was admitted: %+v %v", candidate, queued, err)
				}
			}
		})
	}
}

func TestPXCRestoreRemedyUnresolvedSourceCompletesUnacquiredCleanup(t *testing.T) {
	p, rc, old := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
	st := rc.Store.(*store.Store)
	before, err := loadBackupState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	request := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "unresolved-remedy", Source: domain.RestoreSource{Backup: "missing-full"}, Phase: domain.PhasePending}
	if err := st.CreateRestore(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	next := &domain.Operation{ID: "unresolved-owner", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"unresolved-remedy"}`)}
	if err := st.EnqueueOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	result, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
	if err == nil {
		t.Fatal("unresolved source was accepted")
	}
	var unowned backupCheckpoint
	if err := json.Unmarshal(result.Checkpoint, &unowned); err != nil {
		t.Fatal(err)
	}
	if unowned.Source == nil || unowned.Request != nil || unowned.Step != "" || unowned.StopBoundary != restoreStopUntouched {
		t.Fatalf("source-resolution failure acquired restore controls: %+v", unowned)
	}
	next.Checkpoint = result.Checkpoint
	stamp := time.Now()
	next.FailureCleanupStartedAt = &stamp
	if err := st.UpdateOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	cleaned, err := p.CancelOperation(t.Context(), rc, next)
	if err != nil || !cleaned.Done {
		t.Fatalf("never-acquired remedy remained in failure cleanup: %+v %v", cleaned, err)
	}
	after, err := loadBackupState(rc.Cluster)
	if err != nil || !reflect.DeepEqual(before, after) || after.RestoreOperation.ID != old.ID {
		t.Fatalf("unacquired cleanup changed the old frozen recovery owner: %+v %v", after, err)
	}
	next.Phase, next.Step, next.CompletedAt = domain.PhaseFailed, cleaned.Step, &stamp
	if err := st.UpdateOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	assertPXCRestoreOnlyAdmission(t, p, rc)
	if running, err := st.ClaimRunningOperation(t.Context(), rc.Cluster.Ref()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed remedy still owns the active queue: %+v %v", running, err)
	}
	for _, kind := range []string{domain.OperationBackup, domain.OperationRestore} {
		if err := st.EnqueueOperation(t.Context(), &domain.Operation{ID: "next-" + kind, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	kind, err := p.RecoveryOperationKind(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimRecoveryOperation(t.Context(), rc.Cluster.Ref(), kind)
	if err != nil || claimed.ID != "next-"+domain.OperationRestore || claimed.Phase != domain.PhaseRunning {
		t.Fatalf("a fresh restore could not be claimed immediately after cleanup: %+v %v", claimed, err)
	}
}

func TestPXCBlockedCertificationWithoutNativeOwnerRemainsRestoreOnly(t *testing.T) {
	p, rc, _ := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	bs.RestoreOperation = nil
	if err := saveBackupState(rc.Cluster, bs); err != nil {
		t.Fatal(err)
	}
	assertPXCRestoreOnlyAdmission(t, p, rc)
}

func TestPXCRestoreRemedyCleanupPreservesActiveAndNativeRecoveryOwners(t *testing.T) {
	for _, scenario := range []string{"running-old", "native-recovery", "owned-checkpoint", "no-cleanup-marker"} {
		t.Run(scenario, func(t *testing.T) {
			phase := domain.PhaseFailed
			if scenario == "running-old" {
				phase = domain.PhaseRunning
			}
			p, rc, _ := blockedRestoreRemedyFixture(t, phase)
			st := rc.Store.(*store.Store)
			bs, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "native-recovery" {
				var cp backupCheckpoint
				if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				cp.Step = "reform"
				cp.GTIDCertification.RecoveryCleanupPending = true
				if err := persistRestore(rc, bs.RestoreOperation, cp); err != nil {
					t.Fatal(err)
				}
			}
			before, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Now()
			next := &domain.Operation{ID: "not-unowned", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, FailureCleanupStartedAt: &stamp}
			if scenario == "owned-checkpoint" {
				next.Checkpoint = backupResult(backupCheckpoint{Step: "stopped", Member: "seed"}, false).Checkpoint
			}
			if scenario == "no-cleanup-marker" {
				next.FailureCleanupStartedAt = nil
			}
			if err := st.EnqueueOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CancelOperation(t.Context(), rc, next); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("cleanup bypassed active/frozen ownership: %v", err)
			}
			after, err := loadBackupState(rc.Cluster)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("rejected cleanup changed existing ownership")
			}
		})
	}
}

func TestPXCRestoreRemedyStoppedCheckpointKeepsNewRecoveryOwner(t *testing.T) {
	p, rc, _ := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
	st := rc.Store.(*store.Store)
	stamp := time.Now()
	next := &domain.Operation{ID: "stopped-replacement", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, FailureCleanupStartedAt: &stamp}
	if err := st.EnqueueOperation(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	cp := backupCheckpoint{Step: "preflight", Member: "seed", Source: &restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Backup: "new-full"}, Request: &myspec.Restore{BackupID: "new-full"}}
	if err := handoffBlockedRestore(t.Context(), rc, next, &cp); err != nil {
		t.Fatal(err)
	}
	// Even a retained prior-owner marker cannot authorize rollback after the
	// replacement has acquired its own frozen native recovery checkpoint.
	cp.Step = "stopped"
	cp.StopBoundary = restoreStopStarted
	if err := persistRestore(rc, next, cp); err != nil {
		t.Fatal(err)
	}
	result, err := p.CancelOperation(t.Context(), rc, next)
	if err != nil || !result.Done || result.Step != "restore-recovery" {
		t.Fatalf("native recovery ownership was not retained: %+v %v", result, err)
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil || bs.RestoreOperation == nil || bs.RestoreOperation.ID != next.ID {
		t.Fatal("stopped replacement reverted to the old blocked owner")
	}
	if kind, err := p.RecoveryOperationKind(t.Context(), rc); err != nil || kind != "" {
		t.Fatalf("another restore bypassed the frozen new owner: kind=%q error=%v", kind, err)
	}
}

func queueRestoreRemedySource(t *testing.T, rc *engine.Context, id string) (*domain.Operation, domain.Backup) {
	t.Helper()
	st := rc.Store.(*store.Store)
	storage := &domain.BackupStorage{Namespace: rc.Cluster.Namespace, Name: "archive", Type: "s3", EndpointURL: "https://objects.example.test", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := st.CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	base := inventoryBackup(t, rc, "remedy-full", "full", "", 10, time.Unix(10, 0))
	request := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: id, Source: domain.RestoreSource{Backup: base.Name}, Phase: domain.PhasePending}
	if err := st.CreateRestore(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"restore": request.Name})
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: id, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: payload}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	return op, base
}

func TestPXCRestoreRemedyInheritedSSTStopDoesNotOwnNewPreflight(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		name := "saved-blocked-owner"
		if orphan {
			name = "owner-free-blocked-state"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, old := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
			st := rc.Store.(*store.Store)
			b := rc.Backend.(*fakeBackend)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			target := s.data()[2]
			target.Offline, target.Initialized, target.Stage = true, false, "deployed"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			native := b.native[target.Name]
			native.MemberState = "OFFLINE"
			b.native[target.Name] = native
			bs, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			var blocked backupCheckpoint
			if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &blocked); err != nil {
				t.Fatal(err)
			}
			blocked.GTIDCertification.Target = target.Name
			if err := persistRestore(rc, old, blocked); err != nil {
				t.Fatal(err)
			}
			if orphan {
				bs, err = loadBackupState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				bs.RestoreOperation = nil
				if err := saveBackupState(rc.Cluster, bs); err != nil {
					t.Fatal(err)
				}
			}
			before, err := loadBackupState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			next, base := queueRestoreRemedySource(t, rc, "reject-inherited-hold")
			originalAction := b.action
			mutations := 0
			b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
				if r.URL.Path == "/v1/reload" || strings.HasPrefix(r.URL.Path, "/v1/processes/") {
					mutations++
				}
				if r.URL.Path == "/v1/mysql/restore-preflight" {
					_ = json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "failed", Stage: "metadata", Error: "source preflight rejected"})
					return true
				}
				return originalAction(w, r, member)
			}
			initialized, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
			if err != nil {
				t.Fatal(err)
			}
			var fresh backupCheckpoint
			if err := json.Unmarshal(initialized.Checkpoint, &fresh); err != nil {
				t.Fatal(err)
			}
			if fresh.StopBoundary != restoreStopUntouched || fresh.Step != "preflight" {
				t.Fatal("fresh preflight did not establish its own untouched receipt")
			}
			rejected, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
			if err == nil {
				t.Fatal("rejected preflight was accepted")
			}
			stamp := time.Now()
			next.Checkpoint, next.FailureCleanupStartedAt = rejected.Checkpoint, &stamp
			if err := st.UpdateOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			cleaned, err := p.CancelOperation(t.Context(), rc, next)
			if err != nil || !cleaned.Done || cleaned.Step != "restore-preflight-aborted" {
				t.Fatalf("inherited hold stranded new preflight cleanup: %+v %v", cleaned, err)
			}
			after, err := loadBackupState(rc.Cluster)
			if err != nil || !reflect.DeepEqual(after.RestoreOperation, before.RestoreOperation) {
				t.Fatal("new preflight cleanup changed the inherited blocked owner")
			}
			current, err := load(rc.Cluster)
			if err != nil || !current.find(target.Name).Offline || b.native[target.Name].MemberState != "OFFLINE" {
				t.Fatal("new preflight cleanup released or reused an inherited native stop")
			}
			b.mu.Lock()
			changed := mutations
			b.mu.Unlock()
			if changed != 0 {
				t.Fatalf("untouched remedy issued %d native control actions", changed)
			}
			next.Phase, next.CompletedAt = domain.PhaseFailed, &stamp
			if err := st.UpdateOperation(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			assertPXCRestoreOnlyAdmission(t, p, rc)
			request := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "after-preflight-reject", Source: domain.RestoreSource{Backup: base.Name}, Phase: domain.PhasePending}
			if err := st.CreateRestore(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := st.EnqueueOperation(t.Context(), &domain.Operation{ID: "after-preflight-reject", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"after-preflight-reject"}`)}); err != nil {
				t.Fatal(err)
			}
			kind, err := p.RecoveryOperationKind(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := st.ClaimRecoveryOperation(t.Context(), rc.Cluster.Ref(), kind)
			if err != nil || claimed.ID != "after-preflight-reject" {
				t.Fatalf("another explicit restore remained stranded: %+v %v", claimed, err)
			}
		})
	}
}

func TestPXCRestoreRemedyCommitsStopBoundaryBeforeNativeControls(t *testing.T) {
	p, rc, _ := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
	b := rc.Backend.(*fakeBackend)
	next, _ := queueRestoreRemedySource(t, rc, "stop-intent-remedy")
	initialized, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	var cp backupCheckpoint
	if err := json.Unmarshal(initialized.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	cp.Step, cp.Preflighted, cp.PlanDigest, cp.RecheckID = "fence-recheck-poll", true, fixtureManifestDigest, "fresh-stop-intent"
	if err := persistRestore(rc, next, cp); err != nil {
		t.Fatal(err)
	}
	before, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	originalAction := b.action
	mutations := 0
	b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
		if r.URL.Path == "/v1/reload" || strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			mutations++
		}
		if r.URL.Path == "/v1/mysql/restore-status" {
			_ = json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "prepared", Stage: "ready", PlanDigest: cp.PlanDigest, RecheckID: cp.RecheckID, VerifiedRecheckID: cp.RecheckID})
			return true
		}
		return originalAction(w, r, member)
	}
	started, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := loadBackupState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var durable backupCheckpoint
	if err := json.Unmarshal(saved.RestoreOperation.Checkpoint, &durable); err != nil {
		t.Fatal(err)
	}
	after, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	changed := mutations
	b.mu.Unlock()
	if durable.StopBoundary != restoreStopStarted || durable.Step != "stop" || changed != 0 || after.ConfigVersion != before.ConfigVersion || !reflect.DeepEqual(after.Members, before.Members) {
		t.Fatal("stop intent was not persisted before desired/native control mutation")
	}
	stamp := time.Now()
	next.Checkpoint, next.FailureCleanupStartedAt = started.Checkpoint, &stamp
	recreated := New(Options{Provider: "pxc"})
	cleaned, err := recreated.CancelOperation(t.Context(), rc, next)
	if err != nil || !cleaned.Done || cleaned.Step != "restore-recovery" {
		t.Fatalf("started boundary was rolled back despite its durable intent: %+v %v", cleaned, err)
	}
	retained, err := loadBackupState(rc.Cluster)
	if err != nil || retained.RestoreOperation == nil || retained.RestoreOperation.ID != next.ID {
		t.Fatal("stop-intent cleanup revived the old blocked owner")
	}
}

func TestPXCRestoreRemedyLegacyStopBoundaryRetainsConservativeInference(t *testing.T) {
	for _, offline := range []bool{false, true} {
		name := "no-native-hold"
		if offline {
			name = "unknown-native-hold"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, old := blockedRestoreRemedyFixture(t, domain.PhaseFailed)
			next, _ := queueRestoreRemedySource(t, rc, "legacy-stop-proof")
			initialized, err := p.runRestore(t.Context(), rc, next, backupCheckpoint{})
			if err != nil {
				t.Fatal(err)
			}
			var cp backupCheckpoint
			if err := json.Unmarshal(initialized.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			cp.StopBoundary = "" // Model a persisted checkpoint from before stop receipts.
			if err := persistRestore(rc, next, cp); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.data()[2].Offline = offline
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now()
			next.FailureCleanupStartedAt = &stamp
			result, err := p.CancelOperation(t.Context(), rc, next)
			if err != nil || !result.Done {
				t.Fatalf("legacy cleanup failed: %+v %v", result, err)
			}
			bs, err := loadBackupState(rc.Cluster)
			if err != nil || bs.RestoreOperation == nil {
				t.Fatal("legacy cleanup erased recovery ownership")
			}
			expected := old.ID
			if offline {
				expected = next.ID
			}
			if bs.RestoreOperation.ID != expected {
				t.Fatalf("legacy hold inference changed: owner=%s want=%s", bs.RestoreOperation.ID, expected)
			}
		})
	}
}
