// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type outcomeProvider struct {
	*initialDataSourceTestProvider
	finish            bool
	calls             int
	syncs             int
	abortBeforeNative bool
}

func newOutcomeProvider(name string) *outcomeProvider {
	return &outcomeProvider{initialDataSourceTestProvider: &initialDataSourceTestProvider{&restoreTestProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}, name: name}}}
}
func (*outcomeProvider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	var evidence struct {
		Applied bool   `json:"applied"`
		Failure string `json:"failure"`
	}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &evidence); err != nil {
			return false, err
		}
	}
	if evidence.Failure != "" {
		return false, errors.New(evidence.Failure)
	}
	return evidence.Applied, nil
}
func (p *outcomeProvider) Sync(context.Context, *engine.Context) (engine.Result, error) {
	p.syncs++
	return engine.Result{}, nil
}
func (p *outcomeProvider) RunOperation(ctx context.Context, rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
	p.calls++
	if state := rc.Cluster.Status.InitialDataSource; state != nil && state.AbortMessage != "" {
		p.abortBeforeNative = true
	}
	if !p.finish {
		return engine.OpResult{Step: "recovery", Checkpoint: json.RawMessage(`{"step":"recovery"}`), RequeueAfter: time.Second}, nil
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if err != nil {
		return engine.OpResult{}, err
	}
	stamp := time.Now().UTC()
	r.Phase, r.Error, r.CompletedAt = domain.PhaseSucceeded, "", &stamp
	if err := rc.Store.UpdateRestore(ctx, r); err != nil {
		return engine.OpResult{}, err
	}
	rc.Cluster.Internal = json.RawMessage(`{"applied":true}`)
	return engine.OpResult{Done: true, Step: "done", Checkpoint: json.RawMessage(`{"step":"done"}`)}, nil
}
func (*outcomeProvider) CanCancelOperation(context.Context, *domain.Operation) error { return nil }
func (*outcomeProvider) SupportsFailureCleanup(*domain.Operation) bool               { return true }
func (p *outcomeProvider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	return p.RunOperation(ctx, rc, op)
}
func (*outcomeProvider) Cleanup(context.Context, *engine.Context, domain.DeleteOptions) (bool, error) {
	return true, nil
}

func outcomeFixture(t *testing.T, name string) (*Manager, *store.Store, *outcomeProvider, *domain.Cluster, *domain.Operation, *domain.Operation, string) {
	t.Helper()
	p := newOutcomeProvider(name)
	path := filepath.Join(t.TempDir(), "outcome.db")
	m, st := openAttributionManager(t, p, path)
	if err := st.CreateBackup(t.Context(), &domain.Backup{Cluster: "source", Name: "base", Provider: name, Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
	create := &domain.Operation{ID: "create", Kind: domain.OperationCreate}
	if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
		t.Fatal(err)
	}
	native := &domain.Operation{ID: "native", Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"initial-restore"}`), Checkpoint: json.RawMessage(`{"step":"poll"}`)}
	r := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}, Phase: domain.PhaseRunning}
	if err := st.EnqueueRestore(t.Context(), r, native); err != nil {
		t.Fatal(err)
	}
	return m, st, p, c, create, native, path
}

func TestTrackingOnlyInitialCreateFailureAllowsVerifiedCompletionAndAudits(t *testing.T) {
	for _, reason := range []string{"deadline", "retry-budget"} {
		t.Run(reason, func(t *testing.T) {
			m, st, p, c, create, _, path := outcomeFixture(t, "postgres")
			// Claim and checkpoint native work before the tracker's budget
			// expires; a merely pre-created Running row is not claim evidence.
			_, _ = m.reconcile(t.Context(), c.Ref())
			var err error
			create, err = st.GetOperation(t.Context(), create.ID)
			if err != nil {
				t.Fatal(err)
			}
			if reason == "deadline" {
				create.Elapsed = DefaultOperationTimeout + time.Minute
			} else {
				create.Failures = m.opts.OperationRetryLimit
			}
			if err := st.UpdateOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(t.Context(), c.Ref())
			failed, err := st.GetOperation(t.Context(), create.ID)
			if err != nil || failed.Phase != domain.PhaseFailed {
				t.Fatalf("tracker did not fail budget: %+v %v", failed, err)
			}
			p.finish = true
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, st = openAttributionManager(t, p, path)
			_, _ = m.reconcile(t.Context(), c.Ref())
			for range 2 {
				_, _ = m.reconcile(t.Context(), c.Ref())
			}
			target, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			state := target.Status.InitialDataSource
			if target.Status.Phase != domain.PhaseReady || state == nil || state.TrackingFailure != create.ID || state.AbortMessage != "" || !state.CompletionAudited {
				t.Fatalf("tracking-only failure hid positive data completion: %+v", target.Status)
			}
			current, err := st.GetOperation(t.Context(), create.ID)
			if err != nil || current.Phase != domain.PhaseFailed || current.Message != failed.Message {
				t.Fatalf("terminal tracker history rewritten: %+v %v", current, err)
			}
			events, err := st.ListEvents(t.Context(), store.EventFilter{Namespace: c.Namespace, Cluster: c.Name})
			if err != nil {
				t.Fatal(err)
			}
			audits := 0
			for _, event := range events {
				if event.Reason == "InitialRestoreCompletedAfterTrackerFailure" {
					audits++
					if event.Operation != create.ID {
						t.Fatalf("audit lost failed tracker: %+v", event)
					}
				}
			}
			if audits != 1 {
				t.Fatalf("completion audit count=%d", audits)
			}
		})
	}
}

func TestGenuineInitialAbortSurvivesLateNativeSuccessAndDrains(t *testing.T) {
	for _, reason := range []string{"explicit-cancel", "native-failure", "partly-done", "native-cancelled", "compensation", "before-claim"} {
		t.Run(reason, func(t *testing.T) {
			m, st, p, c, create, native, path := outcomeFixture(t, "postgres")
			stamp := time.Now().UTC().Add(-time.Second)
			switch reason {
			case "explicit-cancel":
				create.Phase, create.CompletedAt, create.Message = domain.PhaseCancelled, &stamp, "Create explicitly cancelled by operator"
				if err := st.UpdateOperation(t.Context(), create); err != nil {
					t.Fatal(err)
				}
			case "before-claim":
				create.Phase, create.CompletedAt, create.Step, create.Message, create.Elapsed = domain.PhaseFailed, &stamp, domain.InitialRestoreTrackerDeadline, "create exceeded its deadline before native claim", DefaultOperationTimeout+time.Minute
				if err := st.UpdateOperation(t.Context(), create); err != nil {
					t.Fatal(err)
				}
			case "native-failure":
				native.Checkpoint = json.RawMessage(`{"step":"mongod-start","failure":"native restore error"}`)
			case "partly-done":
				native.Checkpoint = json.RawMessage(`{"step":"mongod-start","failure":"native restore partlyDone","restoreConsistencyUncertain":true}`)
			case "native-cancelled":
				native.Checkpoint = json.RawMessage(`{"step":"mongod-start","cancelling":true}`)
			case "compensation":
				native.Step, native.LastError, native.FailureCleanupStartedAt = failureCleanupStep, "native failure cleanup", &stamp
			}
			if err := st.UpdateOperation(t.Context(), native); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(t.Context(), c.Ref())
			before, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || before.Status.InitialDataSource == nil || before.Status.InitialDataSource.AbortMessage == "" || !p.abortBeforeNative {
				t.Fatalf("genuine abort not persisted before compensation: status=%+v before=%v err=%v", before, p.abortBeforeNative, err)
			}
			p.finish = true
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, st = openAttributionManager(t, p, path)
			_, _ = m.reconcile(t.Context(), c.Ref())
			_, _ = m.reconcile(t.Context(), c.Ref())
			target, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if target.Status.Phase != domain.PhaseFailed || target.Status.InitialDataSource == nil || target.Status.InitialDataSource.AbortMessage == "" || p.calls < 2 {
				t.Fatalf("late native success cleared abort or starved compensation: calls=%d status=%+v", p.calls, target.Status)
			}
			applied, err := p.InitialDataSourceApplied(target)
			if err != nil || !applied {
				t.Fatalf("test did not produce late native success: applied=%v err=%v", applied, err)
			}
			current, err := st.GetOperation(t.Context(), native.ID)
			if err != nil || !current.Terminal() {
				t.Fatalf("compensation never reached terminal: %+v %v", current, err)
			}
			events, err := st.ListEvents(t.Context(), store.EventFilter{Namespace: c.Namespace, Cluster: c.Name})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Reason == "InitialRestoreCompletedAfterTrackerFailure" {
					t.Fatalf("genuine abort mis-audited as tracking-only: %+v", event)
				}
			}
		})
	}
}

func TestInitialTeardownAbortSurvivesSuspensionAndLateSuccess(t *testing.T) {
	m, st, p, c, _, _, path := outcomeFixture(t, "postgres")
	if err := st.MarkClusterDeleting(t.Context(), c.Ref(), domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
		t.Fatal(err)
	}
	_, _ = m.reconcile(t.Context(), c.Ref())
	suspended, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || suspended.Status.Phase != domain.PhaseSuspended || suspended.Status.InitialDataSource == nil || suspended.Status.InitialDataSource.AbortMessage == "" {
		t.Fatalf("teardown lost durable initial abort: %+v %v", suspended, err)
	}
	// Suspension deliberately rewrites health conditions. The independent abort
	// must still defeat a late native completion after reopen and internal resume.
	suspended.Internal = json.RawMessage(`{"applied":true}`)
	if err := st.UpdateClusterState(t.Context(), suspended); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	m, st = openAttributionManager(t, p, path)
	if err := st.ResumeCluster(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	_, _ = m.reconcile(t.Context(), c.Ref())
	target, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil || target.Status.Phase != domain.PhaseFailed || target.Status.InitialDataSource == nil || target.Status.InitialDataSource.AbortMessage == "" {
		t.Fatalf("late success/resume erased teardown abort: %+v %v", target, err)
	}
}

func TestRunningRestoreRetryDiagnosticDoesNotBecomeInitialAbort(t *testing.T) {
	for _, name := range []string{"elasticsearch", "postgres", "supabase", "psmdb", "rabbitmq"} {
		t.Run(name, func(t *testing.T) {
			m, st, p, c, _, _, path := outcomeFixture(t, name)
			r, err := st.GetRestore(t.Context(), c.Ref(), "initial-restore")
			if err != nil {
				t.Fatal(err)
			}
			r.Error = "temporary connection failure while polling native recovery"
			if err := st.UpdateRestore(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			_, _ = m.reconcile(t.Context(), c.Ref())
			pending, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if pending.Status.InitialDataSource != nil && pending.Status.InitialDataSource.AbortMessage != "" {
				t.Fatalf("retry diagnostic became permanent restore abort: %+v", pending.Status)
			}
			p.finish = true
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, st = openAttributionManager(t, p, path)
			for range 2 {
				_, _ = m.reconcile(t.Context(), c.Ref())
			}
			target, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if target.Status.Phase != domain.PhaseReady || target.Status.InitialDataSource != nil && target.Status.InitialDataSource.AbortMessage != "" {
				t.Fatalf("positive native recovery remained failed after a transient diagnostic: %+v", target.Status)
			}
			r, err = st.GetRestore(t.Context(), c.Ref(), "initial-restore")
			if err != nil || r.Phase != domain.PhaseSucceeded || r.Error != "" {
				t.Fatalf("positive native completion did not clear retry diagnostic: %+v %v", r, err)
			}
		})
	}
}

type bootstrapOutcomeProvider struct {
	*outcomeProvider
}

func (p *bootstrapOutcomeProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	p.syncs++
	applied, err := p.InitialDataSourceApplied(rc.Cluster)
	if err != nil || applied {
		return engine.Result{}, err
	}
	// Model the production PG ordering: bootstrap ownership predates DATA
	// formation, and only now can the deterministic bookkeeping restore enqueue.
	if err := rc.Store.ClaimInitialDataSource(store.WithClusterSnapshot(ctx, rc.Cluster), rc.Cluster.Ref()); err != nil {
		return engine.Result{}, err
	}
	if _, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore"); errors.Is(err, store.ErrNotFound) {
		d := rc.Cluster.Spec.DataSource
		r := &domain.Restore{Name: "initial-restore", Cluster: rc.Cluster.Name, SourceNamespace: d.Namespace, Source: domain.RestoreSource{SourceCluster: d.Cluster, Backup: d.Backup, PITRTime: d.PITRTime}}
		op := &domain.Operation{ID: "bootstrap-native", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"initial-restore"}`)}
		if err := rc.Store.EnqueueRestore(ctx, r, op); err != nil {
			return engine.Result{}, err
		}
	} else if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{RequeueAfter: time.Second}, nil
}

func TestBootstrapClaimBeforeTrackerExpiryContinuesBeforeRestoreEnqueue(t *testing.T) {
	for _, name := range []string{"postgres", "supabase"} {
		t.Run(name, func(t *testing.T) {
			p := &bootstrapOutcomeProvider{outcomeProvider: newOutcomeProvider(name)}
			path := filepath.Join(t.TempDir(), "bootstrap-window.db")
			_, st := openAttributionManager(t, p, path)
			if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: "source", Provider: name, Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
			create := &domain.Operation{ID: "create", Kind: domain.OperationCreate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
				t.Fatal(err)
			}
			if err := st.ClaimInitialDataSource(store.WithClusterSnapshot(t.Context(), c), c.Ref()); err != nil {
				t.Fatal(err)
			}
			claim, err := st.GetInitialDataSourceClaim(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetRestore(t.Context(), c.Ref(), "initial-restore"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("fixture already has bookkeeping restore: %v", err)
			}
			create.Elapsed = DefaultOperationTimeout + time.Minute
			if err := st.UpdateOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, st := openAttributionManager(t, p, path)
			_, _ = m.reconcile(t.Context(), c.Ref())
			failed, err := st.GetOperation(t.Context(), create.ID)
			if err != nil || failed.Phase != domain.PhaseFailed {
				t.Fatalf("tracker did not expire in bootstrap window: %+v %v", failed, err)
			}
			r, err := st.GetRestore(t.Context(), c.Ref(), "initial-restore")
			if err != nil || r.OperationID == "" {
				t.Fatalf("existing bootstrap claim could not enqueue continuation: %+v %v", r, err)
			}
			pending, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			state := pending.Status.InitialDataSource
			if state == nil || state.TrackingFailure != create.ID || state.AbortMessage != "" || state.ClaimedAt == nil || !state.ClaimedAt.Equal(claim.ClaimedAt) {
				t.Fatalf("durable bootstrap claim was lost/misclassified: %+v", pending.Status)
			}
			p.finish = true
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, st = openAttributionManager(t, p, path)
			for range 2 {
				_, _ = m.reconcile(t.Context(), c.Ref())
			}
			target, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || target.Status.Phase != domain.PhaseReady || target.Status.InitialDataSource == nil || !target.Status.InitialDataSource.CompletionAudited || target.Status.InitialDataSource.AbortMessage != "" {
				t.Fatalf("positive bootstrap continuation failed to become Ready: %+v %v", target, err)
			}
			receipt, err := st.GetInitialDataSourceClaim(t.Context(), c.Ref())
			if err != nil || !receipt.ClaimedAt.Equal(claim.ClaimedAt) {
				t.Fatalf("claim continuation rewrote first ownership time: %+v %v", receipt, err)
			}
			original, err := st.GetOperation(t.Context(), create.ID)
			if err != nil || original.Phase != domain.PhaseFailed {
				t.Fatalf("continuation rewrote failed tracker history: %+v %v", original, err)
			}
		})
	}
}
