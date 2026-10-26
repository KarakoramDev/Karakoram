// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type failedStorageDeployBackend struct {
	*testBackend
	member   string
	deploys  int
	outcome  infra.OpState
	retryErr error
}

func (b *failedStorageDeployBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name != b.member || b.outcome.Done && !b.outcome.Failed {
		return b.testBackend.Deploy(ctx, name, d)
	}
	if b.retryErr != nil {
		return infra.Op{}, b.retryErr
	}
	b.deploys++
	return infra.Op{ID: "version:storage-version", ServiceID: name + "-id", Kind: "deploy"}, nil
}

func (b *failedStorageDeployBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "version:storage-version" {
		return b.outcome, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func storageDeployFailureFixture(t *testing.T, outcome infra.OpState) (*Provider, *engine.Context, *failedStorageDeployBackend) {
	t.Helper()
	p, rc, base := setupProvider(t)
	convergeProvider(t, p, rc)
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &domain.BackupStorage{
		Name: "objects", Namespace: "default", Type: "s3", EndpointURL: "https://objects.example.test", Bucket: "objects", Region: "test", VerifyTLS: true,
		Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "test-value"},
	}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
	desired := rc.Cluster.Spec.Components.Extra["storage"]
	desired.Replicas = 1
	rc.Cluster.Spec.Components.Extra["storage"] = desired
	rc.Cluster.Status.ObservedGeneration = 0
	rc.Cluster.Status.Phase = domain.PhaseCreating
	backend := &failedStorageDeployBackend{testBackend: base, member: rc.Cluster.Name + codes["storage"], outcome: outcome}
	rc.Backend = backend
	for range 50 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Pending != nil && s.Pending.Component == "storage" && s.Pending.Action == "deploy" {
			return p, rc, backend
		}
	}
	t.Fatal("Storage deployment was never submitted")
	return nil, nil, nil
}

func TestStorageTerminalDeploymentFailureRemainsActionableUntilRecovery(t *testing.T) {
	p, rc, backend := storageDeployFailureFixture(t, infra.OpState{Done: true, Failed: true, Status: "DEPLOY_FAILED"})
	_, err := p.Sync(t.Context(), rc)
	if err == nil || !strings.Contains(err.Error(), "version:storage-version") || !strings.Contains(err.Error(), backend.member+"-id") {
		t.Errorf("terminal deployment lost its immutable platform identities: %v", err)
	}
	checkFailure := func() {
		t.Helper()
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != domain.PhaseDegraded || status.ObservedGeneration != 0 {
			t.Errorf("terminal deployment hidden as normal creation/convergence: phase=%s observed=%d", status.Phase, status.ObservedGeneration)
		}
		for _, typ := range []string{"Ready", "ComponentsReady"} {
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == typ {
					found = condition.Status == "False" && condition.Reason == "ComponentDeploymentFailed" && strings.Contains(condition.Message, "version:storage-version") && strings.Contains(condition.Message, "DEPLOY_FAILED") && strings.Contains(condition.Message, "preparation")
				}
			}
			if !found {
				t.Errorf("missing actionable %s deployment failure: %+v", typ, status.Conditions)
			}
		}
		for _, member := range status.Members {
			if member.Name != backend.member && !member.Health {
				t.Errorf("optional deployment withdrew healthy sibling %s", member.Name)
			}
		}
	}
	checkFailure()
	// A restart and an in-flight retry must retain the confirmed failure. The
	// previously terminal receipt is consumed; no ambiguous request is replayed.
	p = New(p.options)
	backend.outcome = infra.OpState{Status: "OUTCOME_UNKNOWN"}
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		checkFailure()
	}
	if backend.deploys != 2 {
		t.Errorf("pending/ambiguous deployment was replayed: %d submissions", backend.deploys)
	}
	backend.outcome = infra.OpState{Done: true}
	backend.deployed[backend.member] = true
	convergeProvider(t, p, rc)
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatalf("successful exact deployment did not clear failure: phase=%s err=%v", status.Phase, err)
	}
}

func TestStorageAmbiguousDeploymentRemainsPendingWithoutTerminalFailure(t *testing.T) {
	p, rc, backend := storageDeployFailureFixture(t, infra.OpState{Status: "OUTCOME_UNKNOWN"})
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := loadState(rc.Cluster)
	if err != nil || s.Pending == nil || backend.deploys != 1 {
		t.Fatalf("ambiguous outcome was discarded or replayed: pending=%+v deploys=%d err=%v", s.Pending, backend.deploys, err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatalf("ambiguous deployment became Ready: phase=%s err=%v", status.Phase, err)
	}
	for _, condition := range status.Conditions {
		if condition.Reason == "ComponentDeploymentFailed" {
			t.Fatal("unknown deployment outcome was invented as a confirmed failure")
		}
	}
}

func TestStoragePendingDeploymentShowsExactPhaseAndElapsedTime(t *testing.T) {
	p, rc, backend := storageDeployFailureFixture(t, infra.OpState{Status: "PREPARING_RUNTIME"})
	start := clockNow(rc)
	rc.Clock = importReplayClock{at: start}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	rc.Clock = importReplayClock{at: start.Add(21 * time.Minute)}
	p = New(p.options)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseCreating || status.ObservedGeneration != 0 {
		t.Fatalf("pending platform outcome falsely converged: phase=%s observed=%d err=%v", status.Phase, status.ObservedGeneration, err)
	}
	for _, typ := range []string{"Ready", "ComponentsReady"} {
		found := false
		for _, c := range status.Conditions {
			if c.Type == typ {
				found = c.Status == "False" && c.Reason == "ComponentDeploymentPending" && strings.Contains(c.Message, "PREPARING_RUNTIME") && strings.Contains(c.Message, "21m") && strings.Contains(c.Message, "version:storage-version") && strings.Contains(c.Message, backend.member)
			}
		}
		if !found {
			t.Errorf("missing exact pending platform phase/age on %s: %+v", typ, status.Conditions)
		}
	}
	if backend.deploys != 1 {
		t.Fatalf("long-running unknown outcome was blindly replayed: %d deployments", backend.deploys)
	}
}

func TestStorageUnsubmittedAttemptDoesNotInventOrReplaceTerminalFailure(t *testing.T) {
	for _, priorFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial-pre-send-crash", true: "retry-pre-send-crash"}[priorFailure], func(t *testing.T) {
			p, rc, _ := storageDeployFailureFixture(t, infra.OpState{Done: true, Failed: true, Status: "RETRY_REQUIRED"})
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["storage"]
			var original *componentDeploymentFailure
			if priorFailure {
				original = &componentDeploymentFailure{Op: s.Pending.Op, Status: "DEPLOY_FAILED"}
				m.DeploymentFailure = original
				if err := saveState(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			_, _ = p.Sync(t.Context(), rc)
			// Reload the checkpoint, as a newly constructed provider does.
			s, err = loadState(rc.Cluster)
			if err != nil || s.Pending != nil {
				t.Fatalf("unsubmitted attempt was not retired for normal retry: %+v %v", s, err)
			}
			got := s.Members["storage"].DeploymentFailure
			if !priorFailure && got != nil {
				t.Fatalf("pre-send crash invented a terminal platform deployment failure: %+v", got)
			}
			if priorFailure && (got == nil || got.Status != original.Status || got.Op.ID != original.Op.ID || got.Op.ServiceID != original.Op.ServiceID) {
				t.Fatalf("pre-send retry discarded original terminal evidence: got=%+v want=%+v", got, original)
			}
		})
	}
}

func TestStorageInventoryErrorRetainsDeploymentDiagnosis(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "failed"}[terminal], func(t *testing.T) {
			outcome := infra.OpState{Status: "PREPARING_RUNTIME"}
			reason := "ComponentDeploymentPending"
			if terminal {
				outcome = infra.OpState{Done: true, Failed: true, Status: "DEPLOY_FAILED"}
				reason = "ComponentDeploymentFailed"
			}
			p, rc, backend := storageDeployFailureFixture(t, outcome)
			_, _ = p.Sync(t.Context(), rc)
			rc.Backend = &componentInventoryBackend{testBackend: backend.testBackend, member: backend.member, err: infra.ErrUnavailable}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, typ := range []string{"Ready", "ComponentsReady"} {
				found := false
				for _, c := range status.Conditions {
					if c.Type == typ {
						found = c.Status == "False" && c.Reason == reason && strings.Contains(c.Message, "version:storage-version") && strings.Contains(c.Message, outcome.Status) && strings.Contains(c.Message, "inventory")
					}
				}
				if !found {
					t.Errorf("inventory failure erased %s deployment evidence: %+v", typ, status.Conditions)
				}
			}
		})
	}
}

func TestStorageDeploymentFailurePersistsOperatorRecoveryDetails(t *testing.T) {
	p, rc, backend := storageDeployFailureFixture(t, infra.OpState{Done: true, Failed: true, Status: "DEPLOY_FAILED"})
	_, _ = p.Sync(t.Context(), rc)
	backend.retryErr = &infra.DeploymentExhaustedError{Service: backend.member, Version: "storage-version", Attempts: 4, LastError: "500 Internal Server Error"}
	_, _ = p.Sync(t.Context(), rc)
	p = New(p.options)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"Deployment/" + backend.member} {
		found := false
		for _, c := range status.Conditions {
			if c.Type == typ {
				found = c.Status == "False" && c.Reason == "DeploymentRetriesExhausted" && strings.Contains(c.Message, "4 attempts") && strings.Contains(c.Message, "500 Internal Server Error") && strings.Contains(c.Message, "retry-deployments")
			}
		}
		if !found {
			t.Errorf("checkpoint lost operator recovery details for %s: %+v", typ, status.Conditions)
		}
	}
}

type failedComponentRuntimeBackend struct {
	*testBackend
	target                      string
	canRecover                  bool
	scales, resizes, recoveries int
	pendingPolls                int
}

func (b *failedComponentRuntimeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "retained-component" {
		b.services[b.target].Status = infra.StatusRunning
		return infra.OpState{Done: true, Status: "done"}, nil
	}
	if op.ID == "pending-component-scale" {
		b.pendingPolls++
		return infra.OpState{Status: "starting"}, nil
	}
	if op.ID == "failed-runtime" || op.ID == "component-scale" || op.ID == "component-resize" {
		return infra.OpState{Failed: true, Status: "failed", Message: "native runtime rollout failed"}, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func (b *failedComponentRuntimeBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	if name != b.target {
		return b.testBackend.SetContainerCount(ctx, name, min, max)
	}
	b.scales++
	if _, err := b.testBackend.SetContainerCount(ctx, name, min, max); err != nil {
		return infra.Op{}, err
	}
	b.services[name].Status = infra.StatusFailed
	return infra.Op{ID: "component-scale", Kind: "scale", ServiceID: b.services[name].ID}, nil
}

func (b *failedComponentRuntimeBackend) SetResources(ctx context.Context, name string, resources domain.Resources) (infra.Op, error) {
	if name != b.target {
		return b.testBackend.SetResources(ctx, name, resources)
	}
	b.resizes++
	if _, err := b.testBackend.SetResources(ctx, name, resources); err != nil {
		return infra.Op{}, err
	}
	b.services[name].Status = infra.StatusFailed
	return infra.Op{ID: "component-resize", Kind: "resources", ServiceID: b.services[name].ID}, nil
}

func (b *failedComponentRuntimeBackend) RecoverFailedScale(_ context.Context, name string, min, max int) (infra.Op, bool, error) {
	if !b.canRecover {
		return infra.Op{}, false, nil
	}
	b.recoveries++
	b.services[name].Status = infra.StatusCreating
	return infra.Op{ID: "retained-component", Kind: "scale", ServiceID: b.services[name].ID}, true, nil
}

func TestComponentRuntimeFailuresAreBoundedAcrossReconstruction(t *testing.T) {
	for _, action := range []string{"scale", "resize"} {
		for _, recovery := range []string{"unplaceable", "retained"} {
			t.Run(action+"/"+recovery, func(t *testing.T) {
				p, rc, base := setupProvider(t)
				convergeProvider(t, p, rc)
				s, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				m := s.Members["rest"]
				desired := rc.Cluster.Spec.Components.Extra["rest"]
				if action == "scale" {
					desired.Replicas++
					base.services[m.Name].MinContainers = desired.Replicas
					base.services[m.Name].MaxContainers = desired.Replicas
				} else {
					desired.Resources.MinRAM++
					desired.Resources.MaxRAM++
					base.services[m.Name].Resources = desired.Resources
				}
				rc.Cluster.Spec.Components.Extra["rest"] = desired
				base.services[m.Name].Status = infra.StatusFailed
				s.Pending = &componentPending{Op: infra.Op{ID: "failed-runtime", ServiceID: m.ServiceID}, Component: "rest", Action: action, Replicas: desired.Replicas, Resources: desired.Resources, Target: desired}
				if err := saveState(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				backend := &failedComponentRuntimeBackend{testBackend: base, target: m.Name, canRecover: recovery == "retained"}
				rc.Backend = backend
				if _, err := p.syncComponents(t.Context(), rc); err == nil {
					t.Fatal("terminal runtime receipt was not recorded")
				}
				p = New(p.options)
				if recovery == "retained" {
					for range 2 {
						if _, err := p.syncComponents(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
					s, err = loadState(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					m = s.Members["rest"]
					if backend.recoveries != 1 || backend.scales != 0 || backend.resizes != 0 || m.RuntimeFailure != nil ||
						m.Replicas != desired.Replicas || !reflect.DeepEqual(m.Resources, desired.Resources) {
						t.Fatalf("retention did not recover the exact target: member=%+v recoveries=%d scales=%d resizes=%d", m, backend.recoveries, backend.scales, backend.resizes)
					}
					return
				}
				for range 4 {
					if _, err := p.syncComponents(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "failed-runtime") {
						t.Fatalf("failed runtime became an endless successful wait: %v", err)
					}
				}
				if backend.scales != 0 || backend.resizes != 0 || backend.recoveries != 0 {
					t.Fatal("unchanged failed target was automatically resubmitted")
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, condition := range status.Conditions {
					found = found || condition.Status == "False" && condition.Reason == "ComponentRuntimeFailed"
				}
				if !found {
					t.Fatalf("terminal runtime failure was hidden in status: %+v", status.Conditions)
				}
				if action == "scale" {
					desired.Replicas++
				} else {
					desired.Resources.MinRAM++
					desired.Resources.MaxRAM++
				}
				rc.Cluster.Spec.Components.Extra["rest"] = desired
				for range 10 {
					if _, err := p.syncComponents(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if backend.scales+backend.resizes > 0 {
						break
					}
				}
				if backend.scales+backend.resizes != 1 {
					t.Fatal("genuine corrected target did not receive one new attempt")
				}
				for range 4 {
					if _, err := p.syncComponents(t.Context(), rc); err == nil {
						t.Fatal("corrected terminal target lost its failure")
					}
				}
				if backend.scales+backend.resizes != 1 {
					t.Fatal("corrected target's failure started another resubmission loop")
				}
			})
		}
	}
}

func TestComponentScaleReductionSupersedesPendingCount(t *testing.T) {
	p, rc, base := setupProvider(t)
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members["rest"]
	desired := rc.Cluster.Spec.Components.Extra["rest"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["rest"] = desired
	s.Pending = &componentPending{Op: infra.Op{ID: "pending-component-scale", Kind: "scale"}, Component: "rest", Action: "scale", Replicas: 3}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	backend := &failedComponentRuntimeBackend{testBackend: base, target: m.Name}
	rc.Backend = backend
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if backend.pendingPolls != 0 || backend.scales != 1 || s.Pending == nil || s.Pending.Replicas != 2 || s.Pending.Op.ID != "component-scale" {
		t.Fatalf("obsolete pending count blocked reduction: pending=%+v oldPolls=%d scales=%d", s.Pending, backend.pendingPolls, backend.scales)
	}
}

func TestFailedComponentScaleAppliesResourceCorrectionBeforeRetryingCount(t *testing.T) {
	p, rc, base := setupProvider(t)
	desired := rc.Cluster.Spec.Components.Extra["rest"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["rest"] = desired
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members["rest"]
	desired.Replicas = 3
	rc.Cluster.Spec.Components.Extra["rest"] = desired
	base.services[m.Name].MinContainers, base.services[m.Name].MaxContainers = 3, 3
	base.services[m.Name].Status = infra.StatusFailed
	s.Pending = &componentPending{Op: infra.Op{ID: "failed-runtime", ServiceID: m.ServiceID}, Component: "rest", Action: "scale", Replicas: 3, Target: desired}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	backend := &failedComponentRuntimeBackend{testBackend: base, target: m.Name}
	rc.Backend = backend
	if _, err := p.syncComponents(t.Context(), rc); err == nil {
		t.Fatal("failed scale was not recorded")
	}
	desired.Resources.MinRAM /= 2
	desired.Resources.MaxRAM /= 2
	rc.Cluster.Spec.Components.Extra["rest"] = desired
	p = New(p.options)
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if backend.scales != 0 || backend.resizes != 1 || s.Pending == nil || s.Pending.Action != "resize" ||
		!reflect.DeepEqual(s.Pending.Resources, desired.Resources) || !reflect.DeepEqual(s.Pending.Target, desired) ||
		s.Members["rest"].RuntimeFailure == nil {
		t.Fatalf("resource correction was lost behind count planning: scales=%d resizes=%d pending=%+v", backend.scales, backend.resizes, s.Pending)
	}
	for range 4 {
		if _, err := p.syncComponents(t.Context(), rc); err == nil {
			t.Fatal("failed corrective resize lost its terminal evidence")
		}
	}
	if backend.scales != 0 || backend.resizes != 1 {
		t.Fatalf("corrective resize failure was resubmitted: scales=%d resizes=%d", backend.scales, backend.resizes)
	}
}
