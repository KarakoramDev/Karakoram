// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func legacyUIOperation(t *testing.T) *domain.Operation {
	t.Helper()
	cp := agentUpgradeCheckpoint{operationCheckpoint: operationCheckpoint{Stage: "roll", Order: []string{tspec.UI}}, Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}}
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	return &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
}

func TestLegacyUICancellationBeforePreparedSubmission(t *testing.T) {
	for _, phase := range []string{"intent", "rejected"} {
		t.Run(phase, func(t *testing.T) {
			f := newLegacyUIRolloutFixture(t)
			p, rc := f.provider, f.rc
			backend := &uiSubmissionBackend{testBackend: f.backend, root: f.root, oldBinary: f.binary, uiName: f.name, failure: "rejected"}
			rc.Backend = backend
			op := legacyUIOperation(t)
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = result.Checkpoint
			var cp agentUpgradeCheckpoint
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil || !cp.Waiting || cp.Submitted {
				t.Fatal("fixture did not persist the unsubmitted waiting intent", cp, err)
			}
			wantAttempts := 0
			if phase == "rejected" {
				result, err = p.RunOperation(t.Context(), rc, op)
				if !errors.Is(err, errUISubmission) {
					t.Fatal("fixture did not reject submission", err)
				}
				op.Checkpoint = result.Checkpoint
				wantAttempts = 1
			}
			p = New(p.options)
			result, err = p.CancelOperation(t.Context(), rc, op)
			if err != nil || !result.Done || backend.attempts != wantAttempts {
				t.Fatal("cancellation deployed an untouched legacy UI", result.Done, backend.attempts, err)
			}
			after, err := p.memberConfig(t.Context(), rc, f.name)
			if err != nil || !reflect.DeepEqual(f.before, after) {
				t.Fatal("cancellation changed the incumbent sealed configuration", err)
			}
			s, err := loadState(rc.Cluster)
			if err != nil || s.Members[tspec.UI].Version != "2.56.0" {
				t.Fatal("cancellation changed the installed version", err)
			}
			if _, deployed := f.backend.Deployed(f.name); deployed {
				t.Fatal("cancellation submitted target preparation")
			}
		})
	}
}

func TestLegacyUIPreparationFailuresBoundedAndRearmed(t *testing.T) {
	for _, mode := range []string{"always-rejected", "always-failed-receipt", "backend-exhausted"} {
		for _, resume := range []string{"checkpoint", "new-operation", "cancel"} {
			t.Run(mode+"/"+resume, func(t *testing.T) {
				f := newLegacyUIRolloutFixture(t)
				p, rc := f.provider, f.rc
				f.agents.builds = map[string]string{}
				backend := &uiSubmissionBackend{testBackend: f.backend, root: f.root, oldBinary: f.binary, uiName: f.name, failure: mode, prepared: func(name string) { f.agents.builds[name] = engine.CurrentAgentBuild().Build }}
				rc.Backend = backend
				op := legacyUIOperation(t)
				step := func(cancel bool) (engine.OpResult, error) {
					t.Helper()
					p = New(p.options)
					rc.Clock.(*testClock).now = rc.Clock.Now().Add(time.Minute)
					var result engine.OpResult
					var err error
					if cancel {
						result, err = p.CancelOperation(t.Context(), rc, op)
					} else {
						result, err = p.RunOperation(t.Context(), rc, op)
					}
					if result.Checkpoint != nil {
						op.Checkpoint = result.Checkpoint
					}
					return result, err
				}
				for range 40 {
					result, err := step(false)
					if result.Done {
						t.Fatal("failed preparation was acknowledged as a completed rollout")
					}
					if err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) && !errors.Is(err, errUISubmission) && !strings.Contains(err.Error(), "PREPARING_RUNTIME_FAILED") {
						t.Fatal(err)
					}
				}
				wantAttempts := componentDeploymentAttempts
				if mode == "backend-exhausted" {
					wantAttempts = 1
				}
				if backend.attempts != wantAttempts {
					t.Fatalf("submitted %d attempts, want bounded %d", backend.attempts, wantAttempts)
				}
				var cp agentUpgradeCheckpoint
				if err := json.Unmarshal(op.Checkpoint, &cp); err != nil || cp.Pending != nil || cp.Submitted || !cp.RetryPreparation {
					t.Fatal("failed receipt was not retired into admitted preparation", cp, err)
				}
				if mode != "always-rejected" && backend.failedPolls != wantAttempts {
					t.Fatal("a retired failed receipt was polled repeatedly", backend.failedPolls)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				condition := false
				for _, c := range status.Conditions {
					condition = condition || c.Reason == "DeploymentRetriesExhausted"
				}
				if status.Phase != domain.PhaseDegraded || !condition || !strings.Contains(status.Message, "retry-deployments") {
					t.Fatalf("no actionable terminal deployment condition: %+v", status)
				}

				// The API's explicit retry epoch must repair the unprepared target
				// from either the retained checkpoint or a fresh manual operation.
				rc.Cluster.Spec.RetryEpoch++
				backend.failure = ""
				if resume == "new-operation" {
					payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
					op = &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
				}
				done := false
				for range 20 {
					result, err := step(resume == "cancel")
					if err != nil {
						t.Fatal(err)
					}
					if result.Done {
						done = true
						break
					}
				}
				if !done {
					t.Fatal("explicit re-arm did not recover preparation without manual state repair")
				}
				// Cancelling a still-healthy, rejected incumbent must not install
				// anything. All disrupted cases must finish exactly one repair.
				wantTotal := wantAttempts + 1
				if resume == "cancel" && mode == "always-rejected" {
					wantTotal = wantAttempts
				}
				if backend.attempts != wantTotal {
					t.Fatalf("recovery submitted %d total attempts, want %d", backend.attempts, wantTotal)
				}
				if wantTotal > wantAttempts {
					s, err := loadState(rc.Cluster)
					if err != nil || s.Members[tspec.UI].DeploymentFailure != nil {
						t.Fatal("successful preparation retained its failure budget", err)
					}
					if _, exists := rc.Cluster.Status.DeploymentFailures[f.name]; exists {
						t.Fatal("successful preparation retained the public exhaustion fence")
					}
				}
			})
		}
	}
}

func TestLegacyUIComponentPreparationBudget(t *testing.T) {
	f := newLegacyUIRolloutFixture(t)
	p, rc := f.provider, f.rc
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Members[tspec.UI].Stage = engine.ComponentImported
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	backend := &uiSubmissionBackend{testBackend: f.backend, root: f.root, oldBinary: f.binary, uiName: f.name, failure: "always-failed-receipt"}
	rc.Backend = backend
	for range 40 {
		p = New(p.options)
		_, err := p.Sync(t.Context(), rc)
		if err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) && !strings.Contains(err.Error(), "PREPARING_RUNTIME_FAILED") {
			t.Fatal(err)
		}
	}
	if backend.attempts != componentDeploymentAttempts || backend.failedPolls != componentDeploymentAttempts {
		t.Fatal("ordinary component reconciliation did not bound and retire failed preparations", backend.attempts, backend.failedPolls)
	}
	rc.Cluster.Spec.RetryEpoch++
	backend.failure = ""
	for range 12 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err = loadState(rc.Cluster)
	if err != nil || backend.attempts != componentDeploymentAttempts+1 ||
		s.Members[tspec.UI].DeploymentFailure != nil || s.Members[tspec.UI].Stage != engine.ComponentJoined {
		t.Fatal("ordinary component preparation did not recover after explicit re-arm", backend.attempts, err)
	}
}
