// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type failingComponentBudgetBackend struct {
	*testBackend
	target   string
	mode     string
	attempts int
}

func (b *failingComponentBudgetBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	if name != b.target {
		return b.testBackend.Deploy(ctx, name, deployment)
	}
	b.attempts++
	if b.mode == "rejected" {
		return infra.Op{}, errors.New("preparation submission rejected")
	}
	op, err := b.testBackend.Deploy(ctx, name, deployment)
	if err == nil && b.mode == "failed" {
		op = infra.Op{ID: fmt.Sprintf("version:bounded-failure-%d", b.attempts), Kind: "deploy", ServiceID: name + "-id"}
	}
	return op, err
}

func (b *failingComponentBudgetBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if strings.HasPrefix(op.ID, "version:bounded-failure-") {
		return infra.OpState{Done: true, Failed: true, Status: "PREPARING_RUNTIME_FAILED", Message: "component executable could not be prepared"}, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestComponentDeploymentRetriesBoundedAcrossControlPlaneRestarts(t *testing.T) {
	for _, mode := range []string{"rejected", "failed"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["gateway"]
			m.Stage = engine.ComponentImported // An admitted component deployment.
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			failing := &failingComponentBudgetBackend{testBackend: backend, target: m.Name, mode: mode}
			rc.Backend = failing
			reconcile := func(count int) {
				t.Helper()
				for i := range count {
					p = New(p.options)
					s, err := loadState(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					// Probe selection can change during recovery. A backend runtime
					// fingerprint change must not refresh this software's CP budget.
					native := i%2 == 0
					s.Members["gateway"].NativeReadiness = &native
					if err := saveState(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					_, _ = p.syncComponents(t.Context(), rc)
				}
			}
			reconcile(40)
			if failing.attempts != componentDeploymentAttempts {
				t.Fatalf("CP restarted %s deployment %d times, want bounded %d", mode, failing.attempts, componentDeploymentAttempts)
			}
			s, err = loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			failure := s.Members["gateway"].DeploymentFailure
			if failure == nil || failure.Status != "DeploymentRetriesExhausted" || failure.Attempts != componentDeploymentAttempts || failure.Intent == "" {
				t.Fatalf("missing durable CP retry fence: %+v", failure)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			condition := false
			for _, c := range status.Conditions {
				condition = condition || c.Reason == "DeploymentRetriesExhausted"
			}
			if !condition || !strings.Contains(status.Message, "retry-deployments") {
				t.Fatalf("exhaustion has no actionable public condition: %+v", status)
			}
			// Reconciliation alone cannot grant more attempts. An explicit retry
			// epoch grants exactly one fresh bounded budget, not an unlimited loop.
			rc.Cluster.Spec.RetryEpoch++
			reconcile(40)
			if failing.attempts != 2*componentDeploymentAttempts {
				t.Fatalf("operator re-arm granted %d total attempts", failing.attempts)
			}
			// A later explicitly re-armed successful preparation clears both the
			// component evidence and public guard after normal observation.
			rc.Cluster.Spec.RetryEpoch++
			failing.mode = ""
			reconcile(12)
			s, err = loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if failing.attempts != 2*componentDeploymentAttempts+1 || s.Members["gateway"].DeploymentFailure != nil || s.Members["gateway"].Stage != engine.ComponentJoined {
				t.Fatalf("successful re-arm did not recover: attempts=%d member=%+v", failing.attempts, s.Members["gateway"])
			}
		})
	}
}

func TestAgentRolloutPreparationFailuresHaveBoundedBudget(t *testing.T) {
	for _, mode := range []string{"rejected", "failed"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["gateway"]
			failing := &failingComponentBudgetBackend{testBackend: backend, target: m.Name, mode: mode}
			rc.Backend = failing
			cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, CoreDone: true, Order: []string{"gateway"}}
			raw, _ := json.Marshal(cp)
			op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
			for range 40 {
				p = New(p.options)
				result, _ := p.RunOperation(t.Context(), rc, op)
				if result.Checkpoint != nil {
					op.Checkpoint = result.Checkpoint
				}
				if result.Done {
					t.Fatal("failed preparation was acknowledged as a successful rollout")
				}
			}
			if failing.attempts != componentDeploymentAttempts {
				t.Fatalf("rollout submitted %d failed preparations, want %d", failing.attempts, componentDeploymentAttempts)
			}
			failure, ok := rc.Cluster.Status.DeploymentFailures[m.Name]
			if !ok || failure.Attempts != componentDeploymentAttempts || !strings.Contains(failure.Message, "retry-deployments") {
				t.Fatalf("rollout exhaustion is not actionable: %+v", failure)
			}
		})
	}
}
