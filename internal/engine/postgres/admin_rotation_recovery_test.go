// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type asynchronousAdminBackend struct {
	*fakeBackend
	agents           *distributedAgents
	target           string
	phase            string
	epochs           []int64
	observationError error
	polls            int
}

func (b *asynchronousAdminBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if _, err := b.fakeBackend.Deploy(ctx, name, d); err != nil {
		return infra.Op{}, err
	}
	b.epochs = append(b.epochs, d.RetryEpoch)
	b.target = name
	return infra.Op{ID: fmt.Sprint(len(b.epochs)), Kind: "admin-recovery", Hostname: name}, nil
}
func (b *asynchronousAdminBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind != "admin-recovery" {
		return b.fakeBackend.OpStatus(ctx, op)
	}
	b.polls++
	if b.observationError != nil {
		return infra.OpState{}, b.observationError
	}
	switch b.phase {
	case "failed", "RETRY_REQUIRED":
		return infra.OpState{Done: true, Failed: true, Status: b.phase}, nil
	case "exhausted":
		return infra.OpState{Status: "DeploymentRetriesExhausted", Failed: true}, nil
	case "exhausted-error":
		return infra.OpState{}, &infra.DeploymentExhaustedError{Service: op.Hostname, Attempts: 3}
	case "complete":
		return infra.OpState{Done: true}, nil
	default:
		return infra.OpState{}, nil
	}
}
func (b *asynchronousAdminBackend) complete() {
	b.agents.mu.Lock()
	delete(b.agents.legacyReceipts, b.target)
	b.agents.mu.Unlock()
	b.phase = "complete"
}

func administratorRecoveryFixture(t *testing.T, authorized bool, replicas int) (*Core, *engine.Context, *asynchronousAdminBackend, *adminRotation) {
	t.Helper()
	p, rc, backend, agents := setupDistributed(t, replicas)
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s := converge(t, p, rc)
	rc.Cluster.Spec.Backup.Storage = ""
	j := &adminRotation{Operation: "retained-admin-owner", Stage: "native", Previous: "old-synthetic", Password: "new-synthetic", Verifier: "stable-synthetic", Groups: []int{0, 1}, Completed: 1}
	if err := saveAdminRotation(t.Context(), rc, j); err != nil {
		t.Fatal(err)
	}
	owner := &domain.Operation{ID: j.Operation, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRotateSystemUser, IgnoreMaintenanceWindow: authorized, Payload: json.RawMessage(`{"user":"postgres"}`)}
	if err := rc.Store.EnqueueOperation(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	agents.legacyReceipts = map[string]bool{s.group(1).data()[0].Name: true}
	wrapped := &asynchronousAdminBackend{fakeBackend: backend, agents: agents}
	rc.Backend = wrapped
	return p, rc, wrapped, j
}
func restartAdministratorCore(p *Core) *Core {
	restarted := New(p.options)
	restarted.citusArtifact = p.citusArtifact
	return restarted
}
func assertAdministratorIntent(t *testing.T, rc *engine.Context, original *adminRotation, completed int) *adminRotation {
	t.Helper()
	j, err := readAdminRotation(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if j.Password != original.Password || j.Verifier != original.Verifier || j.Previous != original.Previous || j.Completed != completed {
		t.Fatal("recovery changed the sealed intent or cursor")
	}
	return j
}

func TestAdministratorRecoveryTerminalDeploymentRetry(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"failed", "RETRY_REQUIRED", "exhausted", "exhausted-error"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			p, rc, backend, original := administratorRecoveryFixture(t, false, 1)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			retained := assertAdministratorIntent(t, rc, original, 1)
			if retained.AgentDeploy == nil || len(backend.epochs) != 1 {
				t.Fatal("missing durable deployment receipt")
			}
			p = restartAdministratorCore(p)
			backend.observationError = errors.New("observation temporarily unavailable")
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("unknown observation was ignored")
			}
			if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy == nil || len(backend.epochs) != 1 {
				t.Fatal("unknown observation resubmitted a deployment")
			}
			backend.observationError = nil
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy == nil || len(backend.epochs) != 1 {
				t.Fatal("in-progress deployment was resubmitted")
			}
			backend.phase = phase
			_, err := p.Sync(t.Context(), rc)
			exhausted := phase == "exhausted" || phase == "exhausted-error"
			if exhausted != errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				t.Fatalf("exhaustion evidence lost: %v", err)
			}
			if !exhausted && err != nil {
				t.Fatal(err)
			}
			if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy != nil {
				t.Fatal("terminal receipt still pins recovery")
			}
			if exhausted {
				if _, ok := rc.Cluster.Status.DeploymentFailures[backend.target]; !ok {
					t.Fatal("exhaustion fence was not recorded")
				}
				p = restartAdministratorCore(p)
				if _, err := p.Sync(t.Context(), rc); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || len(backend.epochs) != 1 {
					t.Fatal("exhaustion retried without explicit rearm", err)
				}
				rc.Cluster.Spec.RetryEpoch++
			}
			backend.phase = "pending"
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(backend.epochs) != 2 || backend.epochs[1] != rc.Cluster.Spec.RetryEpoch {
				t.Fatal("same-owner deployment did not rearm", backend.epochs)
			}
			backend.complete()
			p = restartAdministratorCore(p)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			assertAdministratorIntent(t, rc, original, 2)
		})
	}
}

func TestAdministratorRecoveryCapablePrimaryRetiresFailedDeployment(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			p, rc, backend, original := administratorRecoveryFixture(t, false, 3)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			backend.complete()
			backend.phase = "failed"
			if changed {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				backend.agents.mu.Lock()
				backend.agents.leaders[1] = s.group(1).data()[1].Name
				backend.agents.mu.Unlock()
			}
			p = restartAdministratorCore(p)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if j := assertAdministratorIntent(t, rc, original, 2); j.AgentDeploy != nil || len(backend.epochs) != 1 {
				t.Fatal("already-capable primary was pinned by obsolete terminal receipt")
			}
		})
	}
}

func TestAdministratorRecoveryUsesRetainedMaintenanceAuthorization(t *testing.T) {
	t.Parallel()
	for _, via := range []string{"sync", "foreign"} {
		for _, authorized := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/owner-%t", via, authorized), func(t *testing.T) {
				t.Parallel()
				p, rc, backend, original := administratorRecoveryFixture(t, authorized, 1)
				closeRolloutWindow(rc)
				// Even an unrelated desired-generation override must not authorize recovery.
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
				foreign := &domain.Operation{ID: "foreign-maintenance", Kind: domain.OperationUpgradeAgents, IgnoreMaintenanceWindow: !authorized}
				p = restartAdministratorCore(p)
				if via == "foreign" {
					rc.Operation = foreign
					if _, err := p.RunOperation(t.Context(), rc, foreign); err != nil {
						t.Fatal(err)
					}
					if rc.Operation != foreign {
						t.Fatal("recovery leaked operation context")
					}
				} else {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if rc.Operation != nil {
						t.Fatal("recovery leaked operation context")
					}
				}
				want := 0
				if authorized {
					want = 1
				}
				if len(backend.epochs) != want {
					t.Fatalf("deployment used caller authorization: got %d, want %d", len(backend.epochs), want)
				}
				assertAdministratorIntent(t, rc, original, 1)
			})
		}
	}
}

func TestAdministratorRecoveryMissingOwnerFailsClosed(t *testing.T) {
	p, rc, backend, original := administratorRecoveryFixture(t, true, 1)
	original.Operation = "missing-owner-record"
	if err := saveAdminRotation(t.Context(), rc, original); err != nil {
		t.Fatal(err)
	}
	rc.Operation = &domain.Operation{IgnoreMaintenanceWindow: true}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("missing retained authorization allowed deployment")
	}
	if len(backend.epochs) != 0 {
		t.Fatal("deployment started without retained owner")
	}
	assertAdministratorIntent(t, rc, original, 1)
}

func TestAdministratorRecoveryDeploymentWithoutLivePrimary(t *testing.T) {
	t.Parallel()
	for _, outage := range []string{"unreachable", "starting"} {
		for _, phase := range []string{"failed", "RETRY_REQUIRED", "exhausted", "exhausted-error"} {
			t.Run(outage+"/"+phase, func(t *testing.T) {
				t.Parallel()
				p, rc, backend, original := administratorRecoveryFixture(t, false, 1)
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				target := backend.target
				backend.agents.mu.Lock()
				backend.agents.statusFault = map[string]string{target: outage}
				backend.agents.mu.Unlock()
				p = restartAdministratorCore(p)
				backend.observationError = errors.New("platform observation unavailable")
				if _, err := p.Sync(t.Context(), rc); err == nil || backend.polls != 1 {
					t.Fatal("saved deployment was not observed while its runtime was unavailable", err)
				}
				backend.observationError = nil
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy == nil || j.AgentMember != target || backend.polls != 2 || len(backend.epochs) != 1 {
					t.Fatal("unknown/in-progress outage recovery lost or overlapped the deployment")
				}
				backend.phase = phase
				_, err := p.Sync(t.Context(), rc)
				exhausted := phase == "exhausted" || phase == "exhausted-error"
				if exhausted != errors.Is(err, infra.ErrDeploymentRetriesExhausted) || !exhausted && err != nil {
					t.Fatal("terminal outage recovery did not preserve deployment evidence", err)
				}
				if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy != nil || j.AgentMember != target {
					t.Fatal("terminal retry lost its original group/member intent")
				}
				p = restartAdministratorCore(p)
				if exhausted {
					if _, err := p.Sync(t.Context(), rc); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || len(backend.epochs) != 1 {
						t.Fatal("unavailable runtime bypassed deployment exhaustion", err)
					}
					rc.Cluster.Spec.RetryEpoch++
				}
				backend.phase = "pending"
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy == nil || j.AgentMember != target || backend.target != target || len(backend.epochs) != 2 || backend.epochs[1] != rc.Cluster.Spec.RetryEpoch {
					t.Fatal("retry/rearm could not redeploy the unavailable original runtime")
				}
				if len(backend.agents.adminRotations) != 0 {
					t.Fatal("native SQL was dispatched without a proven writable primary")
				}
				backend.complete()
				backend.agents.mu.Lock()
				delete(backend.agents.statusFault, target)
				backend.agents.mu.Unlock()
				p = restartAdministratorCore(p)
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				assertAdministratorIntent(t, rc, original, 2)
			})
		}
	}
}

func TestAdministratorRecoveryUpgradesPeerReachableUnreadyPrimary(t *testing.T) {
	p, rc, backend, original := administratorRecoveryFixture(t, false, 1)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	target := s.group(1).data()[0].Name
	backend.agents.statusFault = map[string]string{target: "unready"}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if j := assertAdministratorIntent(t, rc, original, 1); j.AgentDeploy == nil || j.AgentMember != target || len(backend.epochs) != 1 {
		t.Fatal("cross-group readiness failure prevented owner-local agent recovery")
	}
	if len(backend.agents.adminRotations) != 0 {
		t.Fatal("unready primary received native SQL before local management recovery")
	}
	backend.complete()
	delete(backend.agents.statusFault, target)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	assertAdministratorIntent(t, rc, original, 2)
}

func TestAdministratorRecoveryReselectsPrimaryAfterMaintenanceWait(t *testing.T) {
	p, rc, backend, original := administratorRecoveryFixture(t, false, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	group := s.group(1).data()
	previous, promoted := group[0].Name, group[1].Name
	backend.agents.mu.Lock()
	for _, m := range group {
		backend.agents.legacyReceipts[m.Name] = true
	}
	backend.agents.mu.Unlock()
	closeRolloutWindow(rc)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if j := assertAdministratorIntent(t, rc, original, 1); j.AgentMember != previous || j.AgentDeploy != nil || len(backend.epochs) != 0 {
		t.Fatal("closed window did not retain the unsubmitted primary selection")
	}
	backend.agents.mu.Lock()
	backend.agents.leaders[1] = promoted
	backend.agents.mu.Unlock()
	p = restartAdministratorCore(p)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if j := assertAdministratorIntent(t, rc, original, 1); j.AgentMember != promoted || j.AgentDeploy == nil || backend.target != promoted || len(backend.epochs) != 1 {
		t.Fatal("first deployment used the former primary selected before the maintenance wait")
	}
	backend.complete()
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	assertAdministratorIntent(t, rc, original, 2)
	if len(backend.epochs) != 1 || !backend.agents.legacyReceipts[previous] {
		t.Fatal("recovery unnecessarily rolled the former primary replica")
	}
}
