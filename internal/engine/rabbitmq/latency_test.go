// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestResumeDeploysOverlapAndSurviveReload(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Suspended = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = state(t, rc)
	for _, m := range s.Members {
		m.Stage = "imported"
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.done = false
	before := len(f.deployments)
	for range 6 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s = state(t, rc)
	if len(f.deployments)-before != 2 {
		t.Fatalf("resume submitted %d deployments while first receipt is pending, want cap 2", len(f.deployments)-before)
	}
	if s.find("demom2").ResumeDeploy == nil || s.find("demom1").ResumeDeploy == nil || s.find("demom0").ResumeDeploy != nil {
		t.Fatal("resume did not persist last-stopped-first receipts within cap")
	}
	if s.Pending != nil {
		t.Fatal("resume receipt occupied serial rolling checkpoint")
	}
	// Completion opens a slot even if the broker cannot yet form a Khepri majority.
	f.states = map[string]infra.OpState{s.find("demom2").ResumeDeploy.Op.ID: {Done: true}}
	for range 4 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.deployments)-before != 3 {
		t.Fatal("completed receipt did not admit final retained broker")
	}
}

func TestResumeSubmitsFreedSlotBeforeStartingCompletedBroker(t *testing.T) {
	for _, completed := range []string{"demom2", "demom1"} {
		t.Run(completed, func(t *testing.T) {
			p, rc, f, agents := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			s.Bootstrapped = false
			s.Phase = "resume"
			for _, m := range s.Members {
				m.Resume = true
				m.Stage = "imported"
				if m.Name != "demom0" {
					m.ResumeDeploy = &pending{Op: infra.Op{ID: "resume-" + m.Name}, Action: "deploy", Member: m.Name, Version: m.Version}
				}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			f.done = false
			f.states = map[string]infra.OpState{"resume-" + completed: {Done: true}}
			clock := rc.Clock.(*testClock)
			startedAt := clock.Now()
			agents.requestHook = func(action string) {
				if action == "start" {
					clock.at = clock.at.Add(30 * time.Second)
				}
			}
			before := len(f.deployments)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(f.deployments) != before+1 || state(t, rc).find("demom0").ResumeDeploy == nil {
				t.Fatal("freed deployment slot waited for a completed broker start")
			}
			if !clock.Now().Equal(startedAt) {
				t.Fatal("broker startup consumed time before the queued deployment")
			}
			if resumeDeployments(state(t, rc)) != resumeDeploymentLimit {
				t.Fatal("slot refill did not preserve the two-receipt cap")
			}
		})
	}
}

type resumeAdmissionBackend struct {
	*fakeInfra
	retryAt time.Time
}

func (b *resumeAdmissionBackend) Deploy(context.Context, string, infra.Deployment) (infra.Op, error) {
	return infra.Op{}, &infra.DeploymentWaitError{Reason: "DeploymentSlot", RetryAt: b.retryAt, Detail: "another cluster holds the project slot"}
}

func TestResumeStartsCompletedBrokerWhenRefillAdmissionWaits(t *testing.T) {
	for _, startFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "start-error"}[startFails], func(t *testing.T) {
			p, rc, f, agents := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			s.Bootstrapped = false
			s.Phase = "resume"
			for _, m := range s.Members {
				m.Resume = true
				m.Stage = "imported"
				if m.Name != "demom0" {
					m.ResumeDeploy = &pending{Op: infra.Op{ID: "resume-" + m.Name}, Action: "deploy", Member: m.Name, Version: m.Version}
				}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			f.done = false
			f.states = map[string]infra.OpState{"resume-demom2": {Done: true}}
			retryAt := rc.Clock.Now().Add(time.Minute)
			rc.Backend = &resumeAdmissionBackend{fakeInfra: f, retryAt: retryAt}
			starts := 0
			agents.requestHook = func(action string) {
				if action == "start" {
					starts++
				}
			}
			if startFails {
				agents.failures["demom2:start"] = 500
			}
			before := len(f.deployments)
			_, err := p.Sync(t.Context(), rc)
			var wait *engine.PlatformRetryWait
			if !errors.As(err, &wait) || !wait.RetryAt.Equal(retryAt) {
				t.Fatalf("admission deadline lost: %v", err)
			}
			if starts != 1 {
				t.Fatal("completed broker start was starved by project admission")
			}
			current := state(t, rc)
			if len(f.deployments) != before || current.find("demom0").ResumeDeploy != nil || current.Pending != nil || current.Retry != nil {
				t.Fatal("admission refusal bypassed or became a deployment failure")
			}
			if startFails {
				if current.find("demom2").Stage != "resume-deployed" || err.Error() == wait.Error() {
					t.Fatalf("start failure lost while retaining admission wait: %v", err)
				}
			} else if current.find("demom2").Stage != "resume-revive" {
				t.Fatal("completed broker start was not checkpointed")
			}
		})
	}
}

func TestMutationProgressPacing(t *testing.T) {
	p, rc, _, _ := setup(t)
	result, err := p.Sync(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter > time.Second {
		t.Fatalf("durable mutation checkpoint sleeps %s", result.RequeueAfter)
	}
}

func TestCleanupDrainsEveryResumeReceipt(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Members[0].ResumeDeploy = &pending{Op: infra.Op{ID: "retained-deploy"}, Action: "deploy", Member: "demom0", Version: "4.3.6"}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.done = false
	before := f.mutations
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if err != nil || done || f.mutations != before {
		t.Fatalf("cleanup crossed pending retained deployment: done=%v err=%v mutations=%d", done, err, f.mutations-before)
	}
	if state(t, rc).Members[0].ResumeDeploy == nil {
		t.Fatal("cleanup discarded live platform receipt")
	}
}

func TestResumeReceiptFailureUsesDurableRetry(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.Members[0]
	m.Resume = true
	m.Stage = "imported"
	m.ResumeDeploy = &pending{Op: infra.Op{ID: "retained-deploy"}, Action: "deploy", Member: m.Name, Version: "4.3.6"}
	f.states = map[string]infra.OpState{"retained-deploy": {Failed: true, Message: "platform failed"}}
	if _, err := p.observeResumeDeployments(t.Context(), rc, s); err == nil {
		t.Fatal("failed retained receipt lost its error")
	}
	if s.Pending != nil || m.ResumeDeploy != nil || s.Retry == nil || s.Retry.Member != m.Name {
		t.Fatal("failed retained receipt did not transfer to durable retry")
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if state(t, rc).Retry == nil {
		t.Fatal("retained deployment retry did not survive reload")
	}
}

type retainedRecoveryBackend struct{ *fakeInfra }

func (b *retainedRecoveryBackend) Start(ctx context.Context, name string) (infra.Op, error) {
	op, err := b.fakeInfra.Start(ctx, name)
	if err == nil {
		b.services[name].Status = infra.StatusActive
	}
	return op, err
}

func TestCompletedResumeDeploymentRecoversRuntime(t *testing.T) {
	for _, stage := range []string{"resume-deployed", "resume-revive"} {
		for _, fault := range []string{"deleted", "stopped"} {
			t.Run(stage+"/"+fault, func(t *testing.T) {
				p, rc, f, agents := setup(t)
				ready(t, p, rc)
				rc.Backend = &retainedRecoveryBackend{f}
				s := state(t, rc)
				m := s.Members[0]
				volumeID := m.VolumeID
				m.Resume = true
				m.Stage = stage
				agents.broker(m.Name).running = false
				if fault == "deleted" {
					delete(f.services, m.Name)
				} else {
					f.services[m.Name].Status = infra.StatusStopped
				}
				busy, err := p.recoverRuntimes(t.Context(), rc, s)
				if err != nil || !busy {
					t.Fatalf("completed resume deployment suppressed runtime repair: busy=%v err=%v", busy, err)
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				ready(t, p, rc)
				current := state(t, rc).find(m.Name)
				if current.Resume || current.VolumeID != volumeID || f.services[m.Volume].ID != volumeID || f.services[m.Name] == nil || f.services[m.Name].Status != infra.StatusActive {
					t.Fatal("retained runtime did not recover on original volume and clear Resume")
				}
			})
		}
	}
}

func TestResumeFailureConvergesWithConcurrentReceipt(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	for i := range 2 {
		m := s.Members[i]
		m.Resume = true
		m.Stage = "imported"
		m.ResumeDeploy = &pending{Op: infra.Op{ID: "retained-" + m.Name}, Action: "deploy", Member: m.Name, Version: m.Version}
	}
	f.states = map[string]infra.OpState{"retained-demom1": {Failed: true, Message: "platform failed"}, "retained-demom0": {}}
	if _, err := p.observeResumeDeployments(t.Context(), rc, s); err == nil {
		t.Fatal("failed receipt lost its failure")
	}
	if s.Retry == nil || s.find("demom0").ResumeDeploy == nil {
		t.Fatal("failed receipt discarded its concurrent peer")
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = cloneCluster(t, rc.Cluster)
	// The failed member retries through its durable serial fence while the
	// other accepted deployment retains its independent receipt.
	rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Hour)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if state(t, rc).find("demom0").ResumeDeploy == nil {
		t.Fatal("retry lost concurrent receipt")
	}
	f.states["retained-demom0"] = infra.OpState{Done: true}
	ready(t, p, rc)
	for _, m := range state(t, rc).Members {
		if m.Resume || m.ResumeDeploy != nil {
			t.Fatal("retained resume did not converge after concurrent failure")
		}
	}
}

func TestScaleDownDuringResumeDrainsRetiringDeployment(t *testing.T) {
	for _, stage := range []string{"resume-revive", "imported"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			s.Phase = "resume"
			s.Bootstrapped = false
			volumes := map[string]string{}
			for _, m := range s.Members {
				m.Resume = true
				m.Stage = "resume-revive"
				volumes[m.Name] = m.VolumeID
			}
			retiring := s.find("demom2")
			retiring.Stage = stage
			if stage == "imported" {
				retiring.ResumeDeploy = &pending{Action: "deploy", Member: retiring.Name, Version: retiring.Version, Op: infra.Op{ID: "retiring-deploy"}}
				f.states["retiring-deploy"] = infra.OpState{}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Components.Engine.Replicas = 2
			if stage == "imported" {
				for range 12 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				current := state(t, rc).find(retiring.Name)
				if !current.Removing || current.ResumeDeploy == nil || current.RemoveStep != "" || f.services[retiring.Name] == nil {
					t.Fatal("retirement crossed an accepted deployment fence")
				}
				f.states["retiring-deploy"] = infra.OpState{Done: true}
			}
			ready(t, p, rc)
			current := state(t, rc)
			if current.find(retiring.Name) != nil || f.services[retiring.Name] != nil || f.services[retiring.Volume] != nil {
				t.Fatal("retiring retained member did not finish safe removal")
			}
			for _, m := range current.Members {
				if m.Resume || m.VolumeID != volumes[m.Name] || f.services[m.Volume].ID != volumes[m.Name] {
					t.Fatal("survivor resume changed its retained volume or did not complete")
				}
			}
		})
	}
}
