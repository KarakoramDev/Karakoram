// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type upgradeBackend struct {
	infra.Backend
	agents  *fakeAgents
	target  string
	deploys []string
}

func (b *upgradeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.Backend.Deploy(ctx, name, d)
	if err == nil {
		b.deploys = append(b.deploys, name)
		b.agents.requestMu.Lock()
		b.agents.member(name).build = b.target
		b.agents.requestMu.Unlock()
	}
	return op, err
}
func upgradeOperation(t *testing.T, include bool) *domain.Operation {
	t.Helper()
	raw, err := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
	if err != nil {
		t.Fatal(err)
	}
	return &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: raw}
}
func upgradeTick(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation, cancel bool) bool {
	t.Helper()
	var r engine.OpResult
	var err error
	if cancel {
		r, err = p.CancelOperation(t.Context(), rc, op)
	} else {
		r, err = p.RunOperation(t.Context(), rc, op)
	}
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = r.Checkpoint
	return r.Done
}
func TestAgentUpgradeOrderReplayAndSingleMember(t *testing.T) {
	for _, replicas := range []int{1, 3, 5} {
		t.Run(string(rune('0'+replicas)), func(t *testing.T) {
			p, rc, f, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			ready(t, p, rc)
			before := state(t, rc)
			f.log.clear()
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			for i := range 150 {
				// Reconstruct the provider on every invocation, including deploy/wait boundaries.
				p = New(p.options)
				a.p = p
				if upgradeTick(t, p, rc, op, false) {
					break
				}
				if i == 149 {
					t.Fatal("rollout did not complete")
				}
			}
			expected := []string{}
			for _, active := range []bool{false, true} {
				for _, m := range ordered(before) {
					if m.Active == active {
						expected = append(expected, m.Name)
					}
				}
			}
			if !slices.Equal(b.deploys, expected) {
				t.Fatalf("deploy order %v, want %v", b.deploys, expected)
			}
			if replicas > 1 {
				calls := f.log.snapshot()
				last := expected[len(expected)-1]
				step := slices.Index(calls, "step-down:"+last)
				deploy := slices.Index(calls, "infra:"+last+":deploy")
				if step < 0 || deploy < step {
					t.Fatalf("step-down must precede leader deploy: %v", calls)
				}
			}
			after := state(t, rc)
			for _, m := range before.Members {
				n := after.find(m.Name)
				if n.Volume != m.Volume || n.DesiredVersion != m.DesiredVersion || n.Resources != m.Resources || n.ProcessGeneration != m.ProcessGeneration || n.ConfigVersion != m.ConfigVersion {
					t.Fatalf("changed applied runtime: %s", m.Name)
				}
			}
		})
	}
}
func TestAgentUpgradeBuildFenceAndUnhealthyStandby(t *testing.T) {
	for _, failure := range []string{"build", "sealed", "unhealthy", "structural", "quorum", "recovery-op", "recovery-plan", "recovery-retry", "volume-lost"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			op := upgradeOperation(t, false)
			switch failure {
			case "build":
				op.Payload = json.RawMessage(`{"targetBuild":"different"}`)
			case "sealed":
				a.member("demom1").sealed = true
			case "unhealthy":
				a.member("demom1").healthy = false
			case "structural":
				s := state(t, rc)
				s.Replacing = true
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
			case "recovery-op", "recovery-plan", "recovery-retry", "volume-lost":
				s := state(t, rc)
				q := &s.Members[0].RuntimeRecovery
				switch failure {
				case "recovery-op":
					q.Op = &infra.Op{ID: "recover"}
				case "recovery-plan":
					q.ImportPlan = &engine.ImportPlan{}
				case "recovery-retry":
					q.Action = "start"
				case "volume-lost":
					q.VolumeLost = true
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
			case "quorum":
				// Two observed voters cannot retain a majority during either redeploy.
				a.member("demom2").voter = false
			}
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			_, err := p.RunOperation(t.Context(), rc, op)
			if err == nil {
				t.Fatal("unsafe rollout admitted")
			}
			if len(b.deploys) != 0 {
				t.Fatalf("unsafe deployment: %v", b.deploys)
			}
		})
	}
}
func TestAgentUpgradeIncludeCurrentAndWindowCancellation(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "include"}[include], func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			for _, m := range a.members {
				m.build = engine.CurrentAgentBuild().Build
			}
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, include)
			for range 100 {
				if upgradeTick(t, p, rc, op, false) {
					break
				}
			}
			if include && len(b.deploys) != 3 || !include && len(b.deploys) != 0 {
				t.Fatalf("deploys=%v", b.deploys)
			}
		})
	}
	t.Run("window-and-cancel-inflight", func(t *testing.T) {
		p, rc, f, a := setup(t)
		ready(t, p, rc)
		b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
		rc.Backend = b
		op := upgradeOperation(t, false)
		upgradeTick(t, p, rc, op, false)
		rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
		rc.Clock = &testClock{at: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
		for range 3 {
			upgradeTick(t, p, rc, op, false)
		}
		if len(b.deploys) != 0 {
			t.Fatal("deployed outside window")
		}
		rc.Cluster.Spec.Maintenance = nil
		for range 10 {
			upgradeTick(t, p, rc, op, false)
			if len(b.deploys) > 0 {
				break
			}
		}
		if len(b.deploys) != 1 {
			t.Fatal("member was not submitted")
		}
		rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
		for i := range 50 {
			if upgradeTick(t, p, rc, op, true) {
				break
			}
			if i == 49 {
				t.Fatal("cancel did not finish recovery")
			}
		}
		if len(b.deploys) != 1 {
			t.Fatalf("cancellation deploys=%v", b.deploys)
		}
		if strings.Contains(string(rc.Cluster.Internal), `"sealed":true`) {
			t.Fatal("cancel stranded member sealed")
		}
	})
}

func TestAgentUpgradePostgresStorageAndExactBuild(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	ca, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	p.options.Bindings = &sealBinding{c: pgbinding.Connection{BindingID: "binding", Host: "postgres.example.test", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	rc.Cluster.Generation++
	ready(t, p, rc)
	for _, m := range a.members {
		m.voter = false
	}
	expected := []string{}
	for _, active := range []bool{false, true} {
		for _, m := range ordered(state(t, rc)) {
			if m.Active == active {
				expected = append(expected, m.Name)
			}
		}
	}
	b := &upgradeBackend{Backend: f, agents: a, target: "old-build"}
	rc.Backend = b
	op := upgradeOperation(t, false)
	for range 25 {
		upgradeTick(t, p, rc, op, false)
	}
	if !slices.Equal(b.deploys, expected[:1]) {
		t.Fatalf("advanced without exact build: %v", b.deploys)
	}
	b.target = engine.CurrentAgentBuild().Build
	a.member(expected[0]).build = b.target
	for i := range 100 {
		if upgradeTick(t, p, rc, op, false) {
			break
		}
		if i == 99 {
			t.Fatal("PostgreSQL-storage rollout did not complete")
		}
	}
	if !slices.Equal(b.deploys, expected) {
		t.Fatalf("PostgreSQL HA order: %v", b.deploys)
	}
}

func TestAgentUpgradeRefusesLossOfVoterMajority(t *testing.T) {
	p, rc, f, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 2
	ready(t, p, rc)
	b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
	rc.Backend = b
	op := upgradeOperation(t, false)
	for range 12 {
		upgradeTick(t, p, rc, op, false)
	}
	if len(b.deploys) != 0 {
		t.Fatalf("lost voter majority: %v", b.deploys)
	}
}

func TestAgentUpgradeWindowClosesAfterJournal(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "cancel"}[cancel], func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			f.log.clear()
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			upgradeTick(t, p, rc, op, false) // frozen order
			upgradeTick(t, p, rc, op, false) // undisrupted member journal
			rc.Clock = &testClock{at: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
			for range 3 {
				p = New(p.options)
				a.p = p
				done := upgradeTick(t, p, rc, op, cancel)
				if cancel && !done {
					t.Fatal("undisrupted cancellation did not finish")
				}
			}
			if len(b.deploys) != 0 {
				t.Fatal("deployment started after window closed")
			}
			for _, call := range f.log.snapshot() {
				if strings.HasPrefix(call, "step-down:") {
					t.Fatal("stepped down after window closed")
				}
			}
			if cancel {
				for _, m := range state(t, rc).Members {
					if m.Change != nil {
						t.Fatal("cancel retained undisrupted journal")
					}
				}
				return
			}
			rc.Cluster.Spec.Maintenance = nil
			for i := range 100 {
				if upgradeTick(t, p, rc, op, false) {
					break
				}
				if i == 99 {
					t.Fatal("resumed rollout did not finish")
				}
			}
			if len(b.deploys) != 3 {
				t.Fatalf("resumed deploys: %v", b.deploys)
			}
		})
	}
}

func TestAgentUpgradeRecoversOtherSealedVoter(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "rolling", true: "cancelling"}[cancel], func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			for range 10 {
				upgradeTick(t, p, rc, op, false)
				if len(b.deploys) > 0 {
					break
				}
			}
			if len(b.deploys) != 1 {
				t.Fatal("member was not submitted")
			}
			// A different established voter restarts sealed while this deployment
			// is recovering. Operation-owned sync must recover both serially.
			a.member("demom2").sealed = true
			a.member("demom2").healthy = false
			for i := range 100 {
				p = New(p.options)
				a.p = p
				if upgradeTick(t, p, rc, op, cancel) {
					break
				}
				if i == 99 {
					t.Fatal("other sealed voter stranded recovery")
				}
			}
			if a.member("demom2").sealed {
				t.Fatal("other voter remained sealed")
			}
			if cancel && len(b.deploys) != 1 {
				t.Fatalf("cancel advanced members: %v", b.deploys)
			}
			if !cancel && len(b.deploys) != 3 {
				t.Fatalf("rollout incomplete: %v", b.deploys)
			}
		})
	}
}

func TestAgentUpgradeMissingReceiptSettlesBeforeCancellation(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-build", true: "target-build"}[accepted], func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			upgradeTick(t, p, rc, op, false)
			upgradeTick(t, p, rc, op, false)
			s := state(t, rc)
			m := s.find("demom1")
			m.Change.Step = "deploy"
			m.Change.Started = true // persisted submission intent, receipt was lost
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if accepted {
				a.redeploy(m.Name)
				a.member(m.Name).build = b.target
			}
			rc.Clock = &testClock{at: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
			for range 8 {
				upgradeTick(t, p, rc, op, false)
			}
			if len(b.deploys) != 0 {
				t.Fatal("replay resubmitted outside window")
			}
			for i := range 50 {
				p = New(p.options)
				a.p = p
				if upgradeTick(t, p, rc, op, true) {
					break
				}
				if i == 49 {
					t.Fatal("uncertain member did not settle")
				}
			}
			if len(b.deploys) != 0 {
				t.Fatal("cancel resubmitted missing receipt")
			}
			m = state(t, rc).find(m.Name)
			if m.Change != nil || !m.Healthy || m.Sealed {
				t.Fatalf("cancel left unsafe member: %+v", m)
			}
		})
	}
}

func TestAgentUpgradeBuildMismatchCannotEscapeThroughSync(t *testing.T) {
	for _, boundary := range []string{"apply", "deploy", "submitted", "retry"} {
		t.Run(boundary, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			for _, m := range a.members {
				m.build = "previous-build"
			}
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			upgradeTick(t, p, rc, op, false)
			upgradeTick(t, p, rc, op, false)
			var cp agentUpgradeCheckpoint
			if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			cp.Target.TargetBuild = "frozen-control-plane-build"
			op.Checkpoint, _ = json.Marshal(cp)
			s := state(t, rc)
			m := s.find(cp.Waiting)
			config := m.ConfigVersion
			generation := m.ProcessGeneration
			appliedOptions := string(m.EngineOptions)
			m.Change.Step = boundary
			if boundary == "submitted" || boundary == "retry" {
				m.Change.Step = "deploy"
				m.Change.Started = true
				a.redeploy(m.Name)
				a.member(m.Name).build = cp.Target.TargetBuild
			}
			if boundary == "retry" {
				s.Retry = &pending{Action: "deploy", Member: m.Name, Version: m.DesiredVersion}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			// Even before mismatch handling runs, ordinary convergence may not own
			// an operation's apply/deploy intent or deferred platform retry.
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(b.deploys) != 0 {
				t.Fatal("Sync submitted an operation-owned deploy")
			}
			if got := state(t, rc).find(m.Name); got.ConfigVersion != config || got.ProcessGeneration != generation || string(got.EngineOptions) != appliedOptions {
				t.Fatal("Sync activated config for an agent journal")
			}
			failed := false
			for range 50 {
				r, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					failed = true
					break
				}
				op.Checkpoint = r.Checkpoint
			}
			if !failed {
				t.Fatal("build mismatch was not reported after recovery")
			}
			s = state(t, rc)
			m = s.find(m.Name)
			if m.Change != nil || s.Retry != nil || m.Sealed {
				t.Fatalf("mismatch retained unsafe work: %+v", m)
			}
			for range 5 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			m = state(t, rc).find(m.Name)
			if len(b.deploys) != 0 || m.ConfigVersion != config || m.ProcessGeneration != generation || string(m.EngineOptions) != appliedOptions {
				t.Fatal("ordinary Sync escaped the build fence")
			}
		})
	}
}

func TestAgentUpgradeAdoptsExactTargetAfterLostReceipt(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "crash", true: "submission-error"}[retry], func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			for _, m := range a.members {
				m.build = "previous-build"
			}
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			upgradeTick(t, p, rc, op, false)
			upgradeTick(t, p, rc, op, false)
			s := state(t, rc)
			m := s.find("demom1")
			m.Change.Step = "deploy"
			m.Change.Started = true
			if retry {
				s.Retry = &pending{Action: "deploy", Member: m.Name, Version: m.DesiredVersion}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			a.redeploy(m.Name)
			a.member(m.Name).build = b.target
			for i := range 40 {
				upgradeTick(t, p, rc, op, false)
				var cp agentUpgradeCheckpoint
				if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				if slices.Contains(cp.Done, m.Name) {
					break
				}
				if i == 39 {
					t.Fatal("target completion was not adopted")
				}
			}
			if len(b.deploys) != 0 {
				t.Fatalf("already-complete target was redeployed: %v", b.deploys)
			}
			s = state(t, rc)
			m = s.find(m.Name)
			if s.Retry != nil || m.Change != nil || !m.Healthy || m.Sealed {
				t.Fatal("adoption retained recovery journal")
			}
		})
	}
}

func TestAgentUpgradeTerminalOwnerRecoversWithoutDeployment(t *testing.T) {
	for _, boundary := range []string{"intent", "wait", "retry"} {
		t.Run(boundary, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			for _, m := range a.members {
				m.build = "previous-build"
			}
			b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
			rc.Backend = b
			op := upgradeOperation(t, false)
			op.Cluster = rc.Cluster.Name
			op.Namespace = rc.Cluster.Ref().Namespace
			st := rc.Store.(*store.Store)
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			upgradeTick(t, p, rc, op, false)
			upgradeTick(t, p, rc, op, false)
			s := state(t, rc)
			m := s.find("demom1")
			config := m.ConfigVersion
			m.Change.Step = "deploy"
			m.Change.Started = true
			if boundary == "wait" {
				m.Change.Step = "wait"
			}
			if boundary == "retry" {
				s.Retry = &pending{Action: "deploy", Member: m.Name, Version: m.DesiredVersion}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			a.redeploy(m.Name)
			if boundary != "intent" {
				a.member(m.Name).build = b.target
			}
			op.Phase = domain.PhaseFailed
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if !p.SupportsFailureCleanup(op) {
				t.Fatal("failure cleanup not supported")
			}
			for i := range 40 {
				p = New(p.options)
				a.p = p
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if state(t, rc).find(m.Name).Change == nil {
					break
				}
				if i == 39 {
					t.Fatal("terminal owner stranded member journal")
				}
			}
			m = state(t, rc).find(m.Name)
			if len(b.deploys) != 0 || m.ConfigVersion != config || !m.Healthy || m.Sealed {
				t.Fatal("terminal cleanup deployed or changed config")
			}
			allowed, err := p.CanClaimOperation(rc.Cluster)
			if err != nil || !allowed {
				t.Fatalf("next operation blocked: allowed=%v err=%v", allowed, err)
			}
		})
	}
}

func TestAgentUpgradeSameBuildIntentRequiresGatedRedeploy(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	for _, m := range a.members {
		m.build = engine.CurrentAgentBuild().Build
	}
	b := &upgradeBackend{Backend: f, agents: a, target: engine.CurrentAgentBuild().Build}
	rc.Backend = b
	op := upgradeOperation(t, true)
	upgradeTick(t, p, rc, op, false)
	upgradeTick(t, p, rc, op, false)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change.Step = "deploy"
	m.Change.Started = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Clock = &testClock{at: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
	for range 8 {
		upgradeTick(t, p, rc, op, false)
	}
	var cp agentUpgradeCheckpoint
	if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if len(b.deploys) != 0 || cp.Waiting != m.Name {
		t.Fatal("same-build intent bypassed window or was adopted")
	}
	rc.Cluster.Spec.Maintenance = nil
	for i := range 30 {
		upgradeTick(t, p, rc, op, false)
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			t.Fatal(err)
		}
		if slices.Contains(cp.Done, m.Name) {
			break
		}
		if i == 29 {
			t.Fatal("same-build redeployment did not complete")
		}
	}
	if !slices.Equal(b.deploys, []string{m.Name}) {
		t.Fatalf("same-build redeploys: %v", b.deploys)
	}
}
