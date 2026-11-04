// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func closeFleetWindow(rc *engine.Context) {
	// Keep freshly issued leaves valid: main also heals not-yet-valid
	// certificates, so rewinding the clock would admit emergency recovery.
	next := now(rc).UTC().Add(24 * time.Hour)
	rc.Clock = &retryStepClock{at: time.Date(next.Year(), next.Month(), next.Day(), 12, 0, 0, 0, time.UTC)}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
}

func TestRollingOperationMaintenanceBoundary(t *testing.T) {
	for _, agents := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "agents"}[agents], func(t *testing.T) {
			p, rc, f, _ := setup(t)
			p.ApplyDefaults(&rc.Cluster.Spec)
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			s, _ := load(rc.Cluster)
			s.UID = "window"
			plan(rc.Cluster, s)
			for _, m := range s.Members {
				m.Stage = "active"
				m.Version = rc.Cluster.Spec.Version
				m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
				m.Disk = component(rc.Cluster.Spec, m).Resources
			}
			seedFormedServices(t, p, rc, f, s)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}
			restarts := 0
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					restarts++
				}
				ready := true
				_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: target.TargetBuild}, MongoReady: &ready, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}}})
			})
			closeFleetWindow(rc)
			clock := rc.Clock.(*retryStepClock)
			clock.at = time.Date(2026, 10, 21, 1, 30, 0, 0, time.UTC)
			kind := domain.OperationRestart
			cp := operationState{}
			if agents {
				kind = domain.OperationUpgradeAgents
				cp.AgentTarget = &target
			}
			op := &domain.Operation{ID: "window", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: kind, Phase: domain.PhaseRunning}
			rc.Operation = op
			prepared := []string{}
			prepare := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (bool, error) {
				prepared = append(prepared, m.Name)
				return false, nil
			}
			run := func() engine.OpResult {
				t.Helper()
				r, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, op.ID, prepare, healthyTestReplication)
				if err != nil {
					t.Fatal(err)
				}
				cp = operationState{}
				if err := decodeJSON(r.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				return r
			}
			first := run()
			if cp.Waiting == "" || len(prepared) != 1 {
				t.Fatalf("first member not disrupted: %+v %+v", first, cp)
			}
			firstMember := cp.Waiting
			clock.at = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
			for range 20 {
				if run().Step == "member-healthy" {
					break
				}
			}
			if !slices.Contains(cp.Done, firstMember) {
				t.Fatalf("closed window stranded recovering member: %+v", cp)
			}
			beforeDeploy, beforeRestart := len(f.deployments), restarts
			blocked := run()
			if blocked.Step != "waiting-maintenance-window" || blocked.Done || blocked.RequeueAfter <= 0 || blocked.RequeueAfter > 5*time.Minute || len(prepared) != 1 || len(f.deployments) != beforeDeploy || restarts != beforeRestart {
				t.Fatalf("new member crossed closed window: %+v prepared=%v", blocked, prepared)
			}
			// Resume from the persisted operation, including its explicit override.
			op.Checkpoint = blocked.Checkpoint
			op.IgnoreMaintenanceWindow = true
			st := rc.Store.(*store.Store)
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			var err error
			rc.Operation, err = st.GetOperation(t.Context(), op.ID)
			if err != nil {
				t.Fatal(err)
			}
			cp = operationState{}
			if err := decodeJSON(rc.Operation.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			resumed := run()
			if cp.Waiting == "" || cp.Waiting == firstMember || len(prepared) != 2 || resumed.Step == "waiting-maintenance-window" {
				t.Fatalf("persisted override did not resume next member: %+v %+v", resumed, cp)
			}
		})
	}
}

func TestRollingOperationStepDownRecoveryOutsideWindow(t *testing.T) {
	p, rc, _, _ := setup(t)
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	rc.Operation = &domain.Operation{ID: "stepdown", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		ready := true
		_ = json.NewEncoder(w).Encode(agentapi.Status{MongoReady: &ready, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}}})
	})
	closeFleetWindow(rc)
	rc.Clock.(*retryStepClock).at = time.Date(2026, 10, 21, 1, 30, 0, 0, time.UTC)
	cp := operationState{}
	calls := []string{}
	prepare := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (bool, error) {
		calls = append(calls, m.Name)
		return len(calls) == 1, nil
	}
	first, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "stepdown", prepare, healthyTestReplication)
	if err != nil || first.Step != "primary-stepped-down" {
		t.Fatal(first, err)
	}
	cp = operationState{}
	if err := decodeJSON(first.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Disrupting == "" || cp.Waiting != "" {
		t.Fatalf("step-down boundary not durable: %+v", cp)
	}
	disruptedMember := cp.Disrupting
	closeFleetWindow(rc)
	// Change replica preference after step-down: the checkpoint must still pin
	// the already disrupted member rather than select another secondary.
	replication := func(context.Context, *engine.Context, *internalState, *member) (mongoadmin.Status, error) {
		return mongoadmin.Status{}, nil
	}
	resumed, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "stepdown", prepare, replication)
	if err != nil || resumed.Step != "member-restarted" || len(calls) != 2 || calls[0] != calls[1] {
		t.Fatalf("step-down recovery blocked or switched member: %+v %v %v", resumed, calls, err)
	}
	cp = operationState{}
	if err := decodeJSON(resumed.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Waiting != disruptedMember || cp.Disrupting != disruptedMember {
		t.Fatalf("restart lost pinned recovery receipt: %+v", cp)
	}
	healthy, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "stepdown", prepare, healthyTestReplication)
	if err != nil || healthy.Step != "member-healthy" {
		t.Fatal(healthy, err)
	}
	// Production resumes each receipt into a fresh state, not a JSON merge
	// with the preceding receipt's omitted waiting and disrupting fields.
	cp = operationState{}
	if err := decodeJSON(healthy.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Disrupting != "" || cp.Waiting != "" || !slices.Contains(cp.Done, disruptedMember) {
		t.Fatalf("recovery retained disruption exemption or lost completed member: %+v", cp)
	}
	// Persist the completed recovery and resume as a new reconciliation would.
	// Neither receipt may grant the next member a closed-window exemption.
	rc.Operation.Checkpoint = healthy.Checkpoint
	st := rc.Store.(*store.Store)
	if err := st.EnqueueOperation(t.Context(), rc.Operation); err != nil {
		t.Fatal(err)
	}
	rc.Operation, err = st.GetOperation(t.Context(), rc.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	cp = operationState{}
	if err := decodeJSON(rc.Operation.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Disrupting != "" || cp.Waiting != "" || !slices.Contains(cp.Done, disruptedMember) {
		t.Fatalf("persisted recovery retained disruption exemption or lost completed member: %+v", cp)
	}
	blocked, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "stepdown", prepare, healthyTestReplication)
	if err != nil || blocked.Step != "waiting-maintenance-window" || blocked.Done || blocked.RequeueAfter <= 0 || len(calls) != 2 {
		t.Fatal(blocked, err, calls)
	}
}

func TestAgentUpgradeRollingOrderJournalAndGates(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip-current", true: "include-current"}[include], func(t *testing.T) {
			p, rc, f, _ := setup(t)
			rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
			p.ApplyDefaults(&rc.Cluster.Spec)
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			rc.Cluster.Spec.Components.ConfigServer.Replicas = 3
			s, _ := load(rc.Cluster)
			s.UID = "fleet"
			plan(rc.Cluster, s)
			for _, m := range s.Members {
				m.Stage = "active"
				m.Version = rc.Cluster.Spec.Version
				m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
				m.Disk = component(rc.Cluster.Spec, m).Resources
			}
			seedFormedServices(t, p, rc, f, s)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include}
			observedBuild := target.TargetBuild
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				ready := true
				_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: observedBuild}, MongoReady: &ready, ConfigVersion: 999, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "mongos": {State: "running"}}})
			})
			skip := ordered(s)[1].Name
			rc.Cluster.Status.Members = []domain.MemberStatus{{Name: skip, Agent: domain.AgentStatus{Build: target.TargetBuild, Healthy: true}}}
			cp := operationState{AgentTarget: &target}
			blocked := true
			safetyErr := errors.New("native restart blocked")
			prepare := func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
				if blocked {
					return false, safetyErr
				}
				return false, nil
			}
			replication := healthyTestReplication
			before := len(f.deployments)
			if _, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "fleet", prepare, replication); !errors.Is(err, safetyErr) {
				t.Fatal(err)
			}
			if len(f.deployments) != before {
				t.Fatal("deployed past native gate")
			}
			blocked = false
			st := rc.Store.(*store.Store)
			op := &domain.Operation{ID: "agents", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhaseRunning}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			complete := false
			checkedBuildGate := false
			for range 100 {
				r, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "fleet", prepare, replication)
				if err != nil {
					t.Fatal(err)
				}
				cp = operationState{}
				if err := decodeJSON(r.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				if cp.Waiting != "" && s.Pending == nil && !checkedBuildGate {
					observedBuild = "old"
					wait, err := p.rollingOperationWithGates(t.Context(), rc, s, cp, false, "fleet", prepare, replication)
					if err != nil || wait.Step != "waiting-agent-build" {
						t.Fatal(wait, err)
					}
					observedBuild = target.TargetBuild
					checkedBuildGate = true
				}
				op.Checkpoint = r.Checkpoint
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if err := st.UpdateOperationAndClusterState(t.Context(), op, rc.Cluster); err != nil {
					t.Fatal(err)
				}
				rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				op, err = st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				cp = operationState{}
				if err := decodeJSON(op.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				if r.Done {
					complete = true
					break
				}
			}
			if !complete || !checkedBuildGate {
				t.Fatalf("unfinished %+v", cp)
			}
			want := len(s.Members)
			if !include {
				want--
			}
			if len(f.deployments)-before != want {
				t.Fatalf("deployments=%d want=%d", len(f.deployments)-before, want)
			}
			done := cp.Done
			if !include {
				done = slices.DeleteFunc(slices.Clone(done), func(n string) bool { return n == skip })
			}
			lastRank := -1
			for _, name := range done {
				m := s.find(name)
				rank := 1
				if m.Role == agentapi.RoleConfigServer {
					rank = 0
				}
				if m.Role == agentapi.RoleRouter {
					rank = 2
				}
				if rank < lastRank {
					t.Fatalf("wrong role order %v", done)
				}
				lastRank = rank
				if m.Index == 0 && m.Role != agentapi.RoleRouter {
					for _, peer := range s.setMembers(m.Set) {
						if peer.Name != m.Name && !(peer.Name == skip && !include) && slices.Index(done, peer.Name) > slices.Index(done, m.Name) {
							t.Fatalf("primary before secondary: %v", done)
						}
					}
				}
			}
			for _, d := range f.deployments[before:] {
				if d.Runtime.Volume == "" && !strings.Contains(strings.Join(d.Runtime.Prepare, " "), rc.Cluster.Spec.Version) {
					t.Fatal("lost runtime identity")
				}
			}
			if !slices.Contains(p.Describe().Operations, domain.OperationUpgradeAgents) {
				t.Fatal("not advertised")
			}
		})
	}
}

func TestAgentUpgradeTargetFenceFreezeAndCancellation(t *testing.T) {
	p, rc, f, _ := setup(t)
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: "old"})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "re-run") {
		t.Fatal(err)
	}
	if f.mutations != 0 {
		t.Fatal("disrupted mismatched build")
	}
	payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
	op.Payload = payload
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || r.Step != "agent-target-frozen" {
		t.Fatal(r, err)
	}
	op.Checkpoint = r.Checkpoint
	if err := p.CanCancelOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	r, err = p.CancelOperation(t.Context(), rc, op)
	if err != nil || !r.Done {
		t.Fatal(r, err)
	}
	op.Checkpoint, _ = json.Marshal(operationState{Waiting: "demom0"})
	if err := p.CanCancelOperation(t.Context(), op); err == nil {
		t.Fatal("cancelled active member")
	}
}

func TestFleetWindowsBlockNewDisruptions(t *testing.T) {
	for _, kind := range []string{"upgrade", "resources", "config", "scale-in", "scale-out"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, agents, _ := renewalCluster(t, 3)
			closeFleetWindow(rc)
			switch kind {
			case "upgrade":
				rc.Cluster.Spec.Version = "8.0.29-13"
			case "resources":
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			case "config":
				rc.Cluster.Spec.EngineConfig = "operationProfiling:\n  slowOpThresholdMs: 200\n"
			case "scale-in":
				rc.Cluster.Spec.Components.Engine.Replicas = 1
			case "scale-out":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			}
			calls := 0
			prepare := func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
				calls++
				return false, nil
			}
			r, err := p.sync(t.Context(), rc, prepare, healthyTestReplication)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "scale-out" {
				s, _ := load(rc.Cluster)
				if len(s.Members) != 5 {
					t.Fatal("scale-out blocked")
				}
				return
			}
			for range 3 {
				r, err = p.sync(t.Context(), rc, prepare, healthyTestReplication)
				if err != nil {
					t.Fatal(err)
				}
			}
			if calls != 0 || r.RequeueAfter == 0 {
				t.Fatalf("native calls=%d result=%+v", calls, r)
			}
			for _, a := range agents {
				if a.reloads.Load() != 0 {
					t.Fatal("reloaded outside window")
				}
			}
		})
	}
}

func TestFleetLeafRenewalWindow(t *testing.T) {
	for _, days := range []int{20, 7, 1} {
		t.Run(time.Duration(days).String(), func(t *testing.T) {
			p, rc, agents, ca := renewalCluster(t, 1)
			s, _ := load(rc.Cluster)
			m := s.Members[0]
			crt, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name}, Validity: time.Duration(days) * 24 * time.Hour, Server: true, Client: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, "tls/members/"+m.Name+".pem", append(crt, key...)); err != nil {
				t.Fatal(err)
			}
			rc.Clock = &retryStepClock{at: time.Now().UTC()}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			pending, err := hasPendingCertificate(t.Context(), rc, m)
			if err != nil {
				t.Fatal(err)
			}
			if pending != (days <= 7) {
				t.Fatalf("days=%d pending=%v", days, pending)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			prepare := func(context.Context, *engine.Context, *internalState, *member) (bool, error) { return false, nil }
			for range 3 {
				if _, err := p.sync(t.Context(), rc, prepare, healthyTestReplication); err != nil {
					t.Fatal(err)
				}
			}
			if (agents[m.Name].reloads.Load() > 0) != (days <= 7) {
				t.Fatalf("days=%d database reloads=%d", days, agents[m.Name].reloads.Load())
			}
		})
	}
}

// Keep catch-up bounded by the existing native ten-second lag rule.
func TestAgentUpgradeCatchupGate(t *testing.T) {
	at := time.Now()
	rs := mongoadmin.Status{Members: []mongoadmin.Member{{Name: "primary:27017", State: "PRIMARY", Health: 1, OptimeDate: at}, {Name: "secondary:27017", State: "SECONDARY", Health: 1, OptimeDate: at.Add(-11 * time.Second)}}}
	if caught(rs, "secondary") {
		t.Fatal("accepted lag over ten seconds")
	}
}

func TestFleetWindowFinishesReloadAndAllowsHealing(t *testing.T) {
	for _, healing := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-flight", true: "healing"}[healing], func(t *testing.T) {
			p, rc, agents, _ := renewalCluster(t, 3)
			s, _ := load(rc.Cluster)
			m := s.Members[0]
			if healing {
				agents[m.Name].running.Store(false)
			} else {
				m.Stage = "reload-publish"
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			closeFleetWindow(rc)
			prepare := func(context.Context, *engine.Context, *internalState, *member) (bool, error) { return false, nil }
			for range 6 {
				if _, err := p.sync(t.Context(), rc, prepare, healthyTestReplication); err != nil {
					t.Fatal(err)
				}
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if agents[m.Name].reloads.Load() > 0 {
					agents[m.Name].running.Store(true)
					agents[m.Name].version.Store(current.find(m.Name).ConfigVersion)
					if current.find(m.Name).Stage == "active" {
						return
					}
				}
			}
			t.Fatal("maintenance window interrupted recovery")
		})
	}
}
