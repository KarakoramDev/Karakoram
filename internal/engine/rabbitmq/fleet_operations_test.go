// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func closedFleetWindow(rc *engine.Context) {
	// Keep freshly issued leaves valid instead of simulating clock rollback
	// (which correctly triggers emergency certificate recovery on main).
	next := rc.Clock.Now().UTC().Add(24 * time.Hour)
	rc.Clock.(*testClock).at = time.Date(next.Year(), next.Month(), next.Day(), 12, 0, 0, 0, time.UTC)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "1h"}}}
}

func TestAgentUpgradeJournalAndNativeSequence(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip-current", true: "include-current"}[include], func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, TargetVersion: engine.CurrentAgentBuild().Version, IncludeCurrent: include}
			payload, _ := json.Marshal(target)
			op := &domain.Operation{ID: "agents", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhaseRunning, Payload: payload}
			st := rc.Store.(*store.Store)
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Status.Members = []domain.MemberStatus{{Name: "demom0", Agent: domain.AgentStatus{Healthy: true, Build: target.TargetBuild}}}
			for _, m := range state(t, rc).Members {
				a.broker(m.Name).agentBuild = target.TargetBuild
			}
			a.log.clear()
			complete := false
			for range 150 {
				r, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = r.Checkpoint
				if err := st.UpdateOperationAndClusterState(t.Context(), op, rc.Cluster); err != nil {
					t.Fatal(err)
				}
				rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				op, err = st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Store: st, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
				a.p = p
				if r.Done {
					complete = true
					break
				}
			}
			if !complete {
				t.Fatalf("unfinished: %s", op.Checkpoint)
			}
			calls := a.log.snapshot()
			for _, name := range []string{"demom0", "demom1", "demom2"} {
				want := 1
				if name == "demom0" && !include {
					want = 0
				}
				if got := countCall(calls, "infra:"+name+":deploy"); got != want {
					t.Fatalf("deploy %s=%d want %d: %v", name, got, want, calls)
				}
				if want == 1 {
					assertBefore(t, calls, "agent:"+name+":drain", "agent:"+name+":stop")
					assertBefore(t, calls, "agent:"+name+":stop", "infra:"+name+":deploy")
					assertBefore(t, calls, "infra:"+name+":deploy", "agent:"+name+":revive")
				}
			}
			if !slices.Contains(p.Describe().Operations, domain.OperationUpgradeAgents) {
				t.Fatal("not advertised")
			}
			if err := p.CanCancelOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			r, err := p.CancelOperation(t.Context(), rc, op)
			if err != nil || !r.Done {
				t.Fatal(r, err)
			}
		})
	}
}

func TestAgentUpgradeBuildFenceAndCancellation(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.log.clear()
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: "previous-control-plane"})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil || !strings.Contains(err.Error(), "re-run") {
		t.Fatal(err)
	}
	if len(a.log.snapshot()) != 0 {
		t.Fatal(a.log.snapshot())
	}
	cp, _ := json.Marshal(operationState{Waiting: "demom0"})
	op.Checkpoint = cp
	if err := p.CanCancelOperation(t.Context(), op); err == nil {
		t.Fatal("cancelled mid-member")
	}
}

func TestFleetWindowsBlockNewChanges(t *testing.T) {
	for _, kind := range []string{"upgrade", "resources", "config", "scale-in", "scale-out"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			closedFleetWindow(rc)
			a.log.clear()
			switch kind {
			case "upgrade":
				rc.Cluster.Spec.Version = "4.3.5"
			case "resources":
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			case "config":
				rc.Cluster.Spec.EngineConfig = "consumer_timeout = 3600000"
			case "scale-in":
				rc.Cluster.Spec.Components.Engine.Replicas = 1
			case "scale-out":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			}
			r, err := p.Sync(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "scale-out" {
				if len(state(t, rc).Members) != 5 {
					t.Fatal("scale-out blocked")
				}
				return
			}
			if r.RequeueAfter == 0 {
				t.Fatal("missing window wait")
			}
			for _, m := range state(t, rc).Members {
				if m.ChangeAction != "" || m.RemoveStep != "" {
					t.Fatalf("started disruption: %+v", m)
				}
			}
			for _, call := range a.log.snapshot() {
				if strings.HasSuffix(call, ":drain") || strings.HasSuffix(call, ":stop") || strings.HasPrefix(call, "infra:") {
					t.Fatalf("native disruption: %v", a.log.snapshot())
				}
			}
		})
	}
}

func TestFleetWindowDoesNotInterruptStoppedBroker(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.Members[0]
	beginChange(rc, s, m, "restart")
	m.ChangeStep = "stop"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	closedFleetWindow(rc)
	for range 30 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).Members[0].ChangeAction == "" {
			return
		}
	}
	t.Fatal("in-flight member did not rejoin")
}

func TestFleetCertificateRenewalWindow(t *testing.T) {
	for _, days := range []int{20, 7, 1} {
		t.Run(time.Duration(days).String(), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ready(t, p, rc)
			crt, err := secret(t.Context(), rc, "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			key, err := secret(t.Context(), rc, "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(crt, key)
			if err != nil {
				t.Fatal(err)
			}
			m := state(t, rc).Members[0]
			leaf, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name}, Validity: time.Duration(days) * 24 * time.Hour, Server: true, Client: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, "tls/members/"+m.Name+".pem", append(leaf, key...)); err != nil {
				t.Fatal(err)
			}
			// Use a fake instant relative to the generated leaf, not wall-clock sleeps.
			rc.Clock.(*testClock).at = time.Now().UTC()
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{TimeZone: "UTC", Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1h"}}}
			s := state(t, rc)
			if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
				t.Fatal(err)
			}
			pending, err := hasPendingCertificate(t.Context(), rc, m)
			if err != nil {
				t.Fatal(err)
			}
			if pending != (days <= 7) {
				t.Fatalf("days=%d pending=%v", days, pending)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if (state(t, rc).find(m.Name).ChangeAction != "") != (days <= 7) {
				t.Fatalf("days=%d database disruption=%q", days, state(t, rc).find(m.Name).ChangeAction)
			}
		})
	}
}

func TestAgentUpgradeQuorumAndBuildAcknowledgement(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build}
	payload, _ := json.Marshal(target)
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	a.broker("demom0").critical = true
	a.failures["demom0:await-quorum"] = 503
	a.queueSource = func() []mgmt.Queue {
		return []mgmt.Queue{{Name: "orders", Vhost: "/", Type: "quorum", Members: []string{"rabbit@demom0"}}}
	}
	a.log.clear()
	before := f.mutations
	for range 4 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
	}
	if f.mutations != before || slices.Contains(a.log.snapshot(), "agent:demom0:drain") {
		t.Fatal("agent rollout bypassed quorum", a.log.snapshot())
	}
	a.broker("demom0").critical = false
	delete(a.failures, "demom0:await-quorum")
	for range 30 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
		if r.Step == "waiting-agent-build" {
			if slices.Contains(a.log.snapshot(), "agent:demom1:drain") {
				t.Fatal("advanced before build acknowledgement")
			}
			a.broker("demom0").agentBuild = target.TargetBuild
			r, err = p.RunOperation(t.Context(), rc, op)
			if err != nil || r.Step != "member-healthy" {
				t.Fatal(r, err)
			}
			return
		}
	}
	t.Fatal("missing target-build gate")
}

func TestAgentChangePreservesAppliedState(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.Members[0]
	version, resources, disk, tls, config, options := m.Version, m.ConfigResources, m.ConfigDisk, m.TLSMode, m.EngineConfig, hash(m.EngineOptions)
	rc.Cluster.Spec.Version = "4.3.5"
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	rc.Cluster.Spec.EngineConfig = "consumer_timeout = 3600000"
	rc.Cluster.Spec.TLS.Mode = "disabled"
	beginChange(rc, s, m, "agent")
	if m.TargetVersion != version || !reflect.DeepEqual(m.TargetResources, resources) || m.TargetTLS != tls || m.TargetConfig != config || hash(m.TargetOptions) != options {
		t.Fatalf("agent change adopted desired state: %+v", m)
	}
	// Execute publication directly after the native drain/stop gates.
	m.ChangeStep = "platform"
	if _, err := p.changeStep(t.Context(), rc, s, m); err != nil {
		t.Fatal(err)
	}
	if m.DesiredVersion != version || !reflect.DeepEqual(m.ConfigDisk, disk) || m.TLSMode != tls || m.EngineConfig != config || hash(m.EngineOptions) != options {
		t.Fatalf("agent deployment changed applied state: %+v", m)
	}
}

func TestRollingOperationWindowBoundaries(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS, domain.OperationUpgradeAgents} {
		for _, boundary := range []string{"between-members", "quorum", "critical", "drain", "stop", "platform", "reload"} {
			t.Run(kind+"/"+boundary, func(t *testing.T) {
				p, rc, _, a := setup(t)
				ready(t, p, rc)
				closedFleetWindow(rc)
				rc.Clock.(*testClock).at = time.Date(2026, 10, 21, 1, 15, 0, 0, time.UTC)
				op := &domain.Operation{ID: "window-roll", Kind: kind}
				if kind == domain.OperationUpgradeAgents {
					op.Payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
					for _, m := range state(t, rc).Members {
						a.broker(m.Name).agentBuild = engine.CurrentAgentBuild().Build
					}
				}
				reached := false
				for range 100 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = result.Checkpoint
					var cp operationState
					if err := decodeJSON(op.Checkpoint, &cp); err != nil {
						t.Fatal(err)
					}
					if boundary == "between-members" {
						reached = len(cp.Done) == 1 && cp.Waiting == ""
					} else if cp.Waiting != "" {
						reached = state(t, rc).find(cp.Waiting).ChangeStep == boundary
					}
					if reached {
						break
					}
				}
				if !reached {
					t.Fatalf("never reached %s: %s", boundary, op.Checkpoint)
				}
				closedFleetWindow(rc)
				a.log.clear()
				recovering := boundary == "stop" || boundary == "platform" || boundary == "reload"
				if recovering {
					rejoined := false
					for range 50 {
						result, err := p.RunOperation(t.Context(), rc, op)
						if err != nil {
							t.Fatal(err)
						}
						op.Checkpoint = result.Checkpoint
						var cp operationState
						if err := decodeJSON(op.Checkpoint, &cp); err != nil {
							t.Fatal(err)
						}
						if len(cp.Done) == 1 && cp.Waiting == "" {
							rejoined = true
							break
						}
					}
					if !rejoined || countCall(a.log.snapshot(), "agent:demom0:revive") != 1 {
						t.Fatalf("drained member stranded: %s calls=%v", op.Checkpoint, a.log.snapshot())
					}
					a.log.clear()
				}
				checkpoint := string(op.Checkpoint)
				for range 2 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil || result.Done || result.Step != "waiting-maintenance-window" || result.RequeueAfter <= 0 {
						t.Fatalf("missing member window gate: %+v %v", result, err)
					}
					if string(result.Checkpoint) != checkpoint {
						t.Fatalf("window wait changed checkpoint: %s -> %s", checkpoint, result.Checkpoint)
					}
				}
				for _, call := range a.log.snapshot() {
					if strings.HasSuffix(call, ":drain") || strings.HasSuffix(call, ":stop") || strings.HasPrefix(call, "infra:") {
						t.Fatalf("new disruption outside window: %v", a.log.snapshot())
					}
				}
			})
		}
	}
}

func TestRollingOperationPersistedWindowOverride(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS, domain.OperationUpgradeAgents} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			closedFleetWindow(rc)
			op := &domain.Operation{ID: "override-roll", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: kind, Phase: domain.PhaseRunning, IgnoreMaintenanceWindow: true}
			if kind == domain.OperationUpgradeAgents {
				op.Payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
				for _, m := range state(t, rc).Members {
					a.broker(m.Name).agentBuild = engine.CurrentAgentBuild().Build
				}
			}
			st := rc.Store.(*store.Store)
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			a.log.clear()
			complete := false
			for range 150 {
				var err error
				op, err = st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				if err := st.UpdateOperationAndClusterState(t.Context(), op, rc.Cluster); err != nil {
					t.Fatal(err)
				}
				if result.Done {
					complete = true
					break
				}
			}
			if !complete {
				t.Fatalf("persisted override did not finish: %s", op.Checkpoint)
			}
			for _, name := range []string{"demom0", "demom1", "demom2"} {
				if countCall(a.log.snapshot(), "agent:"+name+":drain") != 1 || countCall(a.log.snapshot(), "agent:"+name+":revive") != 1 {
					t.Fatalf("override failed member %s: %v", name, a.log.snapshot())
				}
			}
		})
	}
}
