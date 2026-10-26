// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type retirementAgents struct {
	engine.AgentDialer
	unreachable string
}

func (a retirementAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if member == a.unreachable {
		return nil, engine.ErrUnavailable
	}
	return a.AgentDialer.Dial(ctx, ref, member)
}

func TestRetirementResumesAndRechecksLeadership(t *testing.T) {
	for _, mode := range []string{"replacement", "scale-in"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			converge(t, p, rc)
			target := "demop1"
			if mode == "scale-in" {
				rc.Cluster.Spec.Components.Engine.Replicas = 5
				converge(t, p, rc)
				target = "demop4"
				rc.Cluster.Spec.Components.Engine.Replicas = 3
			}
			payload, _ := json.Marshal(map[string]string{"member": target})
			op := &domain.Operation{ID: "retirement", Kind: domain.OperationReplaceMember, Payload: payload}
			seen := map[string]bool{}
			changedLeader, rechecked, finished := false, false, false
			for range 200 {
				p = New(p.options)
				if mode == "replacement" {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = result.Checkpoint
					finished = result.Done
					if p.SyncDuringOperation(op) {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
				} else if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				m := s.find(target)
				if m != nil {
					seen[m.Retirement] = true
					if m.Retirement == "stop" && !changedLeader {
						// Model leadership changing after the earlier switchover
						// observation, before the stop checkpoint is resumed.
						b.leader, changedLeader = target, true
					} else if changedLeader && b.leader != target {
						rechecked = true
					}
				}
				if (mode == "replacement" && finished) || (mode == "scale-in" && len(s.data()) == 3) {
					finished = true
					break
				}
			}
			if !finished || !changedLeader || !rechecked {
				t.Fatal("retirement did not recheck leadership and converge")
			}
			for _, stage := range []string{"switchover", "tags", "untag", "stop", "stopped", "removed-etcd"} {
				if !seen[stage] {
					t.Fatalf("retirement checkpoint %q not resumed", stage)
				}
			}
			fence := slices.Index(b.actions, "retire-fence:"+target)
			stop := slices.Index(b.actions, "retire-stop:"+target)
			remove := slices.Index(b.actions, "remove::"+target+"-etcd")
			deleted := slices.Index(b.actions, "delete:"+target)
			if fence < 0 || stop <= fence || remove <= stop || deleted <= remove {
				t.Fatalf("unsafe retirement ordering: %v", b.actions)
			}
		})
	}
}

func TestUnreachableRetirementWaitsForTerminalRuntime(t *testing.T) {
	for _, mode := range []string{"replacement", "scale-in"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			converge(t, p, rc)
			target := "demop0"
			if mode == "scale-in" {
				rc.Cluster.Spec.Components.Engine.Replicas = 5
				converge(t, p, rc)
				target = "demop4"
				rc.Cluster.Spec.Components.Engine.Replicas = 3
			}
			// A surviving /cluster view, not a failed dial or a stale SQL role,
			// establishes that the unreachable predecessor is not the leader.
			b.leader = "demop1"
			rc.Agents = retirementAgents{AgentDialer: rc.Agents, unreachable: target}
			payload, _ := json.Marshal(map[string]string{"member": target})
			op := &domain.Operation{ID: "unreachable", Kind: domain.OperationReplaceMember, Payload: payload}
			seen := map[string]bool{}
			blocked := false
			finished := false
			for range 200 {
				p = New(p.options)
				if mode == "replacement" {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = result.Checkpoint
					finished = result.Done
					if p.SyncDuringOperation(op) {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
				} else if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if m := s.find(target); m != nil {
					seen[m.Retirement] = true
					if m.Retirement == "runtime-wait" && !blocked {
						b.runtimeStopBusy = true
						ok, err := fenceRetirement(t.Context(), rc, s, m)
						if err != nil || ok || m.Retirement != "runtime-wait" || b.services[target] == nil || slices.Contains(b.actions, "remove::"+target+"-etcd") {
							t.Fatal("desired STOPPED state was mistaken for terminal stop completion", err)
						}
						b.runtimeStopBusy, blocked = false, true
					}
				}
				if (mode == "replacement" && finished) || (mode == "scale-in" && len(s.data()) == 3) {
					finished = true
					break
				}
			}
			if !finished || !blocked {
				t.Fatal("unreachable retirement did not fence and converge")
			}
			for _, stage := range []string{"switchover", "runtime-stop", "runtime-wait", "runtime-stopped", "removed-etcd"} {
				if !seen[stage] {
					t.Fatalf("infrastructure retirement checkpoint %q not resumed", stage)
				}
			}
			stop := slices.Index(b.actions, "runtime-stop:"+target)
			remove := slices.Index(b.actions, "remove::"+target+"-etcd")
			deleted := slices.Index(b.actions, "delete:"+target)
			if stop < 0 || remove <= stop || deleted <= remove {
				t.Fatalf("unsafe infrastructure retirement ordering: %v", b.actions)
			}
		})
	}
}

func TestUnreachableRetirementRequiresSurvivingLeader(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop0")
	m.Retirement = "tags"
	rc.Agents = retirementAgents{AgentDialer: rc.Agents, unreachable: m.Name}
	for _, leader := range []string{m.Name, "missing"} {
		b.leader = leader
		ok, err := fenceRetirement(t.Context(), rc, s, m)
		if ok || !errors.Is(err, engine.ErrUnavailable) || m.Retirement != "tags" {
			t.Fatal("unreachable member retirement ignored unknown/old leader", err)
		}
	}
	if slices.Contains(b.actions, "runtime-stop:"+m.Name) {
		t.Fatal("runtime stopped before observing a surviving leader")
	}
}

func TestRetirementLostStopResponseAndRestartAreRechecked(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop1")
	m.Retirement = "stop"
	b.held = map[string]bool{m.Name: true}
	if ok, err := fenceRetirement(t.Context(), rc, s, m); err != nil || ok || m.Retirement != "stopped" {
		t.Fatal("lost stop response did not resume from durable hold", err)
	}
	// A stale checkpoint is not authority to delete a restarted writer.
	b.held[m.Name] = false
	b.leader = m.Name
	if ok, err := fenceRetirement(t.Context(), rc, s, m); err != nil || ok || m.Retirement != "untag" {
		t.Fatal("stop checkpoint authorized deleting a restarted primary", err)
	}
}

func TestSwitchoverPrefersHealthySyncCandidate(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demop2": {Role: "replica", Running: true, Streaming: true, SyncState: "sync"},
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil || b.leader != "demop2" {
		t.Fatal("switchover did not choose the healthy synchronous replica", err)
	}
}

func TestInfrastructureRetirementRechecksFence(t *testing.T) {
	for _, scenario := range []string{"restarted", "leader-changed", "reused-identity", "absent"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demop1")
			m.Retirement = "runtime-stopped"
			m.RetirementOp = &infra.Op{Kind: "retirement-stop"}
			b.services[m.Name].Status = infra.StatusStopped
			switch scenario {
			case "restarted":
				b.services[m.Name].Status = infra.StatusRunning
			case "leader-changed":
				b.leader = m.Name
			case "reused-identity":
				b.services[m.Name].ID = "unrelated"
			case "absent":
				delete(b.services, m.Name)
			}
			ok, err := fenceRetirement(t.Context(), rc, s, m)
			switch scenario {
			case "restarted":
				if ok || err != nil || m.Retirement != "runtime-stop" || m.RetirementOp != nil {
					t.Fatal("restarted runtime retained deletion authority", err)
				}
			case "leader-changed":
				if ok || !errors.Is(err, engine.ErrUnavailable) {
					t.Fatal("stale leader observation retained deletion authority", err)
				}
			case "reused-identity":
				if ok || !errors.Is(err, infra.ErrConflict) {
					t.Fatal("stop receipt crossed runtime identities", err)
				}
			case "absent":
				if !ok || err != nil {
					t.Fatal("already absent runtime could not complete retirement", err)
				}
			}
		})
	}
}

func TestInfrastructureRetirementRetriesFailedStop(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop1")
	m.Retirement = "runtime-stop"
	for range 5 {
		if _, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
			t.Fatal(err)
		}
		if m.Retirement == "runtime-wait" {
			break
		}
	}
	b.runtimeStopFail = true
	if ok, err := fenceRetirement(t.Context(), rc, s, m); err == nil || ok || m.Retirement != "runtime-stop" || m.RetirementOp != nil {
		t.Fatal("failed stop became an infrastructure fence", err)
	}
	b.runtimeStopFail = false
	for range 5 {
		ok, err := fenceRetirement(t.Context(), rc, s, m)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
	}
	t.Fatal("failed infrastructure stop did not resume")
}

func TestRetirementPublishesDurableConfig(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop1")
	if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err != nil {
		t.Fatal("could not publish retirement", err)
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil || spec.Retiring {
		t.Fatal("retirement excluded a member before untagged switchover", err)
	}
	if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err != nil || m.Retirement != "tags" {
		t.Fatal("replica did not reach tag publication", err)
	}
	p = New(p.options)
	config, err = p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil || !spec.Retiring || config.ConfigVersion != m.ConfigVersion {
		t.Fatal("retirement intent lost across control-plane reconstruction", err)
	}
}

func TestRetirementAcceptsQuorumBackedLearnerPrimary(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.newDataMember(rc.Cluster)
	m.EtcdLearner = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.leader = m.Name
	if ok, err := survivingPrimary(t.Context(), rc, s, "demop0"); err != nil || !ok {
		t.Fatal("valid Patroni primary was rejected because its local etcd is a learner", err)
	}
	b.statusOverrides = map[string]pgspec.StatusResponse{
		m.Name: {Role: "primary", Running: true, Leader: m.Name, EtcdHealthy: false},
	}
	if ok, err := survivingPrimary(t.Context(), rc, s, "demop0"); ok || !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("learner primary without a DCS quorum authorized retirement", err)
	}
}

func TestRetirementSwitchesBeforeTaggingPrimary(t *testing.T) {
	for _, checkpoint := range []string{"", "switchover", "tags", "stop", "stopped", "removed-etcd"} {
		t.Run("resume-"+checkpoint, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			target := "demop0"
			s.find(target).Retirement = checkpoint
			b.leader = target
			b.fenced = map[string]bool{target: checkpoint != ""}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			stopped := false
			for range 30 {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				m := s.find(target)
				if _, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
					t.Fatal(err)
				}
				if b.leader == target && retirementTagsDesired(m) {
					t.Fatal("retirement retained nofailover intent on its primary")
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if m.Retirement == "stopped" {
					stopped = true
					break
				}
			}
			clear := slices.Index(b.actions, "retire-unfence:"+target)
			switched := slices.Index(b.actions, "retire-switchover:"+target)
			tagged := slices.Index(b.actions, "retire-fence:"+target)
			if !stopped || clear < 0 || switched <= clear || tagged <= switched {
				t.Fatalf("primary retirement did not clear, switch, then fence: %v", b.actions)
			}
		})
	}
}

func TestRetirementWaitsForReachableDemotion(t *testing.T) {
	for _, stage := range []string{"switchover", "untag", "tags", "stop"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demop1")
			m.Retirement = stage
			b.fenced = map[string]bool{m.Name: stage == "stop"}
			b.statusOverrides = map[string]pgspec.StatusResponse{
				m.Name: {Role: "starting", Running: false, Leader: "demop0", EtcdHealthy: true},
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err != nil || m.Retirement != stage || m.RetirementOp != nil {
					t.Fatal("reachable switchover demotion selected infrastructure fallback", err)
				}
			}
			b.statusOverrides[m.Name] = pgspec.StatusResponse{Role: "replica", Running: true, Streaming: true, Leader: "demop0", EtcdHealthy: true}
			for range 10 {
				if _, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
					t.Fatal(err)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if m.Retirement == "stopped" {
					break
				}
			}
			if m.Retirement != "stopped" || !b.held[m.Name] || slices.Contains(b.actions, "runtime-stop:"+m.Name) {
				t.Fatal("reachable demotion did not finish with a local durable stop hold")
			}
		})
	}
}

func TestRetirementKeepsReachableAgentAfterActionErrors(t *testing.T) {
	for _, failure := range []string{"reload", "postgres-status"} {
		for _, stage := range []string{"switchover", "tags"} {
			t.Run(failure+"/"+stage, func(t *testing.T) {
				p, rc, b := setup(t, "ha")
				s := converge(t, p, rc)
				m := s.find("demop1")
				m.Retirement = stage
				if failure == "reload" {
					b.reloadFailures = 1
				} else {
					b.statusFailures = map[string]int{m.Name: 1}
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err == nil || m.Retirement != stage || m.RetirementOp != nil {
					t.Fatal("engine action failure was discarded or mistaken for unreachable agent", err)
				}
				for range 10 {
					if _, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
						t.Fatal(err)
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					if m.Retirement == "stopped" {
						break
					}
				}
				if m.Retirement != "stopped" || !b.held[m.Name] || slices.Contains(b.actions, "runtime-stop:"+m.Name) {
					t.Fatal("reachable agent did not recover through local stop hold")
				}
			})
		}
	}
}

func TestRetirementSelectsBackendFenceForTransportFailure(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop1")
	m.Retirement = "tags"
	rc.Agents = &unavailableEtcdVoterAgents{fakeAgents: rc.Agents.(fakeAgents), rpcFailure: true}
	if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err != nil || m.Retirement != "runtime-stop" {
		t.Fatal("unreachable status transport did not select infrastructure fence", err)
	}
}

func TestRetirementFencesReachableStoppedSupervisor(t *testing.T) {
	for _, stage := range []string{"switchover", "untag", "tags", "stop"} {
		for _, processState := range []string{"failed", "stopped"} {
			t.Run(stage+"/"+processState, func(t *testing.T) {
				p, rc, b := setup(t, "ha")
				s := converge(t, p, rc)
				m := s.find("demop1")
				m.Retirement = stage
				b.statusOverrides = map[string]pgspec.StatusResponse{
					m.Name: {Role: "unknown", Running: false, Leader: "demop0"},
				}
				b.genericOverrides = map[string]agentapi.Status{
					m.Name: {Processes: map[string]agentapi.ProcessStatus{
						"patroni": {State: processState},
					}},
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				fenced := false
				for range 10 {
					ok, err := fenceRetirement(t.Context(), rc, s, m)
					if err != nil {
						t.Fatal(err)
					}
					if ok {
						fenced = true
						break
					}
					if m.Retirement == "runtime-wait" {
						b.runtimeStopBusy = true
						if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || err != nil || m.Retirement != "runtime-wait" {
							t.Fatal("an accepted stop was mistaken for terminal runtime evidence", err)
						}
						b.runtimeStopBusy = false
					}
				}
				if !fenced || !slices.Contains(b.actions, "runtime-stop:"+m.Name) {
					t.Fatal("reachable stopped supervisor stranded retirement without a runtime fence")
				}
				if slices.Contains(b.actions, "retire-stop:"+m.Name) {
					t.Fatal("stopped supervisor was treated as a streaming, locally fenced replica")
				}
			})
		}
	}
}

func TestStoppedSupervisorRetirementRequiresSurvivingLeader(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop1")
	m.Retirement = "tags"
	b.statusOverrides = map[string]pgspec.StatusResponse{
		m.Name: {Role: "unknown", Running: false},
	}
	b.genericOverrides = map[string]agentapi.Status{
		m.Name: {Processes: map[string]agentapi.ProcessStatus{
			"patroni": {State: "failed"},
		}},
	}
	b.leader = "missing"
	if ok, err := fenceRetirement(t.Context(), rc, s, m); ok || !errors.Is(err, engine.ErrUnavailable) || m.Retirement != "tags" {
		t.Fatal("stopped supervisor bypassed the surviving-primary prerequisite", err)
	}
	if slices.Contains(b.actions, "runtime-stop:"+m.Name) {
		t.Fatal("runtime stop preceded surviving-primary evidence")
	}
}
