// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestAgentBuildChangeRecoversAdmittedMember(t *testing.T) {
	for _, kind := range []string{kindNode, kindKibana} {
		steps := []string{"flush", "apply", "rejoin", "allocation", "recover"}
		if kind == kindKibana {
			steps = []string{"apply", "rejoin"}
		}
		for _, step := range steps {
			for _, ordinarySync := range []bool{false, true} {
				mode := "operation"
				if ordinarySync {
					mode = "sync-journal"
				}
				t.Run(kind+"/"+step+"/"+mode, func(t *testing.T) {
					p, rc, f, es := coreSetup(t, 2)
					if kind == kindKibana {
						component := rc.Cluster.Spec.Components.Extra[kindKibana]
						component.Replicas = 1
						rc.Cluster.Spec.Components.Extra[kindKibana] = component
					}
					s := coreConverge(t, p, rc, f)
					m := s.members(kind)[0]
					// A durable target from the previous control-plane binary.
					target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build + "-previous", IncludeCurrent: true}
					build := "existing-member-build"
					fleetAgents(t, rc, &build)
					if kind == kindNode {
						es.persistent["cluster.routing.allocation.enable"] = "primaries"
						if step == "recover" {
							delete(es.persistent, "cluster.routing.allocation.enable")
						}
					} else if step == "apply" {
						if err := p.issueAction(t.Context(), rc, s, &pending{Action: "stop", Member: m.Name}); err != nil {
							t.Fatal(err)
						}
					}
					s.Rolling = &rolling{Reason: "agents", OperationID: "old-build", AgentTarget: &target, Order: []string{m.Name, "demon1"}, Current: m.Name, Step: step}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					closeFleetWindow(rc)
					checkpoint, _ := json.Marshal(operationState{Stage: "rolling", AgentTarget: &target, Waiting: m.Name})
					payload, _ := json.Marshal(target)
					op := &domain.Operation{ID: "old-build", Kind: domain.OperationUpgradeAgents, Payload: payload, Checkpoint: checkpoint}
					before := len(f.calls)
					finished := false
					for range 20 {
						var done bool
						var err error
						if ordinarySync {
							_, err = p.Sync(t.Context(), rc)
							s, _ = load(rc.Cluster)
							done = s.Rolling == nil
						} else {
							var result engine.OpResult
							result, err = p.RunOperation(t.Context(), rc, op)
							op.Checkpoint, done = result.Checkpoint, result.Done
						}
						if done {
							if err == nil || !strings.Contains(err.Error(), "re-run upgrade-agents") {
								t.Fatalf("missing terminal build failure: %v", err)
							}
							finished = true
							break
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					if !finished {
						t.Fatal("build change stranded admitted member")
					}
					s, _ = load(rc.Cluster)
					if s.Rolling != nil || s.Pending != nil || s.Retry != nil {
						t.Fatal("repair left durable work", s.Rolling, s.Pending, s.Retry)
					}
					if _, restricted := es.persistent["cluster.routing.allocation.enable"]; restricted {
						t.Fatal("replica allocation remains restricted")
					}
					for _, call := range f.calls[before:] {
						if strings.HasPrefix(call, "deploy:") || strings.HasPrefix(call, "stop:") {
							t.Fatal("changed build admitted disruption", call)
						}
					}
					if kind == kindKibana && step == "apply" && !slices.Contains(f.calls[before:], "start:"+m.Name) {
						t.Fatal("stopped component was not restarted")
					}
					// Old checkpoints fail clearly even if ordinary Sync repaired first.
					result, err := p.RunOperation(t.Context(), rc, op)
					if !result.Done || err == nil {
						t.Fatal("old operation did not remain failed", result, err)
					}
					op.Checkpoint = result.Checkpoint
					if err := p.CanCancelOperation(t.Context(), op); err != nil {
						t.Fatal("repaired operation cannot cancel", err)
					}
					if result, err := p.CancelOperation(t.Context(), rc, op); err != nil || !result.Done {
						t.Fatal("cancel after repair", result, err)
					}
					payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
					newOp := &domain.Operation{ID: "new-build", Kind: domain.OperationUpgradeAgents, Payload: payload}
					if _, err := p.RunOperation(t.Context(), rc, newOp); err != nil {
						t.Fatal("new operation could not start after repair", err)
					}
					s, _ = load(rc.Cluster)
					if s.Rolling == nil || s.Rolling.OperationID != newOp.ID || s.Rolling.AgentTarget.TargetBuild != engine.CurrentAgentBuild().Build {
						t.Fatal("new operation did not get its own frozen target")
					}
				})
			}
		}
	}
}

func TestAgentBuildChangeDoesNotReplayDeploymentRetry(t *testing.T) {
	p, rc, f, es := coreSetup(t, 2)
	s := coreConverge(t, p, rc, f)
	m := s.members(kindNode)[0]
	target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build + "-previous"}
	build := "existing-build"
	fleetAgents(t, rc, &build)
	s.Rolling = &rolling{Reason: "agents", OperationID: "old", AgentTarget: &target, Current: m.Name, Step: "rejoin", Order: []string{m.Name, "demon1"}}
	s.Retry = &pending{Action: "upgrade", Member: m.Name, Version: m.Version}
	es.persistent["cluster.routing.allocation.enable"] = "primaries"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before := len(f.calls)
	for range 20 {
		_, err := p.Sync(t.Context(), rc)
		s, _ = load(rc.Cluster)
		if s.Rolling == nil {
			if err == nil || !strings.Contains(err.Error(), "re-run upgrade-agents") {
				t.Fatal("missing terminal failure", err)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if s.Rolling != nil || s.Retry != nil || s.Pending != nil {
		t.Fatal("retry recovery stranded")
	}
	if !slices.Contains(f.calls[before:], "start:"+m.Name) {
		t.Fatal("failed deployment was not restarted on its existing artifact")
	}
	for _, call := range f.calls[before:] {
		if strings.HasPrefix(call, "deploy:") {
			t.Fatal("replayed deployment with changed build", call)
		}
	}
	if _, restricted := es.persistent["cluster.routing.allocation.enable"]; restricted {
		t.Fatal("allocation not restored")
	}
}

func TestAgentBuildChangeRetiresUnadmittedJournal(t *testing.T) {
	for _, step := range []string{"", "prepare"} {
		t.Run(step, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build + "-previous"}
			s.Rolling = &rolling{Reason: "agents", AgentTarget: &target, Order: []string{"demon0"}, Step: step}
			if step != "" {
				s.Rolling.Current = "demon0"
			}
			closeFleetWindow(rc)
			before, esBefore := f.mutations, len(es.mutations)
			done, _, err := p.rollingStep(t.Context(), rc, s)
			if !done || err == nil || s.Rolling != nil {
				t.Fatal("unadmitted journal was not retired", done, err, s.Rolling)
			}
			if before != f.mutations || esBefore != len(es.mutations) {
				t.Fatal("build mismatch caused pre-admission disruption")
			}
		})
	}
}

func TestAgentBuildChangeRejoinConsumesOperatorConfigurationRepair(t *testing.T) {
	for _, kind := range []string{kindNode, kindLogstash} {
		t.Run(kind, func(t *testing.T) {
			var p *Provider
			var rc *engine.Context
			var f *coreInfra
			var es *coreES
			if kind == kindLogstash {
				p, rc, f = pipelineReadinessFixture(t, workingPipeline)
			} else {
				p, rc, f, es = coreSetup(t, 2)
			}
			s := coreConverge(t, p, rc, f)
			m := s.members(kind)[0]
			version := m.Version
			otherHash := s.find("demon1")
			if kind == kindLogstash {
				otherHash = s.find("demon0")
				setTestPipeline(t, rc, brokenPipeline)
			} else {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"elasticsearch":{"settings":{"review.invalid_setting":true}}}`)
			}
			unchangedHash := otherHash.ConfigHash
			if err := p.publishConfig(t.Context(), rc, s, m, version, ""); err != nil {
				t.Fatal(err)
			}
			badHash := m.ConfigHash
			target := engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build + "-previous"}
			s.Rolling = &rolling{Reason: "agents", OperationID: "old", AgentTarget: &target, Order: []string{m.Name, otherHash.Name}, Current: m.Name, Step: "rejoin"}
			if kind == kindNode {
				delete(f.nodes, m.Name)
				es.persistent["cluster.routing.allocation.enable"] = "primaries"
				build := "previous-agent"
				fleetAgents(t, rc, &build)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			closeFleetWindow(rc)
			before := len(f.calls)
			result, err := p.Sync(t.Context(), rc)
			if err != nil {
				t.Fatal("failed configuration should remain recoverable", err)
			}
			if kind == kindLogstash && result.ConvergenceError == nil {
				t.Fatal("fixture did not observe the applied broken pipeline")
			}
			s, _ = load(rc.Cluster)
			if s.Rolling == nil || s.Rolling.Step != "rejoin" || s.find(m.Name).ConfigHash != badHash {
				t.Fatal("failed member did not remain in recovery", s.Rolling)
			}
			if kind == kindLogstash {
				setTestPipeline(t, rc, workingPipeline)
			} else {
				rc.Cluster.Spec.EngineOptions = nil
			}
			rc.Cluster.Generation++
			result, err = p.Sync(t.Context(), rc)
			if err != nil || result.ConvergenceError != nil {
				t.Fatal("operator configuration repair failed", result, err)
			}
			s, _ = load(rc.Cluster)
			if s.Rolling == nil || s.Rolling.Step != "rejoin" || s.find(m.Name).ConfigHash == badHash {
				t.Fatal("configuration repair was not published in place", s.Rolling)
			}
			if s.find(otherHash.Name).ConfigHash != unchangedHash || s.find(m.Name).Version != version || s.Rolling.AgentTarget.TargetBuild != target.TargetBuild {
				t.Fatal("repair changed another member, engine version, or frozen target")
			}
			if kind == kindLogstash {
				raw, err := secret(t.Context(), rc, "members/"+m.Name+"/restart-spec")
				if err != nil {
					t.Fatal("read corrected protected payload", err)
				}
				var published agentspec.Spec
				if err := json.Unmarshal(raw, &published); err != nil {
					t.Fatal("decode corrected protected payload", err)
				}
				if published.Logstash == nil || len(published.Logstash.Pipelines) != 1 || published.Logstash.Pipelines[0].ID != "main" || published.Logstash.Pipelines[0].Config != workingPipeline {
					t.Fatal("corrected pipeline did not reach protected payload")
				}
			} else {
				f.nodes[m.Name] = version
			}
			finished := false
			for range 10 {
				_, err := p.Sync(t.Context(), rc)
				s, _ = load(rc.Cluster)
				if s.Rolling == nil {
					if err == nil || !strings.Contains(err.Error(), "re-run upgrade-agents") {
						t.Fatal("repair did not retain terminal build mismatch", err)
					}
					finished = true
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !finished || s.Pending != nil || s.Retry != nil {
				t.Fatal("corrected configuration could not complete recovery")
			}
			if kind == kindNode {
				if _, restricted := es.persistent["cluster.routing.allocation.enable"]; restricted {
					t.Fatal("configuration repair left allocation restricted")
				}
			}
			for _, call := range f.calls[before:] {
				if strings.HasPrefix(call, "deploy:") || strings.HasPrefix(call, "stop:") {
					t.Fatal("configuration repair deployed a new artifact or stopped another member", call)
				}
			}
		})
	}
}
