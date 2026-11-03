// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestAgentUpgradeBlocksOutstandingRecovery(t *testing.T) {
	for _, topology := range []string{"single", "group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, topology)
			for _, mode := range []string{"confirmed loss", "identity conflict", "runtime operation", "runtime import", "replacement", "queued replacement", "running replacement"} {
				t.Run(mode, func(t *testing.T) {
					s, _ := load(rc.Cluster)
					lost := s.data()[0]
					switch mode {
					case "confirmed loss":
						lost.ConfirmedVolumeLost = true
					case "identity conflict":
						lost.RuntimeRecovery.VolumeLost = true
					case "runtime operation":
						lost.RuntimeRecovery.Op = &infra.Op{ID: "repair"}
					case "runtime import":
						lost.RuntimeRecovery.ImportPlan = &engine.ImportPlan{}
					case "replacement":
						s.Replacement = &replacementState{Operation: "repair"}
					case "queued replacement", "running replacement":
						phase := domain.PhasePending
						if mode == "running replacement" {
							phase = domain.PhaseRunning
						}
						op := &domain.Operation{ID: mode, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: phase}
						if err := rc.Store.EnqueueOperation(t.Context(), op); err != nil {
							t.Fatal(err)
						}
						defer func() {
							op.Phase = domain.PhaseSucceeded
							if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), op); err != nil {
								t.Fatal(err)
							}
						}()
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					// The fake still reports healthy native peers. Durable loss/recovery
					// evidence must win for proxies, singletons and replicated data alike.
					for _, m := range s.Members {
						if err := agentUpgradeGate(t.Context(), rc, s, m); !errors.Is(err, engine.ErrUnavailable) {
							t.Fatalf("gate %s: %v", m.Name, err)
						}
					}
					for _, stage := range []string{"admission", "select", "handoff", "deploy", "waiting", "settling"} {
						op := mysqlAgentRolloutOperation(true)
						if stage != "admission" {
							var target engine.AgentUpgradePayload
							json.Unmarshal(op.Payload, &target)
							cp := agentUpgradeCheckpoint{Target: target, Order: []string{lost.Name}, Stage: stage}
							if stage != "select" {
								cp.Member = lost.Name
							}
							op.Checkpoint, _ = json.Marshal(cp)
						}
						result, err := p.RunOperation(t.Context(), rc, op)
						if stage == "admission" && !errors.Is(err, engine.ErrUnavailable) {
							t.Fatalf("%s: %v", stage, err)
						}
						if stage != "admission" && err != nil && !(result.Done && errors.Is(err, engine.ErrInvalid)) {
							t.Fatalf("settlement blocked at %s: %v", stage, err)
						}
						if stage == "select" && !result.Done {
							t.Fatal("undisrupted rollout retained queue ownership")
						}
					}
					if len(b.names) != 0 {
						t.Fatal("recovery allowed deployment", b.names)
					}
					lost.ConfirmedVolumeLost = false
					lost.RuntimeRecovery = engine.RuntimeRecovery{}
					s.Replacement = nil
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestLostVolumeRejectsWithdrawnSurvivorAndReplacementDrain(t *testing.T) {
	for _, marker := range []string{"withdrawn", "readmitting"} {
		t.Run(marker, func(t *testing.T) {
			p, rc, _, s, lost := recoveryFixture(t)
			survivor := s.data()[0]
			survivor.ProxyDrainOperation = "rollout"
			survivor.ProxyDraining = marker == "withdrawn"
			lost.ConfirmedVolumeLost = true
			if err := p.volumeRecoverySafe(t.Context(), rc, s, lost); err == nil {
				t.Fatal("withdrawn survivor admitted as donor/writer")
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "replace", Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
			if _, err := p.replaceMember(t.Context(), rc, op); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("replacement ignored drain: %v", err)
			}
			current, _ := load(rc.Cluster)
			if current.Replacement != nil || len(current.Members) != len(s.Members) {
				t.Fatal("replacement created a new identity during drain")
			}
		})
	}
}

func TestLostVolumeOrphanDrainSettlesBeforeQueue(t *testing.T) {
	p, rc, _, s, lost := recoveryFixture(t)
	lost.ProxyDraining, lost.ProxyDrainOperation = true, "missing-rollout"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// Loss is not yet confirmed. Sync must discover it before trying native
	// readiness, then release the orphan without readmitting the lost identity.
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	lost = s.find(lost.Name)
	if !lost.ConfirmedVolumeLost || lost.ProxyDraining || lost.ProxyDrainOperation != "" {
		t.Fatalf("unsettled lost orphan: %+v", lost)
	}
	cfg, err := p.MemberConfig(t.Context(), rc, "demomx")
	if err != nil {
		t.Fatal(err)
	}
	var spec myspec.Spec
	if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	for _, peer := range spec.Proxy.Backends {
		if peer.Name == lost.Name {
			t.Fatal("released loss fence readmitted lost member")
		}
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
	if err != nil || len(ops) != 1 || ops[0].Kind != domain.OperationReplaceMember {
		t.Fatalf("recovery starved: %v %v", ops, err)
	}
}

func TestLostVolumeQueuePreservesActiveRolloutOwnership(t *testing.T) {
	p, rc, _, s, _ := recoveryFixture(t)
	st := rc.Store.(*store.Store)
	rollout := mysqlAgentRolloutOperation(false)
	rollout.Cluster, rollout.Namespace, rollout.Phase = rc.Cluster.Name, rc.Cluster.Namespace, domain.PhaseRunning
	if err := st.EnqueueOperation(t.Context(), rollout); err != nil {
		t.Fatal(err)
	}
	if _, err := p.recoverRuntimes(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimOperation(t.Context(), rc.Cluster.Ref())
	if err != nil || claimed.ID != rollout.ID {
		t.Fatalf("replacement stole active ownership: %+v %v", claimed, err)
	}
	rollout.Phase = domain.PhaseSucceeded
	if err := st.UpdateOperation(t.Context(), rollout); err != nil {
		t.Fatal(err)
	}
	claimed, err = st.ClaimOperation(t.Context(), rc.Cluster.Ref())
	if err != nil || claimed.Kind != domain.OperationReplaceMember {
		t.Fatalf("replacement not serialized behind rollout: %+v %v", claimed, err)
	}
}

func TestAgentUpgradeRecoveryRetiresInflightOwner(t *testing.T) {
	for _, tc := range []struct {
		name, topology, stage string
		cancel, lost          bool
	}{
		{"queued GR waiting cancel", "group", "waiting", true, false},
		{"queued PXC waiting cancel", "cluster", "waiting", true, false},
		{"queued PXC withdrawn handoff", "cluster", "handoff", false, false},
		{"lost PXC waiting cancel", "cluster", "waiting", true, true},
		{"lost PXC deploy intent", "cluster", "deploy", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rc, b := setupAgentRollout(t, tc.topology)
			s, _ := load(rc.Cluster)
			m := s.find(b.leader)
			op := mysqlAgentRolloutOperation(true)
			op.Cluster, op.Namespace, op.Phase = rc.Cluster.Name, rc.Cluster.Namespace, domain.PhaseRunning
			var target engine.AgentUpgradePayload
			json.Unmarshal(op.Payload, &target)
			cp := agentUpgradeCheckpoint{Target: target, Order: []string{m.Name, s.data()[1].Name}, Member: m.Name, Stage: tc.stage, Started: true}
			if tc.stage == "waiting" {
				cp.Deploy = &infra.Op{ID: "accepted-rollout"}
				b.builds[m.Name] = target.TargetBuild
			}
			if tc.topology == "cluster" {
				m.ProxyDraining, m.ProxyDrainOperation = true, op.ID
			}
			if tc.lost {
				m.ConfirmedVolumeLost, m.RuntimeRecovery.VolumeLost = true, true
				delete(b.native, m.Name)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			op.Checkpoint, _ = json.Marshal(&cp)
			st := rc.Store.(*store.Store)
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			replacement := &domain.Operation{ID: "queued-replacement", Cluster: rc.Cluster.Name, Namespace: rc.Cluster.Namespace, Kind: domain.OperationReplaceMember, Phase: domain.PhasePending}
			if err := st.EnqueueOperation(t.Context(), replacement); err != nil {
				t.Fatal(err)
			}
			retired := false
			for range 20 {
				p := New(Options{Provider: rc.Cluster.Spec.Provider})
				var result engine.OpResult
				var err error
				if tc.cancel {
					result, err = p.CancelOperation(t.Context(), rc, op)
				} else {
					result, err = p.RunOperation(t.Context(), rc, op)
				}
				if result.Checkpoint != nil {
					op.Checkpoint = result.Checkpoint
				}
				if result.Done {
					if tc.cancel {
						if err != nil {
							t.Fatalf("cleanup failed: %v", err)
						}
					} else if !errors.Is(err, engine.ErrInvalid) || result.Message == "" {
						t.Fatalf("incomplete rollout did not return terminal failure diagnostic: %+v %v", result, err)
					}
					retired = true
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
			}
			if !retired {
				t.Fatal("rollout did not retire")
			}
			if len(b.names) != 0 {
				t.Fatal("recovery settlement submitted another executable", b.names)
			}
			final, _ := load(rc.Cluster)
			if current := final.find(m.Name); current.ProxyDraining || current.ProxyDrainOperation != "" {
				t.Fatal("retired rollout retained drain ownership")
			}
			json.Unmarshal(op.Checkpoint, &cp)
			if cp.StopReason == "" {
				t.Fatal("missing durable retirement diagnostic")
			}
			op.Phase = domain.PhaseFailed
			if tc.cancel {
				op.Phase = domain.PhaseCancelled
			}
			if err := st.UpdateOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			claimed, err := st.ClaimOperation(t.Context(), rc.Cluster.Ref())
			if err != nil || claimed.ID != replacement.ID {
				t.Fatalf("replacement blocked after settlement: %+v %v", claimed, err)
			}
		})
	}
}
