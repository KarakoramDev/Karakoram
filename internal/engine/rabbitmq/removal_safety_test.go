// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestDurableClassicQueueBlocksMemberRemovalBeforeAnyMigration(t *testing.T) {
	for _, queueType := range []string{"classic", ""} {
		t.Run("type="+queueType, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			var mu sync.Mutex
			queues := []mgmt.Queue{{Name: "orders", Vhost: "/customers", Type: queueType, Durable: true, Node: "rabbit@demom2"}}
			a.queueSource = func() []mgmt.Queue {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(queues)
			}
			a.log.clear()
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			before := f.mutations
			blocked := false
			for range 10 {
				_, err := p.Sync(context.Background(), rc)
				if err != nil {
					if !errors.Is(err, engine.ErrConflict) {
						t.Fatal(err)
					}
					blocked = true
					break
				}
			}
			m := state(t, rc).find("demom2")
			if !blocked || m == nil || !strings.Contains(m.RemovalBlocked, "orders") || !strings.Contains(m.RemovalBlocked, "/customers") {
				t.Fatalf("durable classic queue gate lacks persisted actionable detail: %+v", m)
			}
			if f.mutations != before {
				t.Fatal("platform mutated despite a durable classic queue on leaving member")
			}
			for _, call := range a.log.snapshot() {
				if strings.HasPrefix(call, "agent:") {
					t.Fatalf("migration started before classic queue validation: %s", call)
				}
			}
			// The durable gate survives control-plane reconstruction.
			rc.Cluster = cloneCluster(t, rc.Cluster)
			p = New(Options{Store: rc.Store, InternalCAPEM: "control-plane-ca"})
			a.p = p
			if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatal("persisted classic queue block was bypassed", err)
			}
			mu.Lock()
			queues = nil
			mu.Unlock()
			ready(t, p, rc)
		})
	}
}

func TestClassicQueueGateIgnoresNonDurableAndSurvivorQueues(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.queueSource = func() []mgmt.Queue {
		return []mgmt.Queue{
			{Name: "temporary", Vhost: "/", Type: "classic", Node: "rabbit@demom2"},
			{Name: "retained", Vhost: "/", Type: "classic", Durable: true, Node: "rabbit@demom0"},
			{Name: "replicated", Vhost: "/", Type: "quorum", Durable: true, Node: "rabbit@demom2"},
		}
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
}

func TestLostMemberReplacementUsesSurvivorMajorityWithoutDeadAgentActions(t *testing.T) {
	for _, lost := range []string{"demom0", "demom1"} {
		t.Run(lost, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			ctx := context.Background()
			st := rc.Store.(*store.Store)
			a.broker(lost).down = true
			a.log.clear()
			op := &domain.Operation{ID: "lost-replace", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost + `"}`), Phase: domain.PhaseRunning}
			if err := st.EnqueueOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			done := false
			for range 200 {
				result, err := p.RunOperation(ctx, rc, op)
				if err != nil {
					t.Fatalf("lost replacement failed: %v state=%s checkpoint=%s", err, rc.Cluster.Internal, op.Checkpoint)
				}
				op.Checkpoint = result.Checkpoint
				if err := st.UpdateOperationAndClusterState(ctx, op, rc.Cluster); err != nil {
					t.Fatal(err)
				}
				ref := rc.Cluster.Ref()
				rc.Cluster, err = st.GetCluster(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				op, err = st.GetOperation(ctx, op.ID)
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Store: st, InternalCAPEM: "control-plane-ca"})
				a.p = p
				if result.Done {
					done = true
					break
				}
				if p.SyncDuringOperation(op) {
					if _, err := p.Sync(ctx, rc); err != nil {
						t.Fatalf("lost replacement reconciliation failed: %v state=%s", err, rc.Cluster.Internal)
					}
				}
			}
			if !done || state(t, rc).find(lost) != nil || state(t, rc).find("demom3") == nil || f.services[lost] != nil || f.services[lost+"v"] != nil {
				t.Fatalf("lost replacement did not converge: %s", rc.Cluster.Internal)
			}
			calls := a.log.snapshot()
			for _, call := range calls {
				if strings.HasPrefix(call, "agent:"+lost+":") {
					t.Fatalf("lost replacement tried to command unreachable member: %s", call)
				}
			}
			seed := "demom0"
			if lost == seed {
				seed = "demom1"
			}
			forget := "agent:" + seed + ":forget:rabbit@" + lost
			assertBefore(t, calls, "agent:demom3:join", forget)
			assertBefore(t, calls, forget, "infra:"+lost+":delete")
		})
	}
}

func TestLostRemovalRefusesWithoutMetadataVoterMajority(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Members[1].Removing = true
	s.Members[2].Removing = true
	// Disk membership remains three voters even though only one survivor runs.
	a.broker("demom1").down = true
	a.broker("demom2").down = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	a.log.clear()
	before := f.mutations
	blocked := false
	for range 30 {
		_, err := p.Sync(context.Background(), rc)
		if err != nil {
			if !errors.Is(err, engine.ErrConflict) {
				t.Fatal(err)
			}
			blocked = true
			break
		}
	}
	if !blocked || f.mutations != before {
		t.Fatal("lost removal ignored metadata majority or deleted resources")
	}
	if m := state(t, rc).find("demom2"); m == nil || !strings.Contains(m.RemovalBlocked, "Khepri voter majority") {
		t.Fatalf("blocked for the wrong reason: %s", rc.Cluster.Internal)
	}
	for _, call := range a.log.snapshot() {
		if strings.Contains(call, ":forget") {
			t.Fatalf("forgot a metadata voter without surviving majority: %s", call)
		}
	}
}

func TestTwoVoterMetadataForgetKeepsDepartingRuntimeAlive(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	seenTwoVoters := false
	a.forgetHandler = func(node string) {
		leaving := a.broker(strings.TrimPrefix(node, "rabbit@"))
		if !leaving.running || !leaving.appStopped {
			t.Errorf("metadata forget must precede BEAM shutdown and follow stop_app: %s %+v", node, leaving)
		}
		voters := 0
		for _, broker := range a.brokers {
			if broker.joined {
				voters++
			}
		}
		if voters == 2 {
			seenTwoVoters = true
		}
	}
	a.log.clear()
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	if !seenTwoVoters {
		t.Fatal("test never exercised the two-voter metadata transition")
	}
	calls := a.log.snapshot()
	for _, leaving := range []string{"demom2", "demom1"} {
		forget := "agent:demom0:forget:rabbit@" + leaving
		assertBefore(t, calls, "agent:"+leaving+":stop-app", forget)
		assertBefore(t, calls, forget, "agent:"+leaving+":stop")
	}
}

func TestClassicQueueGateRechecksBeforeStopApp(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	syncUntil(t, p, rc, func(s *internalState) bool {
		m := s.find("demom2")
		return m != nil && m.RemoveStep == "stop-app"
	})
	a.queueSource = func() []mgmt.Queue {
		return []mgmt.Queue{{Name: "late", Vhost: "/", Type: "classic", Durable: true, Node: "rabbit@demom2"}}
	}
	if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("queue created during removal bypassed the repeated gate", err)
	}
	m := state(t, rc).find("demom2")
	if m == nil || m.RemoveStep != "stop-app" || !strings.Contains(m.RemovalBlocked, "late") || !a.broker("demom2").running || a.broker("demom2").appStopped {
		t.Fatalf("repeated classic queue gate did not preserve broker: %+v", m)
	}
	a.queueSource = nil
	ready(t, p, rc)
}
