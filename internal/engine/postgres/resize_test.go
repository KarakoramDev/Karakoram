// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type failedResizeBackend struct{ *fakeBackend }

func (b failedResizeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "failed-resize" {
		return infra.OpState{Failed: true}, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

type unavailableResizeAgent struct {
	engine.AgentDialer
	member string
}

func (a unavailableResizeAgent) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == a.member {
		return nil, errors.New("agent cannot start with failed resource bounds")
	}
	return a.AgentDialer.Dial(ctx, ref, name)
}

func TestFailedResizeReplansLatestBoundsWithoutAgent(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, target := range []string{"demop0", "demopx"} {
			for _, pendingFailure := range []bool{false, true} {
				name := topology + "/" + target
				if pendingFailure {
					name += "/pending"
				} else {
					name += "/observed"
				}
				t.Run(name, func(t *testing.T) {
					p, rc, b := setup(t, topology)
					s := converge(t, p, rc)
					m := s.find(target)
					wanted := desiredComponent(rc, m).Resources
					failed := wanted
					failed.MinRAM, failed.MaxRAM = 0.25, 0.25
					b.services[target].Resources = failed
					b.services[target].Status = infra.StatusFailed
					if pendingFailure {
						s.Pending = &pending{Op: infra.Op{ID: "failed-resize"}, Action: "resize", Member: target, Resources: failed}
					} else {
						m.Resources = failed
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					rc.Cluster.Generation++
					agents := rc.Agents
					rc.Agents = unavailableResizeAgent{AgentDialer: agents, member: target}
					rc.Backend = failedResizeBackend{b}
					b.actions = nil
					if _, err := p.Sync(context.Background(), rc); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(b.actions, []string{"resize:" + target}) || !reflect.DeepEqual(b.services[target].Resources, wanted) {
						t.Fatalf("latest resource bounds were not applied: actions=%v resources=%+v", b.actions, b.services[target].Resources)
					}
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if s.Pending == nil || s.Pending.Action != "resize" || s.Pending.Member != target || !reflect.DeepEqual(s.Pending.Resources, wanted) {
						t.Fatalf("corrective resize receipt was not checkpointed: %+v", s.Pending)
					}
					rc.Agents, rc.Backend = agents, b
					b.services[target].Status = infra.StatusRunning
					for range 10 {
						if _, err := p.Sync(context.Background(), rc); err != nil {
							t.Fatal(err)
						}
						if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
							break
						}
					}
					converge(t, p, rc)
					if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
						t.Fatal("corrected resources did not converge to the latest generation")
					}
				})
			}
		}
	}
}

func TestDisconnectedRunningMemberDoesNotBypassResizeSafety(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop0")
	previous := m.Resources
	previous.MinRAM, previous.MaxRAM = 0.25, 0.25
	m.Resources = previous
	b.services[m.Name].Resources = previous
	b.services[m.Name].Status = infra.StatusRunning
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: m.Name}
	b.actions = nil
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	if len(b.actions) != 0 || !reflect.DeepEqual(b.services[m.Name].Resources, previous) {
		t.Fatal("agent disconnection bypassed primary role/switchover safety")
	}
}

func TestFailedResizeSupersessionObservesRunningRuntimeResources(t *testing.T) {
	for _, target := range []string{"demop0", "demop1"} {
		t.Run(target, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find(target)
			wanted := m.Resources
			failed := wanted
			failed.MinRAM, failed.MaxRAM = 0.25, 0.25
			// The backend accepted the previous bounds, but the failed receipt
			// left the member checkpoint at the older, now desired bounds.
			b.services[target].Resources = failed
			b.services[target].Status = infra.StatusRunning
			s.Pending = &pending{Op: infra.Op{ID: "failed-resize"}, Action: "resize", Member: target, Resources: failed}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Backend = failedResizeBackend{b}
			b.actions = nil
			if _, err := p.Sync(context.Background(), rc); err != nil {
				t.Fatal(err)
			}
			if target == "demop0" {
				if b.leader == "" || b.leader == target || len(b.actions) != 0 {
					t.Fatal("superseding the primary's failed resize did not switch leadership first")
				}
				if _, err := p.Sync(context.Background(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(b.actions, []string{"resize:" + target}) || !reflect.DeepEqual(b.services[target].Resources, wanted) {
				t.Fatalf("backend resource drift was not corrected safely: actions=%v resources=%+v", b.actions, b.services[target].Resources)
			}
		})
	}
}

// Exercise the public convergence path: capacity recovery must remain possible
// after a full data/WAL filesystem has stopped PostgreSQL or its agent.
func TestSyncStoppedMemberSubmitsVolumeGrowthBeforeReadiness(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, unavailableAgent := range []bool{false, true} {
			name := topology + "/stopped-postgres"
			if unavailableAgent {
				name = topology + "/unavailable-agent"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, b := setup(t, topology)
				s := converge(t, p, rc)
				target := s.data()[0]
				observedGeneration := rc.Cluster.Status.ObservedGeneration
				rc.Cluster.Generation++
				rc.Cluster.Spec.Components.Engine.Resources.MinDisk += 10
				rc.Cluster.Spec.Components.Engine.Resources.MaxDisk += 10
				wanted := domain.Resources{
					MinDisk: rc.Cluster.Spec.Components.Engine.Resources.MinDisk,
					MaxDisk: rc.Cluster.Spec.Components.Engine.Resources.MaxDisk,
				}
				if unavailableAgent {
					rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: target.Name}
				} else {
					b.statusOverrides = map[string]pgspec.StatusResponse{
						target.Name: {Running: false, Role: "stopped"},
					}
				}
				rc.Backend = growthRecoveryBackend{b}
				b.actions = nil
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(b.actions, []string{"resize:" + target.Volume}) {
					t.Fatalf("stopped member blocked volume growth: actions=%v wait=%s", b.actions, rc.SyncWait)
				}
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if current.Pending == nil || current.Pending.Action != "resize-volume" ||
					current.Pending.Member != target.Name || current.Pending.Resources != wanted {
					t.Fatalf("volume growth lacks serialized receipt: %+v", current.Pending)
				}
				if current.find(target.Name).Disk != target.Disk {
					t.Fatal("volume bounds acknowledged before the operation receipt")
				}
				if rc.Cluster.Status.ObservedGeneration != observedGeneration {
					t.Fatal("volume submission acknowledged the unhealthy generation")
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase == domain.PhaseReady {
					t.Fatal("volume submission made a stopped member Ready")
				}
			})
		}
	}
}
