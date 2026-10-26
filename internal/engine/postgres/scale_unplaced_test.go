// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type unplacedScaleBackend struct {
	*fakeBackend
	deployPolls int
}

func (*unplacedScaleBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (*unplacedScaleBackend) RuntimeNeverPlaced(context.Context, string) (bool, error) {
	return true, nil
}
func (b *unplacedScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "deploy" {
		b.deployPolls++
		return infra.OpState{}, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

type unplacedScaleAgents struct {
	engine.AgentDialer
	targets map[string]bool
	calls   int
}

func (a *unplacedScaleAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if a.targets[name] {
		a.calls++
		return nil, engine.ErrUnavailable
	}
	return a.AgentDialer.Dial(ctx, ref, name)
}
func TestScaleBackRetiresUnplacedMembersWithoutRedeploy(t *testing.T) {
	p, rc, f := setup(t, "ha")
	s := converge(t, p, rc)
	count := len(s.Members)
	f.etcd = map[string]pgspec.EtcdMember{}
	for _, peer := range s.data() {
		id := peer.Name + "-etcd"
		f.etcd[id] = pgspec.EtcdMember{ID: id, Name: peer.Name, PeerURL: []string{"https://" + peer.Name + ":2380"}}
	}
	agents := &unplacedScaleAgents{AgentDialer: rc.Agents, targets: map[string]bool{}}
	for i := 3; i < 5; i++ {
		name := fmt.Sprintf("demop%d", i)
		m := &member{Name: name, Volume: name + "v", Index: i, Role: "data", Stage: "deploying", ServiceID: name, VolumeID: name + "v", DeployOp: &infra.Op{ID: "blocked", Kind: "deploy"}}
		if i == 3 {
			m.EtcdID, m.EtcdState, m.EtcdLearner = name+"-etcd", "existing", true
			f.etcd[m.EtcdID] = pgspec.EtcdMember{ID: m.EtcdID, Name: name, PeerURL: []string{"https://" + name + ":2380"}, Learner: true}
		}
		s.Members = append(s.Members, m)
		agents.targets[name] = true
		f.services[name] = &infra.Service{ID: name, Hostname: name, Kind: infra.KindRuntime, Ownership: map[string]string{}}
		f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Kind: infra.KindVolume, Ownership: map[string]string{}}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &unplacedScaleBackend{fakeBackend: f}
	rc.Backend = b
	rc.Agents = agents
	for range 40 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(rc.Cluster)
		if agents.calls != 0 || b.deployPolls != 0 {
			t.Fatal("scale-back waited for unplaced agent/deployment")
		}
		if len(s.Members) == count {
			if len(f.etcd) != 3 {
				t.Fatal("unplaced learner was not removed while preserving existing voters")
			}
			for name := range agents.targets {
				if f.services[name] != nil || f.services[name+"v"] != nil {
					t.Fatal("unplaced resources remain")
				}
				if f.deployed[name] {
					t.Fatal("obsolete deployment replayed")
				}
			}
			return
		}
	}
	t.Fatal("scale-back did not retire never-placed members")
}
