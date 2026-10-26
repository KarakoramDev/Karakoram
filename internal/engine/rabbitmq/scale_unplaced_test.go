// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type unplacedScaleBackend struct{ *supersedingCleanupBackend }

func (*unplacedScaleBackend) RuntimeNeverPlaced(context.Context, string) (bool, error) {
	return true, nil
}
func TestScaleBackRetiresUnplacedMembersWithoutRedeploy(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	before := len(f.deployments)
	count := len(s.Members)
	for range 2 {
		m := s.add(rc.Cluster.Name)
		m.Stage = "imported"
		m.ServiceID = m.Name
		m.VolumeID = m.Volume
		f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name, Kind: infra.KindRuntime}
		f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Kind: infra.KindVolume}
	}
	s.Pending = &pending{Action: "deploy", Member: s.Members[count].Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	b := &unplacedScaleBackend{&supersedingCleanupBackend{fakeInfra: f}}
	rc.Backend = b
	for range 40 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s = state(t, rc)
		if len(s.Members) == count {
			if len(f.services) != count*2 || len(f.deployments) != before || b.deployPolls != 0 {
				t.Fatal("obsolete provisioning replayed or left resources")
			}
			return
		}
	}
	t.Fatal("scale-back waited for never-placed deploy")
}
