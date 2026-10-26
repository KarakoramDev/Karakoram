// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

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
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	before := f.mutations
	count := len(s.Members)
	for range 2 {
		m, err := p.allocate(rc, s, kindNode)
		if err != nil {
			t.Fatal(err)
		}
		m.Stage = stageImported
		m.ServiceID = m.Name
		m.VolumeID = m.Volume
		f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name, Kind: infra.KindRuntime}
		f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Kind: infra.KindVolume}
	}
	s.Pending = &pending{Action: "deploy", Member: s.Members[count].Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &unplacedScaleBackend{&supersedingCleanupBackend{coreInfra: f}}
	rc.Backend = b
	for range 40 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(rc.Cluster)
		if len(s.Members) == count {
			if len(f.services) != count*2 || b.deployPolls != 0 {
				t.Fatal("retirement left resources or polled obsolete deployment")
			}
			if f.mutations <= before {
				t.Fatal("retirement did not delete resources")
			}
			return
		}
	}
	t.Fatal("scale-back waited for never-placed deploy")
}
