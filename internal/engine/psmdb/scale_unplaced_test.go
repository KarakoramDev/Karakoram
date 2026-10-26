// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type unplacedScaleBackend struct{ *supersedingCleanupBackend }

func (*unplacedScaleBackend) RuntimeNeverPlaced(context.Context, string) (bool, error) {
	return true, nil
}
func TestScaleBackRetiresUnplacedMembersWithoutRedeploy(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "retry"}[retry], func(t *testing.T) {
			p, rc, f, _ := setup(t)
			s, _ := load(rc.Cluster)
			s.UID = "existing"
			s.Sets["rs0"] = &setState{Stage: "ready"}
			base := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
			base.Stage = "active"
			base.Version = rc.Cluster.Spec.Version
			for range 2 {
				m := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
				m.Stage = "imported"
				m.ServiceID = m.Name
				m.VolumeID = m.Volume
				f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name, Kind: infra.KindRuntime}
				f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Kind: infra.KindVolume}
			}
			target := s.Members[1]
			v := &pending{Action: "deploy", Member: target.Name, Op: infra.Op{ID: "blocked", Kind: "deploy"}}
			if retry {
				v.Op = infra.Op{}
				s.Retry = v
				s.PlatformRetries = map[string]*platformRetry{platformRetryKey(v): {Failures: 1, Message: "startup expired"}}
			} else {
				s.Pending = v
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b := &unplacedScaleBackend{&supersedingCleanupBackend{fakeInfra: f}}
			rc.Backend = b
			for range 40 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if len(s.Members) == 1 {
					if len(f.services) != 0 || len(f.deployments) != 0 || b.deployPolls != 0 {
						t.Fatal("retirement left resources or replayed obsolete deployment")
					}
					return
				}
			}
			t.Fatal("scale-back stayed behind never-placed deploy")
		})
	}
}
