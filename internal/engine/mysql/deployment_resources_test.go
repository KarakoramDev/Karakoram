// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestExhaustedInitialDeploymentRearmAppliesResources(t *testing.T) {
	for _, topology := range []string{"single", "group", "pxc", "proxy"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var base *fakeBackend
			switch topology {
			case "group":
				p, rc, base = setup(t, "group")
			case "pxc":
				p, rc, base = setupPXC(t)
			default:
				p, rc, base = setup(t, "single")
			}
			if topology == "proxy" {
				converge(t, p, rc, base)
				proxy := rc.Cluster.Spec.Components.Extra["proxy"]
				proxy.Replicas = 1
				rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			}
			enginetest.AssertDeploymentResourceRearm(t, rc, func() error {
				_, err := p.Sync(t.Context(), rc)
				return err
			}, func() infra.Resources {
				if topology == "proxy" {
					proxy := rc.Cluster.Spec.Components.Extra["proxy"]
					proxy.Resources.MinRAM *= 2
					proxy.Resources.MaxRAM *= 2
					rc.Cluster.Spec.Components.Extra["proxy"] = proxy
					return proxy.Resources
				}
				r := &rc.Cluster.Spec.Components.Engine.Resources
				r.MinRAM *= 2
				r.MaxRAM *= 2
				return *r
			})
		})
	}
}

func TestRuntimeRecoveryPreservesAcceptedResourcesInClosedWindow(t *testing.T) {
	for _, role := range []string{"data", "proxy"} {
		t.Run(role, func(t *testing.T) {
			p, rc, base := setup(t, "group")
			converge(t, p, rc, base)
			before, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := before.data()[0]
			if role == "proxy" {
				m = before.find(rc.Cluster.Name + "mx")
				d := rc.Cluster.Spec.Components.Extra["proxy"]
				d.Resources.MaxCPU++
				rc.Cluster.Spec.Components.Extra["proxy"] = d
			} else {
				rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
			}
			accepted := m.Resources
			rc.Cluster.Generation++
			delete(base.services, m.Name)
			enginetest.AssertRuntimeRecoveryPreservesResources(t, rc, m.Name, accepted, accepted, func() error {
				_, err := p.Sync(t.Context(), rc)
				if err == nil {
					base.apply(t, p, rc)
				}
				return err
			}, func() (infra.Resources, bool, error) {
				s, err := load(rc.Cluster)
				if err != nil {
					return infra.Resources{}, false, err
				}
				current := s.find(m.Name)
				return current.Resources, current.Stage == "joined" && s.Pending == nil, nil
			})
		})
	}
}
