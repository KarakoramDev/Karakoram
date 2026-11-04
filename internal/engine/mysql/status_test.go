// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestStatusBeforeMemberImport(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			if provider == "pxc" {
				p, rc, _ = setupPXC(t)
			} else {
				p, rc, _ = setup(t, "group")
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.allocate(rc.Cluster)
			if err = save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatalf("planned members must not fail availability observation: %v", err)
			}
			if status.Phase == domain.PhaseReady || len(status.Members) != len(s.Members) || len(status.Members) == 0 {
				t.Fatalf("incomplete planned-member observation: %+v", status)
			}
			for _, member := range status.Members {
				if member.Health || member.PlacementGroup != "" {
					t.Fatalf("unimported member reported healthy or placed: %+v", member)
				}
			}
		})
	}
}

func TestCreateWithUnimportedProxyObservation(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "single")
				proxy := rc.Cluster.Spec.Components.Extra["proxy"]
				proxy.Replicas = 1
				rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			}
			// converge observes status after every provisioning checkpoint.
			// The strict fake rejects Containers for any unimported service.
			converge(t, p, rc, b)
		})
	}
}
