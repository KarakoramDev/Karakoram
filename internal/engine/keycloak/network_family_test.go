// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type privateFamilyBackend struct {
	infra.Backend
	family string
}

func (b privateFamilyBackend) Layout() infra.Layout {
	v := b.Backend.Layout()
	v.PrivateAddressFamily = b.family
	return v
}
func TestMemberConfigPrivateAddressFamily(t *testing.T) {
	for _, family := range []string{infra.AddressFamilyIPv4, infra.AddressFamilyIPv6} {
		t.Run(family, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Backend = privateFamilyBackend{rc.Backend, family}
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			cfg, err := p.MemberConfig(t.Context(), rc, s.Member)
			if err != nil {
				t.Fatal(err)
			}
			var spec kcspec.Spec
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.IPv6 != (family == infra.AddressFamilyIPv6) {
				t.Fatal("wrong native listener policy", spec.IPv6)
			}
			pull := *rc
			pull.AgentConfigCapabilities = []string{"unrelated-request-capability"}
			pulled, err := p.MemberConfig(t.Context(), &pull, s.Member)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(pulled.EngineSpec, cfg.EngineSpec) || pulled.ConfigVersion != cfg.ConfigVersion {
				t.Fatal("Keycloak stored payload depends on pull capabilities")
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			after, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if after.ConfigHash != s.ConfigHash || after.ConfigVersion != s.ConfigVersion {
				t.Fatal("capability-free reconciliation changed Keycloak's stable config hash")
			}
		})
	}
}
