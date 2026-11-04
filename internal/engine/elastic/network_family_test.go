// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"bytes"
	"encoding/json"
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
			p, rc, f, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			rc.Backend = privateFamilyBackend{rc.Backend, family}
			for _, m := range s.Members {
				spec, err := p.desiredSpec(t.Context(), rc, s, m, rc.Cluster.Spec.Version)
				if err != nil {
					t.Fatal(err)
				}
				if spec.IPv4 != (family == infra.AddressFamilyIPv4) {
					t.Fatal("wrong component listener policy", m.Name, spec.IPv4)
				}
				pull := *rc
				pull.AgentConfigCapabilities = []string{"unrelated-request-capability"}
				pulled, err := p.desiredSpec(t.Context(), &pull, s, m, rc.Cluster.Spec.Version)
				if err != nil {
					t.Fatal(err)
				}
				plainJSON, err := json.Marshal(spec)
				if err != nil {
					t.Fatal(err)
				}
				pulledJSON, err := json.Marshal(pulled)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(plainJSON, pulledJSON) || spec.RestartToken != pulled.RestartToken {
					t.Fatal("Elastic token/static hash inputs depend on pull capabilities", m.Name)
				}
			}
		})
	}
}
