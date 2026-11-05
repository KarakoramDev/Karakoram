// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	provider "github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
)

// Exercise the actual decoder for every advertised version, not a duplicated
// list of expected releases. Create and upgrade deliver the same member payload.
func TestCatalogVersionsPassSpecValidation(t *testing.T) {
	s := coreTestSpec(t)
	for _, version := range provider.New(provider.Options{}).Describe().Versions {
		t.Run(version.Version, func(t *testing.T) {
			s.Version = version.Version
			payload, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			for _, revision := range []int64{1, 2} {
				got, err := decodeSpec(agentapi.MemberConfig{Engine: "neo4j", ConfigVersion: revision, EngineSpec: payload})
				if err != nil || got.Version != version.Version {
					t.Fatalf("revision %d, version %s: %v", revision, version.Version, err)
				}
			}
		})
	}
}

func TestUnknownVersionsFailSpecValidation(t *testing.T) {
	s := coreTestSpec(t)
	for _, version := range []string{"", "garbage", "2026.10.2", "2026.11.0", "2027.01.0", "2026.10.1\n", "../2026.10.1", "2026.10.1;id"} {
		t.Run(version, func(t *testing.T) {
			s.Version = version
			payload, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeSpec(agentapi.MemberConfig{Engine: "neo4j", EngineSpec: payload}); err == nil {
				t.Fatal("accepted an unlocked version")
			}
		})
	}
}

func TestPatchVersionCreateAndUpgrade(t *testing.T) {
	p := provider.New(provider.Options{})
	current := &domain.Cluster{Name: "graph"}
	p.ApplyDefaults(&current.Spec)
	if current.Spec.Version != "2026.10.1" {
		t.Fatal("default version changed; update this regression")
	}
	if err := p.Validate(t.Context(), nil, current); err != nil {
		t.Fatalf("default create: %v", err)
	}
	old := *current
	old.Spec.Version = "2026.10.0"
	if err := p.Validate(t.Context(), &old, current); err != nil {
		t.Fatalf("patch upgrade: %v", err)
	}
	current.Spec.Version = "2026.10.2"
	if err := p.Validate(t.Context(), nil, current); err == nil {
		t.Fatal("control plane accepted unknown create version")
	}
	if err := p.Validate(t.Context(), &old, current); err == nil {
		t.Fatal("control plane accepted unknown upgrade version")
	}
}
