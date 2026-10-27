// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestCheckDiskGrowthUpdateComponents(t *testing.T) {
	for _, name := range []string{"engine", "configServer", "router", "extra"} {
		for _, bound := range []string{"min", "max"} {
			t.Run(name+"/"+bound, func(t *testing.T) {
				old := domain.ComponentSpec{Replicas: 1, Resources: domain.Resources{MinDisk: 5, MaxDisk: 10}}
				next := old
				next.Replicas = 2
				if bound == "min" {
					next.Resources.MinDisk++
				} else {
					next.Resources.MaxDisk++
				}
				spec := func(c domain.ComponentSpec) domain.ClusterSpec {
					s := domain.ClusterSpec{}
					switch name {
					case "engine":
						s.Components.Engine = c
					case "configServer":
						s.Components.ConfigServer = c
					case "router":
						s.Components.Router = c
					case "extra":
						s.Components.Extra = map[string]domain.ComponentSpec{"storage": c}
					}
					return s
				}
				if err := CheckDiskGrowthUpdate(spec(old), spec(next), "limited", infra.Capabilities{}); !errors.Is(err, ErrNotSupported) {
					t.Fatalf("growth accepted: %v", err)
				}
				if err := CheckDiskGrowthUpdate(spec(old), spec(next), "supported", infra.Capabilities{DiskGrowth: true}); err != nil {
					t.Fatal(err)
				}
				old.Replicas = 0
				if err := CheckDiskGrowthUpdate(spec(old), spec(next), "limited", infra.Capabilities{}); err != nil {
					t.Fatal("new component rejected", err)
				}
				old = next
				next.Replicas++
				if err := CheckDiskGrowthUpdate(spec(old), spec(next), "limited", infra.Capabilities{}); err != nil {
					t.Fatal("scale without growth rejected", err)
				}
			})
		}
	}
}
