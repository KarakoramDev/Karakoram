// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package fake

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestSpreadAvoid(t *testing.T) {
	for _, tc := range []struct {
		name          string
		placement     bool
		groups, avoid []string
		want          string
		unsatisfiable bool
	}{
		{"avoid", true, []string{"a", "b"}, []string{"a"}, "b", false},
		{"unsatisfiable", true, []string{"a"}, []string{"a"}, "", true},
		{"unsupported", false, []string{"a"}, []string{"a"}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := New(Options{Capabilities: infra.Capabilities{Placement: tc.placement, Logs: true}, PlacementGroups: tc.groups})
			specs := []infra.ServiceSpec{{Hostname: "volume", Kind: infra.KindVolume, SpreadAvoid: tc.avoid}, {Hostname: "runtime", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: "volume"}}
			op, err := b.ImportServices(t.Context(), specs)
			if errors.Is(err, infra.ErrPlacementUnsatisfiable) != tc.unsatisfiable || (!tc.unsatisfiable && err != nil) {
				t.Fatal(err)
			}
			if tc.unsatisfiable {
				services, err := b.ListServices(t.Context(), "")
				if err != nil || len(services) != 0 {
					t.Fatalf("partial allocation: %+v %v", services, err)
				}
				return
			}
			if _, err = b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if _, err = b.Start(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 || containers[0].PlacementGroup != tc.want {
				t.Fatalf("placement: %+v %v", containers, err)
			}
		})
	}
}
