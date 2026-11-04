// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestChooseNodeSpreadAvoid(t *testing.T) {
	nodes := []node{{ID: "id-a", Name: "host-a", Status: "ready", SchedulingEligibility: "eligible"}, {ID: "id-b", Name: "host-b", Status: "ready", SchedulingEligibility: "eligible"}, {ID: "id-c", Status: "ready", SchedulingEligibility: "eligible"}}
	for _, tc := range []struct {
		name          string
		avoid         []string
		want          string
		unsatisfiable bool
	}{
		{"none", nil, "id-c", false},
		{"hostname", []string{"host-a", "id-c"}, "id-b", false},
		{"fallback-id", []string{"host-a", "host-b"}, "id-c", false},
		{"allocation-id-with-named-node", []string{"id-a", "id-c"}, "id-b", false},
		{"all", []string{"host-a", "host-b", "id-c"}, "", true},
		{"unknown", []string{"unknown"}, "id-c", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chooseNode(nodes, nil, "g", "", tc.avoid...)
			if got != tc.want || errors.Is(err, infra.ErrPlacementUnsatisfiable) != tc.unsatisfiable || (!tc.unsatisfiable && err != nil) {
				t.Fatalf("node=%q err=%v", got, err)
			}
		})
	}
}

func TestUnsatisfiableVolumeCreatesNothing(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	nodes, err := b.nodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	spec := volumeSpec("move-volume")
	for _, n := range nodes {
		group := n.Name
		if group == "" {
			group = n.ID
		}
		spec.SpreadAvoid = append(spec.SpreadAvoid, group)
	}
	if _, err = b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("earlier-volume"), spec}); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatal(err)
	}
	if service, err := b.GetService(t.Context(), spec.Hostname); err != nil || service != nil {
		t.Fatalf("allocated service: %+v %v", service, err)
	}
	if service, err := b.GetService(t.Context(), "earlier-volume"); err != nil || service != nil {
		t.Fatalf("partially allocated batch: %+v %v", service, err)
	}
}
