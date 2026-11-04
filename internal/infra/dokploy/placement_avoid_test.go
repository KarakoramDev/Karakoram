// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package dokploy

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestChooseNodeSpreadAvoid(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	nodes, err := b.nodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		avoid         []string
		want          string
		unsatisfiable bool
	}{
		{"none", nil, "node-a", false},
		{"hostname", []string{"node-a"}, "node-b", false},
		{"multiple", []string{"node-a", "node-b"}, "node-c", false},
		{"all", []string{"node-a", "node-b", "node-c"}, "", true},
		{"unknown", []string{"unknown"}, "node-a", false},
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
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := volumeSpec("move-volume")
	spec.SpreadAvoid = []string{"node-a", "node-b", "node-c"}
	if _, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("earlier-volume"), spec}); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatal(err)
	}
	if service, err := b.GetService(t.Context(), spec.Hostname); err != nil || service != nil {
		t.Fatalf("allocated service: %+v %v", service, err)
	}
	if service, err := b.GetService(t.Context(), "earlier-volume"); err != nil || service != nil {
		t.Fatalf("partially allocated batch: %+v %v", service, err)
	}
}
