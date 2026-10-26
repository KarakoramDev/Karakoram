// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStatelessScaleReversalRetainsRevokedPortsAcrossReconstruction(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 2, 2
	importFake(t, b, spec)
	if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.stallAllocs(true)
	original, err := b.SetContainerCount(t.Context(), "runtime", 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := b.OpStatus(t.Context(), original); err != nil || state.Done || state.Failed {
		t.Fatalf("scale did not wait for replacement: %+v %v", state, err)
	}
	if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	b = fakeBackend(t, f, nil)
	replacement, err := b.SetContainerCount(t.Context(), "runtime", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := b.OpStatus(t.Context(), original); err != nil || !state.Expired {
		t.Fatalf("old handle survived supersession: %+v %v", state, err)
	}
	if _, err := b.OpStatus(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	if r.Max != 2 || len(r.PublicPorts) != 0 {
		t.Fatalf("supersession revived obsolete target: count=%d ports=%+v", r.Max, r.PublicPorts)
	}
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	group := fakeRows(job.row["TaskGroups"])[0]
	for _, network := range fakeRows(group["Networks"]) {
		if len(fakeRows(network["ReservedPorts"])) != 0 {
			t.Error("superseding jobspec revived public host-port reservations")
		}
	}
	f.mu.Unlock()
	f.stallAllocs(false)
	f.setAutoTick(true)
	awaitFake(t, fakeBackend(t, f, nil), replacement)
	assertFakeServing(t, f, 2)
}
