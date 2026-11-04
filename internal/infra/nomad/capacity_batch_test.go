// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestVolumeCapacityPreservesDistinctNodes(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("old-volume"))
	predecessorMemory := 1024
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.URL.Path == "/v1/node/node-a/allocations" {
			fakeJSON(w, 200, []fakeRow{{"ClientStatus": "running", "DesiredStatus": "stop",
				"AllocatedResources": fakeRow{"Tasks": fakeRow{"predecessor": fakeRow{
					"Cpu": fakeRow{"CpuShares": 100}, "Memory": fakeRow{"MemoryMB": predecessorMemory}}}}}})
			return true, nil
		}
		if strings.HasPrefix(r.URL.Path, "/v1/node/") && !strings.HasSuffix(r.URL.Path, "/allocations") {
			id := strings.TrimPrefix(r.URL.Path, "/v1/node/")
			row := fakeNodeRow(f.nodes[id])
			memory := 744
			if id == "node-a" {
				memory = 2280
			}
			row["NodeResources"] = fakeRow{"Cpu": fakeRow{"CpuShares": 4000}, "Memory": fakeRow{"MemoryMB": memory}}
			fakeJSON(w, 200, row)
			return true, nil
		}
		return false, nil
	}
	r := runtimeSpec("runtime", "volume")
	r.Resources = infra.Resources{MinRAM: 1, MaxRAM: 1}
	_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume"), r})
	if !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatalf("must not colocate when distinct eligible nodes exist: %v", err)
	}
	if len(f.volumes) != 1 {
		t.Fatal("created storage for unsatisfiable distinct placement")
	}
	v := volumeSpec("volume")
	v.SpreadReplaces = "old-volume"
	predecessorMemory = 1536
	if _, err = b.ImportServices(t.Context(), []infra.ServiceSpec{v, r}); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatalf("replacement must retain predecessor's allocation demand: %v", err)
	}
	predecessorMemory = 1024
	if _, err = b.ImportServices(t.Context(), []infra.ServiceSpec{v, r}); err != nil {
		t.Fatal(err)
	}
	rec, err := b.require(t.Context(), "volume")
	if err != nil || rec.Node != "node-a" {
		t.Fatalf("replacement with overlap headroom: %+v %v", rec, err)
	}
}

func TestVolumeCapacityAccountsForWholeBatch(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	delete(f.nodes, "node-b")
	delete(f.nodes, "node-c")
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.URL.Path == "/v1/node/node-a" {
			row := fakeNodeRow(f.nodes["node-a"])
			row["NodeResources"] = fakeRow{"Cpu": fakeRow{"CpuShares": 4000}, "Memory": fakeRow{"MemoryMB": 1536}}
			fakeJSON(w, 200, row)
			return true, nil
		}
		return false, nil
	}
	var specs []infra.ServiceSpec
	for _, name := range []string{"first", "second"} {
		v := volumeSpec(name + "-volume")
		v.SpreadGroup = name
		r := runtimeSpec(name, v.Hostname)
		r.Resources = infra.Resources{MinRAM: 1, MaxRAM: 1}
		specs = append(specs, v, r)
	}
	_, err := b.ImportServices(t.Context(), specs)
	if !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatalf("must reject aggregate capacity demand: %v", err)
	}
	if len(f.volumes) != 0 || len(f.variables) != 0 {
		t.Fatal("infeasible batch created resources")
	}
}

func TestVolumeCapacityPlansWholeBatchWithHiddenAllocations(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	delete(f.nodes, "node-b")
	delete(f.nodes, "node-c")
	sawCombined := false
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if !strings.HasPrefix(r.URL.Path, "/v1/job/kk-placement-") || !strings.HasSuffix(r.URL.Path, "/plan") {
			return false, nil
		}
		body := fakeDecode(w, r)
		groups := fakeRows(fakeMap(body["Job"])["TaskGroups"])
		if len(groups) == 2 {
			sawCombined = true
			for _, group := range groups {
				constraints := fakeRows(group["Constraints"])
				if fakeString(constraints[0], "RTarget") != "node-a" {
					t.Error("batch plan changed earlier placement")
				}
			}
			fakeJSON(w, 200, fakeRow{"FailedTGAllocs": fakeRow{"member-1": fakeRow{"DimensionExhausted": fakeRow{"memory": 1}}}})
		} else {
			fakeJSON(w, 200, fakeRow{"FailedTGAllocs": nil})
		}
		return true, nil
	}
	var specs []infra.ServiceSpec
	for _, name := range []string{"first", "second"} {
		v := volumeSpec(name + "-volume")
		v.SpreadGroup = name
		r := runtimeSpec(name, v.Hostname)
		r.Resources = infra.Resources{MinRAM: 1, MaxRAM: 1}
		specs = append(specs, v, r)
	}
	_, err := b.ImportServices(t.Context(), specs)
	if !errors.Is(err, infra.ErrPlacementUnsatisfiable) || !sawCombined {
		t.Fatalf("must plan cumulative demand: %v; combined=%v", err, sawCombined)
	}
	if len(f.volumes) != 0 || len(f.variables) != 0 {
		t.Fatal("infeasible batch created resources")
	}
}
