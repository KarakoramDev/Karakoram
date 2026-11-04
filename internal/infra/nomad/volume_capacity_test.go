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

func TestVolumeCapacityPlacement(t *testing.T) {
	for _, tc := range []struct {
		name           string
		free           map[string]int
		avoid          []string
		replaces       string
		want           string
		cpuFree        map[string]int
		reservedMemory int
		terminal       bool
		dedicated      bool
	}{
		{name: "memory tie", free: map[string]int{"node-a": 744, "node-b": 2280, "node-c": 744}, want: "node-b"},
		{name: "no capacity", free: map[string]int{"node-a": 744, "node-b": 744, "node-c": 744}},
		{name: "exclusion", free: map[string]int{"node-a": 744, "node-b": 2280, "node-c": 744}, avoid: []string{"node-b"}},
		{name: "replacement needs overlap", free: map[string]int{"node-a": 744, "node-b": 2280, "node-c": 744}, replaces: "old-volume", want: "node-b"},
		{name: "CPU tie", free: map[string]int{"node-a": 2280, "node-b": 2280, "node-c": 2280}, cpuFree: map[string]int{"node-a": 99, "node-b": 100, "node-c": 99}, want: "node-b"},
		{name: "reserved memory", free: map[string]int{"node-a": 1200, "node-b": 2280, "node-c": 1200}, reservedMemory: 200, want: "node-b"},
		{name: "terminal allocations release capacity", free: map[string]int{"node-a": 0, "node-b": 0, "node-c": 0}, terminal: true, want: "node-a"},
		{name: "dedicated core exhaustion", free: map[string]int{"node-a": 2280, "node-b": 2280, "node-c": 2280}, dedicated: true, want: "node-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				if strings.HasPrefix(r.URL.Path, "/v1/node/") {
					id := strings.TrimPrefix(r.URL.Path, "/v1/node/")
					if strings.HasSuffix(id, "/allocations") {
						id = strings.TrimSuffix(id, "/allocations")
						status := "running"
						if tc.terminal {
							status = "complete"
						}
						usedCPU := 100
						if tc.cpuFree != nil {
							usedCPU = 4000 - tc.cpuFree[id]
						}
						var reservedCores []int
						if tc.dedicated && id != "node-b" {
							reservedCores = []int{0, 1, 2, 3}
						}
						fakeJSON(w, 200, []fakeRow{{"Namespace": "other", "ClientStatus": status, "DesiredStatus": "stop", "AllocatedResources": fakeRow{"Tasks": fakeRow{"other": fakeRow{"Cpu": fakeRow{"CpuShares": usedCPU, "ReservedCores": reservedCores}, "Memory": fakeRow{"MemoryMB": 4096 - tc.free[id]}}}}}})
					} else {
						row := fakeNodeRow(f.nodes[id])
						row["NodeResources"] = fakeRow{"Cpu": fakeRow{"CpuShares": 4000, "ReservableCpuCores": []int{0, 1, 2, 3}}, "Memory": fakeRow{"MemoryMB": 4096}}
						row["ReservedResources"] = fakeRow{"Memory": fakeRow{"MemoryMB": tc.reservedMemory}}
						fakeJSON(w, 200, row)
					}
					return true, nil
				}
				return false, nil
			}
			v := volumeSpec("volume")
			v.SpreadAvoid = tc.avoid
			v.SpreadReplaces = tc.replaces
			r := runtimeSpec("runtime", "volume")
			r.Resources = infra.Resources{MinCPU: 0.1, MaxCPU: 0.1, MinRAM: 1, MaxRAM: 1}
			if tc.dedicated {
				r.Resources.CPUMode = infra.CPUDedicated
				r.Resources.MinCPU = 1
				r.Resources.MaxCPU = 1
			}
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{v, r})
			if tc.want == "" {
				if !errors.Is(err, infra.ErrPlacementUnsatisfiable) || !strings.Contains(err.Error(), "capacity") {
					t.Fatalf("want capacity rejection, got %v", err)
				}
				if len(f.volumes) != 0 || len(f.variables) != 0 {
					t.Fatal("capacity rejection created resources")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				rec, err := b.require(t.Context(), "volume")
				if err != nil || rec.Node != tc.want {
					t.Fatalf("placement=%+v err=%v; want %s", rec, err, tc.want)
				}
			}
		})
	}
}
