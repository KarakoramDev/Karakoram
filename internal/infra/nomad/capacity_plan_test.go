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

func TestVolumeCapacityPlanAccountsForHiddenAllocations(t *testing.T) {
	for _, allBlocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "next fitting node", true: "no fitting node"}[allBlocked], func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			plans := 0
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				if !strings.HasPrefix(r.URL.Path, "/v1/job/kk-placement-") || !strings.HasSuffix(r.URL.Path, "/plan") {
					return false, nil
				}
				plans++
				body := fakeDecode(w, r)
				job := fakeMap(body["Job"])
				groups := fakeRows(job["TaskGroups"])
				tasks := fakeRows(groups[0]["Tasks"])
				resources := fakeMap(tasks[0]["Resources"])
				if fakeInt(resources, "CPU") != 100 || fakeInt(resources, "MemoryMB") != 1024 || fakeInt(groups[0], "Count") != 1 {
					t.Errorf("wrong plan resources: %#v", resources)
				}
				constraints := fakeRows(groups[0]["Constraints"])
				pinned := fakeString(constraints[0], "RTarget")
				if fakeString(constraints[0], "LTarget") != "${node.unique.id}" || fakeString(job, "Namespace") != "default" {
					t.Error("plan not node/namespace scoped")
				}
				if allBlocked || pinned != "node-b" {
					fakeJSON(w, 200, fakeRow{"FailedTGAllocs": fakeRow{"member": fakeRow{"DimensionExhausted": fakeRow{"memory": 1}}}})
				} else {
					fakeJSON(w, 200, fakeRow{"FailedTGAllocs": nil})
				}
				return true, nil
			}
			v := volumeSpec("volume")
			r := runtimeSpec("runtime", "volume")
			r.Resources = infra.Resources{MinCPU: 0.1, MaxCPU: 0.1, MinRAM: 1, MaxRAM: 1}
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{v, r})
			if allBlocked {
				if !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
					t.Fatalf("want capacity rejection, got %v", err)
				}
				if len(f.volumes) != 0 || len(f.variables) != 0 {
					t.Error("rejected plan created resources")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				rec, err := b.require(t.Context(), "volume")
				if err != nil || rec.Node != "node-b" {
					t.Fatalf("node not selected by scheduler: %+v %v", rec, err)
				}
			}
			if plans < 2 {
				t.Fatalf("plans=%d; expected deterministic candidate retry", plans)
			}
			if len(f.jobs) != 0 {
				t.Error("planning registered a job")
			}
		})
	}
}
