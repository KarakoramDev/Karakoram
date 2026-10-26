// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func blockedDeployment(t *testing.T, volume bool) (*fakeNomad, *Backend, infra.Op) {
	t.Helper()
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	mount := ""
	if volume {
		mount = "volume"
		importFake(t, b, volumeSpec(mount), runtimeSpec("runtime", mount))
	} else {
		importFake(t, b, runtimeSpec("runtime", ""))
	}
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, mount))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil || state.Failed || state.Done {
			t.Fatalf("prepare blocked job: %+v %v", state, err)
		}
		if strings.Contains(state.Message, "placement blocked") {
			return f, b, op
		}
	}
	t.Fatal("job did not block")
	return nil, nil, infra.Op{}
}

func TestBlockedPlacementDoesNotExpireOrRedeploy(t *testing.T) {
	for _, volume := range []bool{false, true} {
		t.Run(map[bool]string{false: "stateless", true: "volume"}[volume], func(t *testing.T) {
			f, b, op := blockedDeployment(t, volume)
			f.mu.Lock()
			for _, e := range f.evals {
				if e["Status"] == "blocked" {
					e["FailedTGAllocs"] = fakeRow{"member": fakeRow{"NodesEvaluated": 3, "NodesExhausted": 1, "DimensionExhausted": fakeRow{"memory": 1}, "ConstraintFiltered": fakeRow{"${node.unique.id} = node-a": 2}}}
				}
			}
			f.mu.Unlock()
			r, err := b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			r.Pending.Started = time.Now().Add(-startupWindow - time.Minute)
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || state.Failed || state.Done || !strings.Contains(state.Message, "memory") || !strings.Contains(state.Message, "constraint") {
					t.Fatalf("blocked placement consumed startup deadline: %+v %v", state, err)
				}
			}
			failures, err := b.PlacementFailures(t.Context(), []string{"runtime"})
			if err != nil || len(failures) != 1 || !strings.Contains(failures[0].Message, "memory") {
				t.Fatalf("scheduler observation: %+v %v", failures, err)
			}
			f.setNodeEligible("node-a", true)
			awaitFake(t, b, op)
			failures, err = b.PlacementFailures(t.Context(), []string{"runtime"})
			if err != nil || len(failures) != 0 {
				t.Fatalf("historical failure was not cleared: %+v %v", failures, err)
			}
		})
	}
}

func TestDeleteSupersedesNeverPlacedDeployment(t *testing.T) {
	f, b, deploy := blockedDeployment(t, true)
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	del, err := b.DeleteServiceByID(t.Context(), "runtime", r.ID)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, del)
	state, err := b.OpStatus(t.Context(), deploy)
	if err != nil || !state.Expired {
		t.Fatalf("obsolete deployment remained active: %+v %v", state, err)
	}
	f.mu.Lock()
	_, exists := f.jobs[fakeKey("default", "runtime")]
	f.mu.Unlock()
	if exists {
		t.Fatal("never-placed backend job remains")
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service != nil {
		t.Fatalf("runtime record remains: %+v %v", service, err)
	}
}
