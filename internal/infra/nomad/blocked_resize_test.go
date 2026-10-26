// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestCorrectedStatelessResizeSupersedesBlockedVersion(t *testing.T) {
	for _, scenario := range []string{"blocked", "placed_pending_health", "terminal_deployment"} {
		t.Run(scenario, func(t *testing.T) {
			progressing := scenario == "placed_pending_health"
			wantChanged := scenario == "blocked"
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Resources.MinRAM, spec.Resources.MaxRAM = 1, 1
			importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			f.stallAllocs(true)
			if !progressing {
				f.setNodeEligible("node-a", false)
				f.setNodeEligible("node-b", false)
				f.setNodeEligible("node-c", false)
			}
			oversized := spec.Resources
			oversized.MinRAM, oversized.MaxRAM = 2, 2
			op, err := b.SetResources(t.Context(), "runtime", oversized)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			r, err := b.load(t.Context(), "runtime")
			if err != nil || r.Pending.JobVersion != 1 {
				t.Fatalf("resize version: %+v %v", r, err)
			}
			if scenario == "terminal_deployment" {
				f.mu.Lock()
				f.jobs[fakeKey("default", "runtime")].deployment["Status"] = "failed"
				f.mu.Unlock()
			}
			b = fakeBackend(t, f, nil)
			applied := oversized
			changed, err := engine.ResizeUnplacedDeployment(t.Context(), &engine.Context{Cluster: &domain.Cluster{}, Backend: b}, "runtime", spec.Resources, &applied, &op)
			if err != nil || changed != wantChanged {
				t.Fatalf("correction changed=%v, want %v: %v", changed, wantChanged, err)
			}
			if scenario == "terminal_deployment" {
				return
			}
			if progressing {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || state.Done || state.Failed {
					t.Fatalf("progressing deployment lost health fence: %+v %v", state, err)
				}
				return
			}
			if _, err := b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			version := fakeInt(job.row, "Version")
			task := fakeRows(fakeRows(job.row["TaskGroups"])[0]["Tasks"])[0]
			memory := fakeInt(fakeMap(task["Resources"]), "MemoryMB")
			f.mu.Unlock()
			if version != 2 || memory != 1024 {
				t.Fatalf("corrected job version=%d memory=%d, want 2/1024", version, memory)
			}
			assertFakeServing(t, f, 1)
			f.setNodeEligible("node-a", true)
			f.stallAllocs(false)
			f.setAutoTick(true)
			awaitFake(t, b, op)
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service.Status != infra.StatusRunning || service.Resources.MaxRAM != 1 {
				t.Fatalf("corrected runtime did not converge: %+v %v", service, err)
			}
		})
	}
}

func TestCorrectedResizeAdoptsAcceptedSuccessor(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Resources.MinRAM, spec.Resources.MaxRAM = 1, 1
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.setNodeEligible("node-a", false)
	f.setNodeEligible("node-b", false)
	f.setNodeEligible("node-c", false)
	oversized := spec.Resources
	oversized.MinRAM, oversized.MaxRAM = 4, 4
	old, err := b.SetResources(t.Context(), "runtime", oversized)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.OpStatus(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	corrected := spec.Resources
	corrected.MinRAM, corrected.MaxRAM = 2, 2
	accepted, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", old, corrected)
	if err != nil || !changed {
		t.Fatalf("first correction: changed=%v err=%v", changed, err)
	}
	// Lose the provider checkpoint/response after durable backend acceptance.
	// Its pending target is still 4 GiB; its last successful target is 1 GiB.
	b = fakeBackend(t, f, nil)
	rc := &engine.Context{Cluster: &domain.Cluster{}, Backend: b}
	applied, recovered := oversized, old
	changed, err = engine.ResizeUnplacedDeployment(t.Context(), rc, "runtime", corrected, &applied, &recovered)
	if err != nil || !changed || recovered.ID != accepted.ID || applied.MaxRAM != 2 {
		t.Fatalf("accepted successor lost after restart: changed=%v recovered=%+v accepted=%+v applied=%+v err=%v", changed, recovered, accepted, applied, err)
	}
	f.setNodeEligible("node-a", true)
	f.setAutoTick(true)
	awaitFake(t, b, recovered)
	if registrations := jobRegistrations(f); registrations != 3 {
		t.Fatalf("corrected rollout registered %d jobs, want initial + blocked + corrected", registrations)
	}
	// A second lost checkpoint can recover even after the successor completes.
	b = fakeBackend(t, f, nil)
	rc.Backend = b
	applied, recovered = oversized, old
	changed, err = engine.ResizeUnplacedDeployment(t.Context(), rc, "runtime", corrected, &applied, &recovered)
	if err != nil || !changed {
		t.Fatalf("completed successor not recovered: changed=%v err=%v", changed, err)
	}
	state, err := b.OpStatus(t.Context(), recovered)
	if err != nil || !state.Done || state.Failed || jobRegistrations(f) != 3 {
		t.Fatalf("completed correction replayed: %+v err=%v registrations=%d", state, err, jobRegistrations(f))
	}
	other := corrected
	other.MinRAM, other.MaxRAM = 3, 3
	if _, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", old, other); err != nil || changed {
		t.Fatalf("adopted successor for a different correction: changed=%v err=%v", changed, err)
	}
	if _, err := b.SetResources(t.Context(), "runtime", corrected); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", old, corrected); err != nil || changed {
		t.Fatalf("adopted unrelated same-resource successor: changed=%v err=%v", changed, err)
	}
}
