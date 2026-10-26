// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestUnplacedResourcesRegisterReplacementJob(t *testing.T) {
	for _, volume := range []bool{false, true} {
		t.Run(map[bool]string{false: "proxy", true: "data"}[volume], func(t *testing.T) {
			f, b, op := blockedDeployment(t, volume)
			r, err := b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			applied := r.Resources
			desired := applied
			desired.MinCPU, desired.MaxCPU = .25, .25
			desired.MinRAM, desired.MaxRAM = .25, .25
			changed, err := engine.ResizeUnplacedDeployment(t.Context(), &engine.Context{Cluster: &domain.Cluster{}, Backend: b}, "runtime", desired, &applied, &op)
			if !changed || err != nil {
				t.Fatalf("changed=%v err=%v", changed, err)
			}
			for range 30 {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || state.Failed {
					t.Fatalf("replacement: %+v %v", state, err)
				}
				f.mu.Lock()
				job := f.jobs[fakeKey("default", "runtime")]
				registered := job != nil && job.row["Stop"] != true && fakeInt(job.row, "JobModifyIndex") > int(r.Pending.JobModifyIndex)
				memory := 0
				if registered {
					task := fakeRows(fakeRows(job.row["TaskGroups"])[0]["Tasks"])[0]
					memory = fakeInt(fakeMap(task["Resources"]), "MemoryMB")
				}
				f.mu.Unlock()
				if registered {
					if memory != 256 {
						t.Fatal("replacement retained old memory")
					}
					f.setNodeEligible("node-a", true)
					awaitFake(t, b, op)
					service, err := b.GetService(t.Context(), "runtime")
					if err != nil || service.Status != infra.StatusRunning {
						t.Fatalf("service=%+v err=%v", service, err)
					}
					return
				}
			}
			t.Fatal("corrected resources never registered")
		})
	}
}

func TestCorrectedBlockedResourcesRemainResizeable(t *testing.T) {
	_, b, _ := blockedDeployment(t, false)
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	applied := r.Resources
	desired := applied
	desired.MinRAM, desired.MaxRAM = .25, .25
	op, err := b.SetResources(t.Context(), "runtime", desired)
	if err != nil {
		t.Fatal(err)
	}
	for range 30 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil || state.Failed {
			t.Fatalf("replacement: %+v %v", state, err)
		}
		unplaced, err := b.RuntimeNeverPlaced(t.Context(), "runtime")
		if err != nil {
			t.Fatal(err)
		}
		if unplaced {
			applied = desired
			desired.MinRAM, desired.MaxRAM = .125, .125
			changed, err := engine.ResizeUnplacedDeployment(t.Context(), &engine.Context{Cluster: &domain.Cluster{}, Backend: b}, "runtime", desired, &applied, &op)
			if !changed || err != nil {
				t.Fatalf("second correction: changed=%v err=%v", changed, err)
			}
			return
		}
	}
	t.Fatal("corrected blocked registration lost never-placed proof")
}

func TestConditionalUnplacedResizeRejectsStaleReceipt(t *testing.T) {
	_, b, old := blockedDeployment(t, false)
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	newer, err := b.SetResources(t.Context(), "runtime", r.Resources)
	if err != nil {
		t.Fatal(err)
	}
	_, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", old, r.Resources)
	if err != nil || changed {
		t.Fatalf("stale receipt superseded successor: changed=%v err=%v newer=%+v", changed, err, newer)
	}
}

func TestConditionalVolumeResizeRejectsPlacementRace(t *testing.T) {
	f, b, op := blockedDeployment(t, true)
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	fired := false
	f.requestHook = func(_ http.ResponseWriter, req *http.Request) (bool, func()) {
		if req.Method == "POST" && req.URL.Path == "/v1/jobs" && !fired {
			fired = true
			// Placement wins after conditional admission but before its stop
			// fence. This member is still in initial bootstrap, never Ready.
			f.setNodeEligible("node-a", true)
		}
		return false, nil
	}
	f.mu.Unlock()
	desired := r.Resources
	desired.MinRAM, desired.MaxRAM = .25, .25
	_, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", op, desired)
	var exhausted *infra.DeploymentExhaustedError
	if changed || !errors.As(err, &exhausted) || !strings.Contains(exhausted.LastError, "UnplacedResizeDeferred") {
		t.Fatalf("placement race not rejected: changed=%v err=%v", changed, err)
	}
	current, loadErr := b.load(t.Context(), "runtime")
	if loadErr != nil || current.Resources.MaxRAM != r.Resources.MaxRAM || current.Pending.Phase != "failed" {
		t.Fatalf("failed correction lost previous bounds: current=%+v err=%v", current, loadErr)
	}
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	task := fakeRows(fakeRows(job.row["TaskGroups"])[0]["Tasks"])[0]
	memory := fakeInt(fakeMap(task["Resources"]), "MemoryMB")
	count := len(f.allocs)
	f.mu.Unlock()
	if !fired || count == 0 || memory == 256 {
		t.Fatalf("race fixture/replacement: fired=%v allocs=%d memory=%d", fired, count, memory)
	}
}

func TestUnplacedResizeLaterFailureReplaysRollbackAcrossRestart(t *testing.T) {
	f, b, original := blockedDeployment(t, true)
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	desired := r.Resources
	desired.MinRAM, desired.MaxRAM = .25, .25
	replacement, changed, err := b.ResizeUnplacedRuntime(t.Context(), "runtime", original, desired)
	if !changed || err != nil {
		t.Fatalf("admission: changed=%v err=%v", changed, err)
	}
	// Model delayed visibility of an allocation that raced the acknowledged
	// stop: the next stopping-phase poll must reject it durably.
	f.mu.Lock()
	f.allocs["late-bootstrap"] = &fakeAlloc{row: fakeRow{"ID": "late-bootstrap", "Namespace": "default", "JobID": "runtime", "ClientStatus": "complete", "DesiredStatus": "stop"}}
	f.mu.Unlock()
	_, err = b.OpStatus(t.Context(), replacement)
	var exhausted *infra.DeploymentExhaustedError
	if !errors.As(err, &exhausted) || exhausted.PreviousResources == nil || exhausted.PreviousResources.MaxRAM != r.Resources.MaxRAM {
		t.Fatalf("missing later rollback evidence: %v", err)
	}
	b = fakeBackend(t, f, nil)
	for _, receipt := range []infra.Op{original, replacement} {
		_, err := b.OpStatus(t.Context(), receipt)
		if !errors.As(err, &exhausted) || exhausted.PreviousResources == nil || exhausted.PreviousResources.MaxRAM != r.Resources.MaxRAM {
			t.Fatalf("restart/obsolete receipt lost terminal rollback: %v", err)
		}
	}
}

func TestR31BlockedResizeAfterRegistrationEvaluationCollected(t *testing.T) {
	for _, mode := range []string{"stateless", "volume", "relay", "foreign-child", "not-blocked", "different-registration"} {
		t.Run(mode, func(t *testing.T) {
			f, b, op := blockedDeployment(t, mode == "volume")
			r, err := b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "relay" {
				r.Deployment.Runtime.Args = []string{"relay"}
				if err := b.save(t.Context(), r); err != nil {
					t.Fatal(err)
				}
			}
			f.mu.Lock()
			delete(f.evals, r.Pending.EvalID)
			for _, e := range f.evals {
				if e["Status"] != "blocked" {
					continue
				}
				switch mode {
				case "foreign-child":
					e["PreviousEval"] = "unrelated-registration"
				case "not-blocked":
					e["Status"] = "complete"
				case "different-registration":
					e["JobModifyIndex"] = uint64(fakeInt(e, "JobModifyIndex") + 1)
				}
			}
			f.mu.Unlock()
			// Reconstruct the adapter around the pre-upgrade Variables record
			// and original receipt, just as CP-only r31 -> r32 cutover does.
			b = fakeBackend(t, f, nil)
			if never, err := b.RuntimeNeverPlaced(t.Context(), "runtime"); err != nil || never {
				t.Fatalf("collected history became destructive admission proof: never=%v err=%v", never, err)
			}
			desired := r.Resources
			desired.MinRAM, desired.MaxRAM = .25, .25
			applied := r.Resources
			changed, err := engine.ResizeUnplacedDeployment(t.Context(), &engine.Context{Cluster: &domain.Cluster{}, Backend: b}, "runtime", desired, &applied, &op)
			if err != nil || changed != (mode == "stateless") {
				t.Fatalf("mode=%s changed=%v err=%v", mode, changed, err)
			}
			if !changed {
				f.mu.Lock()
				job := f.jobs[fakeKey("default", "runtime")]
				stopped, version := job.row["Stop"] == true, fakeInt(job.row, "Version")
				f.mu.Unlock()
				if stopped || version != 0 {
					t.Fatal("rejected admission mutated the original job")
				}
				return
			}
			f.setNodeEligible("node-a", true)
			awaitFake(t, b, op)
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			task := fakeRows(fakeRows(job.row["TaskGroups"])[0]["Tasks"])[0]
			memory := fakeInt(fakeMap(task["Resources"]), "MemoryMB")
			version := fakeInt(job.row, "Version")
			f.mu.Unlock()
			if memory != 256 || version != 1 {
				t.Fatalf("upgrade resize never registered corrected job: memory=%d version=%d", memory, version)
			}
		})
	}
}
