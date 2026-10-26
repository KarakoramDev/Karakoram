// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStatelessChangesRetainServingPredecessors(t *testing.T) {
	for _, replicas := range []int{1, 2} {
		for _, action := range []string{"deploy", "resources", "scale-up", "ports-on", "ports-off"} {
			t.Run(fmt.Sprintf("%d/%s", replicas, action), func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				spec := runtimeSpec("runtime", "")
				spec.MinContainers, spec.MaxContainers = replicas, replicas
				// Count reductions retain healthy allocations and are covered
				// separately; they must not wait for a new placement.
				importFake(t, b, spec)
				if action == "ports-off" {
					if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
						t.Fatal(err)
					}
				}
				deployFake(t, b, "runtime", "")
				before := len(f.requests())
				f.setAutoTick(false)
				f.stallAllocs(true)
				var op infra.Op
				var err error
				switch action {
				case "deploy":
					d := testDeployment(t, "")
					d.Runtime.Prepare = []string{"printf 'new preparation'"}
					op, err = b.Deploy(t.Context(), "runtime", d)
				case "resources":
					op, err = b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 0.5, MaxRAM: 1})
				case "scale-up":
					op, err = b.SetContainerCount(t.Context(), "runtime", replicas+1, replicas+1)
				case "ports-on":
					err = b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"})
				case "ports-off":
					err = b.DeletePublicPorts(t.Context(), "runtime")
				}
				if err != nil {
					t.Fatal(err)
				}
				r, err := b.require(t.Context(), "runtime")
				if err != nil || r.Pending == nil || r.Pending.Phase != "registering" {
					t.Fatalf("stateless change entered shutdown: %+v %v", r, err)
				}
				if op.ID == "" {
					op = encodeOp(*r.Pending)
				}
				// Reconstruct from Variables before registration, as after CP loss.
				b = fakeBackend(t, f, nil)
				for range 3 {
					state, err := b.OpStatus(t.Context(), roundTripOp(t, op))
					if err != nil || state.Done || state.Failed || state.Status != "starting" {
						t.Fatalf("unready replacement did not wait: %+v %v", state, err)
					}
					assertFakeServing(t, f, spec.MaxContainers)
				}
				f.stallAllocs(false)
				f.setAutoTick(true)
				awaitFake(t, b, op)
				assertFakeServing(t, f, r.Max)
				for _, request := range f.requests()[before:] {
					if request.Method == http.MethodDelete && request.Path == "/v1/job/runtime" || strings.HasSuffix(request.Path, "/restart") {
						t.Fatalf("stateless update used destructive lifecycle endpoint: %+v", request)
					}
				}
			})
		}
	}
}

func assertFakeServing(t *testing.T, f *fakeNomad, want int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	serving := 0
	for _, a := range f.allocs {
		if a.row["JobID"] == "runtime" && a.row["ClientStatus"] == "running" && a.row["DesiredStatus"] == "run" {
			serving++
		}
	}
	if serving != want || f.jobs[fakeKey("default", "runtime")].row["Stop"] == true {
		t.Fatalf("serving=%d, want %d; job must remain running", serving, want)
	}
}

func TestStatelessOperationWaitsForDeploymentHealth(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	f.setAutoTick(false)
	f.mu.Lock()
	for _, a := range f.allocs {
		f.setAllocStatus(a, "running")
	}
	job := f.jobs[fakeKey("default", "runtime")]
	job.deployment["Status"] = "running"
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed || !strings.Contains(state.Message, "deployment health") {
		t.Fatalf("task running bypassed deployment health: %+v %v", state, err)
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusCreating {
		t.Fatalf("running task bypassed deployment health in service status: %+v %v", service, err)
	}
	f.mu.Lock()
	job.deployment["Status"] = "successful"
	for _, evaluation := range f.evals {
		evaluation["Status"] = "complete"
	}
	f.mu.Unlock()
	awaitFake(t, b, op)
}

func TestStatelessCountUpdateAcceptsRetainedAllocationVersions(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.SetContainerCount(t.Context(), "runtime", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	f.setAutoTick(false)
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	// Simulate Nomad retaining the previous deployment and reporting older
	// allocation versions for every live slot of this count-only update.
	job.deployment["JobVersion"] = fakeInt(job.row, "Version") - 1
	job.deployment["Status"] = "successful"
	for _, a := range f.allocs {
		if !fakeTerminal(a) && a.row["DesiredStatus"] == "run" {
			f.setAllocStatus(a, "running")
			a.row["JobVersion"] = fakeInt(job.row, "Version") - 1
		}
	}
	for _, evaluation := range f.evals {
		evaluation["Status"] = "complete"
	}
	f.mu.Unlock()
	awaitFake(t, b, op)
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusRunning {
		t.Fatalf("completed count update rejected retained allocations: %+v %v", service, err)
	}
}

func TestStatelessFailedCanaryPreservesServingPredecessor(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.failNextAlloc("runtime")
	op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 1, MaxRAM: 2})
	if err != nil {
		t.Fatal(err)
	}
	state := failedFake(t, b, op)
	if !strings.Contains(state.Message, "deployment failed") || !strings.Contains(state.Message, "readiness") {
		t.Fatalf("missing actionable deployment failure: %+v", state)
	}
	assertFakeServing(t, f, 1)
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusFailed {
		t.Fatalf("failed canary inherited serving predecessor readiness: %+v %v", service, err)
	}
}

func TestStatelessAutoRevertIsNotReportedAsSuccess(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 1, MaxRAM: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	f.setAutoTick(false)
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	job.row["Version"] = fakeInt(job.row, "Version") + 1
	job.row["Meta"].(fakeRow)["karakoram.operation"] = "prior-stable-operation"
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed || !strings.Contains(state.Message, "automatically reverted") {
		t.Fatalf("reverted rollout was not actionable failure: %+v %v", state, err)
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusFailed {
		t.Fatalf("reverted job acknowledged requested configuration readiness: %+v %v", service, err)
	}
}

func TestStatelessBlockedPlacementPreservesService(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 1, MaxRAM: 2})
	if err != nil {
		t.Fatal(err)
	}
	var state infra.OpState
	for range 3 {
		state, err = b.OpStatus(t.Context(), op)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("blocked rollout terminated: %+v %v", state, err)
		}
		assertFakeServing(t, f, 1)
	}
	if !strings.Contains(state.Message, "placement blocked") || !strings.Contains(state.Message, "host-port") {
		t.Fatalf("missing actionable canary capacity status: %+v", state)
	}
	f.setNodeEligible("node-a", true)
	awaitFake(t, b, op)
}

func TestStatelessLateRecoveryAfterEvaluationDisappears(t *testing.T) {
	for _, deployment := range []string{"successful", "running", "failed", "absent", "superseded"} {
		t.Run(deployment, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			r.Pending.Phase = "failed"
			r.Pending.Message = "startup window expired"
			op := encodeOp(*r.Pending)
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			f.setAutoTick(false)
			f.mu.Lock()
			// A failed operation may recover after its original evaluation
			// has been garbage-collected by Nomad. The exact native
			// deployment's success, not old running tasks alone, proves it.
			delete(f.evals, r.Pending.EvalID)
			job := f.jobs[fakeKey("default", "runtime")]
			switch deployment {
			case "absent":
				job.deployment = nil
			case "superseded":
				job.deployment["JobVersion"] = fakeInt(job.row, "Version") + 1
				job.deployment["Status"] = "successful"
			default:
				job.deployment["Status"] = deployment
			}
			f.mu.Unlock()
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			want := infra.StatusFailed
			if deployment == "successful" {
				want = infra.StatusRunning
			}
			if service.Status != want {
				t.Fatalf("deployment=%s: got %s, want %s", deployment, service.Status, want)
			}
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || state.Done {
				t.Fatalf("late observation rewrote failed history: %+v %v", state, err)
			}
		})
	}
}

func TestStatelessScaleBackSupersedesBlockedCanaryWithoutPlacement(t *testing.T) {
	for _, tc := range []struct {
		desired  int
		retained bool
	}{{1, false}, {2, false}, {2, true}} {
		t.Run(fmt.Sprintf("%d/retained=%t", tc.desired, tc.retained), func(t *testing.T) {
			desired := tc.desired
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.MinContainers, spec.MaxContainers = 2, 2
			importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			retainedIDs := retainedAllocationIDs(t, f)
			for _, id := range []string{"node-a", "node-b", "node-c"} {
				f.setNodeEligible(id, false)
			}
			up, err := b.SetContainerCount(t.Context(), "runtime", 3, 3)
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				state, err := b.OpStatus(t.Context(), up)
				if err != nil || state.Done || state.Failed {
					t.Fatalf("scale-up must remain blocked: %+v %v", state, err)
				}
			}
			assertFakeServing(t, f, 2)
			// Native canonicalization injects constraints that sparse renders
			// do not declare. Both the current and serving jobs contain them.
			canonicalize := func() {
				constraints := []any{
					fakeRow{"LTarget": "${attr.os.signals}", "Operand": "set_contains", "RTarget": "SIGTERM"},
					fakeRow{"LTarget": "${attr.nomad.service_discovery}", "Operand": "=", "RTarget": "true"},
					fakeRow{"LTarget": "${attr.nomad.version}", "Operand": "semver", "RTarget": ">= 1.4.0"},
				}
				for _, job := range f.jobs {
					for _, group := range fakeRows(job.row["TaskGroups"]) {
						group["Constraints"] = constraints
					}
				}
				for _, a := range f.allocs {
					for _, group := range fakeRows(fakeMap(a.row["Job"])["TaskGroups"]) {
						group["Constraints"] = constraints
					}
				}
			}
			f.mu.Lock()
			allocationsBefore := len(f.allocs)
			canonicalize()
			f.mu.Unlock()
			down, err := b.SetContainerCount(t.Context(), "runtime", desired, desired)
			if err != nil {
				t.Fatal(err)
			}
			if tc.retained {
				// Reproduce a pre-upgrade scale-back registration that already
				// has its own blocked start-first deployment and persisted handle.
				f.mu.Lock()
				for _, a := range f.allocs {
					a.row["DeploymentStatus"] = fakeRow{"Healthy": false}
				}
				f.mu.Unlock()
				state, err := b.OpStatus(t.Context(), down)
				if err != nil || state.Done || state.Failed {
					t.Fatalf("legacy scale-back did not retain its running operation: %+v %v", state, err)
				}
				f.mu.Lock()
				for _, a := range f.allocs {
					a.row["DeploymentStatus"] = fakeRow{"Healthy": true}
				}
				// A canary may have placed and then been stopped while its
				// deployment remains running. Terminal unhealthy history must
				// not prevent retaining the healthy serving allocations.
				job := f.jobs[fakeKey("default", "runtime")]
				for _, a := range f.allocs {
					if a.row["ClientStatus"] != "running" {
						continue
					}
					row := fakeCopy(a.row)
					id := f.nextID()
					row["ID"], row["JobVersion"], row["Job"] = id, job.row["Version"], fakeCopy(job.row)
					row["DesiredStatus"], row["ClientStatus"] = "stop", "complete"
					row["DeploymentStatus"] = fakeRow{"Healthy": false, "Canary": true}
					f.allocs[id] = &fakeAlloc{row: row}
					job.deployment["TaskGroups"] = fakeRow{"member": fakeRow{"UnhealthyAllocs": 1, "PlacedCanaries": []any{id}}}
					allocationsBefore++
					break
				}
				canonicalize()
				f.mu.Unlock()
			}
			b = fakeBackend(t, f, nil)
			awaitFake(t, b, down)
			assertFakeServing(t, f, desired)
			f.mu.Lock()
			allocationsAfter := len(f.allocs)
			group := fakeRows(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"])[0]
			update := fakeMap(group["Update"])
			canary := fakeInt(update, "Canary")
			autoPromote, maxParallel := update["AutoPromote"], fakeInt(update, "MaxParallel")
			f.mu.Unlock()
			// Nomad rejects AutoPromote without a canary; other settings stay.
			if allocationsAfter != allocationsBefore || canary != 0 || autoPromote != false || maxParallel != 1 {
				t.Fatalf("scale-back required new placement: before=%d after=%d canary=%d autoPromote=%v maxParallel=%d", allocationsBefore, allocationsAfter, canary, autoPromote, maxParallel)
			}
			for id := range retainedAllocationIDs(t, f) {
				if !retainedIDs[id] {
					t.Fatalf("scale-back replaced a retained allocation: %s", id)
				}
			}
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service.Status != infra.StatusRunning {
				t.Fatalf("scale-back did not release readiness: %+v %v", service, err)
			}
		})
	}
}

func TestStatelessScaleBackKeepsCanaryForUnreadySpecification(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 2, 2
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	deployment := testDeployment(t, "")
	deployment.Runtime.Prepare = []string{"printf 'changed configuration'"}
	update, err := b.Deploy(t.Context(), "runtime", deployment)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.OpStatus(t.Context(), update); err != nil {
		t.Fatal(err)
	}
	scale, err := b.SetContainerCount(t.Context(), "runtime", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		state, err := b.OpStatus(t.Context(), scale)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("unready specification bypassed canary: %+v %v", state, err)
		}
	}
	f.mu.Lock()
	group := fakeRows(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"])[0]
	canary := fakeInt(fakeMap(group["Update"]), "Canary")
	f.mu.Unlock()
	if canary != 1 {
		t.Fatalf("specification change lost start-first safety: canary=%d", canary)
	}
	assertFakeServing(t, f, 2)
}

func TestStatelessScaleBackKeepsCanaryForRemovedPlacementConstraint(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 2, 2
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	constraints := []any{fakeRow{"LTarget": "${meta.tier}", "Operand": "=", "RTarget": "application"}}
	for _, group := range fakeRows(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"]) {
		group["Constraints"] = constraints
	}
	for _, a := range f.allocs {
		for _, group := range fakeRows(fakeMap(a.row["Job"])["TaskGroups"]) {
			group["Constraints"] = constraints
		}
	}
	f.mu.Unlock()
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.SetContainerCount(t.Context(), "runtime", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("removed placement constraint bypassed specification rollout: %+v %v", state, err)
		}
	}
	f.mu.Lock()
	group := fakeRows(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"])[0]
	canary := fakeInt(fakeMap(group["Update"]), "Canary")
	remaining := len(fakeRows(group["Constraints"]))
	f.mu.Unlock()
	if canary != 1 || remaining != 0 {
		t.Fatalf("constraint removal retained stale placement: canary=%d constraints=%d", canary, remaining)
	}
	assertFakeServing(t, f, 2)
}

func TestFailedScaleRecoveryReappliesRevertedOwnedSpecification(t *testing.T) {
	f, b, failed := failedStatelessScaleFixture(t, true)
	retainedIDs := retainedAllocationIDs(t, f)
	state, err := b.OpStatus(t.Context(), failed)
	if err != nil || !state.Failed {
		t.Fatalf("historical operation is not terminal: %+v %v", state, err)
	}
	f.mu.Lock()
	before := len(f.allocs)
	f.mu.Unlock()
	op, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering || op.ID == failed.ID {
		t.Fatalf("owned reverted specification did not get a new operation: %+v %t %v", op, recovering, err)
	}
	awaitFake(t, fakeBackend(t, f, nil), roundTripOp(t, op))
	assertRetainedAllocations(t, f, retainedIDs, 2)
	f.mu.Lock()
	after := len(f.allocs)
	group := fakeRows(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"])[0]
	canary := fakeInt(fakeMap(group["Update"]), "Canary")
	f.mu.Unlock()
	if before != after || canary != 0 {
		t.Fatalf("failed-scale recovery required placement: before=%d after=%d canary=%d", before, after, canary)
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusRunning {
		t.Fatalf("recovery did not release readiness: %+v %v", service, err)
	}
}

func TestFailedScaleRecoveryRejectsUnplaceableOrForeignJobs(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint(foreign), func(t *testing.T) {
			f, b, failed := failedStatelessScaleFixture(t, foreign)
			if foreign {
				f.mu.Lock()
				fakeMap(f.jobs[fakeKey("default", "runtime")].row["Meta"])["karakoram.id"] = "foreign-runtime"
				f.mu.Unlock()
			}
			before := len(f.requests())
			desired := 3
			if foreign {
				desired = 2
			}
			for range 3 {
				_, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", desired, desired)
				if recovering || (foreign && err == nil) || (!foreign && err != nil) {
					t.Fatalf("unsafe recovery was started: recovering=%t err=%v", recovering, err)
				}
			}
			for _, request := range f.requests()[before:] {
				if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
					t.Fatalf("failed proof registered a job: %+v", request)
				}
			}
			state, err := b.OpStatus(t.Context(), failed)
			if err != nil || !state.Failed {
				t.Fatalf("failed proof revived historical operation: %+v %v", state, err)
			}
		})
	}
}

func TestFailedScaleRecoveryDoesNotFallBackToPlacementWhenHealthChanges(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	op, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering {
		t.Fatalf("recovery was not admitted: %t %v", recovering, err)
	}
	f.mu.Lock()
	for _, allocation := range f.allocs {
		if allocation.row["ClientStatus"] == "running" {
			f.setAllocStatus(allocation, "failed")
			break
		}
	}
	f.mu.Unlock()
	before := len(f.requests())
	state := failedFake(t, fakeBackend(t, f, nil), op)
	if !strings.Contains(state.Message, "count-only recovery") {
		t.Fatalf("lost proof was not actionable: %+v", state)
	}
	_, recovering, err = b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || recovering {
		t.Fatalf("failed automatic recovery was retried: %t %v", recovering, err)
	}
	for _, request := range f.requests()[before:] {
		if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
			t.Fatalf("lost retention proof registered a replacement: %+v", request)
		}
	}
}

func failedStatelessScaleFixture(t *testing.T, reverted bool) (*fakeNomad, *Backend, infra.Op) {
	t.Helper()
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 2, 2
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	stable := fakeCopy(f.jobs[fakeKey("default", "runtime")].row)
	f.mu.Unlock()
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.SetContainerCount(t.Context(), "runtime", 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := b.OpStatus(t.Context(), op); err != nil {
			t.Fatal(err)
		}
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	r.Pending.Phase, r.Pending.Message = "failed", "startup window expired"
	if reverted {
		r.Min, r.Max = 2, 2
		f.mu.Lock()
		job := f.jobs[fakeKey("default", "runtime")]
		stable["Version"] = fakeInt(job.row, "Version") + 1
		stable["JobModifyIndex"] = f.nextIndex()
		job.row = stable
		job.previous = nil
		job.deployment = fakeRow{"JobVersion": stable["Version"], "Status": "successful"}
		for _, allocation := range f.allocs {
			if allocation.row["ClientStatus"] == "running" {
				allocation.row["JobVersion"] = stable["Version"]
			}
		}
		f.mu.Unlock()
	}
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return f, b, op
}

func TestFailedScaleRecoveryAdoptsLostReceiptWithoutPlacementFallback(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	admitted, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering {
		t.Fatalf("recovery admission failed: %t %v", recovering, err)
	}
	// The admission was durable but its caller lost the response/engine receipt.
	b = fakeBackend(t, f, nil)
	adopted, err := b.SetContainerCount(t.Context(), "runtime", 2, 2)
	if err != nil || adopted.ID != admitted.ID {
		t.Fatalf("ordinary same-target scale replaced retain-only intent: %+v %v", adopted, err)
	}
	retried, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering || retried.ID != admitted.ID {
		t.Fatalf("recovery caller could not adopt its durable receipt: %+v %t %v", retried, recovering, err)
	}
	f.mu.Lock()
	for _, allocation := range f.allocs {
		if allocation.row["ClientStatus"] == "running" {
			f.setAllocStatus(allocation, "failed")
			break
		}
	}
	f.mu.Unlock()
	before := len(f.requests())
	state := failedFake(t, fakeBackend(t, f, nil), adopted)
	if !strings.Contains(state.Message, "count-only recovery") {
		t.Fatalf("lost health proof did not remain terminal: %+v", state)
	}
	for _, request := range f.requests()[before:] {
		if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
			t.Fatalf("lost receipt weakened into a placement rollout: %+v", request)
		}
	}
}

func TestFailedScaleRecoveryOwnershipLossIsTerminalBeforeRegistration(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	op, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering {
		t.Fatalf("recovery admission failed: %t %v", recovering, err)
	}
	f.mu.Lock()
	fakeMap(f.jobs[fakeKey("default", "runtime")].row["Meta"])["karakoram.id"] = "foreign-runtime"
	f.mu.Unlock()
	before := len(f.requests())
	state := failedFake(t, fakeBackend(t, f, nil), op)
	if !strings.Contains(state.Message, "ownership") {
		t.Fatalf("ownership loss did not produce terminal proof failure: %+v", state)
	}
	for _, request := range f.requests()[before:] {
		if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
			t.Fatalf("foreign runtime was mutated: %+v", request)
		}
	}
}

func TestFailedScaleRecoveryAdoptsLostCompletionBeforeHealthChanges(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	op, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering {
		t.Fatalf("recovery admission failed: %t %v", recovering, err)
	}
	awaitFake(t, b, op)
	completed, err := b.require(t.Context(), "runtime")
	if err != nil || completed.Pending == nil {
		t.Fatalf("durable completion disappeared: %+v %v", completed, err)
	}
	completedHandle := encodeOp(*completed.Pending)
	f.mu.Lock()
	for _, allocation := range f.allocs {
		if allocation.row["ClientStatus"] == "running" {
			f.setAllocStatus(allocation, "failed")
			break
		}
	}
	f.mu.Unlock()
	before := len(f.requests())
	b = fakeBackend(t, f, nil)
	adopted, err := b.SetContainerCount(t.Context(), "runtime", 2, 2)
	if err != nil || adopted.ID != completedHandle.ID {
		t.Fatalf("lost completion started a new rollout: %+v %v", adopted, err)
	}
	retried, recovering, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !recovering || retried.ID != completedHandle.ID {
		t.Fatalf("lost completed recovery was not adoptable: %+v %t %v", retried, recovering, err)
	}
	state, err := b.OpStatus(t.Context(), adopted)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("historical completion was rewritten: %+v %v", state, err)
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status == infra.StatusRunning {
		t.Fatalf("lost completion bypassed current service readiness: %+v %v", service, err)
	}
	for _, request := range f.requests()[before:] {
		if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
			t.Fatalf("lost completion weakened into a placement rollout: %+v", request)
		}
	}
}
