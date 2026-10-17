// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStopBeforeStartAcrossRuntimeUpdates(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	deployFake(t, b, "runtime", "volume")
	op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 1, MaxCPU: 2, MinRAM: 0.25, MaxRAM: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, roundTripOp(t, op))
	if err = b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil || r.Pending == nil {
		t.Fatalf("public port restart was not persisted: %+v %v", r, err)
	}
	awaitFake(t, b, encodeOp(*r.Pending))
	if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
		t.Fatalf("single-writer overlap across deploy/resources/public-port update: %v", overlaps)
	}
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 {
		t.Fatalf("updated runtime not running: %+v %v", containers, err)
	}
}

func TestFencedDeleteAndMountedVolume(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	if _, err := b.DeleteServiceByID(t.Context(), "runtime", "wrong-id"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("wrong identity deletion: %v", err)
	}
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("mounted volume deletion: %v", err)
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.ID != imported.Services["runtime"] {
		t.Fatalf("fenced delete changed runtime: %+v %v", s, err)
	}
	op, err := b.DeleteServiceByID(t.Context(), "runtime", imported.Services["runtime"])
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	op, err = b.DeleteServiceByID(t.Context(), "runtime", "wrong-id")
	if err != nil {
		t.Fatal("absent fenced deletion:", err)
	}
	awaitFake(t, b, op)
	op, err = b.DeleteServiceByID(t.Context(), "volume", imported.Services["volume"])
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
}

func TestRecordsSurvivePurgedJob(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	request, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, f.URL+"/v1/job/runtime?namespace=default&purge=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Nomad-Token", fakeNomadToken)
	response, err := f.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("external purge: %d", response.StatusCode)
	}
	// Reconstruct the backend as after control-plane restart: only Variables persist identity/specification.
	b = fakeBackend(t, f, nil)
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.ID != imported.Services["runtime"] {
		t.Fatalf("job GC lost durable record: %+v %v", s, err)
	}
	op, err := b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, roundTripOp(t, op))
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 {
		t.Fatalf("Start after job GC: %+v %v", containers, err)
	}
}

func TestOperationHandleRejectsInvalidInput(t *testing.T) {
	for _, id := range []string{"garbage", "nomad:v1:%%%", "nomad:v2:e30", "nomad:v1:e30", opPrefix + strings.Repeat("A", 65537)} {
		if _, err := decodeOp(infra.Op{ID: id}); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("invalid handle accepted (length %d): %v", len(id), err)
		}
	}
}

func failedFake(t *testing.T, b *Backend, op infra.Op) infra.OpState {
	t.Helper()
	for range 100 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
		if state.Failed {
			return state
		}
		if state.Done {
			t.Fatalf("failed operation reported success: %+v", state)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("operation never failed")
	return infra.OpState{}
}
func assertSafeMessage(t *testing.T, message, address string) {
	t.Helper()
	host := ""
	if parsed, err := url.Parse(address); err == nil {
		host = parsed.Host
	}
	if message == "" {
		t.Fatal("missing safe failure diagnostic")
	}
	for _, secret := range []string{fakeNomadToken, address, host} {
		if secret != "" && strings.Contains(message, secret) {
			t.Fatal("failure diagnostic exposed credentials or API address")
		}
	}
}
func TestFailedAllocationHasSafeMessage(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	f.failNextAlloc("runtime")
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	state := failedFake(t, b, op)
	assertSafeMessage(t, state.Message, f.URL)
	if !strings.Contains(strings.ToLower(state.Message), "alloc") {
		t.Fatalf("missing allocation diagnostic: %q", state.Message)
	}
	assertJobStillRegistered(t, b)
}
func TestStartupWindowExpiry(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	f.stallAllocs(true)
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("initial pending operation: %+v %v", state, err)
	}
	for range 20 {
		if state.Status == "starting" {
			break
		}
		state, err = b.OpStatus(t.Context(), op)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("allocation preparation did not stay pending: %+v %v", state, err)
		}
	}
	if state.Status != "starting" {
		t.Fatalf("job was not registered before expiry: %+v", state)
	}
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	r.Pending.Started = time.Now().Add(-startupWindow - time.Minute)
	if err = b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	state = failedFake(t, b, encodeOp(*r.Pending))
	if !strings.Contains(state.Message, "startup window") {
		t.Fatalf("expiry diagnostic: %q", state.Message)
	}
	assertSafeMessage(t, state.Message, f.URL)
	assertJobStillRegistered(t, b)
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s.Status != infra.StatusFailed {
		t.Fatalf("expired operation did not report failed service: %+v %v", s, err)
	}
	f.stallAllocs(false)
	for range 10 {
		s, err = b.GetService(t.Context(), "runtime")
		if err != nil {
			t.Fatal(err)
		}
		if s.Status == infra.StatusRunning {
			break
		}
	}
	if s.Status != infra.StatusRunning {
		t.Fatalf("late running allocation masked by failed operation: %+v", s)
	}
	state, err = b.OpStatus(t.Context(), encodeOp(*r.Pending))
	if err != nil || !state.Failed {
		t.Fatalf("late allocation changed historical operation failure: %+v %v", state, err)
	}
}

func TestStartupWindowBeginsAfterRegistration(t *testing.T) {
	for _, action := range []string{"deploy", "stop"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			var err error
			if action == "stop" {
				_, err = b.Stop(t.Context(), "runtime")
			} else {
				_, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			r.Pending.Issued = time.Now().Add(-startupWindow - time.Minute)
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			op := encodeOp(*r.Pending)
			f.mu.Lock()
			for _, a := range f.allocs {
				a.row["ClientStatus"] = "unknown"
			}
			f.mu.Unlock()
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || state.Failed || state.Done || state.Status != "stopping" || !strings.Contains(state.Message, "previous allocations") {
				t.Fatalf("slow shutdown expired startup window: %+v %v", state, err)
			}
			f.mu.Lock()
			for _, a := range f.allocs {
				a.row["ClientStatus"] = "complete"
			}
			f.mu.Unlock()
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || state.Failed {
				t.Fatalf("shutdown completion failed: %+v %v", state, err)
			}
			if action == "stop" {
				if !state.Done {
					t.Fatalf("stop did not finish: %+v", state)
				}
				return
			}
			if state.Status != "registering" {
				t.Fatalf("replacement did not reach registration: %+v", state)
			}
			before := time.Now().UTC()
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || state.Failed || state.Status != "starting" {
				t.Fatalf("old operation expired during registration: %+v %v", state, err)
			}
			r, err = b.require(t.Context(), "runtime")
			if err != nil || r.Pending.Started.Before(before) {
				t.Fatalf("startup clock did not start at registration: %+v %v", r, err)
			}
			b = fakeBackend(t, f, nil)
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || state.Failed || state.Done {
				t.Fatalf("fresh backend did not retain startup window: %+v %v", state, err)
			}
			f.setAutoTick(true)
			awaitFake(t, b, op)
		})
	}
}
func TestEvaluationPlacementFailureMessage(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	var state infra.OpState
	for range 20 {
		state, err = b.OpStatus(t.Context(), op)
		if err != nil || state.Failed || state.Done {
			t.Fatalf("placement failure terminated operation: %+v %v", state, err)
		}
		if strings.Contains(state.Message, "placement blocked") {
			break
		}
	}
	if !strings.Contains(state.Message, "FailedTGAllocs") {
		t.Fatalf("missing pending placement diagnostic: %+v", state)
	}
	assertSafeMessage(t, state.Message, f.URL)
	assertJobStillRegistered(t, b)
	f.setNodeEligible("node-a", true)
	awaitFake(t, b, op)
}

func TestDedicatedCPUImportAndUpdate(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	if !b.Capabilities().DedicatedCPU || !newFactory(testEnv(nil)).Info().Capabilities.DedicatedCPU {
		t.Fatal("backend and factory must advertise dedicated CPU")
	}
	for _, tc := range []struct {
		name      string
		resources infra.Resources
	}{
		{"dedicated-max", infra.Resources{CPUMode: infra.CPUDedicated, MinCPU: 1, MaxCPU: 2}},
		{"dedicated-min", infra.Resources{CPUMode: infra.CPUDedicated, MinCPU: 1}},
	} {
		spec := runtimeSpec(tc.name, "")
		spec.Resources = tc.resources
		importFake(t, b, spec)
		s, err := b.GetService(t.Context(), tc.name)
		if err != nil || s == nil {
			t.Fatalf("dedicated import %s: %+v %v", tc.name, s, err)
		}
		if s.Resources.CPUMode != infra.CPUDedicated || s.Resources.MinCPU != tc.resources.MinCPU || s.Resources.MaxCPU != tc.resources.MaxCPU {
			t.Fatalf("dedicated import lost CPU bounds: %+v", s.Resources)
		}
	}
	spec := runtimeSpec("shared-runtime", "")
	spec.Resources = infra.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 2}
	importFake(t, b, spec)
	// A mode-only update must validate merged bounds, not reject its own zero
	// fields before reusing the service's existing whole-core reservation.
	for _, change := range []infra.Resources{
		{CPUMode: infra.CPUDedicated},
		{MaxCPU: 3},
	} {
		op, err := b.SetResources(t.Context(), "shared-runtime", change)
		if err != nil {
			t.Fatal("dedicated resource update:", err)
		}
		awaitFake(t, b, op)
	}
	s, err := b.GetService(t.Context(), "shared-runtime")
	if err != nil || s == nil {
		t.Fatalf("updated dedicated runtime: %+v %v", s, err)
	}
	if s.Resources.CPUMode != infra.CPUDedicated || s.Resources.MinCPU != 1 || s.Resources.MaxCPU != 3 {
		t.Fatalf("dedicated resource update lost merged bounds: %+v", s.Resources)
	}
}

func TestDedicatedCPURejectsZeroAndFractionalCores(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resources infra.Resources
	}{
		{"zero", infra.Resources{CPUMode: infra.CPUDedicated}},
		{"subcore", infra.Resources{CPUMode: infra.CPUDedicated, MaxCPU: 0.5}},
		{"fractional-max", infra.Resources{CPUMode: infra.CPUDedicated, MaxCPU: 1.5}},
		{"fractional-min", infra.Resources{CPUMode: infra.CPUDedicated, MinCPU: 1.5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Resources = tc.resources
			if _, err := b.ImportServices(t.Context(), []infra.ServiceSpec{spec}); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("invalid dedicated import accepted: %v", err)
			}
			// Import an unbounded shared runtime so that zero fields in an
			// update cannot inherit a valid core count accidentally.
			importFake(t, b, runtimeSpec("runtime", ""))
			if _, err := b.SetResources(t.Context(), "runtime", tc.resources); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("invalid dedicated resource update accepted: %v", err)
			}
			s, err := b.GetService(t.Context(), "runtime")
			if err != nil || s == nil {
				t.Fatalf("runtime after rejected update: %+v %v", s, err)
			}
			if s.Resources.CPUMode == infra.CPUDedicated || s.Resources.MinCPU != 0 || s.Resources.MaxCPU != 0 {
				t.Fatalf("rejected update changed persisted CPU bounds: %+v", s.Resources)
			}
		})
	}
}

func assertJobStillRegistered(t *testing.T, b *Backend) {
	t.Helper()
	var job struct{ Stop bool }
	if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil || job.Stop {
		t.Fatalf("operation failure canceled desired running job: %+v %v", job, err)
	}
}

func TestKeepVolumesReattachRuntime(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("old-runtime", "volume"))
	deployFake(t, b, "old-runtime", "volume")
	volume, err := b.require(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	op, err := b.DeleteService(t.Context(), "old-runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if s, err := b.GetService(t.Context(), "old-runtime"); err != nil || s != nil {
		t.Fatalf("runtime was not deleted: %+v %v", s, err)
	}
	// Reconstruct the backend too: reattachment depends only on persisted state.
	b = fakeBackend(t, f, nil)
	retained, err := b.require(t.Context(), "volume")
	if err != nil || retained.ID != volume.ID || retained.VolumeName != volume.VolumeName || retained.Node != volume.Node {
		t.Fatalf("keepVolumes lost volume identity or placement: %+v %v", retained, err)
	}
	importFake(t, b, runtimeSpec("new-runtime", "volume"))
	deployFake(t, b, "new-runtime", "volume")
	var registered struct {
		TaskGroups []struct {
			Volumes map[string]struct{ Source string }
		}
	}
	if _, err := b.c.get(t.Context(), "/v1/job/new-runtime", nil, &registered); err != nil {
		t.Fatal(err)
	}
	if len(registered.TaskGroups) != 1 || registered.TaskGroups[0].Volumes["data"].Source != volume.VolumeName {
		t.Fatalf("new runtime did not mount retained physical volume: %+v", registered)
	}
	containers, err := b.Containers(t.Context(), "new-runtime")
	if err != nil || len(containers) != 1 || containers[0].PlacementGroup != volume.Node {
		t.Fatalf("reattached runtime placement: %+v %v", containers, err)
	}
	if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
		t.Fatalf("reattach overlapped writers: %v", overlaps)
	}
}

func TestOperationResumesMidPhase(t *testing.T) {
	for _, phase := range []string{"stopping", "registering", "registered-before-save"} {
		t.Run(phase, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
			if err != nil {
				t.Fatal(err)
			}
			if phase != "stopping" {
				for range 20 {
					state, err := b.OpStatus(t.Context(), op)
					if err != nil {
						t.Fatal(err)
					}
					if state.Status == "registering" {
						break
					}
				}
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil || r.Pending == nil {
				t.Fatalf("missing persisted operation: %+v %v", r, err)
			}
			wantPhase := phase
			if phase == "registered-before-save" {
				wantPhase = "registering"
				job, err := b.renderJob(t.Context(), r)
				if err != nil {
					t.Fatal(err)
				}
				job["EnforceIndex"], job["JobModifyIndex"] = true, uint64(0)
				if err := b.c.post(t.Context(), "/v1/jobs", nil, job, nil); err != nil {
					t.Fatal(err)
				}
			}
			if r.Pending.Phase != wantPhase {
				t.Fatalf("persisted phase %q; want %q", r.Pending.Phase, wantPhase)
			}
			b = fakeBackend(t, f, nil)
			awaitFake(t, b, roundTripOp(t, op))
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 {
				t.Fatalf("resumed runtime: %+v %v", containers, err)
			}
			if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
				t.Fatalf("resume overlapped writers: %v", overlaps)
			}
		})
	}
}

func TestPublicPortsReturnAfterInitiationAndResumeOnObservation(t *testing.T) {
	for _, observation := range []string{"get", "list", "containers", "old-operation", "import-operation"} {
		t.Run(observation, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""), runtimeSpec("unrelated", ""))
			old := deployFake(t, b, "runtime", "")
			f.stallAllocs(true)
			for _, remove := range []bool{false, true} {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				var err error
				if remove {
					err = b.DeletePublicPorts(ctx, "runtime")
				} else {
					err = b.EnsurePublicPort(ctx, "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"})
				}
				cancel()
				if err != nil {
					t.Fatalf("port initiation waited for startup: %v", err)
				}
				r, err := b.require(t.Context(), "runtime")
				if err != nil || r.Pending == nil || r.Pending.Phase != "stopping" || r.Pending.Action != "ports" {
					t.Fatalf("port change not durably initiated: %+v %v", r, err)
				}
				if err := b.SetEnvVars(t.Context(), "unrelated", map[string]string{"PLAIN": "value"}); err != nil {
					t.Fatal("port restart blocked unrelated mutation:", err)
				}
				b = fakeBackend(t, f, nil)
				f.stallAllocs(false)
				for range 30 {
					switch observation {
					case "get":
						_, err = b.GetService(t.Context(), "runtime")
					case "list":
						_, err = b.ListServices(t.Context(), "")
					case "containers":
						_, err = b.Containers(t.Context(), "runtime")
					case "old-operation":
						_, err = b.OpStatus(t.Context(), old)
					case "import-operation":
						_, err = b.OpStatus(t.Context(), imported)
					}
					if err != nil {
						t.Fatal(err)
					}
					r, err = b.require(t.Context(), "runtime")
					if err != nil {
						t.Fatal(err)
					}
					if r.Pending.Phase == "done" {
						break
					}
				}
				if r.Pending.Phase != "done" {
					t.Fatalf("observation did not resume ports: %+v", r.Pending)
				}
				f.stallAllocs(true)
			}
		})
	}
}

func TestPendingPublicPortsSupersededByLifecycle(t *testing.T) {
	for _, action := range []string{"deploy", "start", "stop"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
				t.Fatal(err)
			}
			var op infra.Op
			var err error
			switch action {
			case "deploy":
				op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
			case "start":
				op, err = b.Start(t.Context(), "runtime")
			case "stop":
				op, err = b.Stop(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			r, err := b.require(t.Context(), "runtime")
			if err != nil || r.Pending.Action != action || len(r.PublicPorts) != 1 {
				t.Fatalf("lifecycle lost pending ports: %+v %v", r, err)
			}
			if action == "stop" {
				op, err = b.Start(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
			}
			var registered struct {
				TaskGroups []struct {
					Networks []struct{ ReservedPorts []struct{ Value, To int } }
				}
			}
			if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &registered); err != nil {
				t.Fatal(err)
			}
			if len(registered.TaskGroups) != 1 || len(registered.TaskGroups[0].Networks) != 1 {
				t.Fatalf("missing rendered network: %+v", registered)
			}
			ports := registered.TaskGroups[0].Networks[0].ReservedPorts
			if len(ports) != 1 || ports[0].Value != 18080 || ports[0].To != 8080 {
				t.Fatalf("superseding lifecycle lost rendered ports: %+v", ports)
			}
			if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
				t.Fatalf("superseding port change overlapped writers: %v", overlaps)
			}
		})
	}
}

func TestPublicPortTransientFailureIsNotRejected(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "ensure", true: "delete"}[remove], func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			if remove {
				if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
					t.Fatal(err)
				}
			}
			transport := b.c.http.Transport
			b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost && r.URL.Path == "/v1/jobs" {
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
				}
				return transport.RoundTrip(r)
			})
			var err error
			if remove {
				err = b.DeletePublicPorts(t.Context(), "runtime")
			} else {
				err = b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"})
			}
			if !errors.Is(err, infra.ErrUnavailable) || errors.Is(err, infra.ErrRejected) {
				t.Fatalf("transient port failure = %v", err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil || r.Pending == nil || r.Pending.Action != "ports" || (len(r.PublicPorts) == 0) != remove {
				t.Fatalf("transient failure lost persisted port intent: %+v %v", r, err)
			}
			b.c.http.Transport = transport
			b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
			awaitFake(t, b, encodeOp(*r.Pending))
		})
	}
}

func TestStartAndRestartRecoverInactiveJobs(t *testing.T) {
	for _, action := range []string{"start", "restart"} {
		for _, failure := range []string{"failed", "lost", "absent"} {
			t.Run(action+"/"+failure, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
				if failure == "failed" {
					f.failNextAlloc("runtime")
					op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
					if err != nil {
						t.Fatal(err)
					}
					failedFake(t, b, op)
				} else {
					deployFake(t, b, "runtime", "volume")
				}
				volume, err := b.require(t.Context(), "volume")
				if err != nil {
					t.Fatal(err)
				}
				switch failure {
				case "lost":
					f.setNodeDown(volume.Node, true)
				case "absent":
					if err := b.c.delete(t.Context(), "/v1/job/runtime", query("purge", "true"), nil); err != nil {
						t.Fatal(err)
					}
				}
				var op infra.Op
				if action == "start" {
					op, err = b.Start(t.Context(), "runtime")
				} else {
					op, err = b.Restart(t.Context(), "runtime")
				}
				if err != nil || op.ID == "" || op.Kind != action {
					t.Fatalf("inactive recovery did not return %s operation: %+v %v", action, op, err)
				}
				b = fakeBackend(t, f, nil)
				if failure == "lost" {
					f.setNodeDown(volume.Node, false)
				}
				awaitFake(t, b, roundTripOp(t, op))
				containers, err := b.Containers(t.Context(), "runtime")
				if err != nil || len(containers) != 1 || containers[0].PlacementGroup != volume.Node {
					t.Fatalf("recovery lost volume placement: %+v %v", containers, err)
				}
				if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
					t.Fatalf("recovery overlapped writers: %v", overlaps)
				}
			})
		}
	}
}

func TestRestartRunningAllocationDoesNotReplaceJob(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	original, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	before := len(f.requests())
	op, err := b.Restart(t.Context(), "runtime")
	if err != nil || op.ID != "" {
		t.Fatalf("running restart: %+v %v", op, err)
	}
	restarts := 0
	for _, call := range f.requests()[before:] {
		if call.Method == http.MethodPost && strings.HasSuffix(call.Path, "/restart") {
			restarts++
		} else if call.Method != http.MethodGet && !(call.Method == http.MethodPut && call.Path == "/v1/var/"+recordPrefix+"runtime") {
			t.Fatalf("running restart replaced job: %+v", call)
		}
	}
	if restarts != 1 {
		t.Fatalf("allocation restart calls = %d", restarts)
	}
	current, err := b.require(t.Context(), "runtime")
	if err != nil || current.RestartLease != nil || !current.Pending.Issued.Equal(original.Pending.Issued) ||
		current.Pending.Action != original.Pending.Action || current.Pending.Phase != original.Pending.Phase {
		t.Fatalf("allocation restart changed pending operation or retained lease: %+v %v", current, err)
	}
}

func TestPortsWaitForVolumeNodeRecovery(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	volume, err := b.require(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.setNodeDown(volume.Node, true)
	if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil || r.Pending == nil {
		t.Fatalf("missing ports operation: %+v %v", r, err)
	}
	op := encodeOp(*r.Pending)
	var state infra.OpState
	for range 20 {
		state, err = b.OpStatus(t.Context(), op)
		if err != nil || state.Failed || state.Done {
			t.Fatalf("node outage terminated ports operation: %+v %v", state, err)
		}
		if strings.Contains(state.Message, "placement blocked") {
			break
		}
	}
	if !strings.Contains(state.Message, "placement blocked") {
		t.Fatalf("missing blocked placement diagnostic: %+v", state)
	}
	assertJobStillRegistered(t, b)
	f.setNodeDown(volume.Node, false)
	awaitFake(t, b, op)
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusRunning {
		t.Fatalf("recovered ports member: %+v %v", service, err)
	}
	if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
		t.Fatalf("ports recovery overlapped writers: %v", overlaps)
	}
}

func TestFailedPendingOperationIsSuperseded(t *testing.T) {
	for _, action := range []string{"start", "restart", "deploy", "ports", "delete-ports"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}
			if action == "ports" {
				if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
					t.Fatal(err)
				}
				r, err := b.require(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, encodeOp(*r.Pending))
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			r.Pending.Phase, r.Pending.Message = "failed", "startup window expired"
			if action == "delete-ports" {
				r.Pending.Action = "ports"
			}
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			oldIssued := r.Pending.Issued
			var op infra.Op
			switch action {
			case "start":
				op, err = b.Start(t.Context(), "runtime")
			case "restart":
				op, err = b.Restart(t.Context(), "runtime")
			case "deploy":
				op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			case "ports":
				err = b.EnsurePublicPort(t.Context(), "runtime", port)
			case "delete-ports":
				err = b.DeletePublicPorts(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err = b.require(t.Context(), "runtime")
			if err != nil || r.Pending.Phase != "stopping" || r.Pending.Issued.Equal(oldIssued) {
				t.Fatalf("failed operation was not superseded: %+v %v", r, err)
			}
			if op.ID == "" {
				op = encodeOp(*r.Pending)
			}
			awaitFake(t, b, op)
		})
	}
}

func TestFailedOperationDoesNotTrustObsoleteVersionAllocations(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	r.Pending.Phase = "failed"
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	// Model an old version still running while the current version has not
	// acquired a running allocation. It must not mask the current failure.
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	currentVersion := fakeInt(job.row, "Version") + 1
	job.row["Version"] = currentVersion
	f.mu.Unlock()
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s.Status != infra.StatusFailed {
		t.Fatalf("obsolete running allocation masked failure: %+v %v", s, err)
	}
	f.mu.Lock()
	for _, a := range f.allocs {
		if a.row["JobID"] == "runtime" && a.row["ClientStatus"] == "running" {
			a.row["JobVersion"] = currentVersion
		}
	}
	f.mu.Unlock()
	s, err = b.GetService(t.Context(), "runtime")
	if err != nil || s.Status != infra.StatusRunning {
		t.Fatalf("current running allocation masked by failure: %+v %v", s, err)
	}
}

func TestUnchangedPortsDoNotRestartAfterUnrelatedFailure(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "private", true: "public"}[published], func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}
			if published {
				r.PublicPorts = []infra.PublicPort{port}
			}
			r.Pending.Phase = "failed"
			issued := r.Pending.Issued
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if published {
				err = b.EnsurePublicPort(t.Context(), "runtime", port)
			} else {
				err = b.DeletePublicPorts(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err = b.require(t.Context(), "runtime")
			if err != nil || r.Pending.Phase != "failed" || !r.Pending.Issued.Equal(issued) {
				t.Fatalf("idempotent port call restarted an unrelated operation: %+v %v", r, err)
			}
		})
	}
}
