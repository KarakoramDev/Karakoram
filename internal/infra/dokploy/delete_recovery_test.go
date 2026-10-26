// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type deleteFixture struct {
	f        *fakeAPI
	b        *Backend
	op       infra.Op
	ctx      context.Context
	id, name string
}

func stuckDelete(t *testing.T, stateful bool) deleteFixture {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	volume := ""
	if stateful {
		volume = "volume"
		importFake(t, b, volumeSpec(volume))
	}
	spec := runtimeSpec("runtime", volume)
	spec.Vars = map[string]string{"KARAKORAM_OWNER": "owner"}
	id := importFake(t, b, spec).Services["runtime"]
	deployFake(t, b, "runtime", volume)
	op, err := b.DeleteServiceByID(t.Context(), "runtime", id)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a := f.apps[id]
	a.holdTasks = true
	a.tasks[0]["state"] = "running"
	a.tasks[0]["currentState"] = "Preparing 1 second ago"
	name := fakeString(a.row, "appName")
	f.mu.Unlock()
	return deleteFixture{f: f, b: b, op: op, id: id, name: name, ctx: infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{FullDelete: true, Services: map[string]string{"runtime": id}})}
}

func (s deleteFixture) age(t *testing.T) {
	t.Helper()
	o, err := decodeOp(s.op)
	if err != nil {
		t.Fatal(err)
	}
	s.b.now = func() time.Time { return o.Issued.Add(deleteProgressWindow + time.Second) }
	s.f.mu.Lock()
	s.f.apps[s.id].tasks[0]["currentState"] = "Running 1 second ago"
	s.f.mu.Unlock()
}

func TestAgedDeleteEscalatesAndObservesAbsenceAcrossReconstruction(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "current", true: "persisted-legacy"}[legacy], func(t *testing.T) {
			s := stuckDelete(t, false)
			if legacy {
				o, _ := decodeOp(s.op)
				o.ServiceIdentity = ""
				s.op = encodeOp(o)
				s.f.mu.Lock()
				m, _ := parseMetadata(fakeString(s.f.apps[s.id].row, "description"))
				m.Pending = &o
				m.DeleteStopConfirmed = false
				s.f.apps[s.id].row["description"] = m.description()
				s.f.mu.Unlock()
			}
			before, err := s.b.OpStatus(s.ctx, s.op)
			if err != nil || before.Done || before.Message != "" || fakeCalls(s.f, "application.delete") != 0 {
				t.Fatalf("premature removal: %+v %v", before, err)
			}
			s.b = fakeBackend(t, s.f, nil)
			s.op = roundTripOp(t, s.op)
			s.age(t)
			state, err := s.b.OpStatus(s.ctx, s.op)
			for _, want := range []string{"3m1s", "desired replicas=0", "desired[running=1]", "current[running=1]", "Swarm inconsistency", "escalated removal"} {
				if !strings.Contains(state.Message, want) {
					t.Fatalf("missing %q: %+v %v", want, state, err)
				}
			}
			if err != nil || state.Done || fakeCalls(s.f, "application.delete") != 1 {
				t.Fatalf("escalation: %+v %v", state, err)
			}
			done, err := s.b.OpStatus(s.ctx, s.op)
			if err != nil || !done.Done || fakeCalls(s.f, "application.stop") != 1 || fakeCalls(s.f, "application.delete") != 1 {
				t.Fatalf("absence: %+v %v", done, err)
			}
		})
	}
}

func TestAgedDeleteRecoveryFailsClosed(t *testing.T) {
	for _, blocker := range []string{"foreign-label", "recreated-service", "failed-inspection", "null-inspection", "stale-inspection", "missing-replicas", "null-replicas", "active-deployment", "queued-deployment", "unknown-queue", "failed-queue", "pending-reload", "delivery-fence", "port-delivery", "suspend-stateful", "no-cleanup-intent", "wrong-cp-identity", "desired-one", "tasks-stopping", "unknown-task"} {
		t.Run(blocker, func(t *testing.T) {
			s := stuckDelete(t, blocker == "suspend-stateful")
			s.age(t)
			s.f.mu.Lock()
			a := s.f.apps[s.id]
			service := s.f.services[s.name]
			m, _ := parseMetadata(fakeString(a.row, "description"))
			switch blocker {
			case "foreign-label":
				service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"].(map[string]any)["karakoram.application"] = "foreign"
			case "recreated-service":
				service["ID"] = "replacement0000000000000000"
			case "failed-inspection":
				s.f.failure["docker.getConfig"] = fakeResponse{status: 500, message: "inspection unavailable"}
			case "null-inspection":
				s.f.uninspectable[s.name] = true
			case "stale-inspection":
				service["Spec"].(map[string]any)["Name"] = "different-name"
			case "missing-replicas":
				service["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{}}
			case "null-replicas":
				service["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{"Replicas": nil}}
			case "active-deployment":
				a.deployments[0].status = "running"
				a.holdDeployments = true
			case "queued-deployment":
				s.f.deploymentQueue = []map[string]any{{"state": "waiting", "data": map[string]any{"applicationId": s.id}}}
			case "unknown-queue":
				s.f.deploymentQueue = []map[string]any{{"state": "active", "data": map[string]any{}}}
			case "failed-queue":
				s.f.failure["deployment.queueList"] = fakeResponse{status: 403, message: "forbidden"}
			case "pending-reload":
				m.Pending.Action = "restart"
			case "delivery-fence":
				m.MutationUntil = time.Now().Add(time.Hour)
			case "port-delivery":
				m.PortChange = &portChange{ReloadPending: true}
			case "suspend-stateful":
				s.ctx = infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{Services: map[string]string{"runtime": s.id}})
			case "no-cleanup-intent":
				s.ctx = t.Context()
			case "wrong-cp-identity":
				s.ctx = infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{FullDelete: true, Services: map[string]string{"runtime": "replacement"}})
			case "desired-one":
				service["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{"Replicas": 1}}
			case "tasks-stopping":
				a.tasks[0]["state"] = "shutdown"
			case "unknown-task":
				a.tasks[0]["state"] = "credential-must-not-appear"
			}
			a.row["description"] = m.description()
			s.f.mu.Unlock()
			state, err := s.b.OpStatus(s.ctx, s.op)
			if err != nil || state.Done || fakeCalls(s.f, "application.delete") != 0 {
				t.Fatalf("unsafe escalation: %+v %v", state, err)
			}
			if blocker != "pending-reload" && state.Message == "" {
				t.Fatalf("missing diagnostic: %+v", state)
			}
			if blocker == "tasks-stopping" && !strings.Contains(state.Message, "tasks still stopping") {
				t.Fatalf("wrong diagnosis: %+v", state)
			}
			if strings.Contains(state.Message, "credential-must-not-appear") {
				t.Fatal("task text leaked")
			}
		})
	}
}

func TestEscalatedDeleteUncertainDeliveryIsNeverReplayed(t *testing.T) {
	s := stuckDelete(t, false)
	s.age(t)
	s.f.mu.Lock()
	s.f.failure["application.delete"] = fakeResponse{status: 500, message: "unknown outcome"}
	s.f.mu.Unlock()
	state, err := s.b.OpStatus(s.ctx, s.op)
	if err != nil || !strings.Contains(state.Message, "preserving delivery fence") {
		t.Fatalf("lost attempt: %+v %v", state, err)
	}
	s.b = fakeBackend(t, s.f, nil)
	s.age(t)
	for range 2 {
		state, err = s.b.OpStatus(s.ctx, s.op)
		if err != nil || state.Done || !strings.Contains(state.Message, "without replay") {
			t.Fatalf("lost fence: %+v %v", state, err)
		}
	}
	if fakeCalls(s.f, "application.delete") != 1 {
		t.Fatal("replayed ambiguous deletion")
	}
}

func TestCleanupPortWithdrawalDoesNotReloadBeforeDelete(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
	deployFake(t, b, "runtime", "")
	if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	if pending := fakeMeta(t, f, "runtime").Pending; pending != nil {
		awaitFake(t, b, encodeOp(*pending))
	}
	ctx := infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{FullDelete: true, Services: map[string]string{"runtime": id}})
	before := fakeCalls(f, "application.reload")
	if err := b.DeletePublicPorts(ctx, "runtime"); err != nil {
		t.Fatal(err)
	}
	op, err := b.DeleteServiceByID(ctx, "runtime", id)
	if err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before || fakeCalls(f, "application.stop") != 1 {
		t.Fatal("cleanup reloaded before stop")
	}
	awaitFake(t, b, op)
}

func TestDeleteWaitsForExactPortRestartReceipt(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	f.apps[id].holdTasks = true
	f.mu.Unlock()
	if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	receipt := fakeMeta(t, f, "runtime").Pending
	if _, err := b.DeleteServiceByID(t.Context(), "runtime", id); err == nil {
		t.Fatal("delete raced port restart")
	}
	if fakeCalls(f, "application.stop") != 0 || !sameOperation(fakeMeta(t, f, "runtime").Pending, *receipt) {
		t.Fatal("restart receipt discarded")
	}
	f.mu.Lock()
	f.apps[id].holdTasks = false
	f.mu.Unlock()
	op, err := b.DeleteServiceByID(t.Context(), "runtime", id)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
}

func TestAgedStopDiagnosesWithoutEscalating(t *testing.T) {
	s := stuckDelete(t, true)
	o, _ := decodeOp(s.op)
	o.Action = "stop"
	s.op = encodeOp(o)
	s.f.mu.Lock()
	m, _ := parseMetadata(fakeString(s.f.apps[s.id].row, "description"))
	m.Desired = "stopped"
	m.Pending = &o
	s.f.apps[s.id].row["description"] = m.description()
	s.f.mu.Unlock()
	s.age(t)
	state, err := s.b.OpStatus(s.ctx, s.op)
	if err != nil || state.Done || !strings.Contains(state.Message, "Swarm inconsistency") || fakeCalls(s.f, "application.delete") != 0 {
		t.Fatalf("stop escalation: %+v %v", state, err)
	}
}
